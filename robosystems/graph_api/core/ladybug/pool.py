"""
Thread-safe LadybugDB connection pool.

Hands out connections per database under a per-database lock, with a cap on
concurrent connections, a TTL, periodic health checks and cleanup on shutdown.

All connections to one database share a single ``lbug.Database`` object: two
Database objects over the same file do not see each other's committed writes.
"""

import os
import threading
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import ladybug as lbug

from robosystems.logger import logger


@dataclass
class ConnectionInfo:
  """Information about a connection in the pool."""

  connection: lbug.Connection
  database: lbug.Database
  created_at: datetime
  last_used: datetime
  use_count: int
  is_healthy: bool
  # Read-only and read-write connections are pooled separately, never swapped.
  read_only: bool = False


class LadybugConnectionPool:
  """Thread-safe connection pool for the node's LadybugDB databases.

  Bounds connections per database, expires them on a TTL, and drops
  connections that fail a health check. All connections for one database share
  a single ``lbug.Database`` object so writes committed on one are visible on
  the others.
  """

  def __init__(
    self,
    base_path: str,
    max_connections_per_db: int = 3,
    connection_ttl_minutes: int = 30,
    health_check_interval_minutes: int = 5,
    cleanup_interval_minutes: int = 10,
  ):
    """Initialize the pool.

    A database at ``max_connections_per_db`` does not block: the oldest
    connection is closed to make room. Maintenance (expiry cleanup and health
    checks) is not a background thread — it runs opportunistically on acquire,
    at most once per its interval.
    """
    self.base_path = Path(base_path)
    self.max_connections_per_db = max_connections_per_db
    self.connection_ttl = timedelta(minutes=connection_ttl_minutes)
    self.health_check_interval = timedelta(minutes=health_check_interval_minutes)
    self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)

    self._pools: dict[str, dict[str, ConnectionInfo]] = {}
    self._locks: dict[str, threading.RLock] = {}
    self._global_lock = threading.RLock()

    # One Database object per database name, shared by all its connections.
    self._databases: dict[str, lbug.Database] = {}

    self._stats = {
      "connections_created": 0,
      "connections_reused": 0,
      "connections_closed": 0,
      "health_checks": 0,
      "health_failures": 0,
    }

    self._last_cleanup = datetime.now(UTC)
    self._last_health_check = datetime.now(UTC)

    weakref.finalize(self, self._cleanup_all_connections)

    logger.info(
      f"Initialized LadybugDB connection pool: {max_connections_per_db} max per DB, {connection_ttl_minutes}min TTL"
    )

  @contextmanager
  def get_connection(self, database_name: str, read_only: bool = False):
    """Check out a pooled connection for the duration of the ``with`` block.

    Fully consume (and close) any result inside the block — a QueryResult
    holds Arrow buffers that outlive the block otherwise.

    Example:
        with pool.get_connection("my_db") as conn:
            result = conn.execute("MATCH (n) RETURN count(n)")
    """
    connection_info = None
    try:
      connection_info = self._acquire_connection(database_name, read_only)
      yield connection_info.connection
    finally:
      if connection_info:
        self._release_connection(database_name, connection_info)

  def _acquire_connection(self, database_name: str, read_only: bool) -> ConnectionInfo:
    """Reuse a valid pooled connection, or create one."""
    self._maybe_run_maintenance()

    with self._get_database_lock(database_name):
      connection_info = self._get_existing_connection(database_name, read_only)

      if connection_info and self._is_connection_valid(connection_info):
        connection_info.last_used = datetime.now(UTC)
        connection_info.use_count += 1
        self._stats["connections_reused"] += 1
        logger.debug(
          f"Reused connection for {database_name} (use count: {connection_info.use_count})"
        )
        return connection_info

      return self._create_new_connection(database_name, read_only)

  def _release_connection(self, database_name: str, connection_info: ConnectionInfo):
    """Mark a connection as done with. Connections stay in the pool for reuse."""
    logger.debug(f"Released connection for {database_name}")

  def invalidate_connection(self, database_name: str):
    """Close a database's connections and drop its shared Database object.

    Discarding the Database object is the point: a bulk load performed outside
    this process is not visible to the existing object, so the next caller
    must open a fresh one.
    """
    with self._get_database_lock(database_name):
      if database_name in self._pools:
        for conn_id, conn_info in self._pools[database_name].items():
          try:
            conn_info.connection.close()
            self._stats["connections_closed"] += 1
          except Exception as e:
            logger.warning(f"Error closing connection during invalidation: {e}")

        del self._pools[database_name]

        if database_name in self._databases:
          try:
            self._databases[database_name].close()
          except Exception as e:
            logger.warning(f"Error closing database object during invalidation: {e}")
          del self._databases[database_name]
          logger.info(f"Removed shared Database object for {database_name}")

        logger.info(f"Invalidated all connections for database: {database_name}")

  def _get_database_lock(self, database_name: str) -> threading.RLock:
    """Get or create a lock for a specific database."""
    with self._global_lock:
      if database_name not in self._locks:
        self._locks[database_name] = threading.RLock()
      return self._locks[database_name]

  def _get_existing_connection(
    self, database_name: str, read_only: bool
  ) -> ConnectionInfo | None:
    """Find the least recently used healthy connection matching ``read_only``."""
    if database_name not in self._pools:
      return None

    pool = self._pools[database_name]

    best_connection = None
    oldest_time = datetime.now(UTC)

    for conn_id, conn_info in pool.items():
      if (
        conn_info.is_healthy
        and conn_info.read_only == read_only
        and conn_info.last_used < oldest_time
        and self._is_connection_valid(conn_info)
      ):
        best_connection = conn_info
        oldest_time = conn_info.last_used

    return best_connection

  def _create_new_connection(
    self, database_name: str, read_only: bool
  ) -> ConnectionInfo:
    """Open a new connection, creating the shared Database object if needed.

    A database already at its connection cap has its oldest connection closed
    to make room rather than blocking the caller.
    """
    if database_name in self._pools:
      current_count = len(self._pools[database_name])
      if current_count >= self.max_connections_per_db:
        self._remove_oldest_connection(database_name)

    try:
      db_path = self.base_path / f"{database_name}.lbug"

      if database_name not in self._databases:
        logger.info(f"Creating new Database object for {database_name}")

        from .config import get_database_memory_config

        buffer_pool_mb = get_database_memory_config(database_name)
        buffer_pool_size = buffer_pool_mb * 1024 * 1024

        # Replicas run off snapshot-restored volumes; opening read-write there
        # triggers WAL recovery and lock contention against the snapshot.
        lbug_role = os.getenv("LBUG_ROLE", "master")
        is_replica = lbug_role == "replica"

        if is_replica:
          logger.info(
            f"Opening database '{database_name}' in READ-ONLY mode (LBUG_ROLE=replica)"
          )
          db_read_only = True
        else:
          # Masters always open read-write, regardless of the caller's
          # read_only flag: the Database object is shared, so opening it
          # read-only for the first request would lock out every later write.
          db_read_only = False

        # SEC's Fact and Association tables are large enough that the default
        # threshold lets the WAL grow until it exhausts memory.
        if database_name == "sec":
          checkpoint_threshold = 134217728
          logger.info("Using reduced checkpoint threshold (128MB) for SEC database")
        else:
          checkpoint_threshold = 536870912

        self._databases[database_name] = lbug.Database(
          str(db_path),
          read_only=db_read_only,
          buffer_pool_size=buffer_pool_size,
          compression=True,
          max_num_threads=0,  # 0 lets LadybugDB use every available thread
          auto_checkpoint=True,
          checkpoint_threshold=checkpoint_threshold,
        )
        logger.info(
          f"Database '{database_name}' created - read_only: {db_read_only}, buffer pool: {buffer_pool_mb} MB, "
          f"compression: enabled, auto_checkpoint: enabled, threshold: {checkpoint_threshold // (1024 * 1024)}MB"
        )

        # FLOAT[N] columns and vector indexes need the vector extension.
        # Loading it once per Database object is enough — it persists across
        # that object's connections. Baked into the Docker image; the INSTALL
        # path covers local dev only.
        try:
          init_conn = lbug.Connection(self._databases[database_name])
          try:
            init_conn.execute("LOAD EXTENSION vector")
          except Exception:
            init_conn.execute("INSTALL vector")
            init_conn.execute("LOAD EXTENSION vector")
          del init_conn
          logger.info(f"Vector extension loaded for {database_name}")
        except Exception as vec_err:
          logger.debug(f"Vector extension not available for {database_name}: {vec_err}")

      db = self._databases[database_name]

      conn = lbug.Connection(db)

      try:
        result = conn.execute("RETURN 1 as test")
        # execute() returns a QueryResult or a list of them; both must be
        # closed or their Arrow buffers stay allocated.
        if isinstance(result, list):
          for r in result:
            r.close()
        else:
          result.close()
        is_healthy = True

        # Per-connection tuning, applied only once the connection answers.
        # None of it is required — a failure here leaves a usable connection.
        try:
          # Keeps LadybugDB's temporary files on the same fast EBS volume as
          # the databases. It appends ".lbug" itself, so pass the bare path.
          home_dir = str(self.base_path)

          conn.execute(f"CALL home_directory='{home_dir}';")

          conn.execute("CALL progress_bar=false;")

          # Per-query ceiling. Ingestion raises this to 30 minutes for the
          # duration of a load.
          conn.execute("CALL timeout=120000;")

          conn.execute("CALL enable_semi_mask=true;")

          # Caps the memory a pathological query's warnings can consume.
          conn.execute("CALL warning_limit=1024;")

          # Lets large queries spill temporaries to disk instead of OOMing.
          conn.execute("CALL spill_to_disk=true;")

          logger.info(
            f"Applied connection configuration for {database_name} (home_dir={home_dir}, progress_bar=false, timeout=120000ms, semi_mask=true, warning_limit=1024, spill_to_disk=true)"
          )
        except Exception as config_error:
          logger.debug(
            f"Could not apply connection settings (non-critical): {config_error}"
          )

      except Exception as e:
        logger.warning(f"New connection health check failed for {database_name}: {e}")
        is_healthy = False

      now = datetime.now(UTC)
      connection_info = ConnectionInfo(
        connection=conn,
        database=db,
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=is_healthy,
        read_only=read_only,
      )

      if database_name not in self._pools:
        self._pools[database_name] = {}

      conn_id = f"{database_name}_{len(self._pools[database_name])}"
      self._pools[database_name][conn_id] = connection_info

      self._stats["connections_created"] += 1
      logger.info(
        f"Created new connection for {database_name} (total: {len(self._pools[database_name])})"
      )

      return connection_info

    except Exception as e:
      logger.error(f"Failed to create connection for {database_name}: {e}")
      raise

  def _is_connection_valid(self, connection_info: ConnectionInfo) -> bool:
    """Check a connection against the TTL and its last recorded health."""
    if datetime.now(UTC) - connection_info.created_at > self.connection_ttl:
      return False

    return connection_info.is_healthy

  def _remove_oldest_connection(self, database_name: str):
    """Remove the oldest connection from a database pool."""
    if database_name not in self._pools or not self._pools[database_name]:
      return

    pool = self._pools[database_name]

    oldest_conn_id = None
    oldest_time = datetime.now(UTC)

    for conn_id, conn_info in pool.items():
      if conn_info.created_at < oldest_time:
        oldest_conn_id = conn_id
        oldest_time = conn_info.created_at

    if oldest_conn_id:
      self._close_connection(database_name, oldest_conn_id)

  def _close_connection(self, database_name: str, connection_id: str):
    """Close and remove a specific connection."""
    if (
      database_name not in self._pools
      or connection_id not in self._pools[database_name]
    ):
      return

    connection_info = self._pools[database_name][connection_id]

    try:
      connection_info.connection.close()
      # The Database object is shared by the other connections and is closed
      # only in _cleanup_all_connections; closing it here double-closes it.
    except Exception as e:
      logger.warning(f"Error closing connection {connection_id}: {e}")

    del self._pools[database_name][connection_id]
    self._stats["connections_closed"] += 1

    logger.debug(f"Closed connection {connection_id} for {database_name}")

  def _maybe_run_maintenance(self):
    """Run expiry cleanup and health checks if their intervals have elapsed.

    Driven by connection acquisition, not a timer: an idle pool does no work.
    """
    now = datetime.now(UTC)

    if now - self._last_cleanup > self.cleanup_interval:
      self._cleanup_expired_connections()
      self._last_cleanup = now

    if now - self._last_health_check > self.health_check_interval:
      self._check_connection_health()
      self._last_health_check = now

  def _cleanup_expired_connections(self):
    """Clean up expired connections."""
    with self._global_lock:
      expired_connections = []

      for db_name, pool in self._pools.items():
        for conn_id, conn_info in pool.items():
          if not self._is_connection_valid(conn_info):
            expired_connections.append((db_name, conn_id))

      for db_name, conn_id in expired_connections:
        with self._get_database_lock(db_name):
          self._close_connection(db_name, conn_id)

      if expired_connections:
        logger.info(f"Cleaned up {len(expired_connections)} expired connections")

  def _check_connection_health(self):
    """Check health of all connections."""
    with self._global_lock:
      unhealthy_connections = []

      for db_name, pool in self._pools.items():
        for conn_id, conn_info in pool.items():
          if not self._test_connection_health(conn_info):
            conn_info.is_healthy = False
            unhealthy_connections.append((db_name, conn_id))

      for db_name, conn_id in unhealthy_connections:
        with self._get_database_lock(db_name):
          self._close_connection(db_name, conn_id)

      if unhealthy_connections:
        logger.warning(f"Removed {len(unhealthy_connections)} unhealthy connections")

  def _test_connection_health(self, connection_info: ConnectionInfo) -> bool:
    """Test if a connection is healthy."""
    try:
      self._stats["health_checks"] += 1
      result = connection_info.connection.execute("RETURN 1 as health_check")
      if isinstance(result, list):
        for r in result:
          r.close()
      else:
        result.close()
      return True
    except Exception as e:
      self._stats["health_failures"] += 1
      logger.debug(f"Connection health check failed: {e}")
      return False

  def _cleanup_all_connections(self):
    """Close every connection and Database object. Runs on shutdown."""
    with self._global_lock:
      total_closed = 0

      for db_name, pool in self._pools.items():
        for conn_id in list(pool.keys()):
          self._close_connection(db_name, conn_id)
          total_closed += 1

      for db_name in list(self._databases.keys()):
        try:
          db = self._databases[db_name]
          if hasattr(db, "close"):
            db.close()
        except Exception as e:
          logger.warning(f"Error closing database {db_name}: {e}")
        del self._databases[db_name]

      self._pools.clear()
      self._locks.clear()
      self._databases.clear()

  def force_database_cleanup(self, database_name: str, aggressive: bool = True) -> None:
    """Drop a database's connections and its Database object to free memory.

    Dropping the Database object is what releases LadybugDB's buffer pool —
    closing connections alone does not — so this is the call to make after a
    large ingestion. The next access recreates it. ``aggressive`` additionally
    forces full GC and ``malloc_trim`` to hand the pages back to the OS.
    """
    with self._global_lock:
      logger.debug(
        f"Forcing cleanup for database: {database_name} (aggressive={aggressive})"
      )

      if database_name in self._pools:
        pool = self._pools[database_name]
        for conn_id in list(pool.keys()):
          self._close_connection(database_name, conn_id)

        del self._pools[database_name]
        logger.debug(f"Closed all connections for database: {database_name}")

      if database_name in self._databases:
        try:
          db = self._databases[database_name]

          # SEC's WAL is large enough that closing without a checkpoint
          # leaves a long recovery for the next open.
          if database_name == "sec" and hasattr(db, "execute"):
            try:
              temp_conn = lbug.Connection(db)
              temp_conn.execute("CHECKPOINT;")
              temp_conn.close()
              logger.info(f"Executed final checkpoint for {database_name}")
            except Exception as cp_err:
              logger.debug(f"Could not execute checkpoint: {cp_err}")

          if hasattr(db, "close"):
            db.close()
        except Exception as e:
          logger.debug(f"Could not close database object: {e}")

        del self._databases[database_name]
        logger.debug(f"Removed cached Database object for: {database_name}")

        if aggressive:
          import ctypes
          import gc

          # The local reference would otherwise keep the buffer pool alive
          # through the collection below.
          db = None

          # All three generations: the Database object is long-lived enough
          # to have been promoted to generation 2.
          for generation in range(3):
            collected = gc.collect(generation)
            logger.debug(f"GC generation {generation}: collected {collected} objects")

          # glibc holds freed pages in its arenas; malloc_trim returns them to
          # the OS. Linux only — absent elsewhere, and harmless to skip.
          if hasattr(ctypes, "CDLL"):
            try:
              libc = ctypes.CDLL("libc.so.6")
              if libc.malloc_trim(0) == 1:
                logger.debug("Successfully trimmed memory back to OS")
            except Exception as e:
              logger.debug(f"Could not trim memory (not Linux?): {e}")

          try:
            import psutil

            process = psutil.Process()
            mem_info = process.memory_info()
            logger.debug(
              f"Memory after cleanup - RSS: {mem_info.rss / (1024 * 1024):.1f}MB, "
              f"VMS: {mem_info.vms / (1024 * 1024):.1f}MB"
            )
          except ImportError:
            pass
        else:
          import gc

          gc.collect()
          logger.debug(
            f"Triggered garbage collection after cleanup of: {database_name}"
          )

  def get_stats(self) -> dict[str, Any]:
    """Get connection pool statistics."""
    with self._global_lock:
      pool_stats = {}
      total_connections = 0

      for db_name, pool in self._pools.items():
        healthy_count = sum(1 for conn in pool.values() if conn.is_healthy)
        pool_stats[db_name] = {
          "total_connections": len(pool),
          "healthy_connections": healthy_count,
          "max_connections": self.max_connections_per_db,
        }
        total_connections += len(pool)

      return {
        "total_connections": total_connections,
        "database_pools": pool_stats,
        "stats": self._stats.copy(),
        "configuration": {
          "max_connections_per_db": self.max_connections_per_db,
          "connection_ttl_minutes": self.connection_ttl.total_seconds() / 60,
          "health_check_interval_minutes": self.health_check_interval.total_seconds()
          / 60,
        },
      }

  def close_database_connections(self, database_name: str):
    """Close a database's connections and its Database object.

    Required before renaming or deleting the file on disk — the rename in
    :meth:`~..manager.LadybugDatabaseManager.swap_database` is unsafe while a
    handle is open.
    """
    with self._get_database_lock(database_name):
      if database_name in self._pools:
        pool = self._pools[database_name]
        for conn_id in list(pool.keys()):
          self._close_connection(database_name, conn_id)

        if database_name in self._databases:
          try:
            self._databases[database_name].close()
          except Exception as e:
            logger.warning(f"Error closing database {database_name}: {e}")
          del self._databases[database_name]

        logger.debug(f"Closed all connections for database {database_name}")

  def has_active_connections(self, database_name: str) -> bool:
    """Check if there are any active connections for a database."""
    with self._get_database_lock(database_name):
      if database_name in self._pools:
        return len(self._pools[database_name]) > 0
      return False

  def list_databases(self) -> list[str]:
    """List the databases the pool currently holds open."""
    with self._global_lock:
      return list(self._databases.keys())

  def recreate_database(self, database_name: str) -> None:
    """Drop a database's handles so the next access reopens it.

    Buffer pool size is fixed when the Database object is created, so this is
    how a memory override takes effect on an already-open database.

    Example:
        from robosystems.graph_api.core.ladybug.config import set_ladybug_memory_override

        old_limit = set_ladybug_memory_override(50000, graph_id="sec")
        pool.recreate_database("sec")  # reopens with a 50GB buffer pool

        # ... perform materialization ...

        set_ladybug_memory_override(old_limit, graph_id="sec")
        pool.recreate_database("sec")
    """
    logger.info(f"Recreating database {database_name} to apply new memory settings")
    self.close_database_connections(database_name)

  def close_all_connections(self):
    """Close all connections in the pool."""
    self._cleanup_all_connections()


_connection_pool: LadybugConnectionPool | None = None


def initialize_connection_pool(
  base_path: str, max_connections_per_db: int = 3, connection_ttl_minutes: int = 30
) -> LadybugConnectionPool:
  """Create the process-wide connection pool, replacing any existing one."""
  global _connection_pool
  _connection_pool = LadybugConnectionPool(
    base_path=base_path,
    max_connections_per_db=max_connections_per_db,
    connection_ttl_minutes=connection_ttl_minutes,
  )
  return _connection_pool


def get_connection_pool() -> LadybugConnectionPool:
  """Get the global connection pool instance."""
  if _connection_pool is None:
    raise RuntimeError(
      "Connection pool not initialized. Call initialize_connection_pool() first."
    )
  return _connection_pool
