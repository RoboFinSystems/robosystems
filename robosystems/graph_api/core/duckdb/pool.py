"""Thread-safe connection pool for DuckDB staging databases.

Each graph_id gets its own DuckDB file. Two connection kinds are served from
here and they are deliberately different:

- ``get_connection`` — pooled read-write connections used by staging,
  materialization and ingestion. httpfs + parquet + postgres_scanner are
  loaded and S3 credentials configured, so these can read S3 and PostgreSQL.
  Pooled per database (capped, TTL'd, health-checked) and reused.
- ``get_readonly_connection`` — one-shot hardened connection for untrusted
  tenant SQL. External access is disabled and no httpfs/postgres_scanner is
  loaded, so it can only read the graph's own local staging tables.

DuckDB refuses a second connection with a different configuration to the same
file in-process, so the read-only path evicts the read-write connections first.
Database files persist for the life of the graph; ``force_database_cleanup``
deletes one when a graph is dropped.
"""

import os
import threading
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from robosystems.logger import logger
from robosystems.utils.path_validation import get_duckdb_staging_path


@dataclass
class DuckDBConnectionInfo:
  """Information about a DuckDB connection in the pool."""

  connection: duckdb.DuckDBPyConnection
  database_path: Path
  created_at: datetime
  last_used: datetime
  use_count: int
  is_healthy: bool


class DuckDBConnectionPool:
  """Thread-safe pool of DuckDB connections, one file per graph_id.

  Connections beyond ``max_connections_per_db`` evict the oldest rather than
  blocking, so checkout never waits. Expired and unhealthy connections are
  reaped lazily on acquire (``_maybe_run_maintenance``).
  """

  def __init__(
    self,
    base_path: str,
    max_connections_per_db: int = 3,
    connection_ttl_minutes: int = 30,
    health_check_interval_minutes: int = 5,
    cleanup_interval_minutes: int = 10,
  ):
    """Build a pool rooted at ``base_path``.

    Database files are never deleted automatically — they persist as long as
    the graph does. Call ``force_database_cleanup`` when a graph is deleted.
    """
    self.base_path = Path(base_path)
    self.max_connections_per_db = max_connections_per_db
    self.connection_ttl = timedelta(minutes=connection_ttl_minutes)
    self.health_check_interval = timedelta(minutes=health_check_interval_minutes)
    self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)

    try:
      self.base_path.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError) as e:
      logger.warning(f"Could not create DuckDB staging directory: {e}")

    self._pools: dict[str, dict[str, DuckDBConnectionInfo]] = {}
    self._locks: dict[str, threading.RLock] = {}
    self._global_lock = threading.RLock()

    self._stats = {
      "connections_created": 0,
      "connections_reused": 0,
      "connections_closed": 0,
      "health_checks": 0,
      "health_failures": 0,
      "databases_cleaned": 0,
    }

    self._last_cleanup = datetime.now(UTC)
    self._last_health_check = datetime.now(UTC)

    weakref.finalize(self, self._cleanup_all_connections)

    logger.info(
      f"Initialized DuckDB connection pool: {max_connections_per_db} max per DB, "
      f"{connection_ttl_minutes}min TTL (databases persist with graph lifecycle)"
    )

  @contextmanager
  def get_connection(self, graph_id: str):
    """Check out a pooled read-write connection for ``graph_id``.

    The connection has httpfs/parquet/postgres_scanner loaded and S3
    credentials configured. Results must be fully materialized before the
    block exits — the connection may be handed to another caller immediately.

    Example:
        with pool.get_connection("graph123") as conn:
            result = conn.execute("SELECT * FROM my_table").fetchall()
    """
    connection_info = None
    try:
      connection_info = self._acquire_connection(graph_id)
      yield connection_info.connection
    finally:
      if connection_info:
        self._release_connection(graph_id, connection_info)

  @contextmanager
  def get_readonly_connection(self, graph_id: str):
    """Open a hardened, sandboxed read-only connection for untrusted tenant SQL.

    Opens the staging file ``read_only`` with ``enable_external_access=false``
    and ``lock_configuration=true``, and loads no httpfs/postgres_scanner. That
    blocks external ``ATTACH``, httpfs egress, ``read_text``/``read_blob``,
    ``postgres_scan`` and ``COPY ... TO``: the connection can only read the
    graph's own local staging tables. It is separate from the read-write
    connection used by staging/materialization, which needs external access.

    DuckDB forbids a second connection with a different configuration to the
    same file in-process, so any read-write pool connection for this graph is
    closed first, under the per-graph lock. This connection is not pooled — it
    is opened per query and closed on exit.

    Raises FileNotFoundError if no staging database exists yet, ValueError if
    the resolved path escapes the base directory.
    """
    db_path = self._get_database_path(graph_id)

    # Defense-in-depth: verify resolved path stays under base_path
    if not db_path.resolve().is_relative_to(self.base_path.resolve()):
      raise ValueError(f"Path escapes base directory: {db_path}")

    with self._get_database_lock(graph_id):
      # DuckDB won't open a second connection with a different configuration to
      # the same file, so close any read-write connection holding it first.
      #
      # LIMITATION: the staging write methods (create_table /
      # insert_into_table) execute outside this per-graph lock, so evicting
      # here can close a connection a concurrent staging run is mid-statement
      # on, aborting that run (Dagster-retryable, not data loss).
      self.close_database_connections(graph_id)

      if not db_path.exists():
        raise FileNotFoundError(
          f"No staging database for {graph_id} — create staging tables first."
        )

      # temp_directory (spill-to-disk) cannot be set with external access
      # disabled — DuckDB treats it as external file access. Read queries are
      # therefore memory-bound (memory_limit); an over-limit sort/aggregation
      # errors out, which is an acceptable bounded failure for a read surface.
      config = {
        "enable_external_access": "false",
        "lock_configuration": "true",
      }
      # Best-effort resource caps (never let their resolution break the
      # security-critical connection open).
      try:
        config["threads"] = str(self._get_duckdb_max_threads())
      except Exception as e:
        logger.debug(f"Could not resolve DuckDB threads for read-only conn: {e}")
      try:
        config["memory_limit"] = self._get_duckdb_memory_limit(graph_id)
      except Exception as e:
        logger.debug(f"Could not resolve DuckDB memory_limit for read-only conn: {e}")

      conn = duckdb.connect(str(db_path), read_only=True, config=config)
      logger.debug(
        f"Opened hardened read-only DuckDB connection for {graph_id} "
        f"(external access disabled, config locked)"
      )
      try:
        yield conn
      finally:
        try:
          conn.close()
        except Exception as e:
          logger.debug(f"Error closing read-only DuckDB connection: {e}")

  def _acquire_connection(self, graph_id: str) -> DuckDBConnectionInfo:
    """Acquire a connection from the pool."""
    self._maybe_run_maintenance()

    with self._get_database_lock(graph_id):
      connection_info = self._get_existing_connection(graph_id)

      if connection_info and self._is_connection_valid(connection_info):
        connection_info.last_used = datetime.now(UTC)
        connection_info.use_count += 1
        self._stats["connections_reused"] += 1
        logger.debug(
          f"Reused DuckDB connection for {graph_id} (use count: {connection_info.use_count})"
        )
        return connection_info

      return self._create_new_connection(graph_id)

  def _release_connection(self, graph_id: str, _connection_info: DuckDBConnectionInfo):
    """Return a connection to the pool.

    Connections stay open and shared, so there is no state to reset here —
    checkout hands out the least-recently-used healthy connection regardless.
    """
    logger.debug(f"Released DuckDB connection for {graph_id}")

  def _get_database_lock(self, graph_id: str) -> threading.RLock:
    """Get or create a lock for a specific database."""
    with self._global_lock:
      if graph_id not in self._locks:
        self._locks[graph_id] = threading.RLock()
      return self._locks[graph_id]

  def _get_existing_connection(self, graph_id: str) -> DuckDBConnectionInfo | None:
    """Get an existing connection for a database if available."""
    if graph_id not in self._pools:
      return None

    pool = self._pools[graph_id]

    best_connection = None
    oldest_time = datetime.now(UTC)

    for conn_info in pool.values():
      if (
        conn_info.is_healthy
        and conn_info.last_used < oldest_time
        and self._is_connection_valid(conn_info)
      ):
        best_connection = conn_info
        oldest_time = conn_info.last_used

    return best_connection

  def _create_new_connection(self, graph_id: str) -> DuckDBConnectionInfo:
    """Create a new connection for a database."""
    if graph_id in self._pools:
      current_count = len(self._pools[graph_id])
      if current_count >= self.max_connections_per_db:
        self._remove_oldest_connection(graph_id)

    try:
      db_path = self._get_database_path(graph_id)

      # Defense-in-depth: verify resolved path stays under base_path
      if not db_path.resolve().is_relative_to(self.base_path.resolve()):
        raise ValueError(f"Path escapes base directory: {db_path}")

      # Ensure database directory exists
      db_path.parent.mkdir(parents=True, exist_ok=True)

      # On replicas, DuckDB files are downloaded from S3 in the background.
      # Don't create an empty database — wait for the real file to arrive.
      if os.getenv("LBUG_ROLE") == "replica" and not db_path.exists():
        raise FileNotFoundError(
          f"DuckDB staging file not yet available: {db_path} "
          "(background S3 download may still be in progress)"
        )

      conn = duckdb.connect(str(db_path))
      self._configure_connection(conn, graph_id)
      is_healthy = self._test_new_connection(conn)

      now = datetime.now(UTC)
      connection_info = DuckDBConnectionInfo(
        connection=conn,
        database_path=db_path,
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=is_healthy,
      )

      if graph_id not in self._pools:
        self._pools[graph_id] = {}

      conn_id = f"{graph_id}_{len(self._pools[graph_id])}"
      self._pools[graph_id][conn_id] = connection_info

      self._stats["connections_created"] += 1
      logger.info(
        f"Created new DuckDB connection for {graph_id} at {db_path} "
        f"(total: {len(self._pools[graph_id])})"
      )

      return connection_info

    except Exception as e:
      logger.error(f"Failed to create DuckDB connection for {graph_id}: {e}")
      raise

  def _get_database_path(self, graph_id: str) -> Path:
    """Resolve the staging file for a graph_id, rejecting path traversal."""
    try:
      return get_duckdb_staging_path(graph_id, base_path=str(self.base_path))
    except Exception as e:
      logger.error(f"Path validation failed for DuckDB graph_id {graph_id}: {e}")
      raise ValueError(f"Invalid graph_id: {e!s}") from e

  def _get_duckdb_memory_limit(self, graph_id: str = "sec") -> str:
    """Resolve the memory limit for this graph's connections, e.g. "8GB".

    First match wins: per-graph override (set while staging), tier config from
    ``.github/configs/graph.yml`` via CLUSTER_TIER, then DUCKDB_MEMORY_LIMIT.
    """
    from robosystems.config import env

    override = get_duckdb_memory_override(graph_id)
    if override:
      logger.info(f"Using DuckDB memory override: {override}")
      return override

    try:
      from robosystems.config.graph_tier import GraphTierConfig

      tier = env.CLUSTER_TIER

      if tier:
        memory_limit = GraphTierConfig.get_duckdb_memory_limit(tier)
        logger.info(
          f"Using tier-based DuckDB memory limit: {memory_limit} (tier={tier})"
        )
        return memory_limit
    except Exception as e:
      logger.warning(f"Could not load tier-based DuckDB memory config: {e}")

    memory_limit = env.DUCKDB_MEMORY_LIMIT
    logger.info(f"Using default DuckDB memory limit: {memory_limit}")
    return memory_limit

  def _get_duckdb_max_threads(self) -> int:
    """Resolve DuckDB's thread count for this instance.

    Tier config from ``.github/configs/graph.yml`` via CLUSTER_TIER first,
    then DUCKDB_MAX_THREADS. Tier values track the instance's vCPU count so
    DuckDB does not oversubscribe the box it shares with LadybugDB.
    """
    from robosystems.config import env

    try:
      from robosystems.config.graph_tier import GraphTierConfig

      tier = env.CLUSTER_TIER

      if tier:
        max_threads = GraphTierConfig.get_duckdb_max_threads(tier)
        logger.info(f"Using tier-based DuckDB max threads: {max_threads} (tier={tier})")
        return max_threads
    except Exception as e:
      logger.warning(f"Could not load tier-based DuckDB thread config: {e}")

    max_threads = env.DUCKDB_MAX_THREADS
    logger.info(f"Using default DuckDB max threads: {max_threads}")
    return max_threads

  def _configure_connection(
    self, conn: duckdb.DuckDBPyConnection, graph_id: str = "sec"
  ):
    """Load extensions, S3 credentials, and resource caps on a read-write conn.

    httpfs and postgres_scanner are loaded here and NOWHERE on the read-only
    tenant connection — that asymmetry is the sandbox.
    """
    from robosystems.config import env

    try:
      conn.execute("INSTALL httpfs")
      conn.execute("LOAD httpfs")
      conn.execute("INSTALL parquet")
      conn.execute("LOAD parquet")
      conn.execute("INSTALL postgres_scanner")
      conn.execute("LOAD postgres_scanner")

      if env.ENVIRONMENT in ["prod", "staging"]:
        # Production/Staging: always use IAM roles (ECS task roles)
        try:
          conn.execute("CALL load_aws_credentials()")
          conn.execute("SET s3_region=?", [env.AWS_DEFAULT_REGION])
          logger.debug("Loaded AWS credentials from IAM role (production)")
        except Exception as cred_err:
          logger.error(f"Failed to load IAM role credentials in production: {cred_err}")
          raise  # In production, credential failures should fail hard
      elif env.AWS_S3_ACCESS_KEY_ID and env.AWS_S3_SECRET_ACCESS_KEY:
        # Development: Use explicit credentials if provided
        conn.execute("SET s3_access_key_id=?", [env.AWS_S3_ACCESS_KEY_ID])
        conn.execute("SET s3_secret_access_key=?", [env.AWS_S3_SECRET_ACCESS_KEY])
        conn.execute("SET s3_region=?", [env.AWS_DEFAULT_REGION])
        logger.debug("Using explicit S3 credentials (development)")
      else:
        # Development fallback: Try credential provider chain
        try:
          conn.execute("CALL load_aws_credentials()")
          conn.execute("SET s3_region=?", [env.AWS_DEFAULT_REGION])
          logger.debug("Loaded AWS credentials from credential provider chain")
        except Exception as cred_err:
          logger.debug(
            f"Could not load AWS credentials from provider chain: {cred_err}"
          )

      # Configure S3 endpoint if using LocalStack or custom endpoint
      if env.AWS_ENDPOINT_URL:
        # DuckDB expects endpoint without protocol (e.g., "localstack:4566" not "http://localstack:4566")
        endpoint = env.AWS_ENDPOINT_URL.replace("http://", "").replace("https://", "")
        conn.execute("SET s3_endpoint=?", [endpoint])
        conn.execute("SET s3_url_style='path'")
        conn.execute("SET s3_use_ssl=false")  # LocalStack doesn't use SSL
        logger.debug(
          f"Configured DuckDB to use S3 endpoint: {endpoint} (from {env.AWS_ENDPOINT_URL})"
        )

      max_threads = self._get_duckdb_max_threads()
      conn.execute(f"SET threads TO {max_threads}")

      memory_limit = self._get_duckdb_memory_limit(graph_id)
      conn.execute(f"SET memory_limit='{memory_limit}'")

      # Insertion order is irrelevant for staging and preserving it costs a lot
      # of memory on multi-million-row tables (e.g. the SEC Element table).
      conn.execute("SET preserve_insertion_order=false")

      # Spill larger-than-memory operations to the EBS-backed staging volume,
      # not /tmp on the small root volume. DUCKDB_STAGING_PATH is
      # /app/data/staging in the container, mapped to /mnt/ladybug-data/staging
      # on the host.
      spill_dir = f"{env.DUCKDB_STAGING_PATH}/duckdb_spill"
      conn.execute(f"SET temp_directory='{spill_dir}'")

      logger.debug(
        f"Configured DuckDB connection with S3 access, extensions, "
        f"threads={max_threads}, memory_limit={memory_limit}, spill={spill_dir}"
      )

    except Exception as e:
      logger.warning(f"Could not fully configure DuckDB connection: {e}")

  def _test_new_connection(self, conn: duckdb.DuckDBPyConnection) -> bool:
    """Test if a new connection is healthy."""
    try:
      result = conn.execute("SELECT 1 as health_check").fetchone()
      return result is not None and result[0] == 1
    except Exception as e:
      logger.warning(f"New DuckDB connection health check failed: {e}")
      return False

  def _is_connection_valid(self, connection_info: DuckDBConnectionInfo) -> bool:
    """Check if a connection is still valid."""
    if datetime.now(UTC) - connection_info.created_at > self.connection_ttl:
      return False

    return connection_info.is_healthy

  def _remove_oldest_connection(self, graph_id: str):
    """Remove the oldest connection from a database pool."""
    if graph_id not in self._pools or not self._pools[graph_id]:
      return

    pool = self._pools[graph_id]

    oldest_conn_id = None
    oldest_time = datetime.now(UTC)

    for conn_id, conn_info in pool.items():
      if conn_info.created_at < oldest_time:
        oldest_conn_id = conn_id
        oldest_time = conn_info.created_at

    if oldest_conn_id:
      self._close_connection(graph_id, oldest_conn_id)

  def _close_connection(self, graph_id: str, connection_id: str):
    """Close and remove a specific connection."""
    if graph_id not in self._pools or connection_id not in self._pools[graph_id]:
      return

    connection_info = self._pools[graph_id][connection_id]

    try:
      # Flush the WAL into the main database file before dropping the handle.
      try:
        connection_info.connection.execute("CHECKPOINT")
        logger.debug(f"Checkpointed DuckDB connection {connection_id} for {graph_id}")
      except Exception as cp_err:
        logger.debug(f"Could not checkpoint DuckDB connection: {cp_err}")

      connection_info.connection.close()
    except Exception as e:
      logger.warning(f"Error closing DuckDB connection {connection_id}: {e}")

    del self._pools[graph_id][connection_id]
    self._stats["connections_closed"] += 1

    logger.debug(f"Closed DuckDB connection {connection_id} for {graph_id}")

  def _maybe_run_maintenance(self):
    """Run maintenance tasks if needed."""
    now = datetime.now(UTC)

    if now - self._last_cleanup > self.cleanup_interval:
      self._cleanup_expired_connections()
      self._last_cleanup = now

    if now - self._last_health_check > self.health_check_interval:
      self._check_connection_health()
      self._last_health_check = now

    # Database files are deliberately never reaped here — staging databases
    # persist as long as the graph does. See force_database_cleanup().

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
        logger.info(f"Cleaned up {len(expired_connections)} expired DuckDB connections")

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
        logger.warning(
          f"Removed {len(unhealthy_connections)} unhealthy DuckDB connections"
        )

  def _test_connection_health(self, connection_info: DuckDBConnectionInfo) -> bool:
    """Test if a connection is healthy."""
    try:
      self._stats["health_checks"] += 1
      result = connection_info.connection.execute("SELECT 1 as health_check").fetchone()
      return result is not None and result[0] == 1
    except Exception as e:
      self._stats["health_failures"] += 1
      logger.debug(f"DuckDB connection health check failed: {e}")
      return False

  def _cleanup_all_connections(self):
    """Clean up all connections (called on shutdown)."""
    with self._global_lock:
      total_closed = 0

      for db_name, pool in self._pools.items():
        for conn_id in list(pool.keys()):
          self._close_connection(db_name, conn_id)
          total_closed += 1

      self._pools.clear()
      self._locks.clear()

      if total_closed > 0:
        logger.info(f"Closed {total_closed} DuckDB connections on shutdown")

  def invalidate_connection(self, graph_id: str):
    """Close every connection for a database so the next access reopens."""
    with self._get_database_lock(graph_id):
      if graph_id in self._pools:
        for conn_id, conn_info in self._pools[graph_id].items():
          try:
            try:
              conn_info.connection.execute("CHECKPOINT")
              logger.debug(f"Checkpointed DuckDB connection {conn_id} for {graph_id}")
            except Exception as cp_err:
              logger.debug(f"Could not checkpoint DuckDB connection: {cp_err}")

            conn_info.connection.close()
            self._stats["connections_closed"] += 1
          except Exception as e:
            logger.warning(f"Error closing connection during invalidation: {e}")

        del self._pools[graph_id]
        logger.info(f"Invalidated all DuckDB connections for: {graph_id}")

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

      total_databases = 0
      if self.base_path.exists():
        total_databases = len(list(self.base_path.glob("*.duckdb")))

      return {
        "total_connections": total_connections,
        "total_databases_on_disk": total_databases,
        "database_pools": pool_stats,
        "stats": self._stats.copy(),
        "configuration": {
          "max_connections_per_db": self.max_connections_per_db,
          "connection_ttl_minutes": self.connection_ttl.total_seconds() / 60,
          "health_check_interval_minutes": self.health_check_interval.total_seconds()
          / 60,
        },
      }

  def close_database_connections(self, graph_id: str):
    """Close all connections for a specific database."""
    with self._get_database_lock(graph_id):
      if graph_id in self._pools:
        pool = self._pools[graph_id]
        for conn_id in list(pool.keys()):
          self._close_connection(graph_id, conn_id)

        logger.debug(f"Closed all DuckDB connections for {graph_id}")

  def interrupt_connections(self, graph_id: str) -> int:
    """Cancel in-flight queries for a database and close their connections.

    Interrupted connections MUST also be closed, not just marked unhealthy:
    the zombie still holds the DuckDB file lock, and new CREATE/INSERT
    operations would hang forever waiting on it.

    Returns the number of connections interrupted and closed.
    """
    interrupted_count = 0
    with self._get_database_lock(graph_id):
      if graph_id in self._pools:
        # Snapshot: the pool is mutated inside the loop.
        pool_items = list(self._pools[graph_id].items())
        for conn_id, conn_info in pool_items:
          try:
            conn_info.connection.interrupt()
            logger.debug(f"Interrupted DuckDB query on {conn_id} for {graph_id}")
          except Exception as e:
            logger.warning(f"Failed to interrupt connection {conn_id}: {e}")

          try:
            # No CHECKPOINT here: an interrupted connection is in an undefined
            # state and the checkpoint could hang or fail.
            conn_info.connection.close()
            logger.debug(f"Closed interrupted DuckDB connection {conn_id}")
          except Exception as e:
            logger.warning(f"Failed to close interrupted connection {conn_id}: {e}")

          if conn_id in self._pools[graph_id]:
            del self._pools[graph_id][conn_id]
            self._stats["connections_closed"] += 1

          interrupted_count += 1
          logger.info(
            f"Interrupted and closed DuckDB connection {conn_id} for {graph_id}"
          )

    if interrupted_count > 0:
      logger.info(
        f"Interrupted and closed {interrupted_count} DuckDB connection(s) for {graph_id}"
      )
    return interrupted_count

  def has_active_connections(self, graph_id: str) -> bool:
    """Check if there are any active connections for a database."""
    with self._get_database_lock(graph_id):
      if graph_id in self._pools:
        return len(self._pools[graph_id]) > 0
      return False

  def close_all_connections(self):
    """Close all connections in the pool."""
    self._cleanup_all_connections()

  def reconfigure_memory_limit(self, graph_id: str, memory_limit: str) -> int:
    """Apply a new ``memory_limit`` to every open connection for a database.

    DuckDB accepts memory_limit changes at runtime, so this raises or lowers
    the cap without reconnecting. Note that lowering the limit does not hand
    already-allocated buffers back to the OS — only closing the connections
    does that (see ``release_duckdb_memory`` in ``core/memory_manager.py``).

    Returns the number of connections reconfigured.
    """
    reconfigured = 0
    with self._get_database_lock(graph_id):
      if graph_id in self._pools:
        for conn_id, conn_info in self._pools[graph_id].items():
          try:
            conn_info.connection.execute(f"SET memory_limit='{memory_limit}'")
            reconfigured += 1
            logger.debug(f"Reconfigured memory limit to {memory_limit} on {conn_id}")
          except Exception as e:
            logger.warning(f"Failed to reconfigure memory on {conn_id}: {e}")

    if reconfigured > 0:
      logger.info(
        f"Reconfigured {reconfigured} DuckDB connection(s) for {graph_id} "
        f"to memory_limit={memory_limit}"
      )
    return reconfigured

  def force_database_cleanup(self, graph_id: str) -> None:
    """Close all connections for a graph and delete its staging file.

    Call this when a graph is deleted; nothing else removes the file.
    """
    with self._global_lock:
      logger.info(f"Forcing cleanup for DuckDB database: {graph_id}")

      self.close_database_connections(graph_id)

      db_path = self._get_database_path(graph_id)
      if db_path.exists():
        try:
          db_path.unlink()
          logger.info(f"Deleted DuckDB database file: {db_path}")

          wal_file = db_path.with_suffix(".duckdb.wal")
          if wal_file.exists():
            wal_file.unlink()
            logger.debug(f"Deleted WAL file: {wal_file}")

        except Exception as e:
          logger.warning(f"Failed to delete DuckDB file {db_path}: {e}")


# Global connection pool instance (initialized by the application)
_duckdb_pool: DuckDBConnectionPool | None = None


def initialize_duckdb_pool(
  base_path: str,
  max_connections_per_db: int = 3,
  connection_ttl_minutes: int = 30,
) -> DuckDBConnectionPool:
  """Create the process-global DuckDB connection pool."""
  global _duckdb_pool
  _duckdb_pool = DuckDBConnectionPool(
    base_path=base_path,
    max_connections_per_db=max_connections_per_db,
    connection_ttl_minutes=connection_ttl_minutes,
  )
  return _duckdb_pool


def get_duckdb_pool() -> DuckDBConnectionPool:
  """Get the global DuckDB connection pool instance."""
  if _duckdb_pool is None:
    raise RuntimeError(
      "DuckDB connection pool not initialized. Call initialize_duckdb_pool() first."
    )
  return _duckdb_pool


# Memory limit overrides, keyed by graph_id so concurrently staging graphs
# cannot clobber each other's boost.
_memory_limit_overrides: dict[str, str] = {}


def set_duckdb_memory_override(limit: str | None, graph_id: str = "sec") -> str | None:
  """Boost (or, with ``limit=None``, clear) a graph's memory limit and return
  the previous override so the caller can restore it.

  Only affects connections opened afterwards; use
  ``DuckDBConnectionPool.reconfigure_memory_limit`` for connections already
  open. ``core/memory_manager.py`` wraps both.

  Example:
      old_limit = set_duckdb_memory_override("58GB", graph_id="sec")
      try:
          # ... perform staging ...
      finally:
          set_duckdb_memory_override(old_limit, graph_id="sec")
  """
  global _memory_limit_overrides
  old_value = _memory_limit_overrides.get(graph_id)
  if limit:
    _memory_limit_overrides[graph_id] = limit
    logger.info(f"DuckDB memory override for {graph_id} set to: {limit}")
  else:
    _memory_limit_overrides.pop(graph_id, None)
    logger.info(f"DuckDB memory override for {graph_id} cleared (using tier default)")
  return old_value


def get_duckdb_memory_override(graph_id: str = "sec") -> str | None:
  """Get current DuckDB memory override for a graph, if any."""
  return _memory_limit_overrides.get(graph_id)
