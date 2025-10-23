"""
Thread-Safe DuckDB Connection Pool

This module provides a production-ready connection pool for DuckDB staging databases
with proper thread safety, connection limits, TTL, and health checking.

Inspired by the successful KuzuConnectionPool implementation.

Key features:
- Thread-safe connection management using locks
- Configurable connection limits per database
- Connection TTL and automatic cleanup
- Connection health checking and recovery
- Metrics and monitoring integration
- Graceful connection cleanup on shutdown
- Per-database DuckDB instances (one per graph_id)
"""

import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Any
from dataclasses import dataclass
from contextlib import contextmanager
from pathlib import Path
import weakref

import duckdb
from robosystems.logger import logger


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
  """
  Thread-safe connection pool for DuckDB staging databases.

  This pool manages connections with proper lifecycle management,
  health checking, and resource limits to prevent memory leaks
  and ensure optimal performance.

  Each graph_id gets its own DuckDB database file, and connections
  to that database are pooled and reused.
  """

  def __init__(
    self,
    base_path: str,
    max_connections_per_db: int = 3,
    connection_ttl_minutes: int = 30,
    health_check_interval_minutes: int = 5,
    cleanup_interval_minutes: int = 10,
  ):
    """
    Initialize DuckDB connection pool.

    Args:
        base_path: Base directory for DuckDB staging databases
        max_connections_per_db: Maximum connections per database
        connection_ttl_minutes: Connection time-to-live in minutes
        health_check_interval_minutes: How often to check connection health
        cleanup_interval_minutes: How often to cleanup expired connections

    Note:
        Database files are NOT automatically deleted. They persist as long as
        the graph exists. Use force_database_cleanup() to manually delete when
        a graph is deleted.
    """
    self.base_path = Path(base_path)
    self.max_connections_per_db = max_connections_per_db
    self.connection_ttl = timedelta(minutes=connection_ttl_minutes)
    self.health_check_interval = timedelta(minutes=health_check_interval_minutes)
    self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)

    # Ensure base directory exists
    try:
      self.base_path.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError) as e:
      logger.warning(f"Could not create DuckDB staging directory: {e}")

    # Thread-safe storage
    self._pools: Dict[str, Dict[str, DuckDBConnectionInfo]] = {}
    self._locks: Dict[str, threading.RLock] = {}
    self._global_lock = threading.RLock()

    # Monitoring
    self._stats = {
      "connections_created": 0,
      "connections_reused": 0,
      "connections_closed": 0,
      "health_checks": 0,
      "health_failures": 0,
      "databases_cleaned": 0,
    }

    # Cleanup tracking
    self._last_cleanup = datetime.now(timezone.utc)
    self._last_health_check = datetime.now(timezone.utc)

    # Register cleanup on process exit
    weakref.finalize(self, self._cleanup_all_connections)

    logger.info(
      f"Initialized DuckDB connection pool: {max_connections_per_db} max per DB, "
      f"{connection_ttl_minutes}min TTL (databases persist with graph lifecycle)"
    )

  @contextmanager
  def get_connection(self, graph_id: str):
    """
    Get a connection from the pool (context manager).

    Args:
        graph_id: Graph database identifier (used as database name)

    Yields:
        duckdb.DuckDBPyConnection: Database connection

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

  def _acquire_connection(self, graph_id: str) -> DuckDBConnectionInfo:
    """Acquire a connection from the pool."""
    self._maybe_run_maintenance()

    with self._get_database_lock(graph_id):
      connection_info = self._get_existing_connection(graph_id)

      if connection_info and self._is_connection_valid(connection_info):
        connection_info.last_used = datetime.now(timezone.utc)
        connection_info.use_count += 1
        self._stats["connections_reused"] += 1
        logger.debug(
          f"Reused DuckDB connection for {graph_id} (use count: {connection_info.use_count})"
        )
        return connection_info

      return self._create_new_connection(graph_id)

  def _release_connection(self, graph_id: str, connection_info: DuckDBConnectionInfo):
    """Release a connection back to the pool."""
    logger.debug(f"Released DuckDB connection for {graph_id}")

  def _get_database_lock(self, graph_id: str) -> threading.RLock:
    """Get or create a lock for a specific database."""
    with self._global_lock:
      if graph_id not in self._locks:
        self._locks[graph_id] = threading.RLock()
      return self._locks[graph_id]

  def _get_existing_connection(self, graph_id: str) -> Optional[DuckDBConnectionInfo]:
    """Get an existing connection for a database if available."""
    if graph_id not in self._pools:
      return None

    pool = self._pools[graph_id]

    best_connection = None
    oldest_time = datetime.now(timezone.utc)

    for conn_id, conn_info in pool.items():
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

      # Ensure database directory exists
      db_path.parent.mkdir(parents=True, exist_ok=True)

      # Create DuckDB connection
      conn = duckdb.connect(str(db_path))

      # Configure DuckDB connection
      self._configure_connection(conn)

      # Test connection health
      is_healthy = self._test_new_connection(conn)

      now = datetime.now(timezone.utc)
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
    """
    Get the database path for a graph_id.

    Validates that the path is within the base directory to prevent path traversal.
    """
    # Sanitize graph_id to prevent path traversal
    if ".." in graph_id or "/" in graph_id or "\\" in graph_id:
      raise ValueError(f"Invalid graph_id: path traversal detected in '{graph_id}'")

    db_path = self.base_path / f"{graph_id}.duckdb"

    # Double-check resolved path is within base directory
    try:
      db_path.resolve().relative_to(self.base_path.resolve())
    except ValueError:
      raise ValueError(f"Invalid graph_id: path outside base directory '{graph_id}'")

    return db_path

  def _configure_connection(self, conn: duckdb.DuckDBPyConnection):
    """Configure a DuckDB connection with extensions and settings."""
    from robosystems.config import env

    try:
      # Install and load extensions
      conn.execute("INSTALL httpfs")
      conn.execute("LOAD httpfs")
      conn.execute("INSTALL parquet")
      conn.execute("LOAD parquet")

      # Configure S3 access if credentials available
      if env.AWS_ACCESS_KEY_ID and env.AWS_SECRET_ACCESS_KEY:
        conn.execute("SET s3_access_key_id=?", [env.AWS_ACCESS_KEY_ID])
        conn.execute("SET s3_secret_access_key=?", [env.AWS_SECRET_ACCESS_KEY])
        conn.execute("SET s3_region=?", [env.AWS_DEFAULT_REGION])

      # Configure S3 endpoint if using LocalStack or custom endpoint
      if env.AWS_ENDPOINT_URL:
        conn.execute("SET s3_endpoint=?", [env.AWS_ENDPOINT_URL])
        conn.execute("SET s3_url_style='path'")

      # Performance settings
      conn.execute("SET threads TO 4")  # Limit threads to prevent oversubscription
      conn.execute("SET memory_limit='2GB'")  # Per-connection memory limit

      logger.debug("Configured DuckDB connection with S3 access and extensions")

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
    if datetime.now(timezone.utc) - connection_info.created_at > self.connection_ttl:
      return False

    if not connection_info.is_healthy:
      return False

    return True

  def _remove_oldest_connection(self, graph_id: str):
    """Remove the oldest connection from a database pool."""
    if graph_id not in self._pools or not self._pools[graph_id]:
      return

    pool = self._pools[graph_id]

    oldest_conn_id = None
    oldest_time = datetime.now(timezone.utc)

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
      connection_info.connection.close()
    except Exception as e:
      logger.warning(f"Error closing DuckDB connection {connection_id}: {e}")

    del self._pools[graph_id][connection_id]
    self._stats["connections_closed"] += 1

    logger.debug(f"Closed DuckDB connection {connection_id} for {graph_id}")

  def _maybe_run_maintenance(self):
    """Run maintenance tasks if needed."""
    now = datetime.now(timezone.utc)

    if now - self._last_cleanup > self.cleanup_interval:
      self._cleanup_expired_connections()
      self._last_cleanup = now

    if now - self._last_health_check > self.health_check_interval:
      self._check_connection_health()
      self._last_health_check = now

    # NOTE: Database cleanup is intentionally DISABLED by default
    # Staging databases should persist as long as the graph exists
    # Cleanup can be triggered manually via force_database_cleanup() if needed

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

  def _cleanup_old_databases_DISABLED(self):
    """
    [INTENTIONALLY DISABLED]

    This method was originally designed to auto-delete old staging databases,
    but that's too aggressive. Staging tables are part of the user's workflow
    and should persist as long as the graph exists.

    Instead:
    - Databases are cleaned up when the parent graph is deleted (via force_database_cleanup)
    - Connection pooling prevents resource leaks from abandoned connections
    - External tables use ~1KB per table (views only, no materialized data)

    This method is kept for documentation purposes but is never called.
    """
    pass  # Intentionally disabled

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
    """
    Invalidate all connections for a database.

    This forces new connections to be created on next access.

    Args:
        graph_id: Graph database identifier
    """
    with self._get_database_lock(graph_id):
      if graph_id in self._pools:
        for conn_id, conn_info in self._pools[graph_id].items():
          try:
            conn_info.connection.close()
            self._stats["connections_closed"] += 1
          except Exception as e:
            logger.warning(f"Error closing connection during invalidation: {e}")

        del self._pools[graph_id]
        logger.info(f"Invalidated all DuckDB connections for: {graph_id}")

  def get_stats(self) -> Dict[str, Any]:
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

        logger.info(f"Closed all DuckDB connections for {graph_id}")

  def has_active_connections(self, graph_id: str) -> bool:
    """Check if there are any active connections for a database."""
    with self._get_database_lock(graph_id):
      if graph_id in self._pools:
        return len(self._pools[graph_id]) > 0
      return False

  def close_all_connections(self):
    """Close all connections in the pool."""
    self._cleanup_all_connections()

  def force_database_cleanup(self, graph_id: str) -> None:
    """
    Force cleanup of a specific database file and all its connections.

    This should be called when a graph is deleted to remove the staging database.
    Since external tables use ~1KB each, storage reclamation isn't critical.

    Args:
        graph_id: Graph database identifier
    """
    with self._global_lock:
      logger.info(f"Forcing cleanup for DuckDB database: {graph_id}")

      # Close all connections
      self.close_database_connections(graph_id)

      # Try to delete the database file
      db_path = self._get_database_path(graph_id)
      if db_path.exists():
        try:
          db_path.unlink()
          logger.info(f"Deleted DuckDB database file: {db_path}")

          # Delete WAL file if exists
          wal_file = db_path.with_suffix(".duckdb.wal")
          if wal_file.exists():
            wal_file.unlink()
            logger.debug(f"Deleted WAL file: {wal_file}")

        except Exception as e:
          logger.warning(f"Failed to delete DuckDB file {db_path}: {e}")


# Global connection pool instance (initialized by the application)
_duckdb_pool: Optional[DuckDBConnectionPool] = None


def initialize_duckdb_pool(
  base_path: str,
  max_connections_per_db: int = 3,
  connection_ttl_minutes: int = 30,
) -> DuckDBConnectionPool:
  """
  Initialize the global DuckDB connection pool.

  Note:
      Database files persist with graph lifecycle. They are NOT automatically
      deleted based on age. Call force_database_cleanup() when a graph is deleted.
  """
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
