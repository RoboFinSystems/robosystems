"""
Thread-Safe LadybugDB Connection Pool

This module provides a production-ready connection pool for LadybugDB databases
with proper thread safety, connection limits, TTL, and health checking.

Key features:
- Thread-safe connection management using locks
- Configurable connection limits per database
- Connection TTL and automatic cleanup
- Connection health checking and recovery
- Metrics and monitoring integration
- Graceful connection cleanup on shutdown
"""

import os
import threading
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import real_ladybug as lbug

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
  read_only: bool = False  # Track if connection was opened read-only
  s3_attached: bool = False  # S3 ATTACH connections bypass TTL (read-only, expensive to recreate)


class LadybugConnectionPool:
  """
  Thread-safe connection pool for LadybugDB databases.

  This pool manages connections with proper lifecycle management,
  health checking, and resource limits to prevent memory leaks
  and ensure optimal performance.

  IMPORTANT: All connections for a given database share the same Database
  object to ensure proper transaction visibility across connections.
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
    Initialize connection pool.

    Args:
        base_path: Base directory for LadybugDB databases
        max_connections_per_db: Maximum connections per database
        connection_ttl_minutes: Connection time-to-live in minutes
        health_check_interval_minutes: How often to check connection health
        cleanup_interval_minutes: How often to cleanup expired connections
    """
    self.base_path = Path(base_path)
    self.max_connections_per_db = max_connections_per_db
    self.connection_ttl = timedelta(minutes=connection_ttl_minutes)
    self.health_check_interval = timedelta(minutes=health_check_interval_minutes)
    self.cleanup_interval = timedelta(minutes=cleanup_interval_minutes)

    # Thread-safe storage
    self._pools: dict[str, dict[str, ConnectionInfo]] = {}
    self._locks: dict[str, threading.RLock] = {}
    self._global_lock = threading.RLock()

    # Store Database objects to ensure all connections use the same one
    # This is critical for transaction visibility in LadybugDB
    self._databases: dict[str, lbug.Database] = {}

    # Monitoring
    self._stats = {
      "connections_created": 0,
      "connections_reused": 0,
      "connections_closed": 0,
      "health_checks": 0,
      "health_failures": 0,
    }

    # Cleanup tracking
    self._last_cleanup = datetime.now(UTC)
    self._last_health_check = datetime.now(UTC)

    # Register cleanup on process exit
    weakref.finalize(self, self._cleanup_all_connections)

    logger.info(
      f"Initialized LadybugDB connection pool: {max_connections_per_db} max per DB, {connection_ttl_minutes}min TTL"
    )

  @contextmanager
  def get_connection(self, database_name: str, read_only: bool = False):
    """
    Get a connection from the pool (context manager).

    Args:
        database_name: Name of the database
        read_only: Whether to open in read-only mode

    Yields:
        lbug.Connection: Database connection

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
    """Acquire a connection from the pool."""
    # Perform periodic maintenance
    self._maybe_run_maintenance()

    with self._get_database_lock(database_name):
      # Try to get existing connection
      connection_info = self._get_existing_connection(database_name, read_only)

      if connection_info and self._is_connection_valid(connection_info):
        # Reuse existing connection
        connection_info.last_used = datetime.now(UTC)
        connection_info.use_count += 1
        self._stats["connections_reused"] += 1
        logger.debug(
          f"Reused connection for {database_name} (use count: {connection_info.use_count})"
        )
        return connection_info

      # Create new connection
      return self._create_new_connection(database_name, read_only)

  def _release_connection(self, database_name: str, connection_info: ConnectionInfo):
    """Release a connection back to the pool."""
    # Connection is automatically returned to pool
    # No explicit action needed as we're using a simple strategy
    logger.debug(f"Released connection for {database_name}")

  def invalidate_connection(self, database_name: str):
    """
    Invalidate all connections for a database.

    This forces new connections to be created on next access,
    which can help with transaction visibility issues after bulk operations.

    Args:
        database_name: Name of the database
    """
    with self._get_database_lock(database_name):
      if database_name in self._pools:
        # Close all connections for this database
        for conn_id, conn_info in self._pools[database_name].items():
          try:
            conn_info.connection.close()
            self._stats["connections_closed"] += 1
          except Exception as e:
            logger.warning(f"Error closing connection during invalidation: {e}")

        # Clear the pool for this database
        del self._pools[database_name]

        # Remove the shared Database object to force recreation
        # This ensures the next connection sees all committed data
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
    """Get an existing connection for a database if available."""
    if database_name not in self._pools:
      return None

    pool = self._pools[database_name]

    # Find the least recently used healthy connection with matching read_only status
    best_connection = None
    oldest_time = datetime.now(UTC)

    for conn_id, conn_info in pool.items():
      if (
        conn_info.is_healthy
        and conn_info.read_only == read_only  # Match read_only status
        and conn_info.last_used < oldest_time
        and self._is_connection_valid(conn_info)
      ):
        best_connection = conn_info
        oldest_time = conn_info.last_used

    return best_connection

  def _create_new_connection(
    self, database_name: str, read_only: bool
  ) -> ConnectionInfo:
    """Create a new connection for a database."""
    # Check connection limits
    if database_name in self._pools:
      current_count = len(self._pools[database_name])
      if current_count >= self.max_connections_per_db:
        # Remove oldest connection to make room
        self._remove_oldest_connection(database_name)

    try:
      # Check for S3 ATTACH mode (replicas reading from S3-hosted database)
      s3_attach_prefix = os.getenv("LBUG_S3_ATTACH_PREFIX")
      lbug_role = os.getenv("LBUG_ROLE", "master")

      if s3_attach_prefix and lbug_role == "replica":
        # Only ATTACH databases listed in SHARED_REPOSITORIES
        shared_repos = [
          r.strip()
          for r in os.getenv("SHARED_REPOSITORIES", "").split(",")
          if r.strip()
        ]
        if database_name in shared_repos:
          # Construct per-database S3 URI from prefix
          s3_uri = f"{s3_attach_prefix.rstrip('/')}/{database_name}.lbug"
          # S3 ATTACH mode: connect to S3-hosted database via httpfs
          return self._create_s3_attached_connection(database_name, s3_uri)

      # Normal mode: open local database file
      # Construct database path safely (LadybugDB uses .lbug files)
      db_path = self.base_path / f"{database_name}.lbug"

      # Get or create shared Database object
      # CRITICAL: All connections must use the same Database object for transaction visibility
      if database_name not in self._databases:
        logger.info(f"Creating new Database object for {database_name}")

        # Get memory configuration from shared helper (single source of truth)
        from .config import get_database_memory_config

        buffer_pool_mb = get_database_memory_config()

        # Create database with buffer pool size configuration
        # Note: LadybugDB Python API uses buffer_pool_size in bytes
        buffer_pool_size = buffer_pool_mb * 1024 * 1024

        # Check if this is a replica instance - replicas MUST open read-only
        # to avoid WAL recovery and lock contention from snapshot-based volumes
        lbug_role = os.getenv("LBUG_ROLE", "master")
        is_replica = lbug_role == "replica"

        if is_replica:
          logger.info(
            f"Opening database '{database_name}' in READ-ONLY mode (LBUG_ROLE=replica)"
          )
          db_read_only = True
        else:
          # Masters open read_write to allow both read and write operations.
          # This prevents the bug where the first read-only request permanently
          # locks the database in read-only mode.
          db_read_only = False

        # For SEC database, use explicit checkpoint threshold for large tables
        # SEC has huge tables (Fact, Association) that can exhaust memory
        if database_name == "sec":
          checkpoint_threshold = 134217728  # 128MB for SEC (more frequent checkpoints)
          logger.info("Using reduced checkpoint threshold (128MB) for SEC database")
        else:
          checkpoint_threshold = 536870912  # 512MB for regular databases

        # Create database with all optimizations
        self._databases[database_name] = lbug.Database(
          str(db_path),
          read_only=db_read_only,
          buffer_pool_size=buffer_pool_size,
          compression=True,  # Safe: enabled by default in LadybugDB
          max_num_threads=0,  # Use all available threads (LadybugDB decides)
          auto_checkpoint=True,  # Enable automatic checkpointing
          checkpoint_threshold=checkpoint_threshold,  # Adaptive based on database
        )
        logger.info(
          f"Database '{database_name}' created - read_only: {db_read_only}, buffer pool: {buffer_pool_mb} MB, "
          f"compression: enabled, auto_checkpoint: enabled, threshold: {checkpoint_threshold // (1024 * 1024)}MB"
        )

      db = self._databases[database_name]

      # Create connection from shared Database object
      conn = lbug.Connection(db)

      # Test connection
      try:
        result = conn.execute("RETURN 1 as test")
        # Handle both single QueryResult and list[QueryResult] return types
        if isinstance(result, list):
          for r in result:
            r.close()
        else:
          result.close()
        is_healthy = True

        # Apply connection-level configuration AFTER verifying connection is healthy
        # These are non-critical settings that enhance performance but aren't required
        try:
          # Set home directory to the base path (LadybugDB creates .lbug subdirectory)
          # This keeps temporary files on the same fast EBS volume
          # Note: Don't add ".lbug" suffix - LadybugDB creates that internally
          home_dir = str(self.base_path)

          conn.execute(f"CALL home_directory='{home_dir}';")

          # Disable progress bar for server applications (not needed in non-interactive mode)
          conn.execute("CALL progress_bar=false;")

          # Set query timeout to 2 minutes (120000ms) to prevent long-running queries
          # This is a per-query timeout that applies to each individual query
          # Note: Ingestion operations specifically set this to 30 minutes when needed
          conn.execute("CALL timeout=120000;")

          # Enable semi-mask optimization for better query performance
          # This is enabled by default but explicit is better
          conn.execute("CALL enable_semi_mask=true;")

          # Set warning limit to prevent excessive memory usage from warnings
          # 1024 warnings should be plenty for debugging without consuming too much memory
          conn.execute("CALL warning_limit=1024;")

          # Enable spill to disk for large operations to prevent out-of-memory errors
          # This allows LadybugDB to use disk for temporary storage during large queries
          conn.execute("CALL spill_to_disk=true;")

          logger.info(
            f"Applied connection configuration for {database_name} (home_dir={home_dir}, progress_bar=false, timeout=120000ms, semi_mask=true, warning_limit=1024, spill_to_disk=true)"
          )
        except Exception as config_error:
          # These settings are nice-to-have but not critical
          # Log at debug level to avoid noise
          logger.debug(
            f"Could not apply connection settings (non-critical): {config_error}"
          )
          # Connection is still healthy even if these settings fail

      except Exception as e:
        logger.warning(f"New connection health check failed for {database_name}: {e}")
        is_healthy = False

      # Create connection info
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

      # Store in pool
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

  def _create_s3_attached_connection(
    self, database_name: str, s3_uri: str
  ) -> ConnectionInfo:
    """
    Create a connection that ATTACHes to an S3-hosted database.

    This is used by replicas to read from S3-hosted .lbug files using
    LadybugDB's httpfs extension. The database is accessed directly from S3
    with local HTTP caching for performance.

    S3 ATTACH connections are always read-only.

    Args:
        database_name: Name of the database (used as alias for ATTACH)
        s3_uri: Full S3 URI to the .lbug file (e.g., s3://bucket/path/sec.lbug)

    Returns:
        ConnectionInfo with the attached connection
    """

    logger.info(f"Creating S3 ATTACH connection for {database_name} from {s3_uri}")

    # Get memory configuration
    from .config import get_database_memory_config

    buffer_pool_mb = get_database_memory_config()
    buffer_pool_size = buffer_pool_mb * 1024 * 1024

    # Create an in-memory database that will ATTACH to the S3 database
    # We use an in-memory database as the "host" for the ATTACH
    db = lbug.Database(
      ":memory:",
      buffer_pool_size=buffer_pool_size,
    )

    conn = lbug.Connection(db)

    try:
      # Install and load httpfs extension for S3 access
      logger.info("Loading httpfs extension for S3 ATTACH...")
      try:
        conn.execute("INSTALL httpfs")
      except Exception as e:
        if "already installed" not in str(e).lower():
          raise
      try:
        conn.execute("LOAD httpfs")
      except Exception as e:
        if "already loaded" not in str(e).lower():
          raise

      # Configure S3 credentials from EC2 IMDS
      self._configure_s3_credentials(conn)

      # Enable HTTP caching for performance
      # This caches downloaded data locally to avoid repeated S3 fetches
      try:
        conn.execute("CALL HTTP_CACHE_FILE = TRUE")
        logger.info("Enabled HTTP file caching for S3 ATTACH")
      except Exception as cache_err:
        logger.warning(f"Could not enable HTTP caching (non-critical): {cache_err}")

      # ATTACH to the S3-hosted database with retry logic for transient failures
      # The database will be accessible via the alias (database_name)
      logger.info(f"ATTACHing to S3 database: {s3_uri} AS {database_name}")
      attach_query = f"ATTACH '{s3_uri}' AS {database_name} (dbtype lbug, read_only)"

      import time

      max_retries = 3
      for attempt in range(max_retries):
        try:
          conn.execute(attach_query)
          break
        except Exception as attach_err:
          if attempt == max_retries - 1:
            raise  # Re-raise on final attempt
          wait_time = 5 * (attempt + 1)  # Exponential backoff: 5s, 10s, 15s
          logger.warning(
            f"S3 ATTACH attempt {attempt + 1}/{max_retries} failed: {attach_err}. "
            f"Retrying in {wait_time}s..."
          )
          time.sleep(wait_time)

      # Verify the attach worked by running a simple query
      logger.info("Verifying S3 ATTACH connection...")
      result = conn.execute(f"USE {database_name}; RETURN 1 as test")
      if isinstance(result, list):
        for r in result:
          r.close()
      else:
        result.close()

      logger.info(f"✅ S3 ATTACH connection established for {database_name}")

      # Create connection info
      now = datetime.now(UTC)
      connection_info = ConnectionInfo(
        connection=conn,
        database=db,
        created_at=now,
        last_used=now,
        use_count=1,
        is_healthy=True,
        read_only=True,  # S3 ATTACH is always read-only
        s3_attached=True,  # Bypass TTL - expensive to recreate, read-only
      )

      # Store in pool
      if database_name not in self._pools:
        self._pools[database_name] = {}

      # Store database reference (even though it's in-memory, we need to track it)
      self._databases[database_name] = db

      conn_id = f"{database_name}_s3_{len(self._pools[database_name])}"
      self._pools[database_name][conn_id] = connection_info

      self._stats["connections_created"] += 1
      logger.info(
        f"Created S3 ATTACH connection for {database_name} (total: {len(self._pools[database_name])})"
      )

      return connection_info

    except Exception as e:
      # Clean up on failure - log but don't mask the primary exception
      try:
        conn.close()
      except Exception as close_err:
        logger.debug(f"Failed to close connection during cleanup: {close_err}")
      try:
        db.close()
      except Exception as close_err:
        logger.debug(f"Failed to close database during cleanup: {close_err}")
      logger.error(f"Failed to create S3 ATTACH connection for {database_name}: {e}")
      raise

  def _configure_s3_credentials(self, conn: lbug.Connection) -> None:
    """
    Configure S3 credentials for httpfs extension.

    Uses EC2 Instance Metadata Service (IMDS) in production to get
    temporary credentials from the instance's IAM role.
    """
    from robosystems.config import env

    region = (
      env.AWS_DEFAULT_REGION or env.AWS_REGION or os.getenv("AWS_REGION", "us-east-1")
    )
    is_production = env.ENVIRONMENT in ("prod", "staging")
    credentials_loaded = False

    if is_production:
      # Production/Staging: Fetch from EC2 Instance Metadata Service (IMDS)
      try:
        import requests

        # IMDSv2: Get token first
        token_response = requests.put(
          "http://169.254.169.254/latest/api/token",
          headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
          timeout=2,
        )
        token = token_response.text

        # Get IAM role name
        role_response = requests.get(
          "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
          headers={"X-aws-ec2-metadata-token": token},
          timeout=2,
        )
        role_name = role_response.text.strip()

        # Get credentials for the role
        creds_response = requests.get(
          f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{role_name}",
          headers={"X-aws-ec2-metadata-token": token},
          timeout=2,
        )
        creds = creds_response.json()

        access_key = creds["AccessKeyId"].replace("'", "''")
        secret_key = creds["SecretAccessKey"].replace("'", "''")
        session_token = creds.get("Token", "").replace("'", "''")

        conn.execute(f"CALL s3_access_key_id = '{access_key}'")
        conn.execute(f"CALL s3_secret_access_key = '{secret_key}'")
        if session_token:
          # Session tokens are REQUIRED for EC2 IMDS temporary credentials
          # Failure here means httpfs doesn't support session tokens, which breaks S3 auth
          try:
            conn.execute(f"CALL s3_session_token = '{session_token}'")
          except Exception as token_err:
            logger.error(
              f"Failed to set s3_session_token (REQUIRED for IMDS credentials): {token_err}"
            )
            raise RuntimeError(
              "S3 ATTACH requires session token support for EC2 IMDS credentials. "
              "Ensure LadybugDB/httpfs version supports s3_session_token."
            ) from token_err

        credentials_loaded = True
        logger.info(f"Loaded S3 credentials from EC2 IMDS (role: {role_name})")

      except Exception as imds_err:
        logger.warning(f"Failed to fetch credentials from EC2 IMDS: {imds_err}")

    # Dev environment OR fallback if IMDS failed: use explicit credentials
    if not credentials_loaded:
      if env.AWS_S3_ACCESS_KEY_ID and env.AWS_S3_SECRET_ACCESS_KEY:
        escaped_key = env.AWS_S3_ACCESS_KEY_ID.replace("'", "''")
        escaped_secret = env.AWS_S3_SECRET_ACCESS_KEY.replace("'", "''")
        conn.execute(f"CALL s3_access_key_id = '{escaped_key}'")
        conn.execute(f"CALL s3_secret_access_key = '{escaped_secret}'")
        credentials_loaded = True
        logger.info("Configured S3 credentials from environment variables")

    if not credentials_loaded:
      logger.warning(
        "No S3 credentials available for ATTACH. In prod/staging, ensure EC2 has IAM role. "
        "In dev, set AWS_S3_ACCESS_KEY_ID/AWS_S3_SECRET_ACCESS_KEY."
      )

    # Always set region for S3 access
    conn.execute(f"CALL s3_region = '{region}'")
    logger.debug(f"Set S3 region to: {region}")

  def _is_connection_valid(self, connection_info: ConnectionInfo) -> bool:
    """Check if a connection is still valid."""
    # S3 ATTACH connections bypass TTL - they are read-only remote mounts
    # that are expensive to recreate (~11 min ATTACH + full buffer pool reallocation).
    # Cycling them via TTL causes OOM on memory-constrained replicas.
    if not connection_info.s3_attached:
      if datetime.now(UTC) - connection_info.created_at > self.connection_ttl:
        return False

    # Check health status
    return connection_info.is_healthy

  def _remove_oldest_connection(self, database_name: str):
    """Remove the oldest connection from a database pool."""
    if database_name not in self._pools or not self._pools[database_name]:
      return

    pool = self._pools[database_name]

    # Find oldest connection
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
      # Don't close the database here - it's shared and will be closed in _cleanup_all_connections
      # or when the pool is destroyed. This prevents double-closing.
    except Exception as e:
      logger.warning(f"Error closing connection {connection_id}: {e}")

    del self._pools[database_name][connection_id]
    self._stats["connections_closed"] += 1

    logger.debug(f"Closed connection {connection_id} for {database_name}")

  def _maybe_run_maintenance(self):
    """Run maintenance tasks if needed."""
    now = datetime.now(UTC)

    # Run cleanup
    if now - self._last_cleanup > self.cleanup_interval:
      self._cleanup_expired_connections()
      self._last_cleanup = now

    # Run health checks
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
      # Handle both single QueryResult and list[QueryResult] return types
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
    """Clean up all connections (called on shutdown)."""
    with self._global_lock:
      total_closed = 0

      for db_name, pool in self._pools.items():
        for conn_id in list(pool.keys()):
          self._close_connection(db_name, conn_id)
          total_closed += 1

      # Close all Database objects
      for db_name in list(self._databases.keys()):
        try:
          if hasattr(self._databases[db_name], "close"):
            self._databases[db_name].close()
        except Exception as e:
          logger.warning(f"Error closing database {db_name}: {e}")
        del self._databases[db_name]

      self._pools.clear()
      self._locks.clear()
      self._databases.clear()

  def force_database_cleanup(self, database_name: str, aggressive: bool = True) -> None:
    """
    Force cleanup of all connections and optionally the database object for a specific database.

    This is useful after large ingestion operations to release memory held by LadybugDB's buffer pool.
    All existing connections will be closed and new ones will be created on next access.

    Args:
        database_name: Name of the database to clean up
        aggressive: If True, use more aggressive memory cleanup techniques
    """
    with self._global_lock:
      logger.info(
        f"Forcing cleanup for database: {database_name} (aggressive={aggressive})"
      )

      # Close all connections for this database
      if database_name in self._pools:
        pool = self._pools[database_name]
        for conn_id in list(pool.keys()):
          self._close_connection(database_name, conn_id)

        # Clear the pool for this database
        del self._pools[database_name]
        logger.info(f"Closed all connections for database: {database_name}")

      # Remove the Database object to force buffer pool release
      # This will cause it to be recreated with fresh memory on next access
      if database_name in self._databases:
        try:
          # Try to close the database if it has a close method
          db = self._databases[database_name]

          # For SEC database, try to execute CHECKPOINT before closing
          # This ensures all WAL data is flushed to disk
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

        # Remove the database object from cache
        del self._databases[database_name]
        logger.info(f"Removed cached Database object for: {database_name}")

        if aggressive:
          # Aggressive memory cleanup for large operations
          import ctypes
          import gc

          # Clear any references to the database object
          db = None

          # Force multiple rounds of garbage collection
          # Generation 2 contains long-lived objects
          for generation in range(3):
            collected = gc.collect(generation)
            logger.debug(f"GC generation {generation}: collected {collected} objects")

          # Try to trim memory back to OS (Linux/Unix specific)
          if hasattr(ctypes, "CDLL"):
            try:
              libc = ctypes.CDLL("libc.so.6")
              # malloc_trim returns 1 on success, 0 on failure
              if libc.malloc_trim(0) == 1:
                logger.info("Successfully trimmed memory back to OS")
            except Exception as e:
              logger.debug(f"Could not trim memory (not Linux?): {e}")

          # Log memory usage for monitoring
          try:
            import psutil

            process = psutil.Process()
            mem_info = process.memory_info()
            logger.info(
              f"Memory after cleanup - RSS: {mem_info.rss / (1024 * 1024):.1f}MB, "
              f"VMS: {mem_info.vms / (1024 * 1024):.1f}MB"
            )
          except ImportError:
            pass
        else:
          # Standard garbage collection
          import gc

          gc.collect()
          logger.info(f"Triggered garbage collection after cleanup of: {database_name}")

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
    """Close all connections for a specific database."""
    with self._get_database_lock(database_name):
      if database_name in self._pools:
        pool = self._pools[database_name]
        for conn_id in list(pool.keys()):
          self._close_connection(database_name, conn_id)

        # Close and remove the shared Database object
        if database_name in self._databases:
          try:
            self._databases[database_name].close()
          except Exception as e:
            logger.warning(f"Error closing database {database_name}: {e}")
          del self._databases[database_name]

        logger.info(f"Closed all connections for database {database_name}")

  def has_active_connections(self, database_name: str) -> bool:
    """Check if there are any active connections for a database."""
    with self._get_database_lock(database_name):
      if database_name in self._pools:
        return len(self._pools[database_name]) > 0
      return False

  def list_databases(self) -> list[str]:
    """
    List all databases known to the pool.

    This includes both local databases and S3 ATTACH databases.
    Used by the manager to include pool-managed databases (like S3 ATTACH)
    in its database listing.

    Returns:
        List of database names managed by this pool
    """
    with self._global_lock:
      return list(self._databases.keys())

  def recreate_database(self, database_name: str) -> None:
    """
    Close and remove a database so it will be recreated with current settings.

    This is used to apply new memory configuration (via set_ladybug_memory_override)
    to an existing database. The database will be recreated on next connection
    with the current memory settings from get_database_memory_config().

    Args:
        database_name: Name of the database to recreate

    Example:
        from robosystems.graph_api.core.ladybug.config import set_ladybug_memory_override

        # Boost memory for materialization
        old_limit = set_ladybug_memory_override(50000)  # 50GB
        pool.recreate_database("sec")  # Close existing, will recreate with 50GB

        # ... perform materialization ...

        # Restore default memory
        set_ladybug_memory_override(old_limit)
        pool.recreate_database("sec")  # Recreate with restored limit
    """
    logger.info(f"Recreating database {database_name} to apply new memory settings")
    self.close_database_connections(database_name)

  def close_all_connections(self):
    """Close all connections in the pool."""
    self._cleanup_all_connections()


# Global connection pool instance (initialized by the application)
_connection_pool: LadybugConnectionPool | None = None


def initialize_connection_pool(
  base_path: str, max_connections_per_db: int = 3, connection_ttl_minutes: int = 30
) -> LadybugConnectionPool:
  """Initialize the global connection pool."""
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
