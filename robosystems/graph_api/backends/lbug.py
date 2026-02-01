import shutil
import time
from pathlib import Path
from typing import Any

from robosystems.graph_api.core.ladybug import get_connection_pool
from robosystems.logger import logger

from .base import ClusterTopology, DatabaseInfo, GraphBackend, S3IngestionError


class LadybugBackend(GraphBackend):
  def __init__(self, data_path: str = "/data/lbug-dbs"):
    self.data_path = Path(data_path)
    self.data_path.mkdir(parents=True, exist_ok=True)

    # Use the GLOBAL ConnectionPool singleton initialized by database_manager
    # This ensures all operations (queries and ingestion) share the same Database objects
    self.connection_pool = get_connection_pool()
    logger.info(
      f"Initialized LadybugBackend using global ConnectionPool at {data_path}"
    )

  def _load_httpfs_extension(self, conn) -> None:
    """Install and load httpfs extension for S3 access."""
    # LadybugDB 0.13.x requires INSTALL before LOAD for official extensions
    # INSTALL downloads to home_directory path, LOAD activates for this session
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    logger.debug("Loaded httpfs extension")

  async def execute_query(
    self,
    graph_id: str,
    cypher: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
  ) -> list[dict[str, Any]]:
    # Use connection pool's get_connection context manager
    # Note: database parameter is unused for LadybugDB - graph_id directly maps to a .lbug database file
    with self.connection_pool.get_connection(graph_id, read_only=False) as conn:
      try:
        # Bind query parameters if provided
        if parameters:
          result = conn.execute(cypher, parameters)
        else:
          result = conn.execute(cypher)

        # Convert LadybugDB result to list of dicts with column names
        rows = []
        column_names = result.get_column_names()
        while result.has_next():
          row_data = result.get_next()
          row_dict = dict(zip(column_names, row_data, strict=False))
          rows.append(row_dict)

        return rows
      except Exception as e:
        logger.error(f"Query execution failed for {graph_id}: {e}")
        raise

  async def execute_write(
    self,
    graph_id: str,
    cypher: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
  ) -> list[dict[str, Any]]:
    # For LadybugDB, write operations use the same pattern as queries
    return await self.execute_query(graph_id, cypher, parameters, database)

  async def create_database(self, database_name: str) -> bool:
    # Clean up any orphaned WAL file before creating
    # Only if the main database doesn't exist (WAL without database = orphaned)
    db_path = self.data_path / f"{database_name}.lbug"
    wal_path = self.data_path / f"{database_name}.lbug.wal"
    if not db_path.exists() and wal_path.exists():
      wal_path.unlink()
      logger.info(f"Cleaned up orphaned WAL file for {database_name}")

    # Simply getting a connection will create the database if it doesn't exist
    with self.connection_pool.get_connection(database_name, read_only=False) as conn:
      # Test connection
      result = conn.execute("RETURN 1 as test")
      result.close()
    logger.info(f"Created LadybugDB database for {database_name}")
    return True

  async def delete_database(self, database_name: str) -> bool:
    # First, clean up any pooled connections and Database objects
    try:
      self.connection_pool.force_database_cleanup(database_name, aggressive=True)
      logger.debug(f"Cleaned up connection pool for {database_name}")
    except Exception as e:
      logger.warning(
        f"Failed to cleanup connection pool for {database_name}: {e}. Proceeding with file deletion."
      )

    # Then delete the physical files
    db_path = self.data_path / f"{database_name}.lbug"
    wal_path = self.data_path / f"{database_name}.lbug.wal"

    # Delete WAL file first (if it exists)
    if wal_path.exists():
      wal_path.unlink()
      logger.debug(f"Deleted WAL file for {database_name}")

    # Delete main database file/directory
    if db_path.exists():
      if db_path.is_file():
        db_path.unlink()
      else:
        shutil.rmtree(db_path)
      logger.info(f"Deleted LadybugDB database for {database_name}")
    return True

  async def list_databases(self) -> list[str]:
    if not self.data_path.exists():
      return []
    return [f.stem for f in self.data_path.iterdir() if f.suffix == ".lbug"]

  async def get_database_info(self, database_name: str) -> DatabaseInfo:
    node_count = 0
    relationship_count = 0

    try:
      with self.connection_pool.get_connection(database_name, read_only=True) as conn:
        # Get node count
        result = conn.execute("MATCH (n) RETURN count(n) as count")
        if result.has_next():
          node_count = result.get_next()[0]
        result.close()

        # Get relationship count
        result = conn.execute("MATCH ()-[r]->() RETURN count(r) as count")
        if result.has_next():
          relationship_count = result.get_next()[0]
        result.close()
    except Exception as e:
      logger.warning(f"Failed to get database stats for {database_name}: {e}")

    db_path = self.data_path / f"{database_name}.lbug"
    if db_path.exists():
      if db_path.is_file():
        size_bytes = db_path.stat().st_size
      else:
        size_bytes = sum(f.stat().st_size for f in db_path.rglob("*") if f.is_file())
    else:
      size_bytes = 0

    return DatabaseInfo(
      name=database_name,
      node_count=node_count,
      relationship_count=relationship_count,
      size_bytes=size_bytes,
    )

  async def get_cluster_topology(self) -> ClusterTopology:
    return ClusterTopology(mode="embedded", leader={"backend": "ladybug"})

  async def health_check(self) -> bool:
    return True

  async def ingest_from_s3(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str | list[str],
    s3_credentials: dict[str, Any] | None = None,
    ignore_errors: bool = True,
    database: str | None = None,
  ) -> dict[str, Any]:
    # Use ConnectionPool's context manager - this is the 1.0.1 pattern
    with self.connection_pool.get_connection(graph_id, read_only=False) as conn:
      # Load httpfs extension by name - LadybugDB finds it at ~/.lbug/extension/{VERSION}/{PLATFORM}/
      try:
        self._load_httpfs_extension(conn)
      except Exception as e:
        if "already loaded" not in str(e).lower():
          logger.error(f"Failed to load httpfs extension: {e}")
          raise S3IngestionError(f"httpfs extension required for S3 access: {e}")

      # Configure S3 credentials
      if s3_credentials:
        # Explicit credentials provided (LocalStack/MinIO or manual override)
        if s3_credentials.get("aws_access_key_id"):
          escaped_key = s3_credentials["aws_access_key_id"].replace("'", "''")
          conn.execute(f"CALL s3_access_key_id = '{escaped_key}'")
        if s3_credentials.get("aws_secret_access_key"):
          escaped_secret = s3_credentials["aws_secret_access_key"].replace("'", "''")
          conn.execute(f"CALL s3_secret_access_key = '{escaped_secret}'")
        if s3_credentials.get("region"):
          escaped_region = s3_credentials["region"].replace("'", "''")
          conn.execute(f"CALL s3_region = '{escaped_region}'")
        if s3_credentials.get("endpoint_url"):
          endpoint = s3_credentials["endpoint_url"]
          if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
          elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]
          escaped_endpoint = endpoint.replace("'", "''")
          conn.execute(f"CALL s3_endpoint = '{escaped_endpoint}'")
          conn.execute("CALL s3_url_style = 'path'")
          logger.debug(f"Set S3 endpoint to: {endpoint} (path style URLs)")
        logger.debug("Configured S3 with explicit credentials")
      else:
        # Production: Kuzu/LadybugDB httpfs does NOT support credential chains.
        # We must fetch credentials explicitly based on environment.
        from robosystems.config import env

        region = env.AWS_DEFAULT_REGION or env.AWS_REGION or "us-east-1"
        credentials_loaded = False
        is_production = env.ENVIRONMENT in ("prod", "staging")

        if is_production:
          # Production/Staging: Fetch from EC2 Instance Metadata Service (IMDS)
          # EC2 instances have IAM roles attached
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
              try:
                conn.execute(f"CALL s3_session_token = '{session_token}'")
              except Exception as token_err:
                logger.warning(
                  f"Could not set s3_session_token (may not be supported): {token_err}. "
                  "IAM role credentials may not work without session token support."
                )

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
          logger.error(
            "No S3 credentials available. In prod/staging, ensure EC2 has IAM role. "
            "In dev, set AWS_S3_ACCESS_KEY_ID/AWS_S3_SECRET_ACCESS_KEY."
          )

        # Always set region for S3 access
        conn.execute(f"CALL s3_region = '{region}'")
        logger.debug(f"Set S3 region to: {region}")

      # S3 performance settings (apply to all credential modes)
      conn.execute("CALL s3_uploader_threads_limit = 8")
      conn.execute("CALL s3_uploader_max_num_parts_per_file = 10000")
      conn.execute("CALL s3_uploader_max_filesize = 10737418240")
      conn.execute("CALL spill_to_disk = true")
      conn.execute("CALL timeout=1800000")

      logger.debug(
        "Configured S3 performance settings, memory management, and 30-minute timeout for bulk ingestion"
      )

      # Build and execute COPY query
      # Handle both single pattern (string) and multiple patterns (list)
      # LadybugDB supports: COPY table FROM ["file1.parquet", "file2.parquet"]
      if isinstance(s3_pattern, list):
        # Multiple patterns: use LadybugDB's array syntax
        patterns_str = ", ".join(f'"{p}"' for p in s3_pattern)
        query = f"COPY {table_name} FROM [{patterns_str}]"
      else:
        # Single pattern: use direct COPY FROM
        query = f'COPY {table_name} FROM "{s3_pattern}"'

      if ignore_errors:
        query += " (IGNORE_ERRORS=TRUE)"

      pattern_count = len(s3_pattern) if isinstance(s3_pattern, list) else 1
      logger.info(
        f"Executing LadybugDB COPY ({pattern_count} patterns): {query[:200]}..."
        if len(query) > 200
        else f"Executing LadybugDB COPY: {query}"
      )

      start_time = time.time()
      result = conn.execute(query)
      duration = time.time() - start_time

      records_loaded = 0
      if result:
        # Try multiple methods to get row count from COPY result
        # LadybugDB COPY returns different formats depending on version
        try:
          # Method 1: get_as_list() returns tuples like [(count,)]
          if hasattr(result, "get_as_list"):
            result_list = result.get_as_list()
            if result_list and len(result_list) > 0:
              first_row = result_list[0]
              # Check if it's a tuple with a number
              if isinstance(first_row, (list, tuple)) and len(first_row) > 0:
                if isinstance(first_row[0], int):
                  records_loaded = first_row[0]
              # Check if it's a string with "Records loaded:"
              result_str = str(first_row)
              if "Records loaded:" in result_str:
                records_loaded = int(
                  result_str.split("Records loaded:")[-1].strip().split()[0]
                )
          # Method 2: Check for get_next pattern
          if records_loaded == 0 and hasattr(result, "has_next") and result.has_next():
            row = result.get_next()
            if isinstance(row, (list, tuple)) and len(row) > 0:
              if isinstance(row[0], int):
                records_loaded = row[0]
        except Exception as parse_err:
          logger.debug(f"Could not parse COPY result: {parse_err}")

      # CRITICAL: Execute CHECKPOINT while still in the connection context
      # This flushes WAL to disk before connection is returned to pool
      try:
        conn.execute("CHECKPOINT;")
        logger.info(f"Executed checkpoint for {graph_id} after COPY operation")
      except Exception as checkpoint_error:
        logger.warning(
          f"Failed to execute checkpoint for {graph_id}: {checkpoint_error}"
        )

    # Connection is now returned to pool
    # Database object stays alive in ConnectionPool._databases
    # Next query will get a fresh connection on the same Database object

    logger.info(
      f"LadybugDB ingestion completed: {records_loaded:,} records in {duration:.2f}s"
    )

    return {
      "records_loaded": records_loaded,
      "duration_seconds": duration,
      "query": query,
    }

  async def close(self) -> None:
    # Close all connections and database objects in the pool
    self.connection_pool.close_all()
    logger.info("Closed LadybugDB ConnectionPool")
