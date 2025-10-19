from typing import Dict, Any, List, Optional
from pathlib import Path
import shutil
import time

from robosystems.logger import logger
from robosystems.graph_api.core.connection_pool import get_connection_pool
from .base import GraphBackend, DatabaseInfo, ClusterTopology, S3IngestionError


class KuzuBackend(GraphBackend):
  def __init__(self, data_path: str = "/data/kuzu-dbs"):
    self.data_path = Path(data_path)
    self.data_path.mkdir(parents=True, exist_ok=True)

    # Use the GLOBAL ConnectionPool singleton initialized by database_manager
    # This ensures all operations (queries and ingestion) share the same Database objects
    self.connection_pool = get_connection_pool()
    logger.info(f"Initialized KuzuBackend using global ConnectionPool at {data_path}")

  async def execute_query(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    # Use connection pool's get_connection context manager
    with self.connection_pool.get_connection(graph_id, read_only=False) as conn:
      try:
        result = conn.execute(cypher)

        # Convert Kuzu result to list of dicts
        rows = []
        while result.has_next():
          row = result.get_next()
          rows.append(row)

        return rows
      except Exception as e:
        logger.error(f"Query execution failed for {graph_id}: {e}")
        raise

  async def execute_write(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    # For Kuzu, write operations use the same pattern as queries
    return await self.execute_query(graph_id, cypher, parameters, database)

  async def create_database(self, database_name: str) -> bool:
    # Simply getting a connection will create the database if it doesn't exist
    with self.connection_pool.get_connection(database_name, read_only=False) as conn:
      # Test connection
      result = conn.execute("RETURN 1 as test")
      result.close()
    logger.info(f"Created Kuzu database for {database_name}")
    return True

  async def delete_database(self, database_name: str) -> bool:
    # First, clean up any pooled connections and Database objects
    self.connection_pool.force_database_cleanup(database_name, aggressive=True)

    # Then delete the physical files
    db_path = self.data_path / f"{database_name}.kuzu"
    if db_path.exists():
      if db_path.is_file():
        db_path.unlink()
      else:
        shutil.rmtree(db_path)
      logger.info(f"Deleted Kuzu database for {database_name}")
    return True

  async def list_databases(self) -> List[str]:
    if not self.data_path.exists():
      return []
    return [f.stem for f in self.data_path.iterdir() if f.suffix == ".kuzu"]

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

    db_path = self.data_path / f"{database_name}.kuzu"
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
    return ClusterTopology(mode="embedded", leader={"backend": "kuzu"})

  async def health_check(self) -> bool:
    return True

  async def ingest_from_s3(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str,
    s3_credentials: Optional[Dict[str, Any]] = None,
    ignore_errors: bool = True,
    database: Optional[str] = None,
  ) -> Dict[str, Any]:
    # Use ConnectionPool's context manager - this is the 1.0.1 pattern
    with self.connection_pool.get_connection(graph_id, read_only=False) as conn:
      # Load httpfs extension
      try:
        result = conn.execute("CALL show_loaded_extensions() RETURN *")
        loaded_extensions = []
        while result.has_next():
          loaded_extensions.append(str(result.get_next()).lower())

        if "httpfs" not in loaded_extensions:
          try:
            conn.execute("INSTALL httpfs")
            logger.debug("Installed httpfs extension for S3 support")
          except Exception as e:
            logger.debug(f"Could not install httpfs (may already be installed): {e}")

          conn.execute("LOAD httpfs")
          logger.debug("Loaded httpfs extension")
        else:
          logger.debug("httpfs extension already loaded")
      except Exception as e:
        if "already loaded" not in str(e).lower():
          logger.warning(f"Could not load httpfs extension: {e}")
          try:
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")
            logger.debug("Successfully loaded httpfs on retry")
          except Exception as retry_error:
            logger.error(f"Failed to load httpfs after retry: {retry_error}")
            raise S3IngestionError(
              f"httpfs extension required for S3 access: {retry_error}"
            )

      # Configure S3 credentials
      if s3_credentials:
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

        conn.execute("CALL s3_uploader_threads_limit = 8")
        conn.execute("CALL s3_uploader_max_num_parts_per_file = 10000")
        conn.execute("CALL s3_uploader_max_filesize = 10737418240")
        conn.execute("CALL spill_to_disk = true")
        conn.execute("CALL timeout=1800000")

        logger.debug(
          "Configured S3 performance settings, memory management, and 30-minute timeout for bulk ingestion"
        )

      # Build and execute COPY query
      query = f'COPY {table_name} FROM "{s3_pattern}"'
      if ignore_errors:
        if "(" in query:
          query = query[:-1] + ", IGNORE_ERRORS=TRUE)"
        else:
          query += " (IGNORE_ERRORS=TRUE)"

      logger.info(f"Executing Kuzu COPY: {query}")

      start_time = time.time()
      result = conn.execute(query)
      duration = time.time() - start_time

      records_loaded = 0
      if result and hasattr(result, "get_as_list"):
        result_list = result.get_as_list()
        if result_list and len(result_list) > 0:
          result_str = str(result_list[0])
          if "Records loaded:" in result_str:
            try:
              records_loaded = int(
                result_str.split("Records loaded:")[-1].strip().split()[0]
              )
            except (ValueError, IndexError):
              pass

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
      f"Kuzu ingestion completed: {records_loaded:,} records in {duration:.2f}s"
    )

    return {
      "records_loaded": records_loaded,
      "duration_seconds": duration,
      "query": query,
    }

  async def close(self) -> None:
    # Close all connections and database objects in the pool
    self.connection_pool.close_all()
    logger.info("Closed Kuzu ConnectionPool")
