from typing import Dict, Any, List, Optional
from pathlib import Path
import shutil

from robosystems.middleware.graph.engine import Engine
from robosystems.logger import logger
from .base import GraphBackend, DatabaseInfo, ClusterTopology, S3IngestionError


class KuzuBackend(GraphBackend):
  def __init__(self, data_path: str = "/data/kuzu-dbs"):
    self.data_path = data_path
    self._engines: Dict[str, Engine] = {}
    Path(data_path).mkdir(parents=True, exist_ok=True)

  def _get_engine(self, graph_id: str) -> Engine:
    if graph_id not in self._engines:
      database_path = f"{self.data_path}/{graph_id}.kuzu"
      self._engines[graph_id] = Engine(database_path)
      logger.debug(f"Created Kuzu engine for {graph_id}: {database_path}")
    return self._engines[graph_id]

  async def execute_query(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    engine = self._get_engine(graph_id)
    return engine.execute_query(cypher, parameters)

  async def execute_write(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    engine = self._get_engine(graph_id)
    return engine.execute_query(cypher, parameters)

  async def create_database(self, database_name: str) -> bool:
    self._get_engine(database_name)
    logger.info(f"Created Kuzu database for {database_name}")
    return True

  async def delete_database(self, database_name: str) -> bool:
    if database_name in self._engines:
      self._engines[database_name].close()
      del self._engines[database_name]

    db_path = Path(f"{self.data_path}/{database_name}.kuzu")
    if db_path.exists():
      if db_path.is_file():
        db_path.unlink()
      else:
        shutil.rmtree(db_path)
      logger.info(f"Deleted Kuzu database for {database_name}")
    return True

  async def list_databases(self) -> List[str]:
    db_path = Path(self.data_path)
    if not db_path.exists():
      return []
    return [f.stem for f in db_path.iterdir() if f.suffix == ".kuzu"]

  async def get_database_info(self, database_name: str) -> DatabaseInfo:
    engine = self._get_engine(database_name)

    node_count = 0
    relationship_count = 0

    try:
      node_result = engine.execute_single("MATCH (n) RETURN count(n) as count")
      if node_result:
        node_count = node_result.get("count", 0)

      rel_result = engine.execute_single("MATCH ()-[r]->() RETURN count(r) as count")
      if rel_result:
        relationship_count = rel_result.get("count", 0)
    except Exception as e:
      logger.warning(f"Failed to get database stats for {database_name}: {e}")

    db_path = Path(f"{self.data_path}/{database_name}.kuzu")
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
    import time

    engine = self._get_engine(graph_id)
    conn = engine.conn

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

    try:
      conn.execute("CHECKPOINT;")
      logger.debug("Executed checkpoint to flush WAL to disk")
    except Exception as checkpoint_error:
      logger.warning(f"Failed to execute checkpoint: {checkpoint_error}")

    # CRITICAL: Force cleanup after ingestion to ensure data is immediately queryable
    # This closes the Database object and removes it from cache, forcing the next
    # query to create a fresh connection that sees all committed data
    self.force_cleanup(graph_id)

    logger.info(
      f"Kuzu ingestion completed: {records_loaded:,} records in {duration:.2f}s"
    )

    return {
      "records_loaded": records_loaded,
      "duration_seconds": duration,
      "query": query,
    }

  def force_cleanup(self, graph_id: str, aggressive: bool = True) -> None:
    """
    Force cleanup of all connections and database object for a specific database.

    This is critical after ingestion operations to ensure:
    1. All data is flushed to disk via checkpoint
    2. Database object is closed to release buffer pool memory
    3. Next query creates fresh connection that sees committed data

    Args:
        graph_id: The graph database to cleanup
        aggressive: If True, use aggressive memory cleanup (garbage collection, malloc_trim)
    """
    if graph_id in self._engines:
      engine = self._engines[graph_id]

      # Close the engine (closes both connection and database)
      engine.close()

      # Remove from cache
      del self._engines[graph_id]

      logger.info(f"Force cleanup completed for {graph_id} - database object removed from cache")

    if aggressive:
      # Aggressive memory cleanup matching v1.0.1 behavior
      import gc

      # Force multiple rounds of garbage collection
      for generation in range(3):
        collected = gc.collect(generation)
        logger.debug(f"GC generation {generation}: collected {collected} objects")

      logger.info(f"Completed aggressive cleanup for {graph_id}")

  async def close(self) -> None:
    for engine in self._engines.values():
      engine.close()
    self._engines.clear()
