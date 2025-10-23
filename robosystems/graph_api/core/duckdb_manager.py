import re
from typing import Dict, List, Optional, Any

from fastapi import HTTPException, status
from pydantic import BaseModel, Field

from robosystems.logger import logger
from robosystems.graph_api.core.duckdb_pool import get_duckdb_pool


def validate_table_name(table_name: str) -> None:
  if not table_name or not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid table name"
    )


class TableInfo(BaseModel):
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  row_count: int = Field(..., description="Approximate row count")
  size_bytes: int = Field(..., description="Table size in bytes")
  s3_location: Optional[str] = Field(
    None, description="S3 location for external tables"
  )


class TableCreateRequest(BaseModel):
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  s3_pattern: str = Field(..., description="S3 glob pattern for parquet files")

  class Config:
    extra = "forbid"


class TableCreateResponse(BaseModel):
  status: str = Field(..., description="Creation status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  execution_time_ms: float = Field(..., description="Creation time in milliseconds")


class TableQueryRequest(BaseModel):
  graph_id: str = Field(..., description="Graph database identifier")
  sql: str = Field(..., description="SQL query to execute")

  class Config:
    extra = "forbid"


class TableQueryResponse(BaseModel):
  columns: List[str] = Field(..., description="Column names")
  rows: List[List[Any]] = Field(..., description="Query results")
  row_count: int = Field(..., description="Number of rows returned")
  execution_time_ms: float = Field(..., description="Query execution time")


class DuckDBTableManager:
  """
  DuckDB table manager for staging tables.

  All tables are external views over S3 - zero local storage, used only
  for transformation/staging before ingestion into Kuzu graph database.
  """

  def __init__(self, base_path: str = None):
    """
    Initialize DuckDB Table Manager.

    Args:
        base_path: Deprecated - pool manages paths. Kept for backward compatibility.
    """
    if base_path:
      logger.warning(
        "DuckDBTableManager base_path parameter is deprecated - pool manages paths"
      )
    logger.info("Initialized DuckDB Table Manager (staging layer for Kuzu ingestion)")

  def create_table(self, request: TableCreateRequest) -> TableCreateResponse:
    """
    Create an external table (view over S3 files).

    All DuckDB tables are external views - they query S3 directly without
    storing data locally. This is purely a staging layer for Kuzu ingestion.
    """
    import time

    start_time = time.time()

    validate_table_name(request.table_name)

    logger.info(
      f"Creating external table {request.table_name} for graph {request.graph_id} from {request.s3_pattern}"
    )

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(request.graph_id) as conn:
        quoted_table = f'"{request.table_name}"'

        sql = f"CREATE OR REPLACE VIEW {quoted_table} AS SELECT * FROM read_parquet(?)"
        conn.execute(sql, [request.s3_pattern])

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(
          f"Created external table {request.table_name} for graph {request.graph_id} in {execution_time_ms:.2f}ms"
        )

        return TableCreateResponse(
          status="success",
          graph_id=request.graph_id,
          table_name=request.table_name,
          execution_time_ms=execution_time_ms,
        )

    except Exception as e:
      logger.error(f"Failed to create table {request.table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to create table: {str(e)}",
      )

  def query_table(self, request: TableQueryRequest) -> TableQueryResponse:
    import time

    start_time = time.time()

    logger.info(f"Executing query for graph {request.graph_id}: {request.sql[:100]}...")

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(request.graph_id) as conn:
        result = conn.execute(request.sql).fetchall()
        description = conn.description

        columns = [desc[0] for desc in description] if description else []
        rows = [list(row) for row in result]

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(f"Query returned {len(rows)} rows in {execution_time_ms:.2f}ms")

        return TableQueryResponse(
          columns=columns,
          rows=rows,
          row_count=len(rows),
          execution_time_ms=execution_time_ms,
        )

    except Exception as e:
      logger.error(f"Query failed for graph {request.graph_id}: {e}")
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Query failed: {str(e)}",
      )

  def query_table_streaming(self, request: TableQueryRequest, chunk_size: int = 1000):
    """
    Execute SQL query and yield results in chunks for streaming.

    Args:
        request: Query request
        chunk_size: Number of rows per chunk

    Yields:
        Dict containing chunk data with columns, rows, and metadata
    """
    import time

    start_time = time.time()

    logger.info(
      f"Executing streaming query for graph {request.graph_id}: {request.sql[:100]}..."
    )

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(request.graph_id) as conn:
        result = conn.execute(request.sql).fetchall()
        description = conn.description

        columns = [desc[0] for desc in description] if description else []

        total_rows = len(result)
        chunk_index = 0

        for i in range(0, total_rows, chunk_size):
          chunk_rows = [list(row) for row in result[i : i + chunk_size]]
          is_last_chunk = (i + chunk_size) >= total_rows

          chunk_data = {
            "columns": columns,
            "rows": chunk_rows,
            "chunk_index": chunk_index,
            "is_last_chunk": is_last_chunk,
            "row_count": len(chunk_rows),
            "total_rows_sent": min(i + chunk_size, total_rows),
            "total_rows": total_rows,
            "execution_time_ms": (time.time() - start_time) * 1000,
          }

          logger.debug(
            f"Yielding chunk {chunk_index} with {len(chunk_rows)} rows (total: {chunk_data['total_rows_sent']}/{total_rows})"
          )

          yield chunk_data
          chunk_index += 1

        execution_time_ms = (time.time() - start_time) * 1000
        logger.info(
          f"Streaming query completed: {total_rows} rows in {execution_time_ms:.2f}ms"
        )

    except Exception as e:
      logger.error(f"Streaming query failed for graph {request.graph_id}: {e}")
      yield {
        "error": str(e),
        "error_type": type(e).__name__,
        "chunk_index": 0,
        "is_last_chunk": True,
        "row_count": 0,
        "total_rows_sent": 0,
        "execution_time_ms": (time.time() - start_time) * 1000,
      }

  def list_tables(self, graph_id: str) -> List[TableInfo]:
    logger.info(f"Listing tables for graph {graph_id}")

    pool = get_duckdb_pool()

    # Check if database exists by looking for connections or trying to connect
    try:
      with pool.get_connection(graph_id) as conn:
        result = conn.execute(
          "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()

        tables = []
        for (table_name,) in result:
          try:
            quoted_table = f'"{table_name}"'
            count_result = conn.execute(
              f"SELECT COUNT(*) FROM {quoted_table}"
            ).fetchone()
            row_count = count_result[0] if count_result else 0

            tables.append(
              TableInfo(
                graph_id=graph_id,
                table_name=table_name,
                row_count=row_count,
                size_bytes=0,
                s3_location=None,
              )
            )
          except Exception as e:
            logger.warning(f"Could not get info for table {table_name}: {e}")

        return tables

    except Exception as e:
      logger.error(f"Failed to list tables for graph {graph_id}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to list tables: {str(e)}",
      )

  def refresh_table(self, graph_id: str, table_name: str) -> Dict[str, Any]:
    """
    Refresh an external table from current PostgreSQL file registry.

    Rebuilds the table using the current list of files in GraphFile table.
    Use this after file additions, deletions, or replacements in S3.

    Args:
        graph_id: Graph database identifier
        table_name: Table name to refresh

    Returns:
        Dict with refresh details

    Raises:
        HTTPException: If table doesn't exist
    """
    import time
    from robosystems.database import SessionLocal
    from robosystems.models.iam.graph_table import GraphTable
    from robosystems.models.iam.graph_file import GraphFile

    validate_table_name(table_name)

    logger.info(f"Refreshing external table {table_name} for graph {graph_id}")

    pool = get_duckdb_pool()
    db = SessionLocal()

    try:
      graph_table = GraphTable.get_by_name(graph_id, table_name, db)
      if not graph_table:
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"Table {table_name} not found in registry",
        )

      files = GraphFile.get_all_for_table(graph_table.id, db)
      if not files:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"No files found for table {table_name}",
        )

      s3_keys = [f.s3_key for f in files if f.upload_status == "completed"]
      if not s3_keys:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"No completed files found for table {table_name}",
        )

      logger.info(
        f"Refreshing {table_name} with {len(s3_keys)} files from PostgreSQL registry"
      )

      with pool.get_connection(graph_id) as conn:
        quoted_table = f'"{table_name}"'
        start_time = time.time()

        conn.execute(f"DROP VIEW IF EXISTS {quoted_table}")
        conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")

        s3_pattern_list = ", ".join([f"'{key}'" for key in s3_keys])
        create_view_sql = f"CREATE VIEW {quoted_table} AS SELECT * FROM read_parquet([{s3_pattern_list}])"
        conn.execute(create_view_sql)
        logger.info(f"Recreated external table {table_name} with {len(s3_keys)} files")

        execution_time_ms = (time.time() - start_time) * 1000

        count_result = conn.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        row_count = count_result[0] if count_result else 0

        return {
          "status": "success",
          "graph_id": graph_id,
          "table_name": table_name,
          "file_count": len(s3_keys),
          "row_count": row_count,
          "execution_time_ms": execution_time_ms,
          "message": f"Refreshed external table {table_name} from {len(s3_keys)} files",
        }

    except HTTPException:
      raise
    except Exception as e:
      logger.error(f"Failed to refresh table {table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to refresh table: {str(e)}",
      )
    finally:
      db.close()

  def delete_table(self, graph_id: str, table_name: str) -> Dict[str, str]:
    validate_table_name(table_name)

    logger.info(f"Deleting table {table_name} from graph {graph_id}")

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(graph_id) as conn:
        quoted_table = f'"{table_name}"'
        conn.execute(f"DROP TABLE IF EXISTS {quoted_table}")
        conn.execute(f"DROP VIEW IF EXISTS {quoted_table}")

        logger.info(f"Deleted table {table_name} from graph {graph_id}")

        return {"status": "success", "message": f"Table {table_name} deleted"}

    except Exception as e:
      logger.error(f"Failed to delete table {table_name}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to delete table: {str(e)}",
      )
