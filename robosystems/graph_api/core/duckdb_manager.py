import re
from typing import Dict, List, Optional, Any, Union
from functools import wraps

from fastapi import HTTPException, status
from pydantic import BaseModel, Field, field_validator

from robosystems.logger import logger
from robosystems.graph_api.core.duckdb_pool import get_duckdb_pool


def validate_table_name(table_name: str) -> None:
  """
  Validate table name to prevent SQL injection.

  Only allows alphanumeric characters, underscores, and hyphens.
  This prevents SQL injection attacks through table name parameters.

  Args:
      table_name: Table name to validate

  Raises:
      HTTPException: If table name contains invalid characters
  """
  if not table_name or not re.match(r"^[a-zA-Z0-9_-]+$", table_name):
    logger.warning(f"Invalid table name rejected: {table_name[:50]}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid table name. Only alphanumeric, underscore, and hyphen allowed.",
    )


def validate_table_name_decorator(func):
  """
  Decorator to automatically validate table_name parameters.

  Ensures all table operations validate table names to prevent SQL injection.
  Works with both positional and keyword arguments.
  """

  @wraps(func)
  def wrapper(self, *args, **kwargs):
    # Extract table_name from kwargs
    if "table_name" in kwargs:
      validate_table_name(kwargs["table_name"])
    # Extract from request object if present
    elif "request" in kwargs and hasattr(kwargs["request"], "table_name"):
      validate_table_name(kwargs["request"].table_name)
    # Extract from positional args if method signature has table_name
    # (This is a fallback for positional arguments)
    elif len(args) > 0:
      # Check if first arg after self looks like a request object with table_name
      if hasattr(args[0], "table_name"):
        validate_table_name(args[0].table_name)

    return func(self, *args, **kwargs)

  return wrapper


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
  s3_pattern: Union[str, List[str]] = Field(
    ..., description="S3 glob pattern or list of S3 file paths"
  )

  @field_validator("s3_pattern")
  @classmethod
  def validate_s3_pattern(cls, v):
    """Validate s3_pattern is either a non-empty string or a non-empty list of strings."""
    if isinstance(v, str):
      if not v or not v.strip():
        raise ValueError("s3_pattern string cannot be empty")
      if not v.startswith("s3://"):
        raise ValueError("s3_pattern must start with s3://")
    elif isinstance(v, list):
      if not v:
        raise ValueError("s3_pattern list cannot be empty")
      if not all(isinstance(item, str) for item in v):
        raise ValueError("s3_pattern list must contain only strings")
      if not all(item.startswith("s3://") for item in v):
        raise ValueError("All s3_pattern list items must start with s3://")
    else:
      raise ValueError("s3_pattern must be either a string or a list of strings")
    return v

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
  parameters: Optional[List[Any]] = Field(
    default=None, description="Query parameters for safe value substitution"
  )

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

  def __init__(self):
    """
    Initialize DuckDB Table Manager.

    The connection pool manages all database paths automatically.
    """
    logger.info("Initialized DuckDB Table Manager (staging layer for Kuzu ingestion)")

  def _build_table_sql(
    self,
    quoted_table: str,
    has_identifier: bool,
    has_from_to: bool,
    use_list: bool,
  ) -> str:
    """
    Build CREATE TABLE SQL statement with deduplication logic.

    This helper eliminates code duplication between list and pattern branches.
    Uses DuckDB-compatible window functions instead of PostgreSQL's DISTINCT ON.

    Args:
        quoted_table: Quoted table name (e.g., '"table_name"')
        has_identifier: Whether table has 'identifier' column (node table)
        has_from_to: Whether table has 'from' and 'to' columns (relationship table)
        use_list: Whether using list (placeholder) or pattern (parameter binding)

    Returns:
        SQL statement with proper deduplication
    """
    # Use placeholder for list (will be replaced) or ? for parameter binding
    read_pattern = "__FILES_PLACEHOLDER__" if use_list else "?"

    if has_identifier:
      # Node table: deduplicate on identifier using window function
      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT * EXCLUDE (rn)
        FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY identifier ORDER BY identifier) AS rn
          FROM read_parquet({read_pattern}, hive_partitioning=false)
        )
        WHERE rn = 1
      """
    elif has_from_to:
      # Relationship table: deduplicate on (from, to) and rename to src/dst
      # IMPORTANT: Kuzu expects columns in order: src, dst, then properties
      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT
          "from" as src,
          "to" as dst,
          * EXCLUDE ("from", "to", rn)
        FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY "from", "to" ORDER BY "from", "to") AS rn
          FROM read_parquet({read_pattern}, hive_partitioning=false)
        )
        WHERE rn = 1
      """
    else:
      # Unknown table type: just read without deduplication
      return f"""
        CREATE OR REPLACE TABLE {quoted_table} AS
        SELECT *
        FROM read_parquet({read_pattern}, hive_partitioning=false)
      """

  @validate_table_name_decorator
  def create_table(self, request: TableCreateRequest) -> TableCreateResponse:
    """
    Create an external table (materialized from S3 files).

    DuckDB tables are materialized (not views) to enable Kuzu ingestion.

    CRITICAL: We use CREATE TABLE (not CREATE VIEW) because:
    - S3 credentials are session-level in DuckDB, not persisted in .duckdb file
    - When Kuzu's DuckDB extension attaches the database, it creates a new session
    - New session = no S3 config = views fail with "Unsupported duckdb type: NULL"
    - Materialized tables contain the actual data, so no S3 access needed during ingestion

    This is a staging layer for Kuzu ingestion - tables are dropped after use.

    Supports both:
    - s3_pattern as string: wildcard pattern (e.g., "s3://bucket/path/*.parquet")
    - s3_pattern as list: explicit file paths (uses DuckDB list syntax)
    """
    import time

    start_time = time.time()

    # Explicit validation (decorator provides safety net)
    validate_table_name(request.table_name)

    is_list = isinstance(request.s3_pattern, list)
    file_count = len(request.s3_pattern) if is_list else "pattern"

    logger.info(
      f"Creating external table {request.table_name} for graph {request.graph_id} "
      f"from {file_count} {'files' if is_list else ''}"
    )

    pool = get_duckdb_pool()

    try:
      with pool.get_connection(request.graph_id) as conn:
        quoted_table = f'"{request.table_name}"'

        # Determine if this is a node table (has identifier) or relationship table (has from/to)
        # Peek at the first file to check schema
        sample_file = request.s3_pattern[0] if is_list else request.s3_pattern

        # Check if 'identifier' column exists (node table) or 'from' column exists (relationship table)
        # Use hive_partitioning=false to prevent DuckDB from auto-adding partition columns
        probe_result = conn.execute(
          "SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0",
          [sample_file],
        ).description
        column_names = [col[0] for col in probe_result]
        has_identifier = "identifier" in column_names
        has_from_to = "from" in column_names and "to" in column_names

        # Build SQL using helper method
        sql = self._build_table_sql(quoted_table, has_identifier, has_from_to, is_list)

        if is_list:
          # Replace placeholder with DuckDB list syntax: ['file1', 'file2', ...]
          # Use single quotes for strings (DuckDB requirement)
          files_list = "[" + ", ".join(f"'{path}'" for path in request.s3_pattern) + "]"
          sql = sql.replace("__FILES_PLACEHOLDER__", files_list)
          conn.execute(sql)
        else:
          # Use parameter binding for pattern (prevents SQL injection)
          conn.execute(sql, [request.s3_pattern])

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(
          f"Created external table {request.table_name} for graph {request.graph_id} "
          f"in {execution_time_ms:.2f}ms ({file_count} {'files' if is_list else ''})"
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
        if request.parameters:
          result = conn.execute(request.sql, request.parameters).fetchall()
        else:
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
    Execute SQL query and yield results in chunks for TRUE streaming.

    IMPORTANT: Uses fetchmany() to avoid loading entire result set in memory.
    This enables efficient streaming of large result sets without memory exhaustion.

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
        # Execute query and get cursor (does NOT fetch all results)
        if request.parameters:
          cursor = conn.execute(request.sql, request.parameters)
        else:
          cursor = conn.execute(request.sql)
        description = cursor.description

        columns = [desc[0] for desc in description] if description else []

        chunk_index = 0
        total_rows_sent = 0

        # Fetch results in chunks to avoid memory exhaustion
        while True:
          chunk_rows_raw = cursor.fetchmany(chunk_size)

          if not chunk_rows_raw:
            break

          chunk_rows = [list(row) for row in chunk_rows_raw]
          total_rows_sent += len(chunk_rows)
          is_last_chunk = len(chunk_rows) < chunk_size

          chunk_data = {
            "columns": columns,
            "rows": chunk_rows,
            "chunk_index": chunk_index,
            "is_last_chunk": is_last_chunk,
            "row_count": len(chunk_rows),
            "total_rows_sent": total_rows_sent,
            "execution_time_ms": (time.time() - start_time) * 1000,
          }

          # Log only every 10th chunk or the last chunk to reduce overhead
          if chunk_index % 10 == 0 or is_last_chunk:
            logger.debug(
              f"Yielding chunk {chunk_index} with {len(chunk_rows)} rows "
              f"(total: {total_rows_sent})"
            )

          yield chunk_data
          chunk_index += 1

          if is_last_chunk:
            break

        execution_time_ms = (time.time() - start_time) * 1000
        logger.info(
          f"Streaming query completed: {total_rows_sent} rows in {execution_time_ms:.2f}ms"
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

  @validate_table_name_decorator
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
    from robosystems.database import SessionFactory
    from robosystems.models.iam.graph_table import GraphTable
    from robosystems.models.iam.graph_file import GraphFile

    validate_table_name(table_name)

    logger.info(f"Refreshing external table {table_name} for graph {graph_id}")

    pool = get_duckdb_pool()
    db = SessionFactory()

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

  @validate_table_name_decorator
  def delete_table(self, graph_id: str, table_name: str) -> Dict[str, str]:
    # Explicit validation (decorator provides safety net)
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
