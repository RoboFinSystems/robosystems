from fastapi import APIRouter, HTTPException, Depends, Body, Path
from fastapi import status as http_status
from pydantic import BaseModel, Field

from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.logger import logger
from robosystems.config import env

router = APIRouter(prefix="/databases/{graph_id}/tables")


class TableIngestRequest(BaseModel):
  ignore_errors: bool = Field(
    default=True, description="Continue ingestion on row errors"
  )

  class Config:
    extra = "forbid"


class TableIngestResponse(BaseModel):
  status: str = Field(..., description="Ingestion status")
  graph_id: str = Field(..., description="Graph database identifier")
  table_name: str = Field(..., description="Table name")
  rows_ingested: int = Field(..., description="Number of rows ingested")
  execution_time_ms: float = Field(..., description="Ingestion time in milliseconds")


@router.post("/{table_name}/ingest", response_model=TableIngestResponse)
async def ingest_table_to_graph(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name to ingest from DuckDB"),
  request: TableIngestRequest = Body(...),
  cluster_service=Depends(get_cluster_service),
) -> TableIngestResponse:
  import time
  from pathlib import Path as PathLib

  start_time = time.time()

  logger.info(f"Ingesting table {table_name} from DuckDB to Kuzu graph {graph_id}")

  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Ingestion not allowed on read-only nodes",
    )

  try:
    duck_path = f"{env.DUCKDB_STAGING_PATH}/{graph_id}.duckdb"
    duckdb_extension_path = (
      PathLib.home() / ".kuzu" / "extension" / "duckdb" / "libduckdb.kuzu_extension"
    )

    # CRITICAL: Checkpoint DuckDB to flush WAL to main database BEFORE Kuzu attaches
    # Kuzu's DuckDB extension creates a new session that won't see uncommitted WAL data
    logger.info(f"Checkpointing DuckDB database before Kuzu ingestion: {duck_path}")
    from robosystems.graph_api.core.duckdb_pool import get_duckdb_pool

    duckdb_pool = get_duckdb_pool()
    try:
      with duckdb_pool.get_connection(graph_id) as duck_conn:
        duck_conn.execute("CHECKPOINT")
        logger.info(f"✅ DuckDB checkpointed successfully for {graph_id}")
    except Exception as cp_err:
      logger.warning(f"Could not checkpoint DuckDB before ingestion: {cp_err}")

    with cluster_service.db_manager.connection_pool.get_connection(graph_id) as conn:
      try:
        conn.execute(f"LOAD EXTENSION '{duckdb_extension_path}'")
        logger.info(f"Loaded DuckDB extension from {duckdb_extension_path}")
      except Exception as e:
        if "already loaded" not in str(e).lower():
          logger.warning(f"Failed to load DuckDB extension: {e}")
          raise

      # Detach first if already attached (prevents duplicate attachment errors)
      try:
        conn.execute("DETACH duck")
      except Exception:
        pass

      # Attach DuckDB database (Kuzu syntax: ATTACH 'path' AS alias (DBTYPE duckdb))
      # Data is already materialized in DuckDB tables (not views), so no S3 access needed
      conn.execute(f"ATTACH '{duck_path}' AS duck (DBTYPE duckdb)")

      # IMPORTANT: Kuzu COPY command does NOT accept quoted table names anywhere
      # Both the target table and source reference must be unquoted
      # ignore_errors syntax: (ignore_errors=true) with parentheses, lowercase

      # NOTE: Partition columns (like 'year') are excluded when creating DuckDB tables
      # in duckdb_manager.py, so the schemas should match exactly now

      # Both node and relationship tables use the same COPY syntax when columns match
      # For relationships, Kuzu automatically maps 'src' and 'dst' columns to relationship endpoints
      if request.ignore_errors:
        copy_query = f"COPY {table_name} FROM duck.{table_name} (ignore_errors=true)"
      else:
        copy_query = f"COPY {table_name} FROM duck.{table_name}"

      logger.info(f"Executing copy from DuckDB to Kuzu: {table_name}")
      result = conn.execute(copy_query)

      rows_ingested = 0
      if result and hasattr(result, "get_as_arrow"):
        arrow_table = result.get_as_arrow()
        if arrow_table.num_rows > 0 and arrow_table.num_columns > 0:
          result_msg = str(arrow_table.column(0)[0].as_py())
          import re

          match = re.search(r"(\d+)\s+tuples?", result_msg)
          if match:
            rows_ingested = int(match.group(1))

    execution_time_ms = (time.time() - start_time) * 1000

    logger.info(
      f"Ingested {rows_ingested} rows from {table_name} in {execution_time_ms:.2f}ms"
    )

    return TableIngestResponse(
      status="success",
      graph_id=graph_id,
      table_name=table_name,
      rows_ingested=rows_ingested,
      execution_time_ms=execution_time_ms,
    )

  except Exception as e:
    logger.error(f"Failed to ingest table {table_name}: {e}")

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to ingest table: {str(e)}",
    )
