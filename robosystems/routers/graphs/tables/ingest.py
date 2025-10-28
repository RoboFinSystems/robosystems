from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable, UserGraph, GraphFile
from robosystems.models.api.table import (
  BulkIngestRequest,
  BulkIngestResponse,
  TableIngestResult,
)
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.graph.types import GraphTypeRegistry
from robosystems.config import env
import time

router = APIRouter()


@router.post(
  "/tables/ingest",
  response_model=BulkIngestResponse,
  operation_id="ingestTables",
  summary="Ingest Tables to Graph",
  description="Load all files from S3 into DuckDB staging tables and ingest into Kuzu graph database. "
  "Use rebuild=true to regenerate the entire graph from scratch (safe operation - S3 is source of truth).",
  responses={
    200: {"description": "Ingestion completed"},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
    500: {"description": "Ingestion failed", "model": ErrorResponse},
  },
)
async def ingest_tables(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: BulkIngestRequest = Body(..., description="Ingestion request"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> BulkIngestResponse:
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories are read-only. File uploads and data ingestion are not allowed. "
      "Shared repositories provide reference data that cannot be modified.",
    )

  repo = await get_universal_repository_with_auth(graph_id, current_user, "write", db)

  if not repo:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  start_time = time.time()
  logger.info(f"Starting table ingestion for graph {graph_id}")

  from robosystems.graph_api.client.factory import get_graph_client

  client = await get_graph_client(graph_id=graph_id, operation_type="write")

  tables = GraphTable.get_all_for_graph(graph_id, db)
  results: list[TableIngestResult] = []
  total_rows_ingested = 0
  successful_tables = 0
  failed_tables = 0
  skipped_tables = 0
  first_table = True

  user_graph = UserGraph.get_by_user_and_graph(current_user.id, graph_id, db)
  if not user_graph:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"User does not have access to graph {graph_id}",
    )

  bucket = env.AWS_S3_BUCKET

  for table in tables:
    table_start = time.time()

    all_table_files = GraphFile.get_all_for_table(table.id, db)
    uploaded_files = [f for f in all_table_files if f.upload_status == "uploaded"]

    if not uploaded_files:
      logger.info(
        f"Skipping table {table.table_name} - no files with 'uploaded' status (found {len(all_table_files)} files total)"
      )
      results.append(
        TableIngestResult(
          table_name=table.table_name,
          status="skipped",
          rows_ingested=0,
          execution_time_ms=0,
          error="No files with 'uploaded' status",
        )
      )
      skipped_tables += 1
      continue

    try:
      s3_pattern = f"s3://{bucket}/user-staging/{current_user.id}/{graph_id}/{table.table_name}/**/*.parquet"

      logger.info(
        f"Creating/updating DuckDB staging table: {table.table_name} with pattern: {s3_pattern}"
      )

      await client.create_table(
        graph_id=graph_id, table_name=table.table_name, s3_pattern=s3_pattern
      )

      logger.info(f"Ingesting table {table.table_name} from DuckDB to Kuzu")

      response = await client.ingest_table_to_graph(
        graph_id=graph_id,
        table_name=table.table_name,
        ignore_errors=request.ignore_errors,
        rebuild=request.rebuild and first_table,
      )

      table_execution_ms = (time.time() - table_start) * 1000
      rows = response.get("rows_ingested", 0)
      total_rows_ingested += rows
      successful_tables += 1

      results.append(
        TableIngestResult(
          table_name=table.table_name,
          status="success",
          rows_ingested=rows,
          execution_time_ms=table_execution_ms,
          error=None,
        )
      )

      logger.info(
        f"Successfully ingested {table.table_name}: {rows} rows in {table_execution_ms:.2f}ms"
      )

      first_table = False

    except Exception as e:
      table_execution_ms = (time.time() - table_start) * 1000
      failed_tables += 1
      error_msg = str(e)

      results.append(
        TableIngestResult(
          table_name=table.table_name,
          status="failed",
          rows_ingested=0,
          execution_time_ms=table_execution_ms,
          error=error_msg,
        )
      )

      logger.error(f"Failed to ingest table {table.table_name}: {error_msg}")

  total_execution_ms = (time.time() - start_time) * 1000
  overall_status = (
    "success"
    if failed_tables == 0
    else "partial"
    if successful_tables > 0
    else "failed"
  )

  logger.info(
    f"Table ingestion completed for graph {graph_id}: "
    f"{successful_tables} successful, {failed_tables} failed, {skipped_tables} skipped, "
    f"{total_rows_ingested} total rows in {total_execution_ms:.2f}ms"
  )

  return BulkIngestResponse(
    status=overall_status,
    graph_id=graph_id,
    total_tables=len(tables),
    successful_tables=successful_tables,
    failed_tables=failed_tables,
    skipped_tables=skipped_tables,
    total_rows_ingested=total_rows_ingested,
    total_execution_time_ms=total_execution_ms,
    results=results,
  )
