"""
DuckDB Staging Task for v2 Incremental Ingestion

Handles asynchronous staging of uploaded files into DuckDB tables
with file_id provenance tracking and SSE progress updates.

Key features:
- Async staging after file upload completion
- File-level provenance with file_id tracking
- SSE progress updates for client-side monitoring
- Uses Graph API client factory for ECS→EC2 routing
- Tracks multi-layer status: upload → duckdb → graph
"""

import asyncio
from typing import Dict, Any
from datetime import datetime, timezone

from celery import Task

from robosystems.celery import celery_app, QUEUE_DEFAULT
from robosystems.logger import logger
from robosystems.database import session
from robosystems.models.iam import GraphFile, GraphTable
from robosystems.graph_api.client.factory import GraphClientFactory
from robosystems.config import env


@celery_app.task(
  bind=True,
  queue=QUEUE_DEFAULT,
  name="table_operations.stage_file_in_duckdb",
  max_retries=3,
  default_retry_delay=60,
)
def stage_file_in_duckdb(
  self: Task,
  file_id: str,
  graph_id: str,
  table_id: str,
  operation_id: str | None = None,
  ingest_to_graph: bool = False,
) -> Dict[str, Any]:
  """
  Stage a single uploaded file in DuckDB with file_id provenance tracking.

  This task runs asynchronously after a file is uploaded to S3. It:
  1. Collects all uploaded files for the table
  2. Stages them in DuckDB with file_id tracking (UNION ALL pattern)
  3. Marks the file as DuckDB staged
  4. Emits SSE progress events
  5. Optionally chains to graph ingestion (if ingest_to_graph=True)

  Args:
      file_id: GraphFile.id of the uploaded file
      graph_id: Graph database identifier
      table_id: GraphTable.id
      operation_id: Optional operation ID for SSE tracking
      ingest_to_graph: If True, auto-chain to graph ingestion after staging (default: False)

  Returns:
      Dict with status, rows_staged, and timing info
  """
  start_time = datetime.now(timezone.utc)

  logger.info(
    f"Starting DuckDB staging for file {file_id} in graph {graph_id}, table {table_id}"
  )

  # Update task state for SSE
  if operation_id:
    self.update_state(
      state="PROGRESS",
      meta={
        "step": "initializing",
        "progress_percent": 0,
        "file_id": file_id,
        "graph_id": graph_id,
        "operation_id": operation_id,
        "message": "Preparing to stage file in DuckDB",
      },
    )

  try:
    # Get file and table info
    graph_file = GraphFile.get_by_id(file_id, session)
    if not graph_file:
      raise ValueError(f"File {file_id} not found")

    table = GraphTable.get_by_id(table_id, session)
    if not table:
      raise ValueError(f"Table {table_id} not found")

    # Get all uploaded files for this table
    all_files = GraphFile.get_all_for_table(table_id, session)
    uploaded_files = [f for f in all_files if f.upload_status == "uploaded"]

    if not uploaded_files:
      logger.warning(f"No uploaded files found for table {table.table_name}")
      return {
        "status": "skipped",
        "message": "No uploaded files to stage",
        "file_id": file_id,
      }

    # Update progress
    if operation_id:
      self.update_state(
        state="PROGRESS",
        meta={
          "step": "staging_duckdb",
          "progress_percent": 25,
          "file_id": file_id,
          "graph_id": graph_id,
          "operation_id": operation_id,
          "message": f"Staging {len(uploaded_files)} file(s) in DuckDB table {table.table_name}",
          "files_count": len(uploaded_files),
        },
      )

    # Build file list and file_id map (convert s3_key to full S3 URIs)
    bucket = env.AWS_S3_BUCKET
    s3_files = [f"s3://{bucket}/{f.s3_key}" for f in uploaded_files]
    file_id_map = {f"s3://{bucket}/{f.s3_key}": f.id for f in uploaded_files}

    logger.info(
      f"Staging {len(s3_files)} files in DuckDB table {table.table_name} with file_id tracking"
    )

    # Stage in DuckDB using Graph API client factory (ECS worker → EC2 Kuzu instance)
    try:
      client = asyncio.run(
        GraphClientFactory.create_client(graph_id=graph_id, operation_type="write")
      )

      result = asyncio.run(
        client.create_table(
          graph_id=graph_id,
          table_name=table.table_name,
          s3_pattern=s3_files,
          file_id_map=file_id_map,
        )
      )

      logger.info(
        f"Successfully staged {len(s3_files)} files in DuckDB table {table.table_name}"
      )

      # Update progress
      if operation_id:
        self.update_state(
          state="PROGRESS",
          meta={
            "step": "updating_metadata",
            "progress_percent": 75,
            "file_id": file_id,
            "graph_id": graph_id,
            "operation_id": operation_id,
            "message": "Updating file metadata",
            "duckdb_result": result,
          },
        )

      # Mark file as DuckDB staged
      graph_file.mark_duckdb_staged(
        session=session, row_count=graph_file.row_count or 0
      )

      execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

      logger.info(f"File {file_id} marked as DuckDB staged in {execution_time:.2f}s")

      # Chain to graph ingestion if requested
      if ingest_to_graph:
        from robosystems.tasks.table_operations.graph_materialization import (
          materialize_file_to_graph,
        )

        logger.info(
          f"Chaining to graph materialization for file {file_id} in table {table.table_name}"
        )

        try:
          materialize_file_to_graph.apply_async(  # type: ignore[attr-defined]
            args=[file_id, graph_id, table.table_name],
            priority=5,
          )
          logger.info(
            f"Graph ingestion task queued for file {file_id} (lower priority, background)"
          )
        except Exception as chain_err:
          logger.warning(
            f"Failed to chain graph ingestion task for file {file_id}: {chain_err}. "
            f"User can manually trigger ingestion later."
          )

      # Final progress update
      if operation_id:
        self.update_state(
          state="SUCCESS",
          meta={
            "step": "completed",
            "progress_percent": 100,
            "file_id": file_id,
            "graph_id": graph_id,
            "operation_id": operation_id,
            "message": f"File staged successfully in {execution_time:.2f}s",
            "execution_time_seconds": execution_time,
            "files_staged": len(s3_files),
            "table_name": table.table_name,
          },
        )

      return {
        "status": "success",
        "file_id": file_id,
        "graph_id": graph_id,
        "table_name": table.table_name,
        "files_staged": len(s3_files),
        "execution_time_seconds": execution_time,
        "duckdb_status": "staged",
      }

    except Exception as e:
      logger.error(f"Failed to stage files in DuckDB: {e}")
      raise

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

    logger.error(
      f"DuckDB staging failed for file {file_id} after {execution_time:.2f}s: {e}"
    )

    # Update task state for error
    if operation_id:
      self.update_state(
        state="FAILURE",
        meta={
          "step": "failed",
          "progress_percent": 0,
          "file_id": file_id,
          "graph_id": graph_id,
          "operation_id": operation_id,
          "message": f"Staging failed: {str(e)}",
          "error": str(e),
          "execution_time_seconds": execution_time,
        },
      )

    # Retry if retriable error
    raise self.retry(exc=e, countdown=self.default_retry_delay)
