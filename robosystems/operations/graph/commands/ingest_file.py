"""Ingest-file command — mark an uploaded file + trigger DuckDB staging.

Extracted verbatim from the old ``PATCH /files/{id}`` status=uploaded router
branch; called by the ``ingest-file`` content-op handler. Returns a dict with
``status`` and (for async staging) ``operation_id`` so the caller wraps it in an
OperationEnvelope.
"""

from __future__ import annotations

import uuid

from fastapi import BackgroundTasks, HTTPException, status
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.constants import (
  FALLBACK_BYTES_PER_ROW_CSV,
  FALLBACK_BYTES_PER_ROW_JSON,
  FALLBACK_BYTES_PER_ROW_PARQUET,
  MAX_FILE_SIZE_MB,
  SMALL_FILE_STAGING_THRESHOLD_MB,
)
from robosystems.config.graph_tier import GraphTierConfig
from robosystems.logger import logger
from robosystems.models.api.graphs.tables import FileUploadStatus
from robosystems.models.core import Graph, GraphFile, GraphTable, User
from robosystems.operations.aws.s3 import S3Client

__all__ = ["ingest_file_cmd"]


async def ingest_file_cmd(
  graph_id: str,
  file_id: str,
  ingest_to_graph: bool,
  current_user: User,
  db: Session,
  background_tasks: BackgroundTasks,
) -> dict:
  """Mark the file uploaded and trigger DuckDB staging (direct or Dagster)."""
  graph_file = GraphFile.get_by_id(file_id, db)
  if not graph_file or graph_file.graph_id != graph_id:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND, detail=f"File {file_id} not found"
    )

  s3_client = S3Client()
  bucket = env.USER_DATA_BUCKET

  try:
    head_response = s3_client.s3_client.head_object(
      Bucket=bucket, Key=graph_file.s3_key
    )
    actual_file_size = head_response["ContentLength"]
  except Exception as e:
    logger.error(f"Failed to get file size from S3: {e}")
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"File not found in S3: {graph_file.s3_key}",
    )

  if actual_file_size <= 0:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="File is empty",
    )

  max_file_size_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
  if actual_file_size > max_file_size_bytes:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"File size {actual_file_size / (1024 * 1024):.2f} MB exceeds maximum of {MAX_FILE_SIZE_MB} MB",
    )

  graph = Graph.get_by_id(graph_id, db)
  if graph:
    storage_limit_gb = GraphTierConfig.get_instance_storage_limit_gb(
      str(graph.graph_tier)
    )
    storage_limit_bytes = storage_limit_gb * 1024 * 1024 * 1024

    all_tables = GraphTable.get_all_for_graph(graph_id, db)
    current_storage_bytes = sum(t.total_size_bytes or 0 for t in all_tables)

    if current_storage_bytes + actual_file_size > storage_limit_bytes:
      raise HTTPException(
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        detail=f"Storage limit exceeded. Current: {current_storage_bytes / (1024 * 1024 * 1024):.2f} GB, "
        f"Limit: {storage_limit_gb} GB, "
        f"Attempted upload: {actual_file_size / (1024 * 1024 * 1024):.2f} GB",
      )

  actual_row_count = None
  try:
    file_obj = s3_client.s3_client.get_object(Bucket=bucket, Key=graph_file.s3_key)
    file_content = file_obj["Body"].read()

    file_format = str(graph_file.file_format)
    if file_format == "parquet":
      from io import BytesIO

      import pyarrow.parquet as pq

      parquet_file = pq.read_table(BytesIO(file_content))
      actual_row_count = parquet_file.num_rows
    elif file_format == "csv":
      import csv
      from io import StringIO

      csv_content = file_content.decode("utf-8")
      reader = csv.reader(StringIO(csv_content))
      actual_row_count = sum(1 for _ in reader) - 1
    elif graph_file.file_format == "json":
      import json

      json_data = json.loads(file_content)
      if isinstance(json_data, list):
        actual_row_count = len(json_data)
      else:
        actual_row_count = 1

    logger.info(f"Calculated row count for {graph_file.file_name}: {actual_row_count}")
  except Exception as e:
    logger.warning(
      f"Could not calculate row count for {graph_file.file_name}: {e}. Row count will be estimated."
    )
    if graph_file.file_format == "parquet":
      actual_row_count = actual_file_size // FALLBACK_BYTES_PER_ROW_PARQUET
    elif graph_file.file_format == "csv":
      actual_row_count = actual_file_size // FALLBACK_BYTES_PER_ROW_CSV
    elif graph_file.file_format == "json":
      actual_row_count = actual_file_size // FALLBACK_BYTES_PER_ROW_JSON
    else:
      actual_row_count = actual_file_size // FALLBACK_BYTES_PER_ROW_CSV
    logger.info(
      f"Estimated row count for {graph_file.file_name} ({graph_file.file_format}): {actual_row_count}"
    )

  graph_file.file_size_bytes = actual_file_size
  graph_file.row_count = actual_row_count
  graph_file.upload_status = FileUploadStatus.UPLOADED.value
  db.commit()
  db.refresh(graph_file)

  table = (
    db.query(GraphTable)
    .filter(GraphTable.id == graph_file.table_id)
    .with_for_update()
    .first()
  )
  if table:
    all_files = GraphFile.get_all_for_table(table.id, db)
    uploaded_files = [
      f for f in all_files if f.upload_status == FileUploadStatus.UPLOADED.value
    ]

    new_file_count = len(uploaded_files)

    table.update_stats(
      session=db,
      file_count=new_file_count,
      total_size_bytes=sum(f.file_size_bytes for f in uploaded_files),
      row_count=sum(f.row_count for f in uploaded_files if f.row_count is not None),
    )

    if new_file_count > 0:
      # Size-based routing: small files use direct staging, large files use Dagster
      small_file_threshold_bytes = SMALL_FILE_STAGING_THRESHOLD_MB * 1024 * 1024

      if actual_file_size < small_file_threshold_bytes:
        # Fast path: Direct staging for small files
        from robosystems.operations.graph.engine.direct_staging import (
          stage_file_directly,
        )

        logger.info(
          f"Small file detected ({actual_file_size / (1024 * 1024):.2f} MB < {SMALL_FILE_STAGING_THRESHOLD_MB} MB). "
          f"Using direct staging for file {file_id}"
        )

        try:
          staging_result = await stage_file_directly(
            db=db,
            file_id=file_id,
            graph_id=graph_id,
            table_id=str(table.id),
            s3_key=graph_file.s3_key,
            file_size_bytes=actual_file_size,
            row_count=actual_row_count,
          )

          if staging_result.get("status") == "success":
            graph_file.duckdb_status = "staged"
            db.commit()
            db.refresh(graph_file)

            logger.info(
              f"Direct staging completed for file {file_id} in {staging_result.get('duration_ms', 0):.2f}ms"
            )

            # If ingest_to_graph requested, trigger Dagster job for that (still async)
            if ingest_to_graph:
              from robosystems.middleware.sse import (
                build_graph_job_config,
                run_and_monitor_dagster_job,
              )
              from robosystems.middleware.sse.event_storage import get_event_storage

              operation_id = str(uuid.uuid4())
              event_storage = get_event_storage()
              await event_storage.create_operation(
                operation_type="graph_ingestion",
                user_id=str(current_user.id),
                graph_id=graph_id,
                operation_id=operation_id,
              )

              run_config = build_graph_job_config(
                "materialize_file_job",
                file_id=file_id,
                graph_id=graph_id,
                table_name=table.table_name,
              )

              background_tasks.add_task(
                run_and_monitor_dagster_job,
                job_name="materialize_file_job",
                operation_id=operation_id,
                run_config=run_config,
              )

              graph_file.operation_id = operation_id
              db.commit()
              db.refresh(graph_file)

              logger.info(
                f"Direct staging done, graph ingestion job started for file {file_id}. "
                f"Monitor at /v1/operations/{operation_id}/stream"
              )
          else:
            logger.warning(
              f"Direct staging failed for file {file_id}: {staging_result.get('message')}. "
              f"File will be staged on next upload or query attempt."
            )

        except Exception as e:
          logger.warning(
            f"Direct staging error for file {file_id}: {e}. "
            f"File will be staged on next upload or query attempt."
          )

      else:
        # Standard path: Dagster job for large files
        from robosystems.middleware.sse import (
          build_graph_job_config,
          run_and_monitor_dagster_job,
        )
        from robosystems.middleware.sse.event_storage import get_event_storage

        operation_id = str(uuid.uuid4())

        logger.info(
          f"Large file detected ({actual_file_size / (1024 * 1024):.2f} MB >= {SMALL_FILE_STAGING_THRESHOLD_MB} MB). "
          f"Using Dagster job for file {file_id}"
        )

        try:
          # Register operation with SSE
          event_storage = get_event_storage()
          await event_storage.create_operation(
            operation_type="duckdb_staging",
            user_id=str(current_user.id),
            graph_id=graph_id,
            operation_id=operation_id,
          )

          # Build Dagster job config
          run_config = build_graph_job_config(
            "stage_file_job",
            file_id=file_id,
            graph_id=graph_id,
            table_id=str(table.id),
            ingest_to_graph=ingest_to_graph,
          )

          # Run Dagster job with SSE monitoring in background
          background_tasks.add_task(
            run_and_monitor_dagster_job,
            job_name="stage_file_job",
            operation_id=operation_id,
            run_config=run_config,
          )

          graph_file.operation_id = operation_id

          db.commit()
          db.refresh(graph_file)

          if ingest_to_graph:
            logger.info(
              f"v2 Incremental Ingestion: Dagster staging job started for file {file_id} "
              f"with auto-ingest to graph enabled. Monitor at /v1/operations/{operation_id}/stream"
            )
          else:
            logger.info(
              f"v2 Incremental Ingestion: Dagster staging job started for file {file_id}. "
              f"Monitor at /v1/operations/{operation_id}/stream"
            )

        except Exception as e:
          logger.warning(
            f"Failed to start Dagster staging job for file {file_id}: {e}. "
            f"File will be staged on next upload or query attempt."
          )

  logger.info(
    f"File {file_id} marked as uploaded: {graph_file.file_size_bytes or 0:,} bytes, {graph_file.row_count or 0:,} rows"
  )

  response = {
    "status": "success",
    "file_id": file_id,
    "upload_status": "uploaded",
    "file_size_bytes": graph_file.file_size_bytes,
    "row_count": graph_file.row_count,
    "message": "File validated and ready for ingestion",
  }

  # Check if file was staged directly (small file fast path)
  if graph_file.duckdb_status == "staged":
    response["duckdb_status"] = "staged"
    response["staged"] = True

    if graph_file.operation_id:
      # Operation_id means graph ingestion is in progress
      response["operation_id"] = graph_file.operation_id
      response["monitor_url"] = f"/v1/operations/{graph_file.operation_id}/stream"
      response["message"] = (
        f"File staged to DuckDB. Graph ingestion in progress. "
        f"Monitor at {response['monitor_url']}"
      )
      response["ingest_to_graph"] = True
    else:
      response["message"] = "File validated and staged to DuckDB (fast path)"
      response["ingest_to_graph"] = False
  elif graph_file.operation_id:
    # Large file: Dagster job handling staging (and possibly ingestion)
    response["operation_id"] = graph_file.operation_id
    response["monitor_url"] = f"/v1/operations/{graph_file.operation_id}/stream"
    response["staged"] = False

    if ingest_to_graph:
      response["message"] = (
        f"File validated. DuckDB staging in progress, then auto-ingesting to graph. "
        f"Monitor at {response['monitor_url']}"
      )
      response["ingest_to_graph"] = True
    else:
      response["message"] = (
        f"File validated. DuckDB staging in progress. Monitor at {response['monitor_url']}"
      )
      response["ingest_to_graph"] = False

  return response
