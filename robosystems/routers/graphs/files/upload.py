"""File upload via presigned S3 URL and upload status management."""

import uuid
from datetime import UTC, datetime
from pathlib import Path as PathLib

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Body,
  Depends,
  HTTPException,
  Path,
  status,
)
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.constants import (
  FALLBACK_BYTES_PER_ROW_CSV,
  FALLBACK_BYTES_PER_ROW_JSON,
  FALLBACK_BYTES_PER_ROW_PARQUET,
  MAX_FILE_SIZE_MB,
  PRESIGNED_URL_EXPIRY_SECONDS,
  SMALL_FILE_STAGING_THRESHOLD_MB,
)
from robosystems.config.graph_tier import GraphTierConfig
from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.database import get_db_session
from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import (
  GRAPH_OR_SUBGRAPH_ID_PATTERN,
  SHARED_REPO_WRITE_ERROR_MESSAGE,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.tables import (
  FileStatusUpdate,
  FileUploadRequest,
  FileUploadResponse,
  FileUploadStatus,
)
from robosystems.models.core import Graph, GraphFile, GraphTable, User
from robosystems.operations.aws.s3 import S3Client

router = APIRouter()


@router.post(
  "/files",
  response_model=FileUploadResponse,
  operation_id="createFileUpload",
  summary="Create File Upload",
  description="Returns a presigned S3 URL for direct upload. After uploading, call `PATCH /files/{file_id}` with `status=uploaded` to trigger DuckDB staging. Tables are auto-created if missing. Not allowed on entity graphs or shared repositories.",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/files", business_event_type="file_upload_created"
)
async def create_file_upload(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  request: FileUploadRequest = Body(..., description="Upload request with table_name"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> FileUploadResponse:
  start_time = datetime.now(UTC)

  # Enforce graph lifecycle and subscription status (write operation)
  from robosystems.middleware.billing.enforcement import require_graph_access

  graph = require_graph_access(graph_id, db, require_write=True)

  if getattr(graph, "graph_type", None) == "entity":
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Entity graphs do not support file uploads. "
      "Entity graph data is managed through the extensions pipeline "
      "(connectors and OLTP APIs). Use POST /materialize with source='extensions' instead.",
    )

  table_name = getattr(request, "table_name", None)
  if not table_name:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="table_name is required in request body",
    )

  if is_shared_repository_or_subgraph(graph_id.lower()):
    logger.warning(
      f"User {current_user.id} attempted file upload on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_WRITE_ERROR_MESSAGE,
    )

  try:
    repository = await get_universal_repository(graph_id, "write")

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    api_logger.info(
      "Upload URL generation started",
      extra={
        "component": "files_api",
        "action": "upload_url_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "file_name": request.file_name,
        "content_type": request.content_type,
      },
    )

    table = GraphTable.get_by_name(graph_id, table_name, db)
    if not table:
      from robosystems.operations.graph.table_service import infer_table_type

      inferred_type = infer_table_type(table_name)
      logger.info(
        f"Auto-creating table {table_name} ({inferred_type}) for graph {graph_id} on first file upload"
      )
      table = GraphTable.create(
        graph_id=graph_id,
        table_name=table_name,
        table_type=inferred_type,
        schema_json={"columns": []},
        session=db,
      )

    allowed_formats = ["application/x-parquet", "text/csv", "application/json"]
    if request.content_type not in allowed_formats:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Unsupported file format: {request.content_type}. Allowed: {', '.join(allowed_formats)}",
      )

    file_extension = PathLib(request.file_name).suffix.lstrip(".").lower()
    expected_extensions = {
      "application/x-parquet": "parquet",
      "text/csv": "csv",
      "application/json": "json",
    }
    expected_ext = expected_extensions.get(request.content_type, "")
    if expected_ext and file_extension != expected_ext:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"File extension '.{file_extension}' does not match content type '{request.content_type}'. Expected '.{expected_ext}'",
      )

    if not request.file_name or len(request.file_name) > 255:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="File name must be between 1 and 255 characters",
      )

    if (
      ".." in request.file_name or "/" in request.file_name or "\\" in request.file_name
    ):
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="File name contains invalid characters",
      )

    logger.info(
      f"Generating upload URL for {request.file_name} to table {table_name} in graph {graph_id}"
    )

    file_id = str(uuid.uuid4())
    s3_key = f"user-staging/{current_user.id}/{graph_id}/{table_name}/{file_id}/{request.file_name}"

    s3_client = S3Client()
    bucket = env.USER_DATA_BUCKET

    upload_url = s3_client.s3_client.generate_presigned_url(
      "put_object",
      Params={
        "Bucket": bucket,
        "Key": s3_key,
        "ContentType": request.content_type,
      },
      ExpiresIn=PRESIGNED_URL_EXPIRY_SECONDS,
    )

    file_format_map = {
      "application/x-parquet": "parquet",
      "text/csv": "csv",
      "application/json": "json",
    }
    file_format = file_format_map.get(request.content_type, "unknown")

    graph_file = GraphFile.create(
      graph_id=graph_id,
      table_id=table.id,
      file_name=request.file_name,
      s3_key=s3_key,
      file_format=file_format,
      file_size_bytes=0,
      upload_method="presigned_url",
      upload_status=FileUploadStatus.PENDING.value,
      row_count=None,
      session=db,
    )

    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/files",
      method="POST",
      event_type="upload_url_generated_successfully",
      event_data={
        "graph_id": graph_id,
        "table_name": table_name,
        "file_id": graph_file.id,
        "file_name": request.file_name,
        "file_format": file_format,
        "expires_in": PRESIGNED_URL_EXPIRY_SECONDS,
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "Upload URL generated successfully",
      extra={
        "component": "files_api",
        "action": "upload_url_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "file_id": graph_file.id,
        "file_name": request.file_name,
        "duration_ms": execution_time,
        "success": True,
      },
    )

    logger.info(f"Generated upload URL for file {graph_file.id}: {s3_key}")

    return FileUploadResponse(
      upload_url=upload_url,
      expires_in=PRESIGNED_URL_EXPIRY_SECONDS,
      file_id=graph_file.id,
      s3_key=s3_key,
    )

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/files",
      method="POST",
      event_type="upload_url_generation_failed",
      event_data={
        "graph_id": graph_id,
        "table_name": table_name,
        "file_name": request.file_name,
        "error_type": type(e).__name__,
        "error_message": str(e),
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.error(
      "Upload URL generation failed",
      extra={
        "component": "files_api",
        "action": "upload_url_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "file_name": request.file_name,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    logger.error(
      f"Failed to generate upload URL for {request.file_name}: {e}", exc_info=True
    )
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to generate upload URL.",
    )


@router.patch(
  "/files/{file_id}",
  response_model=dict,
  operation_id="updateFile",
  summary="Update File Status",
  description="Setting `status=uploaded` validates the file in S3, calculates row count, and triggers DuckDB staging. Small files use direct staging; large files use a background Dagster job with an `operation_id` for SSE monitoring. Set `ingest_to_graph=true` to auto-chain graph materialization.",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/files/{file_id}", business_event_type="file_updated"
)
async def update_file(
  background_tasks: BackgroundTasks,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  file_id: str = Path(..., description="File ID"),
  request: FileStatusUpdate = Body(..., description="Status update request"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  start_time = datetime.now(UTC)

  if is_shared_repository_or_subgraph(graph_id.lower()):
    logger.warning(
      f"User {current_user.id} attempted file status update on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_WRITE_ERROR_MESSAGE,
    )

  try:
    repository = await get_universal_repository(graph_id, "write")

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    graph_file = GraphFile.get_by_id(file_id, db)
    if not graph_file or graph_file.graph_id != graph_id:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"File {file_id} not found",
      )

    valid_statuses = {
      FileUploadStatus.UPLOADED.value,
      FileUploadStatus.DISABLED.value,
      FileUploadStatus.ARCHIVED.value,
    }
    if request.status not in valid_statuses:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid status '{request.status}'. Must be one of: {', '.join(valid_statuses)}. "
        f"Note: Files cannot be reset to '{FileUploadStatus.PENDING.value}' status after upload.",
      )

    api_logger.info(
      "File status update initiated",
      extra={
        "component": "files_api",
        "action": "status_update_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "requested_status": request.status,
      },
    )

    logger.info(
      f"Updating file {file_id} status to '{request.status}' in graph {graph_id}"
    )

    if request.status == FileUploadStatus.DISABLED.value:
      graph_file.upload_status = FileUploadStatus.DISABLED.value
      db.commit()
      db.refresh(graph_file)

      execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/graphs/{graph_id}/files/{file_id}",
        method="PATCH",
        event_type="file_disabled",
        event_data={
          "graph_id": graph_id,
          "file_id": file_id,
          "execution_time_ms": execution_time,
        },
        user_id=current_user.id,
      )

      logger.info(f"File {file_id} disabled (excluded from ingestion)")
      return {
        "status": "success",
        "file_id": file_id,
        "upload_status": "disabled",
        "message": "File disabled and excluded from ingestion",
      }

    if request.status == FileUploadStatus.ARCHIVED.value:
      graph_file.upload_status = FileUploadStatus.ARCHIVED.value
      db.commit()
      db.refresh(graph_file)

      execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/graphs/{graph_id}/files/{file_id}",
        method="PATCH",
        event_type="file_archived",
        event_data={
          "graph_id": graph_id,
          "file_id": file_id,
          "execution_time_ms": execution_time,
        },
        user_id=current_user.id,
      )

      logger.info(f"File {file_id} archived (soft deleted)")
      return {
        "status": "success",
        "file_id": file_id,
        "upload_status": "archived",
        "message": "File archived",
      }

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

      logger.info(
        f"Calculated row count for {graph_file.file_name}: {actual_row_count}"
      )
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
          from robosystems.operations.lbug.direct_staging import stage_file_directly

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
              if request.ingest_to_graph:
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
              ingest_to_graph=request.ingest_to_graph,
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

            if request.ingest_to_graph:
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

      if request.ingest_to_graph:
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

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

    api_logger.error(
      "File status update failed",
      extra={
        "component": "files_api",
        "action": "status_update_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    logger.error(f"Failed to update file {file_id}: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to update file.",
    )
