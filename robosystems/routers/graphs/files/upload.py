"""File upload via presigned S3 URL and upload status management."""

import uuid
from datetime import UTC, datetime
from pathlib import Path as PathLib

from fastapi import (
  APIRouter,
  Body,
  Depends,
  HTTPException,
  Path,
  status,
)
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.constants import (
  PRESIGNED_URL_EXPIRY_SECONDS,
)
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
  FileUploadRequest,
  FileUploadResponse,
  FileUploadStatus,
)
from robosystems.models.core import GraphFile, GraphTable, User
from robosystems.operations.aws.s3 import S3Client

router = APIRouter()


@router.post(
  "/files",
  response_model=FileUploadResponse,
  operation_id="createFileUpload",
  summary="Create File Upload",
  description="Returns a presigned S3 URL for direct upload. After uploading, call `POST /v1/graphs/{graph_id}/operations/ingest-file` to stage the file into DuckDB. Tables are auto-created if missing. Not allowed on entity graphs or shared repositories.",
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
