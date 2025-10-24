import uuid
from pathlib import Path as PathLib

from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable, GraphFile
from robosystems.models.api.table import (
  FileUploadRequest,
  FileUploadResponse,
  FileUpdateRequest,
)
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_db_session
from robosystems.adapters.s3 import S3Client
from robosystems.config import env
from robosystems.config.constants import MAX_FILE_SIZE_MB
from robosystems.config.tier_config import get_tier_storage_limit
from robosystems.logger import logger
from robosystems.middleware.graph.types import GraphTypeRegistry

router = APIRouter()


@router.post(
  "/tables/{table_name}/files",
  response_model=FileUploadResponse,
  summary="Create File Upload",
  description="Create a new file upload for a table and get a presigned S3 URL",
  responses={
    200: {"description": "Upload URL generated successfully"},
    400: {"description": "Invalid file format or name", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph or table not found", "model": ErrorResponse},
  },
)
async def get_upload_url(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name"),
  request: FileUploadRequest = Body(..., description="Upload request"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> FileUploadResponse:
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories are read-only. File uploads and data ingestion are not allowed. "
      "Shared repositories provide reference data that cannot be modified.",
    )

  graph, _ = await get_universal_repository_with_auth(graph_id, current_user.id, db)

  if not graph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  table = GraphTable.get_by_name(graph_id, table_name, db)
  if not table:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Table {table_name} not found",
    )

  # Validate file format
  allowed_formats = ["application/x-parquet", "text/csv", "application/json"]
  if request.content_type not in allowed_formats:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Unsupported file format: {request.content_type}. Allowed: {', '.join(allowed_formats)}",
    )

  # Validate file extension matches content type (using pathlib for robust extraction)
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

  # Validate filename
  if not request.file_name or len(request.file_name) > 255:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="File name must be between 1 and 255 characters",
    )

  # Prevent path traversal
  if ".." in request.file_name or "/" in request.file_name or "\\" in request.file_name:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="File name contains invalid characters",
    )

  max_file_size_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
  if request.file_size_bytes > max_file_size_bytes:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"File size {request.file_size_bytes / (1024 * 1024):.2f} MB exceeds maximum of {MAX_FILE_SIZE_MB} MB",
    )

  storage_limit_gb = get_tier_storage_limit(graph.tier)
  storage_limit_bytes = storage_limit_gb * 1024 * 1024 * 1024

  all_tables = GraphTable.get_all_for_graph(graph_id, db)
  current_storage_bytes = sum(t.total_size_bytes or 0 for t in all_tables)

  if current_storage_bytes + request.file_size_bytes > storage_limit_bytes:
    raise HTTPException(
      status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
      detail=f"Storage limit exceeded. Current: {current_storage_bytes / (1024 * 1024 * 1024):.2f} GB, "
      f"Limit: {storage_limit_gb} GB, "
      f"Attempted upload: {request.file_size_bytes / (1024 * 1024 * 1024):.2f} GB",
    )

  logger.info(
    f"Generating upload URL for {request.file_name} to table {table_name} in graph {graph_id}"
  )

  file_id = str(uuid.uuid4())
  s3_key = f"user-staging/{current_user.id}/{graph_id}/{table_name}/{file_id}/{request.file_name}"

  s3_client = S3Client()

  try:
    bucket = env.AWS_S3_BUCKET_NAME

    upload_url = s3_client.s3_client.generate_presigned_url(
      "put_object",
      Params={
        "Bucket": bucket,
        "Key": s3_key,
        "ContentType": request.content_type,
      },
      ExpiresIn=3600,
    )

    graph_file = GraphFile.create(
      graph_id=graph_id,
      table_id=table.id,
      file_name=request.file_name,
      s3_key=s3_key,
      file_format=request.content_type.split("/")[-1],
      file_size_bytes=0,
      upload_method="presigned_url",
      upload_status="pending",
      row_count=None,
      session=db,
    )

    logger.info(f"Generated upload URL for file {file_id}: {s3_key}")

    return FileUploadResponse(
      upload_url=upload_url,
      expires_in=3600,
      file_id=graph_file.id,
      s3_key=s3_key,
    )

  except Exception as e:
    logger.error(f"Failed to generate upload URL for {request.file_name}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to generate upload URL: {str(e)}",
    )


@router.patch(
  "/tables/files/{file_id}",
  response_model=dict,
  summary="Update File",
  description="Update file metadata after upload (size, row count). Marks file as completed.",
  responses={
    200: {"description": "File updated successfully"},
    400: {"description": "Invalid file size", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph or file not found", "model": ErrorResponse},
  },
)
async def update_file(
  graph_id: str = Path(..., description="Graph database identifier"),
  file_id: str = Path(..., description="File identifier"),
  request: FileUpdateRequest = Body(..., description="File update details"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories are read-only. File uploads and data ingestion are not allowed. "
      "Shared repositories provide reference data that cannot be modified.",
    )

  graph, _ = await get_universal_repository_with_auth(graph_id, current_user.id, db)

  if not graph:
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

  logger.info(f"Updating file metadata for {file_id} in graph {graph_id}")

  # Validate file size
  if request.file_size_bytes <= 0:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="File size must be greater than 0",
    )

  # Validate row count if provided
  if request.row_count is not None and request.row_count < 0:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Row count cannot be negative",
    )

  try:
    graph_file.file_size_bytes = request.file_size_bytes
    if request.row_count is not None:
      graph_file.row_count = request.row_count

    graph_file.mark_uploaded(db)

    table = (
      db.query(GraphTable)
      .filter(GraphTable.id == graph_file.table_id)
      .with_for_update()
      .first()
    )
    if table:
      all_files = GraphFile.get_all_for_table(table.id, db)
      completed_files = [f for f in all_files if f.upload_status == "completed"]

      new_file_count = len(completed_files)

      table.update_stats(
        session=db,
        file_count=new_file_count,
        total_size_bytes=sum(f.file_size_bytes for f in completed_files),
        row_count=sum(f.row_count for f in completed_files if f.row_count is not None),
      )

      if new_file_count > 0:
        from robosystems.operations.graph.table_service import TableService
        from robosystems.graph_api.client.factory import get_graph_client

        table_service = TableService(db)
        s3_pattern = table_service.get_s3_pattern_for_table(
          graph_id=graph_id,
          table_name=table.table_name,
          user_id=current_user.id,
        )

        try:
          client = await get_graph_client(graph_id=graph_id, operation_type="write")
          await client.create_table(
            graph_id=graph_id,
            table_name=table.table_name,
            s3_pattern=s3_pattern,
          )
          logger.info(
            f"Registered/updated DuckDB external table {table.table_name} with pattern: {s3_pattern}"
          )
        except Exception as e:
          logger.warning(
            f"Failed to register DuckDB table {table.table_name}: {e}. "
            f"Table queries may fail until registration succeeds. "
            f"Registration will be automatically retried on next upload or query attempt."
          )

    logger.info(
      f"Updated file {file_id}: {graph_file.file_size_bytes} bytes, status=completed"
    )

    return {
      "status": "success",
      "file_id": file_id,
      "upload_status": "completed",
    }

  except Exception as e:
    logger.error(f"Failed to update file {file_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to update file: {str(e)}",
    )
