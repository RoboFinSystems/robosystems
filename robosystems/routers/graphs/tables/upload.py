import uuid
from pathlib import Path as PathLib

from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable, GraphFile
from robosystems.models.api.table import (
  FileUploadRequest,
  FileUploadResponse,
  FileStatusUpdate,
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
  operation_id="getUploadUrl",
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

  repository = await get_universal_repository_with_auth(
    graph_id, current_user, "write", db
  )

  if not repository:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
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

  logger.info(
    f"Generating upload URL for {request.file_name} to table {table_name} in graph {graph_id}"
  )

  file_id = str(uuid.uuid4())
  s3_key = f"user-staging/{current_user.id}/{graph_id}/{table_name}/{file_id}/{request.file_name}"

  s3_client = S3Client()

  try:
    bucket = env.AWS_S3_BUCKET

    upload_url = s3_client.s3_client.generate_presigned_url(
      "put_object",
      Params={
        "Bucket": bucket,
        "Key": s3_key,
        "ContentType": request.content_type,
      },
      ExpiresIn=3600,
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
  operation_id="updateFileStatus",
  summary="Update File Status",
  description="Update file status. When status is set to 'uploaded', backend validates file exists in S3, calculates actual file size and row count. Only files with 'uploaded' status are eligible for ingestion.",
  responses={
    200: {"description": "File status updated successfully"},
    400: {"description": "Invalid status or file too large", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph, file, or S3 object not found", "model": ErrorResponse},
    413: {"description": "Storage limit exceeded", "model": ErrorResponse},
  },
)
async def update_file_status(
  graph_id: str = Path(..., description="Graph database identifier"),
  file_id: str = Path(..., description="File identifier"),
  request: FileStatusUpdate = Body(..., description="Status update"),
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

  repository = await get_universal_repository_with_auth(
    graph_id, current_user, "write", db
  )

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

  VALID_STATUSES = {"uploaded", "disabled", "archived"}
  if request.status not in VALID_STATUSES:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Invalid status '{request.status}'. Must be one of: {', '.join(VALID_STATUSES)}",
    )

  logger.info(
    f"Updating file {file_id} status to '{request.status}' in graph {graph_id}"
  )

  if request.status == "disabled":
    graph_file.upload_status = "disabled"
    db.commit()
    db.refresh(graph_file)
    logger.info(f"File {file_id} disabled (excluded from ingestion)")
    return {
      "status": "success",
      "file_id": file_id,
      "upload_status": "disabled",
      "message": "File disabled and excluded from ingestion",
    }

  if request.status == "archived":
    graph_file.upload_status = "archived"
    db.commit()
    db.refresh(graph_file)
    logger.info(f"File {file_id} archived (soft deleted)")
    return {
      "status": "success",
      "file_id": file_id,
      "upload_status": "archived",
      "message": "File archived",
    }

  try:
    s3_client = S3Client()
    bucket = env.AWS_S3_BUCKET

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

    from robosystems.models.iam import Graph

    graph = Graph.get_by_id(graph_id, db)
    if graph:
      storage_limit_gb = get_tier_storage_limit(graph.graph_tier)
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

      if graph_file.file_format == "parquet":
        import pyarrow.parquet as pq
        from io import BytesIO

        parquet_file = pq.read_table(BytesIO(file_content))
        actual_row_count = parquet_file.num_rows
      elif graph_file.file_format == "csv":
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
      actual_row_count = actual_file_size // 100

    graph_file.file_size_bytes = actual_file_size
    graph_file.row_count = actual_row_count
    graph_file.upload_status = "uploaded"
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
      uploaded_files = [f for f in all_files if f.upload_status == "uploaded"]

      new_file_count = len(uploaded_files)

      table.update_stats(
        session=db,
        file_count=new_file_count,
        total_size_bytes=sum(f.file_size_bytes for f in uploaded_files),
        row_count=sum(f.row_count for f in uploaded_files if f.row_count is not None),
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
      f"File {file_id} marked as uploaded: {graph_file.file_size_bytes or 0:,} bytes, {graph_file.row_count or 0:,} rows"
    )

    return {
      "status": "success",
      "file_id": file_id,
      "upload_status": "uploaded",
      "file_size_bytes": graph_file.file_size_bytes,
      "row_count": graph_file.row_count,
      "message": "File validated and ready for ingestion",
    }

  except Exception as e:
    logger.error(f"Failed to update file {file_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to update file: {str(e)}",
    )
