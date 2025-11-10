"""
File Upload Management Endpoints.

This module provides secure file upload capabilities for staging tables,
using presigned S3 URLs and comprehensive validation before ingestion.

Key Features:
- Presigned S3 URLs for secure direct uploads
- Automatic file validation (size, format, row count)
- Table statistics recalculation after upload
- Automatic DuckDB table registration
- File status lifecycle management
- Storage limit enforcement per tier
- Comprehensive error handling and recovery

Upload Workflow:
1. Request presigned URL: `POST /tables/{table_name}/files`
2. Upload file directly to S3 using presigned URL
3. Update status to 'uploaded': `PATCH /tables/files/{file_id}`
4. Backend validates file, calculates size and row count
5. Table statistics updated automatically
6. DuckDB table registered for queries
7. File ready for ingestion

File Lifecycle States:
- pending: URL generated, awaiting upload
- uploaded: Successfully uploaded and validated
- disabled: Excluded from ingestion
- archived: Soft deleted
- failed: Upload or validation failed

Validation Features:
- File format validation (parquet, csv, json)
- Extension matching (e.g., .parquet for application/x-parquet)
- Size limits per file and tier storage cap
- Row count calculation or estimation
- S3 existence verification
- Path traversal prevention

Auto-Creation:
- Tables are automatically created on first file upload
- Table type inferred from naming conventions
- Schema populated incrementally

Performance:
- Direct S3 uploads (no API bottleneck)
- Presigned URLs expire in configurable seconds
- Optimized S3 head operations for validation
- Concurrent uploads supported
- Atomic table statistics updates

Security:
- Write access required for uploads
- Presigned URLs with time expiration
- Shared repositories block uploads
- Path traversal protection
- Full audit logging of operations
- Rate limited per subscription tier
"""

import uuid
from datetime import datetime, timezone
from pathlib import Path as PathLib

from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable, GraphFile
from robosystems.models.api.graphs.tables import (
  FileUploadRequest,
  FileUploadResponse,
  FileStatusUpdate,
  FileUploadStatus,
)
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph import get_universal_repository
from robosystems.database import get_db_session
from robosystems.adapters.s3 import S3Client
from robosystems.config import env
from robosystems.config.constants import (
  MAX_FILE_SIZE_MB,
  PRESIGNED_URL_EXPIRY_SECONDS,
  FALLBACK_BYTES_PER_ROW_PARQUET,
  FALLBACK_BYTES_PER_ROW_CSV,
  FALLBACK_BYTES_PER_ROW_JSON,
)
from robosystems.config.billing.storage import StorageBillingConfig
from robosystems.logger import logger, api_logger
from robosystems.middleware.graph.types import (
  GraphTypeRegistry,
  SHARED_REPO_WRITE_ERROR_MESSAGE,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)

router = APIRouter()


@router.post(
  "/tables/{table_name}/files",
  response_model=FileUploadResponse,
  operation_id="getUploadUrl",
  summary="Get File Upload URL",
  description="""Generate a presigned S3 URL for secure file upload.

Initiates file upload to a staging table by generating a secure, time-limited
presigned S3 URL. Files are uploaded directly to S3, bypassing the API for
optimal performance.

**Upload Workflow:**
1. Call this endpoint to get presigned URL
2. PUT file directly to S3 URL
3. Call PATCH /tables/files/{file_id} with status='uploaded'
4. Backend validates file and calculates metrics
5. File ready for ingestion

**Supported Formats:**
- Parquet (`application/x-parquet` with `.parquet` extension)
- CSV (`text/csv` with `.csv` extension)
- JSON (`application/json` with `.json` extension)

**Validation:**
- File extension must match content type
- File name 1-255 characters
- No path traversal characters (.. / \\)
- Auto-creates table if it doesn't exist

**Auto-Table Creation:**
Tables are automatically created on first file upload with type inferred from name
(e.g., "Transaction" → relationship) and empty schema populated during ingestion.

**Important Notes:**
- Presigned URLs expire (default: 1 hour)
- Use appropriate Content-Type header when uploading to S3
- File extension must match content type
- Upload URL generation is included - no credit consumption""",
  responses={
    200: {
      "description": "Upload URL generated successfully",
      "content": {
        "application/json": {
          "example": {
            "upload_url": "https://bucket.s3.amazonaws.com/path?signature",
            "expires_in": 3600,
            "file_id": "f123",
            "s3_key": "user-staging/user123/kg123/Entity/data.parquet",
          }
        }
      },
    },
    400: {
      "description": "Invalid file format, name, or extension mismatch",
      "model": ErrorResponse,
    },
    403: {
      "description": "Access denied - shared repositories or insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "Graph not found",
      "model": ErrorResponse,
    },
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/{table_name}/files",
  business_event_type="upload_url_generated",
)
async def get_upload_url(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  table_name: str = Path(..., description="Table name"),
  request: FileUploadRequest = Body(..., description="Upload request"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> FileUploadResponse:
  """
  Generate a presigned S3 URL for file upload.

  Creates a secure, time-limited URL for direct upload to S3, bypassing
  the API for optimal performance.
  """
  start_time = datetime.now(timezone.utc)

  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
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
        "component": "tables_api",
        "action": "upload_url_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "file_name": request.file_name,
        "content_type": request.content_type,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/tables/{table_name}/files",
        },
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
    bucket = env.AWS_S3_BUCKET

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

    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/{table_name}/files",
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
        "component": "tables_api",
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
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/{table_name}/files",
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
        "component": "tables_api",
        "action": "upload_url_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "file_name": request.file_name,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    logger.error(f"Failed to generate upload URL for {request.file_name}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to generate upload URL: {str(e)}",
    )


@router.patch(
  "/tables/files/{file_id}",
  response_model=dict,
  operation_id="updateFileStatus",
  summary="Update File Upload Status",
  description="""Update file status after upload completes.

Marks files as uploaded after successful S3 upload. The backend validates
the file, calculates size and row count, enforces storage limits, and
registers the DuckDB table for queries.

**Status Values:**
- `uploaded`: File successfully uploaded to S3 (triggers validation)
- `disabled`: Exclude file from ingestion
- `archived`: Soft delete file

**What Happens on 'uploaded' Status:**
1. Verify file exists in S3
2. Calculate actual file size
3. Enforce tier storage limits
4. Calculate or estimate row count
5. Update table statistics
6. Register DuckDB external table
7. File ready for ingestion

**Row Count Calculation:**
- **Parquet**: Exact count from file metadata
- **CSV**: Count rows (minus header)
- **JSON**: Count array elements
- **Fallback**: Estimate from file size if reading fails

**Storage Limits:**
Enforced per subscription tier. Returns HTTP 413 if limit exceeded.
Check current usage before large uploads.

**Important Notes:**
- Always call this after S3 upload completes
- Check response for actual row count
- Storage limit errors (413) mean tier upgrade needed
- DuckDB registration failures are non-fatal (retried later)
- Status updates are included - no credit consumption""",
  responses={
    200: {
      "description": "File status updated successfully",
      "content": {
        "application/json": {
          "example": {
            "status": "success",
            "file_id": "f123",
            "upload_status": "uploaded",
            "file_size_bytes": 1048576,
            "row_count": 5000,
            "message": "File validated and ready for ingestion",
          }
        }
      },
    },
    400: {
      "description": "Invalid status, file too large, or empty file",
      "model": ErrorResponse,
    },
    403: {
      "description": "Access denied - shared repositories or insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "Graph, file, or S3 object not found",
      "model": ErrorResponse,
    },
    413: {
      "description": "Storage limit exceeded for tier",
      "model": ErrorResponse,
    },
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/files/{file_id}",
  business_event_type="file_status_updated",
)
async def update_file_status(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  file_id: str = Path(..., description="File identifier"),
  request: FileStatusUpdate = Body(..., description="Status update"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  """
  Update file upload status with validation.

  Marks files as uploaded after S3 upload completes. Validates file existence,
  calculates size and row count, enforces storage limits, and registers DuckDB table.
  """
  start_time = datetime.now(timezone.utc)

  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
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

    # Validate status using enum (exclude PENDING - users can't set files back to pending)
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
        "component": "tables_api",
        "action": "status_update_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "requested_status": request.status,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/tables/files/{file_id}",
        },
      },
    )

    logger.info(
      f"Updating file {file_id} status to '{request.status}' in graph {graph_id}"
    )

    if request.status == FileUploadStatus.DISABLED.value:
      graph_file.upload_status = FileUploadStatus.DISABLED.value
      db.commit()
      db.refresh(graph_file)

      execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/graphs/{graph_id}/tables/files/{file_id}",
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

      execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/graphs/{graph_id}/tables/files/{file_id}",
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
      storage_limit_gb = StorageBillingConfig.STORAGE_INCLUDED.get(
        str(graph.graph_tier), 100
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
        import pyarrow.parquet as pq
        from io import BytesIO

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
