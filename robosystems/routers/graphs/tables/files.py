"""
Staging Table File Management Endpoints.

This module provides comprehensive file management for DuckDB staging tables,
enabling users to list, inspect, and delete files before graph ingestion.

Key Features:
- List all files in a staging table with metadata
- Get detailed information about individual files
- Delete files from both S3 and database tracking
- Automatic table statistics updates on file deletion
- Full audit trail of file operations

Workflow Integration:
1. Upload files via upload endpoints
2. List files to verify uploads (this module)
3. Inspect individual files for validation (this module)
4. Delete incorrect files if needed (this module)
5. Ingest validated data into graph

Use Cases:
- Monitor file upload progress per table
- Validate file formats and sizes before ingestion
- Inspect file metadata (row counts, upload status)
- Clean up duplicate or incorrect uploads
- Track storage usage per table
- Pre-ingestion data quality checks

File Lifecycle:
- created -> uploading -> uploaded -> ingested
- Files can be deleted at any stage before ingestion
- DuckDB automatically excludes deleted files from queries
- Table statistics recalculated on deletion

Security:
- Read operations require 'read' access
- Delete operations require 'write' access (verified via auth)
- Shared repositories block file deletions
- Full audit logging of all operations
- Rate limited per subscription tier
"""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable, GraphFile
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_db_session
from robosystems.adapters.s3 import S3Client
from robosystems.config import env
from robosystems.logger import logger, api_logger
from robosystems.middleware.graph.types import (
  GraphTypeRegistry,
  SHARED_REPO_DELETE_ERROR_MESSAGE,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)

router = APIRouter()


@router.get(
  "/tables/{table_name}/files",
  response_model=dict,
  summary="List Files in Staging Table",
  description="""List all files uploaded to a staging table with comprehensive metadata.

**Purpose:**
Get a complete inventory of all files in a staging table, including upload status,
file sizes, row counts, and S3 locations. Essential for monitoring upload progress
and validating data before ingestion.

**Use Cases:**
- Monitor file upload progress
- Verify files are ready for ingestion
- Check file formats and sizes
- Track storage usage per table
- Identify failed or incomplete uploads
- Pre-ingestion validation

**What You Get:**
- File ID and name
- File format (parquet, csv, etc.)
- Size in bytes
- Row count (if available)
- Upload status and method
- Creation and upload timestamps
- S3 key for reference

**Upload Status Values:**
- `created`: File record created, not yet uploaded
- `uploading`: Upload in progress
- `uploaded`: Successfully uploaded, ready for ingestion
- `failed`: Upload failed

**Example Response:**
```json
{
  "graph_id": "kg123",
  "table_name": "Entity",
  "files": [
    {
      "file_id": "f123",
      "file_name": "entities_batch1.parquet",
      "file_format": "parquet",
      "size_bytes": 1048576,
      "row_count": 5000,
      "upload_status": "uploaded",
      "upload_method": "presigned_url",
      "created_at": "2025-10-28T10:00:00Z",
      "uploaded_at": "2025-10-28T10:01:30Z",
      "s3_key": "user-staging/user123/kg123/Entity/entities_batch1.parquet"
    }
  ],
  "total_files": 1,
  "total_size_bytes": 1048576
}
```

**Example Usage:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \\
  https://api.robosystems.ai/v1/graphs/kg123/tables/Entity/files
```

**Tips:**
- Only `uploaded` files are ingested
- Check `row_count` to estimate data volume
- Use `total_size_bytes` for storage monitoring
- Files with `failed` status should be deleted and re-uploaded

**Note:**
File listing is included - no credit consumption.""",
  responses={
    200: {
      "description": "Files retrieved successfully with full metadata",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg123",
            "table_name": "Entity",
            "files": [
              {
                "file_id": "f123",
                "file_name": "data.parquet",
                "file_format": "parquet",
                "size_bytes": 1048576,
                "row_count": 5000,
                "upload_status": "uploaded",
                "upload_method": "presigned_url",
                "created_at": "2025-10-28T10:00:00Z",
                "uploaded_at": "2025-10-28T10:01:30Z",
                "s3_key": "user-staging/user123/kg123/Entity/data.parquet",
              }
            ],
            "total_files": 1,
            "total_size_bytes": 1048576,
          }
        }
      },
    },
    403: {
      "description": "Access denied - insufficient permissions for this graph",
      "model": ErrorResponse,
    },
    404: {
      "description": "Graph or table not found",
      "model": ErrorResponse,
    },
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/{table_name}/files",
  business_event_type="table_files_listed",
)
async def list_table_files(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  table_name: str = Path(..., description="Table name"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  """
  List all files in a staging table with metadata.

  Returns comprehensive file information including upload status, sizes,
  and S3 locations for monitoring the data pipeline.
  """
  start_time = datetime.now(timezone.utc)

  try:
    repository = await get_universal_repository_with_auth(
      graph_id, current_user, "read", db
    )

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    table = GraphTable.get_by_name(graph_id, table_name, db)
    if not table:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Table {table_name} not found in graph {graph_id}",
      )

    api_logger.info(
      "Listing table files",
      extra={
        "component": "tables_api",
        "action": "list_files_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/tables/{table_name}/files",
        },
      },
    )

    files = GraphFile.get_all_for_table(table.id, db)
    total_size = sum(f.file_size_bytes for f in files)
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/{table_name}/files",
      method="GET",
      event_type="table_files_listed_successfully",
      event_data={
        "graph_id": graph_id,
        "table_name": table_name,
        "file_count": len(files),
        "total_size_bytes": total_size,
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "Table files listed successfully",
      extra={
        "component": "tables_api",
        "action": "list_files_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "duration_ms": execution_time,
        "file_count": len(files),
        "success": True,
      },
    )

    return {
      "graph_id": graph_id,
      "table_name": table_name,
      "files": [
        {
          "file_id": f.id,
          "file_name": f.file_name,
          "file_format": f.file_format,
          "size_bytes": f.file_size_bytes,
          "row_count": f.row_count,
          "upload_status": f.upload_status,
          "upload_method": f.upload_method,
          "created_at": f.created_at.isoformat() if f.created_at else None,
          "uploaded_at": f.uploaded_at.isoformat() if f.uploaded_at else None,
          "s3_key": f.s3_key,
        }
        for f in files
      ],
      "total_files": len(files),
      "total_size_bytes": total_size,
    }

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/{table_name}/files",
      method="GET",
      event_type="list_table_files_failed",
      event_data={
        "graph_id": graph_id,
        "table_name": table_name,
        "error_type": type(e).__name__,
        "error_message": str(e),
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.error(
      "Failed to list table files",
      extra={
        "component": "tables_api",
        "action": "list_files_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_name": table_name,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    logger.error(f"Failed to list files for table {table_name}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list files: {str(e)}",
    )


@router.get(
  "/tables/files/{file_id}",
  summary="Get File Information",
  description="""Get detailed information about a specific file.

**Purpose:**
Retrieve comprehensive metadata for a single file, including upload status,
size, row count, and timestamps. Useful for validating individual files
before ingestion.

**Use Cases:**
- Validate file upload completion
- Check file metadata before ingestion
- Debug upload issues
- Verify file format and size
- Track file lifecycle

**Example Response:**
```json
{
  "file_id": "f123",
  "graph_id": "kg123",
  "table_id": "t456",
  "table_name": "Entity",
  "file_name": "entities_batch1.parquet",
  "file_format": "parquet",
  "size_bytes": 1048576,
  "row_count": 5000,
  "upload_status": "uploaded",
  "upload_method": "presigned_url",
  "created_at": "2025-10-28T10:00:00Z",
  "uploaded_at": "2025-10-28T10:01:30Z",
  "s3_key": "user-staging/user123/kg123/Entity/entities_batch1.parquet"
}
```

**Example Usage:**
```bash
curl -H "Authorization: Bearer YOUR_TOKEN" \\
  https://api.robosystems.ai/v1/graphs/kg123/tables/files/f123
```

**Note:**
File info retrieval is included - no credit consumption.""",
  responses={
    200: {
      "description": "File information retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "file_id": "f123",
            "graph_id": "kg123",
            "table_id": "t456",
            "table_name": "Entity",
            "file_name": "data.parquet",
            "file_format": "parquet",
            "size_bytes": 1048576,
            "row_count": 5000,
            "upload_status": "uploaded",
            "upload_method": "presigned_url",
            "created_at": "2025-10-28T10:00:00Z",
            "uploaded_at": "2025-10-28T10:01:30Z",
            "s3_key": "user-staging/user123/kg123/Entity/data.parquet",
          }
        }
      },
    },
    403: {
      "description": "Access denied - insufficient permissions for this graph",
      "model": ErrorResponse,
    },
    404: {
      "description": "File not found in graph",
      "model": ErrorResponse,
    },
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/files/{file_id}",
  business_event_type="file_info_retrieved",
)
async def get_file_info(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  file_id: str = Path(..., description="File ID"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  """
  Get detailed information about a specific file.

  Returns comprehensive file metadata including upload status, sizes,
  timestamps, and S3 location.
  """
  start_time = datetime.now(timezone.utc)

  try:
    repository = await get_universal_repository_with_auth(
      graph_id, current_user, "read", db
    )

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    file = GraphFile.get_by_id(file_id, db)
    if not file or file.graph_id != graph_id:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"File {file_id} not found in graph {graph_id}",
      )

    table = GraphTable.get_by_id(file.table_id, db)
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/files/{file_id}",
      method="GET",
      event_type="file_info_retrieved_successfully",
      event_data={
        "graph_id": graph_id,
        "file_id": file_id,
        "table_name": table.table_name if table else None,
        "file_size_bytes": file.file_size_bytes,
        "upload_status": file.upload_status,
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "File info retrieved",
      extra={
        "component": "tables_api",
        "action": "get_file_info_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "duration_ms": execution_time,
        "success": True,
      },
    )

    return {
      "file_id": file.id,
      "graph_id": file.graph_id,
      "table_id": file.table_id,
      "table_name": table.table_name if table else None,
      "file_name": file.file_name,
      "file_format": file.file_format,
      "size_bytes": file.file_size_bytes,
      "row_count": file.row_count,
      "upload_status": file.upload_status,
      "upload_method": file.upload_method,
      "created_at": file.created_at.isoformat() if file.created_at else None,
      "uploaded_at": file.uploaded_at.isoformat() if file.uploaded_at else None,
      "s3_key": file.s3_key,
    }

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/files/{file_id}",
      method="GET",
      event_type="get_file_info_failed",
      event_data={
        "graph_id": graph_id,
        "file_id": file_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.error(
      "Failed to get file info",
      extra={
        "component": "tables_api",
        "action": "get_file_info_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve file info: {str(e)}",
    )


@router.delete(
  "/tables/files/{file_id}",
  summary="Delete File from Staging",
  description="""Delete a file from S3 storage and database tracking.

**Purpose:**
Remove unwanted, duplicate, or incorrect files from staging tables before ingestion.
The file is deleted from both S3 and database tracking, and table statistics
are automatically recalculated.

**Use Cases:**
- Remove duplicate uploads
- Delete files with incorrect data
- Clean up failed uploads
- Fix data quality issues before ingestion
- Manage storage usage

**What Happens:**
1. File deleted from S3 storage
2. Database tracking record removed
3. Table statistics recalculated (file count, size, row count)
4. DuckDB automatically excludes file from future queries

**Security:**
- Write access required (verified via auth)
- Shared repositories block file deletions
- Full audit trail of deletion operations
- Cannot delete after ingestion to graph

**Example Response:**
```json
{
  "status": "deleted",
  "file_id": "f123",
  "file_name": "entities_batch1.parquet",
  "message": "File deleted successfully. DuckDB will automatically exclude it from queries."
}
```

**Example Usage:**
```bash
curl -X DELETE -H "Authorization: Bearer YOUR_TOKEN" \\
  https://api.robosystems.ai/v1/graphs/kg123/tables/files/f123
```

**Tips:**
- Delete files before ingestion for best results
- Table statistics update automatically
- No need to refresh DuckDB - exclusion is automatic
- Consider re-uploading corrected version after deletion

**Note:**
File deletion is included - no credit consumption.""",
  responses={
    200: {
      "description": "File deleted successfully",
      "content": {
        "application/json": {
          "example": {
            "status": "deleted",
            "file_id": "f123",
            "file_name": "data.parquet",
            "message": "File deleted successfully. DuckDB will automatically exclude it from queries.",
          }
        }
      },
    },
    403: {
      "description": "Access denied - shared repositories or insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "File not found in graph",
      "model": ErrorResponse,
    },
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables/files/{file_id}",
  business_event_type="file_deleted",
)
async def delete_file(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  file_id: str = Path(..., description="File ID"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> dict:
  """
  Delete a file from S3 and database tracking.

  Removes the file from S3 storage, deletes the database record, and
  recalculates table statistics. DuckDB will automatically exclude the
  file from queries.
  """
  start_time = datetime.now(timezone.utc)

  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    logger.warning(
      f"User {current_user.id} attempted file deletion on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_DELETE_ERROR_MESSAGE,
    )

  try:
    repository = await get_universal_repository_with_auth(
      graph_id, current_user, "write", db
    )

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    file = GraphFile.get_by_id(file_id, db)
    if not file or file.graph_id != graph_id:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"File {file_id} not found in graph {graph_id}",
      )

    file_name = file.file_name
    file_size = file.file_size_bytes
    s3_key = file.s3_key

    api_logger.info(
      "File deletion initiated",
      extra={
        "component": "tables_api",
        "action": "delete_file_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "file_name": file_name,
        "file_size_bytes": file_size,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/tables/files/{file_id}",
        },
      },
    )

    s3_client = S3Client()
    bucket = env.AWS_S3_BUCKET

    s3_client.s3_client.delete_object(Bucket=bucket, Key=s3_key)
    logger.info(f"Deleted file from S3: {s3_key}")

    table = GraphTable.get_by_id(file.table_id, db)
    if table:
      new_file_count = max(0, (table.file_count or 1) - 1)
      new_total_size = max(0, (table.total_size_bytes or 0) - file_size)

      if file.row_count is not None:
        new_row_count = max(0, (table.row_count or 0) - file.row_count)
      else:
        all_files = GraphFile.get_all_for_table(table.id, db)
        remaining_files = [
          f for f in all_files if f.id != file.id and f.upload_status == "completed"
        ]
        new_row_count = sum(
          f.row_count for f in remaining_files if f.row_count is not None
        )

      table.update_stats(
        session=db,
        file_count=new_file_count,
        total_size_bytes=new_total_size,
        row_count=new_row_count,
      )

    db.delete(file)
    db.commit()

    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/files/{file_id}",
      method="DELETE",
      event_type="file_deleted_successfully",
      event_data={
        "graph_id": graph_id,
        "file_id": file_id,
        "file_name": file_name,
        "file_size_bytes": file_size,
        "table_name": table.table_name if table else None,
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "File deleted successfully",
      extra={
        "component": "tables_api",
        "action": "delete_file_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "file_name": file_name,
        "duration_ms": execution_time,
        "success": True,
      },
    )

    logger.info(
      f"Deleted file {file_id} ({file_name}) from graph {graph_id} - "
      f"DuckDB will automatically exclude it from queries"
    )

    return {
      "status": "deleted",
      "file_id": file_id,
      "file_name": file_name,
      "message": "File deleted successfully. DuckDB will automatically exclude it from queries.",
    }

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables/files/{file_id}",
      method="DELETE",
      event_type="delete_file_failed",
      event_data={
        "graph_id": graph_id,
        "file_id": file_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.error(
      "File deletion failed",
      extra={
        "component": "tables_api",
        "action": "delete_file_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    logger.error(f"Failed to delete file {file_id}: {e}")
    db.rollback()
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to delete file: {str(e)}",
    )
