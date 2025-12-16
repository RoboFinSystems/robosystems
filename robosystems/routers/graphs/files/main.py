"""
File Management - Main Endpoints.

This module provides core file operations at the graph level, treating files as
first-class citizens with their own namespace.

Key Features:
- List files across graph with filtering
- Get detailed file information with multi-layer status
- Delete files with cascade support
- Independent file lifecycle management

Multi-Layer Status Tracking:
- S3 layer: Immutable source with upload status
- DuckDB layer: Mutable staging with immediate queryability
- Graph layer: Immutable materialized view

Architecture:
Files are first-class resources queried by file_id, independent of table context.
This enables clean REST semantics and file-centric operations.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable, GraphFile, Graph
from robosystems.models.api.common import ErrorResponse
from robosystems.models.api.graphs.tables import (
  FileUploadStatus,
  ListTableFilesResponse,
  GetFileInfoResponse,
  DeleteFileResponse,
  EnhancedFileStatusLayers,
  FileLayerStatus,
)
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph import get_universal_repository
from robosystems.database import get_db_session
from robosystems.operations.aws.s3 import S3Client
from robosystems.config import env
from robosystems.logger import logger, api_logger
from robosystems.middleware.graph.types import (
  GraphTypeRegistry,
  SHARED_REPO_DELETE_ERROR_MESSAGE,
  GRAPH_OR_SUBGRAPH_ID_PATTERN,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)

router = APIRouter()


@router.get(
  "/files",
  response_model=ListTableFilesResponse,
  operation_id="listFiles",
  summary="List Files in Graph",
  description="""List all files in the graph with optional filtering.

Get a complete inventory of files across all tables or filtered by table name,
status, or other criteria. Files are first-class resources with independent lifecycle.

**Query Parameters:**
- `table_name` (optional): Filter by table name
- `status` (optional): Filter by upload status (uploaded, pending, failed, etc.)

**Use Cases:**
- Monitor file upload progress across all tables
- Verify files are ready for ingestion
- Check file metadata and sizes
- Track storage usage per graph
- Identify failed or incomplete uploads
- Audit file provenance

**Returned Metadata:**
- File ID, name, and format (parquet, csv, json)
- Size in bytes and row count (if available)
- Upload status and timestamps
- DuckDB and graph ingestion status
- Table association

**File Lifecycle Tracking:**
Multi-layer status across S3 → DuckDB → Graph pipeline

**Important Notes:**
- Files are graph-scoped, not table-scoped
- Use table_name parameter to filter by table
- File listing is included - no credit consumption""",
  responses={
    200: {
      "description": "Files retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg123",
            "table_name": None,
            "files": [
              {
                "file_id": "f123",
                "file_name": "data.parquet",
                "file_format": "parquet",
                "size_bytes": 1048576,
                "row_count": 5000,
                "upload_status": "uploaded",
                "table_name": "Entity",
                "created_at": "2025-10-28T10:00:00Z",
                "uploaded_at": "2025-10-28T10:01:30Z",
              }
            ],
            "total_files": 1,
            "total_size_bytes": 1048576,
          }
        }
      },
    },
    403: {
      "description": "Access denied - insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "Graph not found",
      "model": ErrorResponse,
    },
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/files", business_event_type="files_listed"
)
async def list_files(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  table_name: str | None = Query(
    default=None, description="Filter by table name (optional)"
  ),
  file_status: str | None = Query(
    default=None, description="Filter by upload status (optional)", alias="status"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> ListTableFilesResponse:
  """
  List all files in the graph with optional filtering.

  Files are first-class resources queried at graph level, with optional
  filtering by table or status.
  """
  start_time = datetime.now(timezone.utc)

  try:
    repository = await get_universal_repository(graph_id, "read")

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    api_logger.info(
      "Listing files",
      extra={
        "component": "files_api",
        "action": "list_files_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_filter": table_name,
        "status_filter": file_status,
      },
    )

    if table_name:
      table = GraphTable.get_by_name(graph_id, table_name, db)
      if not table:
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"Table {table_name} not found in graph {graph_id}",
        )
      files = GraphFile.get_all_for_table(table.id, db)
    else:
      files = db.query(GraphFile).filter(GraphFile.graph_id == graph_id).all()

    if file_status:
      files = [f for f in files if f.upload_status == file_status]

    total_size = sum(f.file_size_bytes for f in files)
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/files",
      method="GET",
      event_type="files_listed_successfully",
      event_data={
        "graph_id": graph_id,
        "table_filter": table_name,
        "status_filter": file_status,
        "file_count": len(files),
        "total_size_bytes": total_size,
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "Files listed successfully",
      extra={
        "component": "files_api",
        "action": "list_files_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "table_filter": table_name,
        "duration_ms": execution_time,
        "file_count": len(files),
        "success": True,
      },
    )

    return ListTableFilesResponse(
      graph_id=graph_id,
      table_name=table_name,
      files=[
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
      total_files=len(files),
      total_size_bytes=total_size,
    )

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    api_logger.error(
      "Failed to list files",
      extra={
        "component": "files_api",
        "action": "list_files_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list files: {str(e)}",
    )


@router.get(
  "/files/{file_id}",
  response_model=GetFileInfoResponse,
  operation_id="getFile",
  summary="Get File Information",
  description="""Get detailed information about a specific file.

Retrieve comprehensive metadata for a single file by file_id, independent of
table context. Files are first-class resources with complete lifecycle tracking.

**Returned Information:**
- File ID, name, format, size
- Upload status and timestamps
- **Enhanced Multi-Layer Status** (new in this version):
  - S3 layer: upload_status, uploaded_at, size_bytes, row_count
  - DuckDB layer: duckdb_status, duckdb_staged_at, duckdb_row_count
  - Graph layer: graph_status, graph_ingested_at
- Table association
- S3 location

**Multi-Layer Pipeline Visibility:**
The `layers` object provides independent status tracking across the three-tier
data pipeline:
- **S3 (Immutable Source)**: File upload and validation
- **DuckDB (Mutable Staging)**: Immediate queryability with file provenance
- **Graph (Immutable View)**: Optional graph database materialization

Each layer shows its own status, timestamp, and row count (where applicable),
enabling precise debugging and monitoring of the data ingestion flow.

**Use Cases:**
- Validate file upload completion
- Monitor multi-layer ingestion progress in real-time
- Debug upload or staging issues at specific layers
- Verify file metadata and row counts
- Track file provenance through the pipeline
- Identify bottlenecks in the ingestion process

**Note:**
File info retrieval is included - no credit consumption""",
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
            "created_at": "2025-10-28T10:00:00Z",
            "uploaded_at": "2025-10-28T10:01:30Z",
            "layers": {
              "s3": {
                "status": "uploaded",
                "timestamp": "2025-10-28T10:01:30Z",
                "row_count": 5000,
                "size_bytes": 1048576,
              },
              "duckdb": {
                "status": "staged",
                "timestamp": "2025-10-28T10:02:15Z",
                "row_count": 5000,
                "size_bytes": None,
              },
              "graph": {
                "status": "ingested",
                "timestamp": "2025-10-28T10:05:45Z",
                "row_count": None,
                "size_bytes": None,
              },
            },
          }
        }
      },
    },
    403: {
      "description": "Access denied - insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "File not found",
      "model": ErrorResponse,
    },
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/files/{file_id}", business_event_type="file_retrieved"
)
async def get_file(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  file_id: str = Path(..., description="File ID"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> GetFileInfoResponse:
  """
  Get detailed information about a specific file.

  Returns comprehensive file metadata by file_id, independent of table context.
  """
  start_time = datetime.now(timezone.utc)

  try:
    repository = await get_universal_repository(graph_id, "read")

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
      endpoint="/v1/graphs/{graph_id}/files/{file_id}",
      method="GET",
      event_type="file_retrieved_successfully",
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

    layers = EnhancedFileStatusLayers(
      s3=FileLayerStatus(
        status=file.upload_status,
        timestamp=file.uploaded_at.isoformat() if file.uploaded_at else None,
        row_count=file.row_count,
        size_bytes=file.file_size_bytes,
      ),
      duckdb=FileLayerStatus(
        status=file.duckdb_status,
        timestamp=file.duckdb_staged_at.isoformat() if file.duckdb_staged_at else None,
        row_count=file.duckdb_row_count,
        size_bytes=None,
      ),
      graph=FileLayerStatus(
        status=file.graph_status,
        timestamp=file.graph_ingested_at.isoformat()
        if file.graph_ingested_at
        else None,
        row_count=None,
        size_bytes=None,
      ),
    )

    return GetFileInfoResponse(
      file_id=file.id,
      graph_id=file.graph_id,
      table_id=file.table_id,
      table_name=table.table_name if table else None,
      file_name=file.file_name,
      file_format=file.file_format,
      size_bytes=file.file_size_bytes,
      row_count=file.row_count,
      upload_status=file.upload_status,
      upload_method=file.upload_method,
      created_at=file.created_at.isoformat() if file.created_at else None,
      uploaded_at=file.uploaded_at.isoformat() if file.uploaded_at else None,
      s3_key=file.s3_key,
      layers=layers,
    )

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    api_logger.error(
      "Failed to get file",
      extra={
        "component": "files_api",
        "action": "get_file_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
      },
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve file: {str(e)}",
    )


@router.delete(
  "/files/{file_id}",
  response_model=DeleteFileResponse,
  operation_id="deleteFile",
  summary="Delete File",
  description="""Delete file from all layers.

Remove file from S3, database tracking, and optionally from DuckDB and graph.
Files are deleted by file_id, independent of table context.

**Query Parameters:**
- `cascade` (optional, default=false): Delete from all layers including DuckDB

**What Happens (cascade=false):**
1. File deleted from S3
2. Database record removed
3. Table statistics updated

**What Happens (cascade=true):**
1. File data deleted from all DuckDB tables (by file_id)
2. Graph marked as stale
3. File deleted from S3
4. Database record removed
5. Table statistics updated

**Use Cases:**
- Remove incorrect or duplicate files
- Clean up failed uploads
- Delete files before graph ingestion
- Surgical data removal with cascade

**Security:**
- Write access required
- Shared repositories block deletions
- Full audit trail

**Important:**
- Use cascade=true for immediate DuckDB cleanup
- Graph rebuild recommended after cascade deletion
- File deletion is included - no credit consumption""",
  responses={
    200: {
      "description": "File deleted successfully",
      "content": {
        "application/json": {
          "example": {
            "status": "deleted",
            "file_id": "f123",
            "file_name": "data.parquet",
            "message": "File deleted successfully. Removed data from 2 DuckDB table(s). Graph marked as stale - rebuild recommended.",
            "cascade_deleted": True,
            "tables_affected": ["Fact", "Element"],
            "graph_marked_stale": True,
          }
        }
      },
    },
    403: {
      "description": "Access denied - shared repositories or insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "File not found",
      "model": ErrorResponse,
    },
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/files/{file_id}", business_event_type="file_deleted"
)
async def delete_file(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  file_id: str = Path(..., description="File ID"),
  cascade: bool = Query(
    default=False,
    description="If true, delete from all layers including DuckDB and mark graph stale",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> DeleteFileResponse:
  """
  Delete file from all layers.

  Removes file by file_id, independent of table context. Supports cascade
  deletion across S3, DuckDB, and graph layers.
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
    repository = await get_universal_repository(graph_id, "write")

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
        "component": "files_api",
        "action": "delete_file_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "file_name": file_name,
        "file_size_bytes": file_size,
        "cascade": cascade,
      },
    )

    tables_affected = []
    graph_marked_stale = False

    if cascade:
      logger.info(
        f"Cascade deletion enabled for file {file_id} - deleting from DuckDB tables"
      )

      from robosystems.graph_api.client.factory import get_graph_client

      client = await get_graph_client(graph_id=graph_id, operation_type="write")
      all_tables = GraphTable.get_all_for_graph(graph_id, db)

      for table in all_tables:
        try:
          result = await client.delete_file_data(
            graph_id=graph_id, table_name=table.table_name, file_id=file_id
          )
          if result.get("rows_deleted", 0) > 0:
            tables_affected.append(table.table_name)
            logger.info(
              f"Deleted {result['rows_deleted']} rows from table {table.table_name}"
            )
        except Exception as e:
          logger.warning(
            f"Failed to delete file data from table {table.table_name}: {e}"
          )

      if tables_affected:
        graph = Graph.get_by_id(graph_id, db)
        if graph:
          graph.mark_stale(
            session=db,
            reason=f"file_deleted: {file_name} from tables {', '.join(tables_affected)}",
          )
          graph_marked_stale = True
          logger.info(f"Marked graph {graph_id} as stale due to file deletion")

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
          f
          for f in all_files
          if f.id != file.id and f.upload_status == FileUploadStatus.UPLOADED.value
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
      endpoint="/v1/graphs/{graph_id}/files/{file_id}",
      method="DELETE",
      event_type="file_deleted_successfully",
      event_data={
        "graph_id": graph_id,
        "file_id": file_id,
        "file_name": file_name,
        "file_size_bytes": file_size,
        "table_name": table.table_name if table else None,
        "cascade": cascade,
        "tables_affected_count": len(tables_affected),
        "graph_marked_stale": graph_marked_stale,
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "File deleted successfully",
      extra={
        "component": "files_api",
        "action": "delete_file_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "file_id": file_id,
        "file_name": file_name,
        "cascade": cascade,
        "tables_affected_count": len(tables_affected),
        "graph_marked_stale": graph_marked_stale,
        "duration_ms": execution_time,
        "success": True,
      },
    )

    logger.info(
      f"Deleted file {file_id} ({file_name}) from graph {graph_id} - "
      f"DuckDB will automatically exclude it from queries"
    )

    message = "File deleted successfully."
    if cascade and tables_affected:
      message += f" Removed data from {len(tables_affected)} DuckDB table(s)."
    if graph_marked_stale:
      message += " Graph marked as stale - rebuild recommended."
    elif not cascade:
      message += " DuckDB will automatically exclude it from queries."

    return DeleteFileResponse(
      status="deleted",
      file_id=file_id,
      file_name=file_name,
      message=message,
      cascade_deleted=cascade,
      tables_affected=tables_affected if cascade else None,
      graph_marked_stale=graph_marked_stale,
    )

  except HTTPException:
    raise

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/files/{file_id}",
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
        "component": "files_api",
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
