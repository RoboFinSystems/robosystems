"""Graph file listing, retrieval, and deletion endpoints."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import api_logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import (
  GRAPH_OR_SUBGRAPH_ID_PATTERN,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.tables import (
  EnhancedFileStatusLayers,
  FileLayerStatus,
  GetFileInfoResponse,
  ListTableFilesResponse,
)
from robosystems.models.core import GraphFile, GraphTable, User

router = APIRouter()


@router.get(
  "/files",
  response_model=ListTableFilesResponse,
  operation_id="listFiles",
  summary="List Files in Graph",
  description="Filter by `table_name` or upload `status`. Files are graph-scoped resources with S3 → DuckDB → Graph lifecycle tracking.",
  responses={**RESOURCE_ERROR_RESPONSES},
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
  start_time = datetime.now(UTC)

  # Enforce graph lifecycle and subscription status (read operation)
  from robosystems.middleware.billing.enforcement import require_graph_access

  require_graph_access(graph_id, db, require_write=False)

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
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

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
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

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
      exc_info=True,
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list files.",
    )


@router.get(
  "/files/{file_id}",
  response_model=GetFileInfoResponse,
  operation_id="getFile",
  summary="Get File Information",
  description="Returns per-layer status (`layers.s3`, `layers.duckdb`, `layers.graph`) for pipeline debugging.",
  responses={**RESOURCE_ERROR_RESPONSES},
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
  start_time = datetime.now(UTC)

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
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

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
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

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
      exc_info=True,
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve file.",
    )
