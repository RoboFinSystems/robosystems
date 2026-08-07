"""DuckDB staging table listing endpoint."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.tables import TableInfo, TableListResponse
from robosystems.models.core import GraphTable, User

router = APIRouter()


@router.get(
  "/tables",
  response_model=TableListResponse,
  operation_id="listTables",
  summary="List Staging Tables",
  description="Returns file count, storage size, row count, and S3 location per table. Tables with `file_count=0` are skipped during ingestion.",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables", business_event_type="tables_listed"
)
async def list_tables(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> TableListResponse:
  start_time = datetime.now(UTC)

  try:
    # Verify graph access
    repository = await get_universal_repository(graph_id, "read")

    if not repository:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Graph {graph_id} not found",
      )

    # Log structured operation
    api_logger.info(
      "Listing staging tables",
      extra={
        "component": "tables_api",
        "action": "list_tables_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/tables",
        },
      },
    )

    db_tables = GraphTable.get_all_for_graph(graph_id, db)

    from robosystems.models.core import GraphUser
    from robosystems.operations.graph.table_service import TableService

    table_service = TableService(db)
    # S3 paths are namespaced by the graph creator — the oldest access row.
    # Ordering matters once a graph has multiple users.
    user_graph = (
      db.query(GraphUser)
      .filter(GraphUser.graph_id == graph_id)
      .order_by(GraphUser.created_at.asc())
      .first()
    )
    user_id = user_graph.user_id if user_graph else "unknown"

    tables = [
      TableInfo(
        table_name=table.table_name,
        row_count=table.row_count or 0,
        file_count=table.file_count or 0,
        total_size_bytes=table.total_size_bytes or 0,
        s3_location=table_service.get_s3_pattern_for_table(
          graph_id=graph_id,
          table_name=table.table_name,
          user_id=user_id,
        ),
      )
      for table in db_tables
    ]

    # Calculate execution time
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables",
      method="GET",
      event_type="tables_listed_successfully",
      event_data={
        "graph_id": graph_id,
        "table_count": len(tables),
        "total_files": sum(t.file_count for t in tables),
        "total_storage_bytes": sum(t.total_size_bytes for t in tables),
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    # Log structured completion
    api_logger.info(
      "Tables listed successfully",
      extra={
        "component": "tables_api",
        "action": "list_tables_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time,
        "table_count": len(tables),
        "success": True,
      },
    )

    return TableListResponse(tables=tables, total_count=len(tables))

  except HTTPException:
    raise

  except Exception as e:
    # Record business event for failure
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/tables",
      method="GET",
      event_type="list_tables_failed",
      event_data={
        "graph_id": graph_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
      user_id=current_user.id,
    )

    logger.error(
      f"Failed to list tables for graph {graph_id}: {e}",
      extra={
        "component": "tables_api",
        "action": "list_tables_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "error_type": type(e).__name__,
      },
      exc_info=True,
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list tables.",
    )
