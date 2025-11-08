"""
Staging Tables List Endpoint.

This module provides comprehensive listing of all DuckDB staging tables
for a graph, including detailed metrics on storage, file counts, and
readiness for ingestion.

Key Features:
- Complete table inventory with metrics
- S3 location tracking for each table
- File count and storage size per table
- Row count estimates for capacity planning
- Ready-for-ingestion status indicators

Workflow Integration:
1. Upload files to tables via file upload endpoints
2. List tables to monitor upload progress
3. Validate table metrics and readiness
4. Trigger ingestion when tables are ready

Use Cases:
- Monitor data pipeline status
- Track storage usage across tables
- Identify which tables have uploaded files
- Capacity planning and optimization
- Pre-ingestion validation

Security:
- User can only see their own graph tables
- Read-only operation
- Rate limited per subscription tier
- Full audit logging
"""

from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable
from robosystems.models.api.graphs.tables import TableInfo, TableListResponse
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph import get_universal_repository
from robosystems.database import get_db_session
from robosystems.logger import logger, api_logger
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)

router = APIRouter()


@router.get(
  "/tables",
  response_model=TableListResponse,
  operation_id="listTables",
  summary="List Staging Tables",
  description="""List all DuckDB staging tables with comprehensive metrics and status.

Get a complete inventory of all staging tables for a graph, including
file counts, storage sizes, and row estimates. Essential for monitoring
the data pipeline and determining which tables are ready for ingestion.

**Returned Metrics:**
- Table name and type (node/relationship)
- File count per table
- Total storage size in bytes
- Estimated row count
- S3 location pattern
- Ready-for-ingestion status

**Use Cases:**
- Monitor data upload progress
- Check which tables have files ready
- Track storage consumption
- Validate pipeline before ingestion
- Capacity planning

**Workflow:**
1. List tables to see current state
2. Upload files to empty tables
3. Re-list to verify uploads
4. Check file counts and sizes
5. Ingest when ready

**Important Notes:**
- Tables with `file_count > 0` have data ready
- Check `total_size_bytes` for storage monitoring
- Use `s3_location` to verify upload paths
- Empty tables (file_count=0) are skipped during ingestion
- Table queries are included - no credit consumption""",
  responses={
    200: {
      "description": "Tables retrieved successfully with full metrics",
      "content": {
        "application/json": {
          "example": {
            "tables": [
              {
                "table_name": "Entity",
                "row_count": 5000,
                "file_count": 3,
                "total_size_bytes": 2457600,
                "s3_location": "s3://bucket/staging/Entity/**/*.parquet",
              }
            ],
            "total_count": 1,
          }
        }
      },
    },
    403: {
      "description": "Access denied - insufficient permissions for this graph",
      "model": ErrorResponse,
    },
    404: {"description": "Graph not found", "model": ErrorResponse},
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/tables", business_event_type="tables_listed"
)
async def list_tables(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> TableListResponse:
  """
  List all staging tables for a graph with metrics.

  Returns comprehensive information about all tables including file counts,
  storage sizes, and S3 locations for monitoring the data pipeline.
  """
  start_time = datetime.now(timezone.utc)

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

    # Get all tables for graph
    db_tables = GraphTable.get_all_for_graph(graph_id, db)

    from robosystems.operations.graph.table_service import TableService
    from robosystems.models.iam import GraphUser

    table_service = TableService(db)
    user_graph = db.query(GraphUser).filter(GraphUser.graph_id == graph_id).first()
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
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

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
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list tables: {str(e)}",
    )
