from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User, GraphTable
from robosystems.models.api.table import TableInfo, TableListResponse
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_db_session
from robosystems.logger import logger

router = APIRouter()


@router.get(
  "/tables",
  response_model=TableListResponse,
  summary="List Staging Tables",
  description="List all DuckDB staging tables for a graph",
  responses={
    200: {"description": "Tables retrieved successfully"},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
  },
)
async def list_tables(
  graph_id: str = Path(..., description="Graph database identifier"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> TableListResponse:
  repository = await get_universal_repository_with_auth(
    graph_id, current_user, "read", db
  )

  if not repository:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  logger.info(f"Listing tables for graph {graph_id}")

  try:
    db_tables = GraphTable.get_all_for_graph(graph_id, db)

    from robosystems.operations.graph.table_service import TableService
    from robosystems.models.iam import UserGraph

    table_service = TableService(db)
    user_graph = db.query(UserGraph).filter(UserGraph.graph_id == graph_id).first()
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

    return TableListResponse(tables=tables, total_count=len(tables))

  except Exception as e:
    logger.error(f"Failed to list tables for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list tables: {str(e)}",
    )
