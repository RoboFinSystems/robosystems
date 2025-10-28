from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User
from robosystems.models.api.table import TableQueryRequest, TableQueryResponse
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.graph.types import GraphTypeRegistry

router = APIRouter()


@router.post(
  "/tables/query",
  response_model=TableQueryResponse,
  summary="Query Staging Tables with SQL",
  description="Execute SQL queries on DuckDB staging tables",
  responses={
    200: {"description": "Query executed successfully"},
    400: {"description": "Invalid SQL query", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
  },
)
async def query_tables(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: TableQueryRequest = Body(..., description="SQL query request"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> TableQueryResponse:
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories do not allow direct SQL table queries. "
      "Use the graph query endpoint (POST /query) to access shared repository data through the structured graph interface.",
    )

  graph, _ = await get_universal_repository_with_auth(graph_id, current_user, db)

  if not graph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  # Log query length but not content (may contain PII)
  logger.info(
    f"Executing SQL query for graph {graph_id} ({len(request.sql)} characters)"
  )

  from robosystems.graph_api.client.factory import get_graph_client

  client = await get_graph_client(graph_id=graph_id, operation_type="read")

  try:
    response = await client.query_table(graph_id=graph_id, sql=request.sql)

    return TableQueryResponse(**response)

  except Exception as e:
    logger.error(f"Query failed for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Query failed: {str(e)}",
    )
