from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User
from robosystems.models.api.table import TableIngestRequest, TableIngestResponse
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.graph.types import GraphTypeRegistry

router = APIRouter()


@router.post(
  "/tables/ingest",
  response_model=TableIngestResponse,
  summary="Ingest Table to Graph",
  description="Ingest a DuckDB staging table directly into the Kuzu graph database",
  responses={
    200: {"description": "Ingestion completed successfully"},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
    500: {"description": "Ingestion failed", "model": ErrorResponse},
  },
)
async def ingest_table(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: TableIngestRequest = Body(..., description="Ingestion request"),
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> TableIngestResponse:
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories are read-only. File uploads and data ingestion are not allowed. "
      "Shared repositories provide reference data that cannot be modified.",
    )

  graph, repo = await get_universal_repository_with_auth(graph_id, current_user.id, db)

  if not graph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  logger.info(
    f"Ingesting table {request.table_name} from DuckDB to Kuzu graph {graph_id}"
  )

  from robosystems.graph_api.client.factory import get_graph_client

  client = await get_graph_client(graph_id=graph_id, operation_type="write")

  try:
    response = await client.ingest_table_to_graph(
      graph_id=graph_id,
      table_name=request.table_name,
      ignore_errors=request.ignore_errors,
      rebuild=False,
    )

    return TableIngestResponse(**response)

  except Exception as e:
    logger.error(f"Failed to ingest table {request.table_name}: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to ingest table: {str(e)}",
    )
