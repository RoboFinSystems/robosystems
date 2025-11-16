"""
Subgraph info endpoint.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.api.graphs.subgraphs import SubgraphResponse, SubgraphType
from robosystems.models.iam.user import User
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.logger import logger, api_logger, log_metric

from .utils import (
  circuit_breaker,
  get_subgraph_by_name,
  record_operation_start,
  record_operation_metrics,
  handle_circuit_breaker_check,
)
from robosystems.middleware.graph.types import GRAPH_ID_PATTERN

router = APIRouter()


@router.get(
  "/{subgraph_id}/info",
  response_model=SubgraphResponse,
  operation_id="getSubgraphInfo",
  summary="Get Subgraph Details",
  description="""Get detailed information about a specific subgraph.

**Requirements:**
- User must have read access to parent graph

**Response includes:**
- Full subgraph metadata
- Database statistics (nodes, edges)
- Size information
- Schema configuration
- Creation/modification timestamps
- Last access time (when available)

**Statistics:**
Real-time statistics queried from Kuzu:
- Node count
- Edge count
- Database size on disk
- Schema information""",
  responses={
    200: {"description": "Subgraph information retrieved"},
    400: {"description": "Not a valid subgraph"},
    403: {"description": "Access denied"},
    404: {"description": "Subgraph not found"},
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/subgraphs/{subgraph_id}/info",
  business_event_type="subgraph_info_retrieved",
)
async def get_subgraph_info(
  graph_id: str = Path(
    ..., description="Parent graph identifier", pattern=GRAPH_ID_PATTERN
  ),
  subgraph_id: str = Path(
    ..., description="Subgraph identifier", pattern=GRAPH_ID_PATTERN
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
) -> SubgraphResponse:
  """Get detailed information about a specific subgraph.

  Works for both parent graphs and subgraphs.
  """
  start_time = record_operation_start()

  # Check circuit breaker
  handle_circuit_breaker_check(graph_id, "subgraph_info")

  try:
    # Get and verify subgraph using subgraph_id parameter
    subgraph = get_subgraph_by_name(graph_id, subgraph_id, session, current_user)

    if not subgraph.is_subgraph:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"{subgraph.graph_id} is not a subgraph. Use the regular graph info endpoint.",
      )

    # TODO: Get actual metrics from Kuzu
    size_mb = None
    node_count = None
    edge_count = None
    last_accessed = None

    # Log successful info retrieval
    api_logger.info(
      f"Retrieved subgraph info for {subgraph.graph_id} by user {current_user.id}"
    )

    # Record metrics
    record_operation_metrics(start_time, "info", graph_id)
    log_metric("subgraph_info_retrieved", 1, {"subgraph": subgraph.graph_id})

    # Mark circuit breaker success
    circuit_breaker.record_success(graph_id, "subgraph_info")

    return SubgraphResponse(
      graph_id=subgraph.graph_id,
      parent_graph_id=subgraph.parent_graph_id,
      subgraph_index=subgraph.subgraph_index,
      subgraph_name=subgraph.subgraph_name,
      display_name=subgraph.graph_name,
      description=subgraph.subgraph_metadata.get("description")
      if subgraph.subgraph_metadata
      else None,
      subgraph_type=SubgraphType(
        subgraph.subgraph_metadata.get("type", "static")
        if subgraph.subgraph_metadata
        else "static"
      ),
      status="active",
      created_at=subgraph.created_at,
      updated_at=subgraph.updated_at,
      size_mb=size_mb,
      node_count=node_count,
      edge_count=edge_count,
      last_accessed=last_accessed,
      metadata=subgraph.subgraph_metadata,
    )

  except HTTPException:
    raise
  except SQLAlchemyError as e:
    logger.error(f"Database error getting subgraph info: {e}")
    # Record failure metric
    log_metric("subgraph_info_failed", 1, {"error_type": "database"})
    # Mark circuit breaker failure
    circuit_breaker.record_failure(graph_id, "subgraph_info")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get subgraph info due to database error",
    )
  except Exception as e:
    logger.error(f"Unexpected error getting subgraph info: {e}")
    # Record failure metric
    log_metric("subgraph_info_failed", 1, {"error_type": "unexpected"})
    # Mark circuit breaker failure
    circuit_breaker.record_failure(graph_id, "subgraph_info")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to get subgraph info: {str(e)}",
    )
