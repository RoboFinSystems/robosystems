"""
Subgraph info endpoint.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.logger import api_logger, log_metric, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_ID_PATTERN, SUBGRAPH_NAME_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.subgraphs import SubgraphResponse, SubgraphType
from robosystems.models.core.user import User

from .utils import (
  circuit_breaker,
  get_subgraph_by_name,
  handle_circuit_breaker_check,
  record_operation_metrics,
  record_operation_start,
)

router = APIRouter(dependencies=[Depends(subscription_aware_rate_limit_dependency)])


@router.get(
  "/{subgraph_name}",
  response_model=SubgraphResponse,
  operation_id="getSubgraphInfo",
  summary="Get Subgraph Details",
  description="Pass the subgraph name (e.g., `dev`) not the full subgraph ID (e.g., `kg0123_dev`).",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/subgraphs/{subgraph_id}/info",
  business_event_type="subgraph_info_retrieved",
)
async def get_subgraph_info(
  graph_id: str = Path(
    ..., description="Parent graph identifier", pattern=GRAPH_ID_PATTERN
  ),
  subgraph_name: str = Path(
    ...,
    description="Subgraph name (e.g., 'dev', 'staging')",
    pattern=SUBGRAPH_NAME_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
) -> SubgraphResponse:
  start_time = record_operation_start()

  # Check circuit breaker
  handle_circuit_breaker_check(graph_id, "subgraph_info")

  try:
    # Get and verify subgraph using subgraph name
    subgraph = get_subgraph_by_name(graph_id, subgraph_name, session, current_user)

    if not subgraph.is_subgraph:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"{subgraph.graph_id} is not a subgraph. Use the regular graph info endpoint.",
      )

    # TODO: Get actual metrics from LadybugDB
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
    logger.error(f"Unexpected error getting subgraph info: {e}", exc_info=True)
    # Record failure metric
    log_metric("subgraph_info_failed", 1, {"error_type": "unexpected"})
    # Mark circuit breaker failure
    circuit_breaker.record_failure(graph_id, "subgraph_info")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get subgraph info.",
    )
