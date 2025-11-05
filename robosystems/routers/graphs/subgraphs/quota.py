"""
Subgraph quota endpoint.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.api.graphs.subgraphs import SubgraphQuotaResponse
from robosystems.models.iam.graph import Graph
from robosystems.models.iam.user import User
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.logger import logger, api_logger, log_metric

from .utils import (
  circuit_breaker,
  verify_parent_graph_access,
  record_operation_start,
  record_operation_metrics,
  handle_circuit_breaker_check,
)
from robosystems.config.tier_config import get_tier_max_subgraphs

router = APIRouter()


@router.get(
  "/quota",
  response_model=SubgraphQuotaResponse,
  operation_id="getSubgraphQuota",
  summary="Get Subgraph Quota",
  description="""Get subgraph quota and usage information for a parent graph.

**Shows:**
- Current subgraph count
- Maximum allowed subgraphs per tier
- Remaining capacity
- Total size usage across all subgraphs

**Tier Limits:**
- Standard: 0 subgraphs (not supported)
- Enterprise: Configurable limit (default: 10 subgraphs)
- Premium: Unlimited subgraphs
- Limits are defined in deployment configuration

**Size Tracking:**
Provides aggregate size metrics when available.
Individual subgraph sizes shown in list endpoint.""",
  responses={
    200: {"description": "Quota information retrieved"},
    403: {"description": "Access denied to parent graph"},
    404: {"description": "Parent graph not found"},
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/subgraphs/quota", business_event_type="subgraph_quota_checked"
)
async def get_subgraph_quota(
  graph_id: str = Path(..., description="Parent graph identifier"),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
) -> SubgraphQuotaResponse:
  """Get subgraph quota information for a parent graph.

  Shows current usage and limits for subgraphs.
  """
  start_time = record_operation_start()

  # Check circuit breaker
  handle_circuit_breaker_check(graph_id, "subgraph_quota")

  try:
    # Verify parent graph access (read access required)
    parent_graph = verify_parent_graph_access(
      graph_id, current_user, session, required_role="read"
    )

    # Get current subgraphs
    subgraphs = Graph.get_subgraphs(graph_id, session)
    current_count = len(subgraphs)

    # Get max subgraphs from tier configuration
    max_allowed = get_tier_max_subgraphs(parent_graph.graph_tier)

    remaining = None
    if max_allowed is not None:
      remaining = max(0, max_allowed - current_count)

    # TODO: Calculate actual sizes
    total_size_mb = None
    max_size_mb = None

    # Log successful quota check
    api_logger.info(
      f"Checked subgraph quota for parent {graph_id} "
      f"by user {current_user.id}: {current_count}/{max_allowed if max_allowed else 'unlimited'}"
    )

    # Record metrics
    record_operation_metrics(
      start_time, "quota_check", graph_id, {"tier": parent_graph.graph_tier}
    )
    log_metric("subgraph_quota_usage", current_count, {"parent_graph": graph_id})

    # Mark circuit breaker success
    circuit_breaker.record_success(graph_id, "subgraph_quota")

    return SubgraphQuotaResponse(
      parent_graph_id=graph_id,
      tier=parent_graph.graph_tier,
      current_count=current_count,
      max_allowed=max_allowed,
      remaining=remaining,
      total_size_mb=total_size_mb,
      max_size_mb=max_size_mb,
    )

  except HTTPException:
    raise
  except SQLAlchemyError as e:
    logger.error(f"Database error getting subgraph quota: {e}")
    # Record failure metric
    log_metric("subgraph_quota_check_failed", 1, {"error_type": "database"})
    # Mark circuit breaker failure
    circuit_breaker.record_failure(graph_id, "subgraph_quota")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get subgraph quota due to database error",
    )
  except Exception as e:
    logger.error(f"Unexpected error getting subgraph quota: {e}")
    # Record failure metric
    log_metric("subgraph_quota_check_failed", 1, {"error_type": "unexpected"})
    # Mark circuit breaker failure
    circuit_breaker.record_failure(graph_id, "subgraph_quota")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to get subgraph quota: {str(e)}",
    )
