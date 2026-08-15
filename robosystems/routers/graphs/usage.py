"""Graph content metrics and consumption usage API endpoints.

Two distinct questions, two routes:

- ``GET /metrics`` — what is *in* the graph (node/relationship counts, size, health)
- ``GET /usage`` — what the graph *consumed* (storage, credits, performance, events)

Neither is analytics: ``/analytics`` is reserved for the BI surface.
"""

import asyncio
import time
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  OperationStatus,
  OperationType,
  TimeoutCoordinator,
  get_operation_logger,
  record_operation_metric,
)
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.metrics import (
  CreditSummary,
  GraphMetricsResponse,
  GraphUsageResponse,
  PerformanceInsights,
  StorageSummary,
)
from robosystems.models.core import GraphUsage, User
from robosystems.operations.graph.metrics_service import GraphMetricsService

router = APIRouter(tags=["Usage"])

_METRICS_ENDPOINT = "/v1/graphs/{graph_id}/metrics"
_USAGE_ENDPOINT = "/v1/graphs/{graph_id}/usage"

graph_metrics_service = GraphMetricsService()


@router.get(
  "/metrics",
  response_model=GraphMetricsResponse,
  summary="Get Graph Metrics",
  operation_id="getGraphMetrics",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name=_METRICS_ENDPOINT,
  business_event_type="graph_metrics_accessed",
)
async def get_graph_metrics(
  graph_id: str = Path(
    ...,
    description="The graph ID to get metrics for",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphMetricsResponse:
  # Metrics are per-label/per-type COUNTs — a full scan each — issued against
  # the graph's WRITE node. On a shared repository that is the shared master
  # (asleep most of the day, and the corpus is hundreds of millions of rows),
  # so the call either times out or lands a fleet of full scans on the node
  # that materializes. Shared repositories are platform-managed; their size is
  # published elsewhere. Refuse up front, before the circuit breaker records
  # a failure against the graph.
  if MultiTenantUtils.is_shared_repository_or_subgraph(graph_id.lower()):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=(
        f"Metrics are not available for shared repository '{graph_id}'. "
        "Repository sizes are platform-managed and published with the repository."
      ),
    )

  circuit_breaker = CircuitBreakerManager()
  timeout_coordinator = TimeoutCoordinator()
  operation_logger = get_operation_logger()

  operation_start_time = time.time()
  operation_timeout = 30.0

  record_operation_metric(
    operation_type=OperationType.ANALYTICS_QUERY,
    status=OperationStatus.SUCCESS,
    duration_ms=0.0,
    endpoint=_METRICS_ENDPOINT,
    graph_id=graph_id,
    user_id=current_user.id,
    operation_name="get_graph_metrics",
    metadata={
      "analytics_type": "comprehensive_metrics",
    },
  )

  operation_timeout = 30.0

  try:
    circuit_breaker.check_circuit(graph_id, "analytics_metrics")

    operation_timeout = timeout_coordinator.calculate_timeout(
      operation_type="analytics_query",
      complexity_factors={
        "operation": "comprehensive_metrics",
        "is_comprehensive": True,
        "expected_complexity": "high",
      },
    )

    operation_logger.log_external_service_call(
      endpoint=_METRICS_ENDPOINT,
      service_name="graph_metrics_service",
      operation="collect_comprehensive_metrics",
      duration_ms=0.0,
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "analytics_type": "comprehensive_metrics",
      },
    )

    metrics = await asyncio.wait_for(
      graph_metrics_service.collect_metrics_for_graph_async(graph_id),
      timeout=operation_timeout,
    )

    if not metrics or "error" in metrics:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Metrics not available for graph {graph_id}",
      )

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint=_METRICS_ENDPOINT,
      method="GET",
      event_type="graph_metrics_accessed",
      event_data={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "total_nodes": metrics.get("total_nodes", 0),
        "total_relationships": metrics.get("total_relationships", 0),
      },
      user_id=current_user.id,
    )

    operation_duration_ms = (time.time() - operation_start_time) * 1000
    circuit_breaker.record_success(graph_id, "analytics_metrics")

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.SUCCESS,
      duration_ms=operation_duration_ms,
      endpoint=_METRICS_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_graph_metrics",
      metadata={
        "analytics_type": "comprehensive_metrics",
        "total_nodes": metrics.get("total_nodes", 0) if "metrics" in locals() else 0,
        "total_relationships": metrics.get("total_relationships", 0)
        if "metrics" in locals()
        else 0,
      },
    )

    return GraphMetricsResponse(**metrics)

  except TimeoutError:
    circuit_breaker.record_failure(graph_id, "analytics_metrics")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint=_METRICS_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_graph_metrics",
      metadata={
        "analytics_type": "comprehensive_metrics",
        "error_type": "timeout",
        "timeout_seconds": operation_timeout
        if "operation_timeout" in locals()
        else None,
      },
    )

    logger.error(
      f"Analytics operation timeout after {operation_timeout}s for user {current_user.id}"
    )
    raise HTTPException(status_code=504, detail="Analytics operation timed out")
  except HTTPException:
    circuit_breaker.record_failure(graph_id, "analytics_metrics")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint=_METRICS_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_graph_metrics",
      metadata={
        "analytics_type": "comprehensive_metrics",
        "error_type": "http_exception",
      },
    )
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "analytics_metrics")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint=_METRICS_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_graph_metrics",
      metadata={
        "analytics_type": "comprehensive_metrics",
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    logger.error(
      f"Error getting graph metrics for user {current_user.id}: {e!s}", exc_info=True
    )
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve graph metrics.",
    )


@router.get(
  "/usage",
  response_model=GraphUsageResponse,
  summary="Get Graph Usage",
  description="Time ranges: 24h, 7d, 30d, current_month, last_month. Toggle storage, credits, performance, and events sections via query params.",
  operation_id="getGraphUsage",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name=_USAGE_ENDPOINT,
  business_event_type="graph_usage_accessed",
)
async def get_graph_usage(
  graph_id: str = Path(
    ...,
    description="The graph ID to get usage analytics for",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  time_range: str = Query(
    "30d",
    description="Time range: 24h, 7d, 30d, current_month, last_month",
    pattern="^(24h|7d|30d|current_month|last_month)$",
  ),
  include_storage: bool = Query(True, description="Include storage usage summary"),
  include_credits: bool = Query(True, description="Include credit consumption summary"),
  include_performance: bool = Query(
    False, description="Include performance insights (may be slower)"
  ),
  include_events: bool = Query(False, description="Include recent usage events"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphUsageResponse:
  circuit_breaker = CircuitBreakerManager()
  timeout_coordinator = TimeoutCoordinator()
  operation_logger = get_operation_logger()

  operation_start_time = time.time()
  operation_timeout = 30.0

  record_operation_metric(
    operation_type=OperationType.ANALYTICS_QUERY,
    status=OperationStatus.SUCCESS,
    duration_ms=0.0,
    endpoint=_USAGE_ENDPOINT,
    graph_id=graph_id,
    user_id=current_user.id,
    operation_name="get_usage_analytics",
    metadata={
      "analytics_type": "usage_analytics",
      "time_range": time_range,
      "include_storage": include_storage,
      "include_credits": include_credits,
      "include_performance": include_performance,
      "include_events": include_events,
    },
  )

  operation_timeout = 30.0

  try:
    circuit_breaker.check_circuit(graph_id, "analytics_usage")

    operation_timeout = timeout_coordinator.calculate_timeout(
      operation_type="analytics_query",
      complexity_factors={
        "operation": "usage_analytics",
        "include_performance": include_performance,
        "expected_complexity": "high" if include_performance else "medium",
      },
    )

    operation_logger.log_external_service_call(
      endpoint=_USAGE_ENDPOINT,
      service_name="graph_usage",
      operation="query_usage_analytics",
      duration_ms=0.0,
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "analytics_type": "usage_analytics",
        "time_range": time_range,
      },
    )

    now = datetime.now(UTC)
    year, month = _parse_time_range(time_range, now)

    storage_summary = None
    credit_summary = None
    performance_insights = None
    recent_events = []

    if include_storage:
      storage_data = GraphUsage.get_monthly_storage_summary(
        graph_id=graph_id,
        year=year,
        month=month,
        session=db,
      )

      if graph_id in storage_data:
        graph_storage = storage_data[graph_id]
        storage_summary = StorageSummary(
          graph_tier=graph_storage["graph_tier"],
          avg_storage_gb=graph_storage["avg_storage_gb"],
          max_storage_gb=graph_storage["max_storage_gb"],
          min_storage_gb=graph_storage["min_storage_gb"],
          measurement_count=graph_storage["measurement_count"],
        )

    if include_credits:
      credit_data = GraphUsage.get_monthly_credit_summary(
        user_id=current_user.id,
        year=year,
        month=month,
        session=db,
      )

      if graph_id in credit_data:
        graph_credits = credit_data[graph_id]
        credit_summary = CreditSummary(
          graph_tier=graph_credits["graph_tier"],
          total_credits_consumed=graph_credits["total_credits_consumed"],
          total_base_cost=graph_credits["total_base_cost"],
          operation_breakdown=graph_credits["operation_breakdown"],
          cached_operations=graph_credits["cached_operations"],
          billable_operations=graph_credits["billable_operations"],
          transaction_count=graph_credits["transaction_count"],
        )

    if include_performance:
      performance_days = _get_days_from_time_range(time_range)
      perf_data = GraphUsage.get_performance_insights(
        user_id=current_user.id,
        graph_id=graph_id,
        session=db,
        days=performance_days,
      )

      if "message" not in perf_data:
        performance_insights = PerformanceInsights(
          analysis_period_days=perf_data["analysis_period_days"],
          total_operations=perf_data["total_operations"],
          operation_stats=perf_data["operation_stats"],
          slow_queries=perf_data["slow_queries"],
          performance_score=perf_data["performance_score"],
        )

    if include_events:
      cutoff_date = now - timedelta(days=_get_days_from_time_range(time_range))
      events = (
        db.query(GraphUsage)
        .filter(
          GraphUsage.user_id == current_user.id,
          GraphUsage.graph_id == graph_id,
          GraphUsage.recorded_at >= cutoff_date,
        )
        .order_by(GraphUsage.recorded_at.desc())
        .limit(50)
        .all()
      )

      recent_events = [event.to_dict() for event in events]

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint=_USAGE_ENDPOINT,
      method="GET",
      event_type="graph_usage_accessed",
      event_data={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "time_range": time_range,
        "include_storage": include_storage,
        "include_credits": include_credits,
        "include_performance": include_performance,
      },
      user_id=current_user.id,
    )

    operation_duration_ms = (time.time() - operation_start_time) * 1000
    circuit_breaker.record_success(graph_id, "analytics_usage")

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.SUCCESS,
      duration_ms=operation_duration_ms,
      endpoint=_USAGE_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_analytics",
      metadata={
        "analytics_type": "usage_analytics",
        "time_range": time_range,
      },
    )

    return GraphUsageResponse(
      graph_id=graph_id,
      time_range=time_range,
      storage_summary=storage_summary,
      credit_summary=credit_summary,
      performance_insights=performance_insights,
      recent_events=recent_events,
      timestamp=now.isoformat(),
    )

  except TimeoutError:
    circuit_breaker.record_failure(graph_id, "analytics_usage")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint=_USAGE_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_analytics",
      metadata={
        "analytics_type": "usage_analytics",
        "time_range": time_range,
        "error_type": "timeout",
        "timeout_seconds": operation_timeout
        if "operation_timeout" in locals()
        else None,
      },
    )

    logger.error(
      f"Usage analytics operation timeout after {operation_timeout}s for user {current_user.id}"
    )
    raise HTTPException(status_code=504, detail="Usage analytics operation timed out")
  except HTTPException:
    circuit_breaker.record_failure(graph_id, "analytics_usage")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint=_USAGE_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_analytics",
      metadata={
        "analytics_type": "usage_analytics",
        "time_range": time_range,
        "error_type": "http_exception",
      },
    )
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "analytics_usage")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint=_USAGE_ENDPOINT,
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_analytics",
      metadata={
        "analytics_type": "usage_analytics",
        "time_range": time_range,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    logger.error(
      f"Error getting usage analytics for graph {graph_id}: {e!s}", exc_info=True
    )
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to retrieve usage analytics.",
    )


def _parse_time_range(time_range: str, now: datetime) -> tuple[int, int]:
  """Parse time range string into year and month for billing queries."""
  if time_range == "current_month":
    return now.year, now.month
  elif time_range == "last_month":
    last_month = now - timedelta(days=now.day)
    return last_month.year, last_month.month
  else:
    return now.year, now.month


def _get_days_from_time_range(time_range: str) -> int:
  """Convert time range string to number of days."""
  if time_range == "24h":
    return 1
  elif time_range == "7d":
    return 7
  elif time_range == "30d":
    return 30
  elif time_range == "current_month":
    now = datetime.now(UTC)
    return now.day
  elif time_range == "last_month":
    now = datetime.now(UTC)
    last_month = now - timedelta(days=now.day)
    return (now - last_month).days
  else:
    return 30
