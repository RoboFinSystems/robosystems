"""Graph usage analytics and monitoring API endpoints."""

import asyncio
import time
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException, status, Query, Path

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.iam import User, GraphUsage
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.models.api.graphs.metrics import (
  GraphMetricsResponse,
  GraphUsageResponse,
  StorageSummary,
  CreditSummary,
  PerformanceInsights,
)
from robosystems.operations.graph.metrics_service import GraphMetricsService
from robosystems.database import get_db_session
from sqlalchemy.orm import Session
from robosystems.logger import logger
from robosystems.models.api.common import ErrorResponse

from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
  OperationType,
  OperationStatus,
  record_operation_metric,
  get_operation_logger,
)


router = APIRouter(tags=["Usage"])

graph_metrics_service = GraphMetricsService()


@router.get(
  "",
  response_model=GraphMetricsResponse,
  summary="Get Graph Metrics",
  description="""Get comprehensive metrics for the graph database.

Provides detailed analytics including:
- **Node Statistics**: Counts by type (Entity, Report, Account, Transaction)
- **Relationship Metrics**: Connection counts and patterns
- **Data Quality**: Completeness scores and validation results
- **Performance Metrics**: Query response times and database health
- **Storage Analytics**: Database size and growth trends

This data helps with:
- Monitoring data completeness
- Identifying data quality issues
- Capacity planning
- Performance optimization

Note:
This operation is included - no credit consumption required.""",
  status_code=status.HTTP_200_OK,
  operation_id="getGraphMetrics",
  responses={
    200: {
      "description": "Graph metrics retrieved successfully",
      "model": GraphMetricsResponse,
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {
      "description": "Graph not found or metrics unavailable",
      "model": ErrorResponse,
    },
    500: {"description": "Failed to retrieve metrics", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/analytics",
  business_event_type="graph_metrics_accessed",
)
async def get_graph_metrics(
  graph_id: str = Path(..., description="The graph ID to get metrics for"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphMetricsResponse:
  """
  Get detailed metrics for the specified graph.

  This endpoint provides comprehensive metrics including:
  - Node counts by label
  - Relationship counts by type
  - Database size estimates
  - Health status
  """
  circuit_breaker = CircuitBreakerManager()
  timeout_coordinator = TimeoutCoordinator()
  operation_logger = get_operation_logger()

  operation_start_time = time.time()
  operation_timeout = 30.0

  record_operation_metric(
    operation_type=OperationType.ANALYTICS_QUERY,
    status=OperationStatus.SUCCESS,
    duration_ms=0.0,
    endpoint="/v1/graphs/{graph_id}/analytics",
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
      endpoint="/v1/graphs/{graph_id}/analytics",
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
      endpoint=f"/v1/graphs/{graph_id}/analytics",
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
      endpoint="/v1/graphs/{graph_id}/analytics",
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

  except asyncio.TimeoutError:
    circuit_breaker.record_failure(graph_id, "analytics_metrics")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/analytics",
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
      endpoint="/v1/graphs/{graph_id}/analytics",
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
      endpoint="/v1/graphs/{graph_id}/analytics",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_graph_metrics",
      metadata={
        "analytics_type": "comprehensive_metrics",
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    logger.error(f"Error getting graph metrics for user {current_user.id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve graph metrics: {str(e)}",
    )


@router.get(
  "/usage",
  response_model=GraphUsageResponse,
  summary="Get Graph Usage Analytics",
  description="""Get comprehensive usage analytics tracked by the GraphUsage model.

Provides temporal usage patterns including:
- **Storage Analytics**: GB-hours for billing, breakdown by type (files, tables, graphs, subgraphs)
- **Credit Analytics**: Consumption patterns, operation breakdown, cached vs billable
- **Performance Insights**: Operation stats, slow queries, performance scoring
- **Recent Events**: Latest usage events with full details

Time ranges available:
- `24h` - Last 24 hours (hourly breakdown)
- `7d` - Last 7 days (daily breakdown)
- `30d` - Last 30 days (daily breakdown)
- `current_month` - Current billing month
- `last_month` - Previous billing month

Include options:
- `storage` - Storage usage summary (GB-hours, averages, peaks)
- `credits` - Credit consumption analytics
- `performance` - Performance insights and optimization opportunities
- `events` - Recent usage events (last 50)

Useful for:
- Billing and cost analysis
- Capacity planning
- Performance optimization
- Usage trend analysis

Note:
This operation is included - no credit consumption required.""",
  status_code=status.HTTP_200_OK,
  operation_id="getGraphUsageAnalytics",
  responses={
    200: {
      "description": "Usage analytics retrieved successfully",
      "model": GraphUsageResponse,
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve usage analytics", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/usage",
  business_event_type="graph_usage_accessed",
)
async def get_graph_usage_analytics(
  graph_id: str = Path(..., description="The graph ID to get usage analytics for"),
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
  """
  Get comprehensive usage analytics from GraphUsage model.

  This endpoint queries the graph_usage table for:
  - Storage usage summaries (GB-hours, breakdown by type)
  - Credit consumption analytics (operation breakdown, cached vs billable)
  - Performance insights (slow queries, optimization opportunities)
  - Recent usage events (last 50 events)
  """
  circuit_breaker = CircuitBreakerManager()
  timeout_coordinator = TimeoutCoordinator()
  operation_logger = get_operation_logger()

  operation_start_time = time.time()
  operation_timeout = 30.0

  record_operation_metric(
    operation_type=OperationType.ANALYTICS_QUERY,
    status=OperationStatus.SUCCESS,
    duration_ms=0.0,
    endpoint="/v1/graphs/{graph_id}/usage",
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
      endpoint="/v1/graphs/{graph_id}/usage",
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

    now = datetime.now(timezone.utc)
    year, month = _parse_time_range(time_range, now)

    storage_summary = None
    credit_summary = None
    performance_insights = None
    recent_events = []

    if include_storage:
      storage_data = GraphUsage.get_monthly_storage_summary(
        user_id=current_user.id,
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
          total_gb_hours=graph_storage["total_gb_hours"],
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
      endpoint=f"/v1/graphs/{graph_id}/usage",
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
      endpoint="/v1/graphs/{graph_id}/usage",
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

  except asyncio.TimeoutError:
    circuit_breaker.record_failure(graph_id, "analytics_usage")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/usage",
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
      endpoint="/v1/graphs/{graph_id}/usage",
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
      endpoint="/v1/graphs/{graph_id}/usage",
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

    logger.error(f"Error getting usage analytics for graph {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve usage analytics: {str(e)}",
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
    now = datetime.now(timezone.utc)
    return now.day
  elif time_range == "last_month":
    now = datetime.now(timezone.utc)
    last_month = now - timedelta(days=now.day)
    return (now - last_month).days
  else:
    return 30
