"""Analytics and usage monitoring API endpoints."""

import asyncio
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import DefaultDict

from fastapi import APIRouter, Depends, HTTPException, status, Query, Path

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.iam import User
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.models.api.graph import GraphMetricsResponse, GraphUsageResponse
from robosystems.operations.graph.metrics_service import GraphMetricsService
from robosystems.database import get_db_session
from sqlalchemy.orm import Session
from robosystems.logger import logger
from robosystems.models.api.common import ErrorResponse

# Import robustness middleware
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
  OperationType,
  OperationStatus,
  record_operation_metric,
  get_operation_logger,
)


# Create router for analytics endpoints
router = APIRouter(tags=["Graph Analytics"])

# Initialize services
graph_metrics_service = GraphMetricsService()

# In-memory query analytics tracking
query_analytics_cache: DefaultDict[str, dict] = defaultdict(
  lambda: {
    "total_queries": 0,
    "queries_today": 0,
    "last_query_time": None,
    "total_execution_time": 0.0,
    "query_count_by_day": defaultdict(int),
  }
)


def track_query_execution(graph_id: str, execution_time_ms: float):
  """Track query execution for analytics."""
  from datetime import datetime, timezone

  now = datetime.now(timezone.utc)
  today_key = now.strftime("%Y-%m-%d")

  cache = query_analytics_cache[graph_id]
  cache["total_queries"] += 1
  cache["queries_today"] = cache["query_count_by_day"][today_key] + 1
  cache["query_count_by_day"][today_key] += 1
  cache["last_query_time"] = now.isoformat()
  cache["total_execution_time"] += execution_time_ms


def get_query_analytics(graph_id: str) -> dict:
  """Get query analytics for a specific graph."""
  from datetime import datetime, timezone

  cache = query_analytics_cache[graph_id]
  today_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")

  # Calculate average query time
  avg_time = 0
  if cache["total_queries"] > 0:
    avg_time = cache["total_execution_time"] / cache["total_queries"]

  # Get today's queries from the day-specific counter
  queries_today = cache["query_count_by_day"][today_key]

  return {
    "total_queries": cache["total_queries"],
    "queries_today": queries_today,
    "avg_query_time_ms": round(avg_time, 2),
  }


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
  # Initialize robustness components
  circuit_breaker = CircuitBreakerManager()
  timeout_coordinator = TimeoutCoordinator()
  operation_logger = get_operation_logger()

  # Record operation start and get timing
  operation_start_time = time.time()
  operation_timeout = 30.0  # Initialize to avoid unbound variable error

  # Record operation start metrics
  record_operation_metric(
    operation_type=OperationType.ANALYTICS_QUERY,
    status=OperationStatus.SUCCESS,  # Will be updated on completion
    duration_ms=0.0,  # Will be updated on completion
    endpoint="/v1/graphs/{graph_id}/analytics",
    graph_id=graph_id,
    user_id=current_user.id,
    operation_name="get_graph_metrics",
    metadata={
      "analytics_type": "comprehensive_metrics",
    },
  )

  # Initialize timeout (will be overridden in try block)
  operation_timeout = 30.0

  try:
    # Check circuit breaker before processing
    circuit_breaker.check_circuit(graph_id, "analytics_metrics")

    # Set up timeout coordination for analytics operations
    operation_timeout = timeout_coordinator.calculate_timeout(
      operation_type="analytics_query",
      complexity_factors={
        "operation": "comprehensive_metrics",
        "is_comprehensive": True,
        "expected_complexity": "high",
      },
    )

    # Log the request with operation logger
    operation_logger.log_external_service_call(
      endpoint="/v1/graphs/{graph_id}/analytics",
      service_name="graph_metrics_service",
      operation="collect_comprehensive_metrics",
      duration_ms=0.0,  # Will be updated on completion
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "analytics_type": "comprehensive_metrics",
      },
    )

    # Validate user has access to this graph
    from robosystems.models.iam.user_graph import UserGraph

    if not UserGraph.user_has_access(current_user.id, graph_id, db):
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"You don't have access to graph {graph_id}",
      )

    # Collect metrics for the specific graph with timeout coordination
    import asyncio

    metrics = await asyncio.wait_for(
      graph_metrics_service.collect_metrics_for_graph_async(graph_id),
      timeout=operation_timeout,
    )

    if not metrics or "error" in metrics:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Metrics not available for graph {graph_id}",
      )

    # Record business event
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

    # Record successful operation
    operation_duration_ms = (time.time() - operation_start_time) * 1000
    circuit_breaker.record_success(graph_id, "analytics_metrics")

    # Record success metrics
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

  except asyncio.TimeoutError:  # type: ignore[name-defined]
    # Record circuit breaker failure and timeout metrics
    circuit_breaker.record_failure(graph_id, "analytics_metrics")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record timeout failure metrics
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
    # Record circuit breaker failure for HTTP exceptions
    circuit_breaker.record_failure(graph_id, "analytics_metrics")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
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
    # Record circuit breaker failure for general exceptions
    circuit_breaker.record_failure(graph_id, "analytics_metrics")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
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
  summary="Get Usage Statistics",
  description="""Get detailed usage statistics for the graph.

Provides temporal usage patterns including:
- **Query Volume**: API calls per day/hour
- **Credit Consumption**: Usage patterns and trends
- **Operation Breakdown**: Usage by operation type
- **User Activity**: Access patterns by user role
- **Peak Usage Times**: Identify high-activity periods

Time ranges available:
- Last 24 hours (hourly breakdown)
- Last 7 days (daily breakdown)
- Last 30 days (daily breakdown)
- Custom date ranges

Useful for:
- Capacity planning
- Cost optimization
- Usage trend analysis
- Performance tuning

Note:
This operation is included - no credit consumption required.""",
  status_code=status.HTTP_200_OK,
  operation_id="getGraphUsageStats",
  responses={
    200: {
      "description": "Usage statistics retrieved successfully",
      "model": GraphUsageResponse,
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve usage statistics", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/graph/analytics/usage",
  business_event_type="graph_usage_accessed",
)
async def get_graph_usage_stats(
  graph_id: str = Path(..., description="The graph ID to get usage stats for"),
  include_details: bool = Query(
    False, description="Include detailed metrics (may be slower)"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> GraphUsageResponse:
  """
  Get usage statistics for the specified graph.

  This endpoint provides usage information for a specific graph including:
  - Storage usage
  - Query statistics
  - Recent activity
  - Optional detailed metrics
  """
  # Initialize robustness components
  circuit_breaker = CircuitBreakerManager()
  timeout_coordinator = TimeoutCoordinator()
  operation_logger = get_operation_logger()

  # Record operation start and get timing
  operation_start_time = time.time()
  operation_timeout = 30.0  # Initialize to avoid unbound variable error

  # Record operation start metrics
  record_operation_metric(
    operation_type=OperationType.ANALYTICS_QUERY,
    status=OperationStatus.SUCCESS,  # Will be updated on completion
    duration_ms=0.0,  # Will be updated on completion
    endpoint="/v1/graphs/{graph_id}/graph/analytics/usage",
    graph_id=graph_id,
    user_id=current_user.id,
    operation_name="get_usage_stats",
    metadata={
      "analytics_type": "usage_statistics",
      "include_details": include_details,
    },
  )

  # Initialize timeout (will be overridden in try block)
  operation_timeout = 30.0

  try:
    # Check circuit breaker before processing
    circuit_breaker.check_circuit(graph_id, "analytics_usage")

    # Set up timeout coordination based on detail level
    operation_timeout = timeout_coordinator.calculate_timeout(
      operation_type="analytics_query",
      complexity_factors={
        "operation": "usage_statistics",
        "include_details": include_details,
        "expected_complexity": "high" if include_details else "medium",
      },
    )

    # Log the request with operation logger
    operation_logger.log_external_service_call(
      endpoint="/v1/graphs/{graph_id}/graph/analytics/usage",
      service_name="graph_metrics_service",
      operation="collect_usage_statistics",
      duration_ms=0.0,  # Will be updated on completion
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "analytics_type": "usage_statistics",
        "include_details": include_details,
      },
    )

    # Validate user has access to this graph
    from robosystems.models.iam.user_graph import UserGraph

    user_graph = UserGraph.get_by_user_and_graph(current_user.id, graph_id, db)
    if not user_graph:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"You don't have access to graph {graph_id}",
      )

    # Get graph usage statistics with timeout coordination
    if include_details:
      graph_metrics = await asyncio.wait_for(
        graph_metrics_service.collect_metrics_for_graph_async(graph_id),
        timeout=operation_timeout,
      )
    else:
      # Use the same method but with less detail for basic usage
      graph_metrics = await asyncio.wait_for(
        graph_metrics_service.collect_metrics_for_graph_async(graph_id),
        timeout=operation_timeout,
      )

    # Compile usage response
    storage_usage = graph_metrics.get("estimated_size", {})
    query_statistics = get_query_analytics(graph_id)
    recent_activity = {
      "last_modified": graph_metrics.get("last_modified"),
      "last_accessed": datetime.now(timezone.utc).isoformat(),
    }

    # Record business event
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint=f"/v1/graphs/{graph_id}/graph/analytics/usage",
      method="GET",
      event_type="graph_usage_accessed",
      event_data={
        "user_id": current_user.id,
        "graph_id": graph_id,
        "include_details": include_details,
      },
      user_id=current_user.id,
    )

    # Record successful operation
    operation_duration_ms = (time.time() - operation_start_time) * 1000
    circuit_breaker.record_success(graph_id, "analytics_usage")

    # Record success metrics
    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.SUCCESS,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/graph/analytics/usage",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_stats",
      metadata={
        "analytics_type": "usage_statistics",
        "include_details": include_details,
      },
    )

    return GraphUsageResponse(
      graph_id=graph_id,
      storage_usage=storage_usage,
      query_statistics=query_statistics,
      recent_activity=recent_activity,
      timestamp=datetime.now(timezone.utc).isoformat(),
    )

  except asyncio.TimeoutError:
    # Record circuit breaker failure and timeout metrics
    circuit_breaker.record_failure(graph_id, "analytics_usage")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record timeout failure metrics
    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/graph/analytics/usage",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_stats",
      metadata={
        "analytics_type": "usage_statistics",
        "include_details": include_details,
        "error_type": "timeout",
        "timeout_seconds": operation_timeout
        if "operation_timeout" in locals()
        else None,
      },
    )

    logger.error(
      f"Analytics usage operation timeout after {operation_timeout}s for user {current_user.id}"
    )
    raise HTTPException(status_code=504, detail="Analytics usage operation timed out")
  except HTTPException:
    # Record circuit breaker failure for HTTP exceptions
    circuit_breaker.record_failure(graph_id, "analytics_usage")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/graph/analytics/usage",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_stats",
      metadata={
        "analytics_type": "usage_statistics",
        "include_details": include_details,
        "error_type": "http_exception",
      },
    )
    raise
  except Exception as e:
    # Record circuit breaker failure for general exceptions
    circuit_breaker.record_failure(graph_id, "analytics_usage")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
    record_operation_metric(
      operation_type=OperationType.ANALYTICS_QUERY,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/graph/analytics/usage",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="get_usage_stats",
      metadata={
        "analytics_type": "usage_statistics",
        "include_details": include_details,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    logger.error(f"Error getting usage stats for graph {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to retrieve usage statistics: {str(e)}",
    )
