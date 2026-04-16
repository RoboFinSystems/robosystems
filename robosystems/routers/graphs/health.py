"""
Graph database health endpoint.

This module provides REST API endpoints for database health monitoring.
"""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.graph_api.client import GraphClient
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)
from robosystems.models.api.graphs.health import DatabaseHealthResponse
from robosystems.models.core import User

# Create router
router = APIRouter(tags=["Graph Health"])

# Initialize robustness components
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def _get_graph_client(graph_id: str) -> GraphClient:
  """Get Graph client for the specified graph using factory for endpoint discovery."""
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.graph_api.client.factory import GraphClientFactory

  # Determine operation type based on graph
  # Shared repositories and their subgraphs (e.g. sec_historical) are read-only
  operation_type = "read" if is_shared_repository_or_subgraph(graph_id) else "write"

  # Create client using factory for endpoint discovery
  # Factory automatically handles routing:
  # - Shared repos: Routes to shared_master/shared_replica
  # - User graphs: Looks up tier from database and routes appropriately
  client = await GraphClientFactory.create_client(
    graph_id=graph_id, operation_type=operation_type
  )

  return client


@router.get(
  "/health",
  response_model=DatabaseHealthResponse,
  summary="Database Health Check",
  description="""Get comprehensive health information for the graph database.

Returns detailed health metrics including:
- **Connection Status**: Database connectivity and responsiveness
- **Performance Metrics**: Query execution times and throughput
- **Resource Usage**: Memory and storage utilization
- **Error Monitoring**: Recent error rates and patterns
- **Uptime Statistics**: Service availability metrics

Health indicators:
- **Status**: healthy, degraded, or unhealthy
- **Query Performance**: Average execution times
- **Error Rates**: Recent failure percentages
- **Resource Usage**: Memory and storage consumption
- **Alerts**: Active warnings or issues

**Subgraph Support:**
This endpoint accepts both parent graph IDs and subgraph IDs.
- Parent graph: Use `graph_id` like `kg0123456789abcdef`
- Subgraph: Use full subgraph ID like `kg0123456789abcdef_dev`
Health metrics are specific to the requested graph/subgraph. Subgraphs share the
same physical instance as their parent but have independent health indicators.

This endpoint provides essential monitoring data for operational visibility.""",
  operation_id="getDatabaseHealth",
  responses={
    200: {
      "description": "Database health retrieved successfully",
      "model": DatabaseHealthResponse,
    },
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph not found"},
    500: {"description": "Failed to retrieve health information"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/health",
  business_event_type="database_health_checked",
)
async def get_database_health(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> DatabaseHealthResponse:
  """
  Get comprehensive health information for the graph database.

  This endpoint provides real-time health metrics and status information
  for operational monitoring and troubleshooting.

  Args:
      graph_id: Graph database identifier
      current_user: Authenticated user
      session: Database session

  Returns:
      DatabaseHealthResponse with comprehensive health metrics
  """
  # Check circuit breaker
  circuit_breaker.check_circuit(graph_id, "database_health")

  try:
    _repository = await get_universal_repository(graph_id, "read")

    # Get Graph client and health information
    graph_client = await _get_graph_client(graph_id)

    try:
      # Calculate timeout for health check
      health_timeout = timeout_coordinator.calculate_timeout(
        "database_health", {"complexity": "low"}
      )

      # Get database metrics and general health from Graph API
      db_metrics = await asyncio.wait_for(
        graph_client.get_database_metrics(graph_id=graph_id),
        timeout=health_timeout,
      )

      # Also get cluster health for uptime
      cluster_health = await graph_client.health_check()

      # Record successful operation
      circuit_breaker.record_success(graph_id, "database_health")

      logger.debug(f"Database health retrieved for graph {graph_id}")

      # Pull staleness data from the platform DB (Graph model)
      from datetime import UTC, datetime

      from robosystems.models.core import Graph

      graph_record = Graph.get_by_id(graph_id.split("_")[0], session)
      is_stale = False
      stale_reason = None
      stale_since = None
      last_materialized_at = None
      hours_since_materialization = None
      staleness_alert: list[str] = []

      if graph_record:
        is_stale = graph_record.graph_stale or False
        stale_reason = graph_record.graph_stale_reason
        stale_since = (
          graph_record.graph_stale_at.isoformat()
          if graph_record.graph_stale_at
          else None
        )
        metadata = graph_record.graph_metadata or {}
        last_materialized_at = metadata.get("last_materialized_at")
        if last_materialized_at:
          try:
            from dateutil import parser as date_parser

            last_mat_dt = date_parser.isoparse(last_materialized_at)
            hours_since_materialization = (
              datetime.now(UTC) - last_mat_dt
            ).total_seconds() / 3600
          except Exception:
            logger.debug(
              f"Could not parse last_materialized_at for graph {graph_id}: {last_materialized_at!r}"
            )
        if is_stale:
          staleness_alert = ["Graph is stale — materialization recommended"]

      return DatabaseHealthResponse(
        graph_id=graph_id,
        status="degraded" if is_stale else ("healthy" if db_metrics else "unknown"),
        connection_status="connected",
        uptime_seconds=cluster_health.get("uptime_seconds", 0.0),
        last_query_time=db_metrics.get("last_modified"),
        query_count_24h=0,
        avg_query_time_ms=0.0,
        error_rate_24h=0.0,
        memory_usage_mb=None,
        storage_usage_mb=db_metrics.get("size_mb"),
        alerts=staleness_alert,
        is_stale=is_stale,
        stale_reason=stale_reason,
        stale_since=stale_since,
        last_materialized_at=last_materialized_at,
        hours_since_materialization=hours_since_materialization,
      )

    except TimeoutError:
      circuit_breaker.record_failure(graph_id, "database_health")
      raise HTTPException(
        status_code=status.HTTP_408_REQUEST_TIMEOUT,
        detail="Health check timed out",
      )
    except Exception as e:
      circuit_breaker.record_failure(graph_id, "database_health")
      if "not found" in str(e).lower():
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail="Database not found",
        )
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to retrieve health information",
      )
    finally:
      await graph_client.close()

  except HTTPException:
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "database_health")
    logger.error(f"Unexpected error getting health for graph {graph_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while retrieving health information",
    )
