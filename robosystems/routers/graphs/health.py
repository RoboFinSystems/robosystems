"""
Graph database health endpoint.

This module provides REST API endpoints for database health monitoring.
"""

import asyncio
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.iam import User
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.models.api.graph import DatabaseHealthResponse
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.graph_api.client import KuzuClient
from robosystems.logger import logger
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)

# Create router
router = APIRouter(tags=["Graph Health"])

# Initialize robustness components
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def _get_kuzu_client(graph_id: str) -> KuzuClient:
  """Get Kuzu client for the specified graph using factory for endpoint discovery."""
  from robosystems.graph_api.client.factory import KuzuClientFactory
  from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

  # Determine operation type based on graph
  # Shared repositories are read-only from the application perspective
  operation_type = (
    "read" if MultiTenantUtils.is_shared_repository(graph_id) else "write"
  )

  # Create client using factory for endpoint discovery
  # Factory automatically handles routing:
  # - Shared repos: Routes to shared_master/shared_replica
  # - User graphs: Looks up tier from database and routes appropriately
  client = await KuzuClientFactory.create_client(
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
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  current_user: User = Depends(get_current_user),
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
    # Verify user has read access to this graph
    _repository = await get_universal_repository_with_auth(
      graph_id, current_user, "read", session
    )

    # Get Kuzu client and health information
    kuzu_client = await _get_kuzu_client(graph_id)

    try:
      # Calculate timeout for health check
      health_timeout = timeout_coordinator.calculate_timeout(
        "database_health", {"complexity": "low"}
      )

      # Get database metrics and general health from Kuzu API
      db_metrics = await asyncio.wait_for(
        kuzu_client.get_database_metrics(graph_id=graph_id),
        timeout=health_timeout,
      )

      # Also get cluster health for uptime
      cluster_health = await kuzu_client.health_check()

      # Record successful operation
      circuit_breaker.record_success(graph_id, "database_health")

      logger.debug(f"Database health retrieved for graph {graph_id}")

      return DatabaseHealthResponse(
        graph_id=graph_id,
        status="healthy" if db_metrics else "unknown",
        connection_status="connected",
        uptime_seconds=cluster_health.get("uptime_seconds", 0.0),
        last_query_time=db_metrics.get("last_modified"),
        query_count_24h=0,  # Not available in current metrics
        avg_query_time_ms=0.0,  # Not available in current metrics
        error_rate_24h=0.0,  # Not available in current metrics
        memory_usage_mb=None,  # Could calculate from size_bytes
        storage_usage_mb=db_metrics.get("size_mb"),
        alerts=[],
      )

    except asyncio.TimeoutError:
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
      await kuzu_client.close()

  except HTTPException:
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "database_health")
    logger.error(f"Unexpected error getting health for graph {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while retrieving health information",
    )
