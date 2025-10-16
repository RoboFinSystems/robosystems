"""
Graph database information endpoint.

This module provides REST API endpoints for database information and statistics.
"""

import asyncio
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.iam import User
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.graph.dependencies import get_universal_repository_with_auth
from robosystems.models.api.graph import DatabaseInfoResponse
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.graph_api.client import KuzuClient
from robosystems.logger import logger
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)

# Create router
router = APIRouter(tags=["Graph Info"])

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
  "/info",
  response_model=DatabaseInfoResponse,
  summary="Database Information",
  description="""Get comprehensive database information and statistics.

Returns detailed database metrics including:
- **Database Metadata**: Name, path, size, and timestamps
- **Schema Information**: Node labels, relationship types, and counts
- **Storage Statistics**: Database size and usage metrics
- **Data Composition**: Node and relationship counts
- **Backup Information**: Available backups and last backup date
- **Configuration**: Read-only status and schema version

Database statistics:
- **Size**: Storage usage in bytes and MB
- **Content**: Node and relationship counts
- **Schema**: Available labels and relationship types
- **Backup Status**: Backup availability and recency
- **Timestamps**: Creation and modification dates

This endpoint provides essential database information for capacity planning and monitoring.""",
  operation_id="getDatabaseInfo",
  responses={
    200: {
      "description": "Database information retrieved successfully",
      "model": DatabaseInfoResponse,
    },
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph not found"},
    500: {"description": "Failed to retrieve database information"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/info",
  business_event_type="database_info_retrieved",
)
async def get_database_info(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
  ),
  current_user: User = Depends(get_current_user),
  session: Session = Depends(get_async_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> DatabaseInfoResponse:
  """
  Get comprehensive database information and statistics.

  This endpoint provides detailed database metadata, schema information,
  and statistics for capacity planning and monitoring.

  Args:
      graph_id: Graph database identifier
      current_user: Authenticated user
      session: Database session

  Returns:
      DatabaseInfoResponse with comprehensive database information
  """
  # Check circuit breaker
  circuit_breaker.check_circuit(graph_id, "database_info")

  try:
    # Verify user has read access to this graph
    _repository = await get_universal_repository_with_auth(
      graph_id, current_user, "read", session
    )

    # Get Kuzu client and database information
    kuzu_client = await _get_kuzu_client(graph_id)

    try:
      # Calculate timeout for database info request
      info_timeout = timeout_coordinator.calculate_timeout(
        "database_info", {"complexity": "medium"}
      )

      # Get database information from Kuzu API
      info_result = await asyncio.wait_for(
        kuzu_client.get_database_info(graph_id=graph_id),
        timeout=info_timeout,
      )

      # Record successful operation
      circuit_breaker.record_success(graph_id, "database_info")

      logger.debug(f"Database info retrieved for graph {graph_id}")

      # Calculate derived fields
      database_size_bytes = info_result.get("database_size_bytes", 0)
      database_size_mb = round(database_size_bytes / (1024 * 1024), 2)

      return DatabaseInfoResponse(
        graph_id=graph_id,
        database_name=info_result.get("database_name", graph_id),
        # database_path removed - no need to expose file system paths
        database_size_bytes=database_size_bytes,
        database_size_mb=database_size_mb,
        node_count=info_result.get("node_count", 0),
        relationship_count=info_result.get("relationship_count", 0),
        node_labels=info_result.get("node_labels", []),
        relationship_types=info_result.get("relationship_types", []),
        created_at=info_result.get(
          "created_at", datetime.now(timezone.utc).isoformat()
        ),
        last_modified=info_result.get(
          "last_modified", datetime.now(timezone.utc).isoformat()
        ),
        schema_version=info_result.get("schema_version"),
        read_only=info_result.get("read_only", False),
        backup_count=info_result.get("backup_count", 0),
        last_backup_date=info_result.get("last_backup_date"),
      )

    except asyncio.TimeoutError:
      circuit_breaker.record_failure(graph_id, "database_info")
      raise HTTPException(
        status_code=status.HTTP_408_REQUEST_TIMEOUT,
        detail="Database info request timed out",
      )
    except Exception as e:
      circuit_breaker.record_failure(graph_id, "database_info")
      if "not found" in str(e).lower():
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail="Database not found",
        )
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to retrieve database information",
      )
    finally:
      await kuzu_client.close()

  except HTTPException:
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "database_info")
    logger.error(f"Unexpected error getting info for graph {graph_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while retrieving database information",
    )
