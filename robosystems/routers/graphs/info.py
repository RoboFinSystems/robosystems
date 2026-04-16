"""Graph database information endpoint."""

import asyncio
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.graph_api.client import GraphClient
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  TimeoutCoordinator,
)
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.health import DatabaseInfoResponse
from robosystems.models.core import User

router = APIRouter(tags=["Graph Info"])

# Initialize robustness components
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()


async def _get_graph_client(graph_id: str) -> GraphClient:
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
  "/info",
  response_model=DatabaseInfoResponse,
  summary="Database Information",
  operation_id="getDatabaseInfo",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/info",
  business_event_type="database_info_retrieved",
)
async def get_database_info(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_async_db_session),
  _: None = Depends(subscription_aware_rate_limit_dependency),
) -> DatabaseInfoResponse:
  circuit_breaker.check_circuit(graph_id, "database_info")

  try:
    # Access validated by get_current_user_with_graph dependency

    # Get Graph client and database information
    graph_client = await _get_graph_client(graph_id)

    try:
      # Calculate timeout for database info request
      info_timeout = timeout_coordinator.calculate_timeout(
        "database_info", {"complexity": "medium"}
      )

      # Get database information from Graph API
      info_result = await asyncio.wait_for(
        graph_client.get_database_info(graph_id=graph_id),
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
        created_at=info_result.get("created_at", datetime.now(UTC).isoformat()),
        last_modified=info_result.get("last_modified", datetime.now(UTC).isoformat()),
        schema_version=info_result.get("schema_version"),
        read_only=info_result.get("read_only", False),
        backup_count=info_result.get("backup_count", 0),
        last_backup_date=info_result.get("last_backup_date"),
      )

    except TimeoutError:
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
      await graph_client.close()

  except HTTPException:
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, "database_info")
    logger.error(f"Unexpected error getting info for graph {graph_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while retrieving database information",
    )
