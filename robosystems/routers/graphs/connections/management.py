"""
Connection management endpoints (create, list, get, delete).
"""

from typing import Optional, List
from fastapi import APIRouter, HTTPException, Depends, Query, Request, Path, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.operations.connection_service import ConnectionService
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.models.api.graphs.connections import (
  ProviderType,
  CreateConnectionRequest,
  ConnectionResponse,
)
from robosystems.models.api.common import (
  ErrorResponse,
  SuccessResponse,
  ErrorCode,
  create_error_response,
)

from .utils import (
  provider_registry,
  create_robustness_components,
  record_operation_start,
  record_operation_success,
  record_operation_failure,
)

import asyncio
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN

router = APIRouter()


@router.post(
  "",
  response_model=ConnectionResponse,
  status_code=status.HTTP_201_CREATED,
  operation_id="createConnection",
  summary="Create Connection",
  description="""Create a new data connection for external system integration.

This endpoint initiates connections to external data sources:

**SEC Connections**:
- Provide entity CIK for automatic filing retrieval
- No authentication needed
- Begins immediate data sync

**QuickBooks Connections**:
- Returns OAuth URL for authorization
- Requires admin permissions in QuickBooks
- Complete with OAuth callback

**Plaid Connections**:
- Returns Plaid Link token
- User completes bank authentication
- Exchange public token for access

Note:
This operation is included - no credit consumption required.""",
  responses={
    201: {
      "description": "Connection created successfully",
      "model": ConnectionResponse,
    },
    400: {"description": "Invalid connection configuration", "model": ErrorResponse},
    403: {"description": "Access denied - admin role required", "model": ErrorResponse},
    409: {"description": "Connection already exists", "model": ErrorResponse},
    500: {"description": "Failed to create connection", "model": ErrorResponse},
  },
)
async def create_connection(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  request: CreateConnectionRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> ConnectionResponse:
  """
  Create a new connection for data synchronization.

  Supports multiple providers:
  - SEC: Requires CIK for public entity filings
  - QuickBooks: Requires OAuth authentication (separate flow)
  - Plaid: Requires Link token flow for bank connections
  """
  # Initialize robustness components
  components = create_robustness_components()

  # Record operation start metrics
  record_operation_start(
    operation_name="create_connection",
    endpoint="/v1/graphs/{graph_id}/connections",
    graph_id=graph_id,
    user_id=current_user.id,
    metadata={
      "provider": request.provider,
      "entity_id": request.entity_id,
    },
  )

  # Initialize timeout (will be overridden in try block)
  operation_timeout = 30.0

  try:
    # Check circuit breaker before processing
    components["circuit_breaker"].check_circuit(graph_id, "connection_create")

    # Set up timeout coordination for external service calls
    operation_timeout = components["timeout_coordinator"].calculate_timeout(
      operation_type="external_service",
      complexity_factors={
        "provider": request.provider,
        "operation": "create_connection",
        "expected_complexity": "medium",
      },
    )

    # Log the request with operation logger
    components["operation_logger"].log_external_service_call(
      endpoint="/v1/graphs/{graph_id}/connections",
      service_name="connection_service",
      operation="create_connection",
      duration_ms=0.0,  # Will be updated on completion
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "provider": request.provider,
        "entity_id": request.entity_id,
      },
    )

    # Get the appropriate config based on provider
    config = None
    if request.provider == "sec":
      config = request.sec_config
    elif request.provider == "quickbooks":
      config = request.quickbooks_config
    elif request.provider == "plaid":
      config = request.plaid_config

    # Validate provider is enabled before any database operations
    provider_registry.get_provider(request.provider)

    # Create connection using provider registry with timeout coordination
    connection_id = await asyncio.wait_for(
      provider_registry.create_connection(
        request.provider, request.entity_id, config, current_user.id, graph_id, db
      ),
      timeout=operation_timeout,
    )

    # Get the created connection
    connection = await ConnectionService.get_connection(connection_id, current_user.id)

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to retrieve created connection",
        code=ErrorCode.INTERNAL_ERROR,
      )

    # Record successful operation
    record_operation_success(
      components=components,
      operation_name="create_connection",
      endpoint="/v1/graphs/{graph_id}/connections",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "provider": request.provider,
        "entity_id": request.entity_id,
        "connection_id": connection_id,
      },
    )

    return ConnectionResponse(
      connection_id=connection["connection_id"],
      provider=connection["provider"].lower(),
      entity_id=connection["entity_id"],
      status=connection["status"],
      created_at=connection["created_at"],
      updated_at=connection.get("updated_at"),
      last_sync=connection["metadata"].get("last_sync"),
      metadata=connection["metadata"],
    )

  except asyncio.TimeoutError:
    # Record circuit breaker failure and timeout metrics
    record_operation_failure(
      components=components,
      operation_name="create_connection",
      endpoint="/v1/graphs/{graph_id}/connections",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="timeout",
      timeout_seconds=operation_timeout if "operation_timeout" in locals() else None,
    )

    logger.error(
      f"Connection creation timeout after {operation_timeout}s for user {current_user.id}"
    )
    raise create_error_response(
      status_code=status.HTTP_504_GATEWAY_TIMEOUT,
      detail="Connection creation timed out",
      code=ErrorCode.TIMEOUT,
    )
  except HTTPException:
    # Record circuit breaker failure for HTTP exceptions
    record_operation_failure(
      components=components,
      operation_name="create_connection",
      endpoint="/v1/graphs/{graph_id}/connections",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="http_exception",
    )
    raise
  except ValueError as e:
    # Handle disabled provider errors as client errors
    record_operation_failure(
      components=components,
      operation_name="create_connection",
      endpoint="/v1/graphs/{graph_id}/connections",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="provider_disabled",
      error_message=str(e),
    )

    logger.warning(f"Provider not available for connection: {e}")
    raise create_error_response(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=str(e),
      code=ErrorCode.FORBIDDEN,
    )
  except Exception as e:
    # Record circuit breaker failure for general exceptions
    record_operation_failure(
      components=components,
      operation_name="create_connection",
      endpoint="/v1/graphs/{graph_id}/connections",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type=type(e).__name__,
      error_message=str(e),
    )

    logger.error(f"Failed to create connection: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to create connection: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "",
  response_model=List[ConnectionResponse],
  summary="List Connections",
  description="""List all data connections in the graph.

Returns active and inactive connections with their current status.
Connections can be filtered by:
- **Entity**: Show connections for a specific entity
- **Provider**: Filter by connection type (sec, quickbooks, plaid)

Each connection shows:
- Current sync status and health
- Last successful sync timestamp
- Configuration metadata
- Error messages if any

No credits are consumed for listing connections.""",
  operation_id="listConnections",
  responses={
    200: {
      "description": "Connections retrieved successfully",
      "content": {
        "application/json": {
          "example": [
            {
              "connection_id": "conn_123",
              "provider": "quickbooks",
              "entity_id": "entity_456",
              "status": "active",
              "created_at": "2024-01-01T00:00:00Z",
              "last_sync": "2024-01-02T00:00:00Z",
            }
          ]
        }
      },
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    500: {"description": "Failed to list connections", "model": ErrorResponse},
  },
)
async def list_connections(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  entity_id: Optional[str] = Query(None, description="Filter by entity ID"),
  provider: Optional[ProviderType] = Query(None, description="Filter by provider type"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> List[ConnectionResponse]:
  """
  List all connections accessible to the current user.

  Can be filtered by entity_id and/or provider type.
  """
  try:
    # Get connections from service
    connections = ConnectionService.list_connections(
      entity_id=entity_id or "",
      provider=provider.upper() if provider else "",
      user_id=current_user.id,
      graph_id=graph_id,
    )

    # Convert to response models
    response_connections = []
    for conn in connections:
      response_connections.append(
        ConnectionResponse(
          connection_id=conn["connection_id"],
          provider=conn["provider"].lower(),
          entity_id=conn["entity_id"],
          status=conn["status"],
          created_at=conn["created_at"],
          updated_at=conn.get("updated_at"),
          last_sync=conn["metadata"].get("last_sync"),
          metadata=conn["metadata"],
        )
      )

    return response_connections

  except Exception as e:
    logger.error(f"Failed to list connections: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list connections: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "/{connection_id}",
  response_model=ConnectionResponse,
  summary="Get Connection",
  description="""Get detailed information about a specific connection.

Returns comprehensive connection details including:
- Current status and health indicators
- Authentication state
- Sync history and statistics
- Error details if any
- Provider-specific metadata

No credits are consumed for viewing connection details.""",
  operation_id="getConnection",
  responses={
    200: {
      "description": "Connection details retrieved successfully",
      "model": ConnectionResponse,
    },
    403: {"description": "Access denied to connection", "model": ErrorResponse},
    404: {"description": "Connection not found", "model": ErrorResponse},
    500: {"description": "Failed to retrieve connection", "model": ErrorResponse},
  },
)
async def get_connection(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  connection_id: str = Path(..., description="Unique connection identifier"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> ConnectionResponse:
  """
  Get details of a specific connection.

  Retrieves comprehensive information about a data connection.

  Args:
      graph_id: The graph containing the connection
      connection_id: The connection to retrieve
      current_user: The authenticated user
      db: Database session

  Returns:
      ConnectionResponse: Detailed connection information

  Raises:
      HTTPException: If connection not found or access denied
  """
  try:
    connection = await ConnectionService.get_connection(connection_id, current_user.id)

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    return ConnectionResponse(
      connection_id=connection["connection_id"],
      provider=connection["provider"].lower(),
      entity_id=connection["entity_id"],
      status=connection["status"],
      created_at=connection["created_at"],
      updated_at=connection.get("updated_at"),
      last_sync=connection["metadata"].get("last_sync"),
      metadata=connection["metadata"],
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to get connection: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to get connection: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.delete(
  "/{connection_id}",
  response_model=SuccessResponse,
  summary="Delete Connection",
  description="""Delete a data connection and clean up related resources.

This operation:
- Removes the connection configuration
- Preserves any imported data in the graph
- Performs provider-specific cleanup
- Revokes stored credentials

Note:
This operation is included - no credit consumption required.

Only users with admin role can delete connections.""",
  operation_id="deleteConnection",
  responses={
    200: {"description": "Connection deleted successfully", "model": SuccessResponse},
    403: {"description": "Access denied - admin role required", "model": ErrorResponse},
    404: {"description": "Connection not found", "model": ErrorResponse},
    500: {"description": "Failed to delete connection", "model": ErrorResponse},
  },
)
async def delete_connection(
  request: Request,
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  connection_id: str = Path(..., description="Connection identifier"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """
  Delete a connection and optionally clean up related data.

  This will remove the connection configuration but preserve any imported data.
  """
  try:
    # Get connection before deletion for cleanup
    connection = await ConnectionService.get_connection(connection_id, current_user.id)

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    # Delete the connection
    success = ConnectionService.delete_connection(
      connection_id, current_user.id, graph_id
    )

    if not success:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found or access denied",
        code=ErrorCode.NOT_FOUND,
      )

    # Provider-specific cleanup
    provider = connection["provider"].lower()

    # Validate provider is enabled before cleanup operations
    provider_registry.get_provider(provider)

    await provider_registry.cleanup_connection(provider, connection, graph_id)

    logger.info(f"Connection {connection_id} deleted successfully")

    return SuccessResponse(
      success=True,
      message=f"Connection {connection_id} deleted successfully",
      data={"connection_id": connection_id, "provider": provider},
    )

  except HTTPException:
    raise
  except ValueError as e:
    # Handle disabled provider errors as client errors
    logger.warning(f"Provider not available for connection cleanup: {e}")
    raise create_error_response(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=str(e),
      code=ErrorCode.FORBIDDEN,
    )
  except Exception as e:
    logger.error(f"Failed to delete connection {connection_id}: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to delete connection: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )
