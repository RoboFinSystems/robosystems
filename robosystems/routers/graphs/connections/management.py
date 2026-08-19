"""
Connection management endpoints (create, list, get, delete).
"""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user_with_graph,
  require_graph_write_role,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import (
  RESOURCE_ERROR_RESPONSES,
  ErrorCode,
  SuccessResponse,
  create_error_response,
)
from robosystems.models.api.graphs.connections import (
  ConnectionResponse,
  CreateConnectionRequest,
  ProviderType,
  SetWritePolicyRequest,
)
from robosystems.models.core import GraphUser, User
from robosystems.operations.connection_service import ConnectionService

from .utils import (
  create_robustness_components,
  provider_registry,
  record_operation_failure,
  record_operation_start,
  record_operation_success,
)

router = APIRouter()


@router.post(
  "",
  response_model=ConnectionResponse,
  status_code=status.HTTP_201_CREATED,
  operation_id="createConnection",
  summary="Create Connection",
  description="SEC: provide entity CIK, no auth needed. QuickBooks: returns an OAuth URL — complete the flow to activate. External: registers a source namespace for an integration that writes through the public API. One connection allowed per provider per graph, except 'external' which allows one per source_name.",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    409: {"description": "Connection already exists for this provider"},
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
  # Registering a source is a write to the graph (it seeds sync, the fiscal
  # calendar and the mapping operator); membership alone is not enough.
  require_graph_write_role(str(current_user.id), graph_id)

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
      "entity_id": request.entity_id or "",
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
        "entity_id": request.entity_id or "",
      },
    )

    # Get the appropriate config based on provider
    config = None
    if request.provider == "sec":
      config = request.sec_config
    elif request.provider == "quickbooks":
      config = request.quickbooks_config
    elif request.provider == "external":
      config = request.external_config
    # Validate provider is enabled before any database operations
    provider_registry.get_provider(request.provider)

    # Prevent duplicate connections: one connection per provider per graph —
    # except 'external', where the identity is the source_name (a graph can
    # register several external sources; the same source_name early-returns
    # its existing registration).
    existing_connections = await ConnectionService.list_connections(
      user_id=str(current_user.id),
      graph_id=graph_id,
      provider=request.provider,
    )
    if request.provider == "external" and request.external_config is not None:
      existing_connections = [
        c
        for c in existing_connections
        if c.get("source_name") == request.external_config.source_name
      ]
    if existing_connections:
      existing = existing_connections[0]
      return ConnectionResponse(
        connection_id=existing["connection_id"],
        provider=existing["provider"],
        entity_id=existing.get("entity_id"),
        status=existing["status"],
        created_at=existing["created_at"],
        updated_at=existing.get("updated_at"),
        last_sync=existing.get("metadata", {}).get("last_sync"),
        write_policy=existing.get("write_policy"),
        source_name=existing.get("source_name"),
        metadata=existing.get("metadata", {}),
      )

    # Create connection using provider registry with timeout coordination
    connection_id = await asyncio.wait_for(
      provider_registry.create_connection(
        request.provider,
        request.entity_id or "",
        config,
        current_user.id,
        graph_id,
        db,
      ),
      timeout=operation_timeout,
    )

    # Get the created connection
    connection = await ConnectionService.get_connection(
      connection_id, current_user.id, graph_id=graph_id
    )

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
        "entity_id": request.entity_id or "",
        "connection_id": connection_id,
      },
    )

    return ConnectionResponse(
      connection_id=connection["connection_id"],
      provider=connection["provider"].lower(),
      entity_id=connection.get("entity_id"),
      status=connection["status"],
      created_at=connection["created_at"],
      updated_at=connection.get("updated_at"),
      last_sync=connection["metadata"].get("last_sync"),
      write_policy=connection.get("write_policy"),
      source_name=connection.get("source_name"),
      metadata=connection["metadata"],
    )

  except TimeoutError:
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
      code=ErrorCode.OPERATION_FAILED,
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
      detail="The requested provider is not available.",
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

    logger.error("Failed to create connection", exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create connection",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "",
  response_model=list[ConnectionResponse],
  summary="List Connections",
  operation_id="listConnections",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_connections(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  entity_id: str | None = Query(None, description="Filter by entity ID"),
  provider: ProviderType | None = Query(None, description="Filter by provider type"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> list[ConnectionResponse]:
  try:
    # Get connections from service
    connections = await ConnectionService.list_connections(
      entity_id=entity_id or None,
      provider=provider or None,
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
          entity_id=conn.get("entity_id"),
          status=conn["status"],
          created_at=conn["created_at"],
          updated_at=conn.get("updated_at"),
          last_sync=conn["metadata"].get("last_sync"),
          write_policy=conn.get("write_policy"),
          source_name=conn.get("source_name"),
          metadata=conn["metadata"],
        )
      )

    return response_connections

  except Exception:
    logger.error("Failed to list connections", exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list connections",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.get(
  "/{connection_id}",
  response_model=ConnectionResponse,
  summary="Get Connection",
  operation_id="getConnection",
  responses={**RESOURCE_ERROR_RESPONSES},
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
  try:
    connection = await ConnectionService.get_connection(
      connection_id, current_user.id, graph_id=graph_id
    )

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    return ConnectionResponse(
      connection_id=connection["connection_id"],
      provider=connection["provider"].lower(),
      entity_id=connection.get("entity_id"),
      status=connection["status"],
      created_at=connection["created_at"],
      updated_at=connection.get("updated_at"),
      last_sync=connection["metadata"].get("last_sync"),
      write_policy=connection.get("write_policy"),
      source_name=connection.get("source_name"),
      metadata=connection["metadata"],
    )

  except HTTPException:
    raise
  except Exception:
    logger.error("Failed to get connection", exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get connection",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.put(
  "/{connection_id}/write-policy",
  response_model=ConnectionResponse,
  summary="Set Connection Write Policy",
  operation_id="setConnectionWritePolicy",
  description=(
    "Opt a connection into or out of outbound write-back. "
    "'qb_authoritative' makes QuickBooks the source of truth — "
    "RoboSystems-originated entries (manual JEs, schedule drafts) publish "
    "to QuickBooks when executed or at close. 'native' keeps RoboSystems "
    "authoritative with no write-back. This is the explicit operator opt-in "
    "for writing to your books of record."
  ),
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def set_connection_write_policy(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  connection_id: str = Path(..., description="Unique connection identifier"),
  request: SetWritePolicyRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> ConnectionResponse:
  # `get_current_user_with_graph` proves graph membership only. Opting a
  # connection into outbound write-back (e.g. QuickBooks) is a privileged
  # mutation — a read-only `viewer` must not reach it. Enforce member/admin.
  require_graph_write_role(str(current_user.id), graph_id)

  try:
    connection = await ConnectionService.set_write_policy(
      connection_id=connection_id,
      write_policy=request.write_policy,
      user_id=current_user.id,
      graph_id=graph_id,
    )

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    logger.info(
      "Set write_policy=%s on connection %s (graph %s)",
      request.write_policy,
      connection_id,
      graph_id,
    )

    return ConnectionResponse(
      connection_id=connection["connection_id"],
      provider=connection["provider"].lower(),
      entity_id=connection.get("entity_id"),
      status=connection["status"],
      created_at=connection["created_at"],
      updated_at=connection.get("updated_at"),
      last_sync=connection["metadata"].get("last_sync"),
      write_policy=connection.get("write_policy"),
      source_name=connection.get("source_name"),
      metadata=connection["metadata"],
    )

  except HTTPException:
    raise
  except Exception:
    logger.error("Failed to set connection write policy", exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to set connection write policy",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.delete(
  "/{connection_id}",
  response_model=SuccessResponse,
  summary="Delete Connection",
  description="Removes the connection and revokes credentials. Imported data is preserved in the graph. Requires admin role.",
  operation_id="deleteConnection",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def delete_connection(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  connection_id: str = Path(..., description="Connection identifier"),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    # Deleting a connection revokes credentials — enforce the admin role the
    # endpoint has always documented.
    if not GraphUser.user_has_admin_access(str(current_user.id), graph_id, db):
      raise create_error_response(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access to the graph is required to delete connections",
        code=ErrorCode.FORBIDDEN,
      )

    # Get connection before deletion for cleanup
    connection = await ConnectionService.get_connection(
      connection_id, current_user.id, graph_id=graph_id
    )

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    # Provider-specific cleanup BEFORE deletion (e.g., revoke OAuth tokens)
    provider = connection["provider"].lower()
    try:
      provider_registry.get_provider(provider)
      await provider_registry.cleanup_connection(provider, connection, graph_id)
    except ValueError:
      # Provider disabled — skip cleanup, still allow deletion
      logger.warning(
        "Provider %s disabled, skipping cleanup for connection %s",
        provider,
        connection_id,
      )

    success = await ConnectionService.delete_connection(
      connection_id, current_user.id, graph_id
    )

    if not success:
      raise create_error_response(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to delete connection",
        code=ErrorCode.INTERNAL_ERROR,
      )

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
      detail="The requested provider is not available.",
      code=ErrorCode.FORBIDDEN,
    )
  except Exception as e:
    logger.error(f"Failed to delete connection {connection_id}: {e}", exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to delete connection.",
      code=ErrorCode.INTERNAL_ERROR,
    )
