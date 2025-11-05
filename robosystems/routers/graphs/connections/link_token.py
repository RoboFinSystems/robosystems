"""
Link token endpoints for embedded authentication.
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Request, status

from robosystems.models.iam import User
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph import get_graph_repository
from robosystems.operations.connection_service import ConnectionService
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.models.api.graphs.connections import (
  LinkTokenRequest,
  ExchangeTokenRequest,
)
from robosystems.models.api.common import (
  ErrorResponse,
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

router = APIRouter()


@router.post(
  "/link/token",
  summary="Create Link Token",
  description="""Create a link token for embedded authentication providers.

This endpoint generates a temporary token used to initialize embedded authentication UI.

Currently supported providers:
- **Plaid**: Bank account connections with real-time transaction access

The link token:
- Expires after 4 hours
- Is single-use only
- Must be used with the matching frontend SDK
- Includes user and entity context

No credits are consumed for creating link tokens.""",
  operation_id="createLinkToken",
  responses={
    200: {
      "description": "Link token created successfully",
      "content": {
        "application/json": {
          "example": {
            "link_token": "link-sandbox-af1a0311-da53-4636-b754-dd15cc058176",
            "expiration": "2024-01-01T04:00:00Z",
            "message": "Use this token to initialize the embedded authentication UI",
            "provider": "plaid",
          }
        }
      },
    },
    400: {"description": "Invalid provider or request", "model": ErrorResponse},
    404: {"description": "Entity not found", "model": ErrorResponse},
    500: {"description": "Failed to create link token", "model": ErrorResponse},
  },
)
async def create_link_token(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: LinkTokenRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """
  Create a link token for embedded authentication providers.

  This token is used by the frontend to initialize embedded auth UI (e.g., Plaid Link).
  Currently supports: Plaid for bank connections.
  """
  # Initialize robustness components
  components = create_robustness_components()

  # Record operation start metrics
  record_operation_start(
    operation_name="create_link_token",
    endpoint="/v1/graphs/{graph_id}/connections/link/token",
    graph_id=graph_id,
    user_id=current_user.id,
    metadata={
      "provider": request.provider or "plaid",
      "entity_id": request.entity_id,
    },
  )

  # Initialize timeout (will be overridden in try block)
  operation_timeout = 30.0

  try:
    # Check circuit breaker before processing
    components["circuit_breaker"].check_circuit(graph_id, "link_token_create")

    # Set up timeout coordination for external service calls
    operation_timeout = components["timeout_coordinator"].calculate_timeout(
      operation_type="external_service",
      complexity_factors={
        "provider": request.provider or "plaid",
        "operation": "create_link_token",
        "expected_complexity": "medium",
      },
    )

    # Log the request with operation logger
    components["operation_logger"].log_external_service_call(
      endpoint="/v1/graphs/{graph_id}/connections/link/token",
      service_name="plaid_provider",
      operation="create_link_token",
      duration_ms=0.0,  # Will be updated on completion
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "provider": request.provider or "plaid",
        "entity_id": request.entity_id,
      },
    )

    # Validate entity exists
    repository = await get_graph_repository(graph_id, operation_type="read")

    entity_query = """
            MATCH (e:Entity {identifier: $entity_id})
            RETURN e.identifier as identifier, e.name as name
            """
    entity_result = repository.execute_single(
      entity_query, {"entity_id": request.entity_id}
    )

    if not entity_result:
      raise create_error_response(
        status_code=404, detail="Entity not found", code=ErrorCode.NOT_FOUND
      )

    # Determine provider from request or default to Plaid for now
    provider = request.provider or "plaid"

    # Create link token using appropriate provider with timeout coordination
    if provider == "plaid":
      plaid_provider = provider_registry.get_plaid_provider()
      link_token, expiration = await asyncio.wait_for(
        plaid_provider.create_link_token(request.entity_id, request.user_id),
        timeout=operation_timeout,
      )
    else:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Provider {provider} does not support link tokens",
        code=ErrorCode.INVALID_INPUT,
      )

    logger.info(f"Created {provider} link token for entity {request.entity_id}")

    # Record successful operation
    record_operation_success(
      components=components,
      operation_name="create_link_token",
      endpoint="/v1/graphs/{graph_id}/connections/link/token",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "provider": provider,
        "entity_id": request.entity_id,
      },
    )

    return {
      "link_token": link_token,
      "expiration": expiration,
      "message": "Use this token to initialize the embedded authentication UI",
      "provider": provider,  # Let frontend know which UI to initialize
    }

  except asyncio.TimeoutError:
    # Record circuit breaker failure and timeout metrics
    record_operation_failure(
      components=components,
      operation_name="create_link_token",
      endpoint="/v1/graphs/{graph_id}/connections/link/token",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="timeout",
      timeout_seconds=operation_timeout if "operation_timeout" in locals() else None,
    )

    logger.error(
      f"Link token creation timeout after {operation_timeout}s for user {current_user.id}"
    )
    raise create_error_response(
      status_code=status.HTTP_504_GATEWAY_TIMEOUT,
      detail="Link token creation timed out",
      code=ErrorCode.TIMEOUT,
    )
  except HTTPException:
    # Record circuit breaker failure for HTTP exceptions
    record_operation_failure(
      components=components,
      operation_name="create_link_token",
      endpoint="/v1/graphs/{graph_id}/connections/link/token",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="http_exception",
    )
    raise
  except ValueError as e:
    # Handle disabled provider errors as client errors
    record_operation_failure(
      components=components,
      operation_name="create_link_token",
      endpoint="/v1/graphs/{graph_id}/connections/link/token",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type="provider_disabled",
      error_message=str(e),
    )

    logger.warning(f"Provider not available for link token: {e}")
    raise create_error_response(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=str(e),
      code=ErrorCode.FORBIDDEN,
    )
  except Exception as e:
    # Record circuit breaker failure for general exceptions
    record_operation_failure(
      components=components,
      operation_name="create_link_token",
      endpoint="/v1/graphs/{graph_id}/connections/link/token",
      graph_id=graph_id,
      user_id=current_user.id,
      error_type=type(e).__name__,
      error_message=str(e),
    )

    logger.error(f"Link token creation failed: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Link token creation failed: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.post(
  "/link/exchange",
  summary="Exchange Link Token",
  description="""Exchange a public token for permanent access credentials.

This completes the embedded authentication flow after user authorization.

The exchange process:
1. Validates the temporary public token
2. Exchanges it for permanent access credentials
3. Updates the connection with account information
4. Optionally triggers initial data sync

Supported providers:
- **Plaid**: Exchanges Plaid Link public token for access token

Security:
- Public tokens expire after 30 minutes
- Each token can only be exchanged once
- Full audit trail is maintained

No credits are consumed for token exchange.""",
  operation_id="exchangeLinkToken",
  responses={
    200: {
      "description": "Token exchanged successfully",
      "content": {
        "application/json": {
          "example": {
            "success": True,
            "message": "Plaid connection established successfully",
            "connection_id": "conn_123456",
            "auto_sync_task_id": "task_789012",
          }
        }
      },
    },
    400: {"description": "Invalid token or provider", "model": ErrorResponse},
    404: {"description": "Connection not found", "model": ErrorResponse},
    500: {"description": "Token exchange failed", "model": ErrorResponse},
  },
)
async def exchange_link_token(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: ExchangeTokenRequest = ...,
  fastapi_request: Request = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """
  Exchange temporary public token for permanent access credentials.

  This completes the embedded auth flow after user authorization.
  Currently supports: Plaid bank connections.
  """
  try:
    # Get existing connection
    connection = await ConnectionService.get_connection(
      request.connection_id, current_user.id
    )

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    provider = connection["provider"].lower()
    if provider != "plaid":
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Provider {provider} does not support link/embedded authentication",
        code=ErrorCode.INVALID_INPUT,
      )

    # Exchange token with provider API
    if provider == "plaid":
      plaid_provider = provider_registry.get_plaid_provider()
      access_token, item_id, accounts = await plaid_provider.exchange_public_token(
        request.public_token
      )
    else:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Token exchange not implemented for provider {provider}",
        code=ErrorCode.PROVIDER_ERROR,
      )

    # Extract provider-specific metadata
    provider_metadata = request.metadata or {}

    # For Plaid, handle legacy fields and metadata
    if provider == "plaid" and not accounts:
      if "accounts" in provider_metadata:
        accounts = provider_metadata["accounts"]

    institution = provider_metadata.get("institution")

    # Update connection metadata based on provider
    metadata = connection["metadata"]
    if provider == "plaid":
      metadata.update(
        {
          "item_id": item_id,
          "institution_name": institution.get("name")
          if institution
          else "Unknown Bank",
          "accounts": accounts,
          "status": "connected",
        }
      )

    # Update connection in database with new metadata
    await ConnectionService.update(
      connection_id=request.connection_id,
      user_id=str(current_user.id),
      metadata=metadata,
      status="connected",
      graph_id=graph_id,
    )

    # Log security event
    client_ip = fastapi_request.client.host if fastapi_request.client else None
    user_agent = fastapi_request.headers.get("user-agent")

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=str(current_user.id),
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=f"/v1/graphs/{graph_id}/connections/link/exchange",
      details={
        "action": "link_token_exchanged",
        "connection_id": request.connection_id,
        "item_id": item_id,
      },
      risk_level="medium",
    )

    # Optionally trigger initial sync
    auto_sync = True  # Always auto-sync on connect
    task_id = None

    if auto_sync:
      # Use provider registry to sync
      task_id = await provider_registry.sync_connection(
        "plaid",
        {**connection, "metadata": metadata},  # Use updated metadata
        None,
        graph_id,
      )
      logger.info(f"Auto-sync initiated for {provider} connection: task_id={task_id}")

    return {
      "success": True,
      "message": f"{provider.title()} connection established successfully",
      "connection_id": request.connection_id,
      "auto_sync_task_id": task_id,
    }

  except HTTPException:
    raise
  except Exception as e:
    # Log security failure
    client_ip = fastapi_request.client.host if fastapi_request.client else None
    user_agent = fastapi_request.headers.get("user-agent")

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
      user_id=str(current_user.id),
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=f"/v1/graphs/{graph_id}/connections/link/exchange",
      details={
        "action": "link_token_exchange_failed",
        "connection_id": request.connection_id,
        "error": str(e),
      },
      risk_level="high",
    )

    logger.error(f"Link token exchange failed: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Token exchange failed: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )
