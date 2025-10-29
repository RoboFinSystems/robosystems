"""
OAuth endpoints for connection authentication.
"""

from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.models.iam import User
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.operations.connection_service import ConnectionService
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.models.api.oauth import (
  OAuthInitRequest,
  OAuthInitResponse,
  OAuthCallbackRequest,
)
from robosystems.models.api.common import (
  ErrorResponse,
  ErrorCode,
  create_error_response,
)

from .utils import provider_registry

router = APIRouter()


@router.post("/oauth/init", operation_id="initOAuth", response_model=OAuthInitResponse)
async def init_oauth(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: OAuthInitRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OAuthInitResponse:
  """
  Initialize OAuth flow for a connection.

  This generates an authorization URL that the frontend should redirect the user to.
  Currently supports: QuickBooks
  """
  try:
    # Get connection to verify it exists and get provider
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

    # Only QuickBooks supports OAuth currently
    if provider != "quickbooks":
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"OAuth not supported for provider: {provider}",
        code=ErrorCode.PROVIDER_ERROR,
      )

    # Get OAuth handler for provider
    from robosystems.operations.providers.quickbooks_provider import (
      quickbooks_oauth_handler,
    )

    # Generate authorization URL
    auth_url, state = quickbooks_oauth_handler.get_authorization_url(
      connection_id=request.connection_id,
      user_id=str(current_user.id),
      redirect_uri=request.redirect_uri,
    )

    # Return OAuth init response
    return OAuthInitResponse(
      auth_url=auth_url,
      state=state,
      expires_at=datetime.now(timezone.utc) + timedelta(minutes=10),
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"OAuth initialization failed: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"OAuth initialization failed: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.post(
  "/oauth/callback/{provider}",
  summary="OAuth Callback",
  description="""Handle OAuth callback from provider after user authorization.

This endpoint completes the OAuth flow:
1. Validates the OAuth state parameter
2. Exchanges authorization code for access tokens
3. Stores tokens securely
4. Updates connection status
5. Optionally triggers initial sync

Supported providers:
- **QuickBooks**: Accounting data integration

Security measures:
- State validation prevents session hijacking
- User context is verified
- Tokens are encrypted before storage
- Full audit trail is maintained

No credits are consumed for OAuth callbacks.""",
  operation_id="oauthCallback",
  responses={
    200: {
      "description": "OAuth flow completed successfully",
      "content": {
        "application/json": {
          "example": {
            "success": True,
            "message": "QuickBooks connection established successfully",
            "connection_id": "conn_123456",
            "auto_sync_task_id": "task_789012",
          }
        }
      },
    },
    400: {"description": "OAuth error or invalid state", "model": ErrorResponse},
    403: {"description": "State does not match user", "model": ErrorResponse},
    404: {"description": "Connection not found", "model": ErrorResponse},
    500: {"description": "OAuth callback processing failed", "model": ErrorResponse},
  },
)
async def oauth_callback(
  provider: str = Path(..., description="OAuth provider name"),
  graph_id: str = Path(..., description="Graph database identifier"),
  request: OAuthCallbackRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """
  Handle OAuth callback from provider.

  This endpoint is called by the OAuth provider after user authorization.
  It exchanges the authorization code for tokens and updates the connection.
  """
  try:
    # Handle OAuth errors
    if request.error:
      logger.error(f"OAuth error: {request.error} - {request.error_description}")
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"OAuth authorization failed: {request.error_description or request.error}",
        code=ErrorCode.PROVIDER_ERROR,
      )

    # Validate state
    from robosystems.operations.providers.oauth_handler import OAuthState

    state_data = OAuthState.validate(request.state)
    if not state_data:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Invalid or expired OAuth state",
        code=ErrorCode.INVALID_INPUT,
      )

    # Verify user matches
    if str(current_user.id) != state_data["user_id"]:
      raise create_error_response(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="OAuth state does not match current user",
        code=ErrorCode.FORBIDDEN,
      )

    connection_id = state_data["connection_id"]
    redirect_uri = state_data["redirect_uri"]

    # Get connection
    connection = await ConnectionService.get_connection(connection_id, current_user.id)

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    # Verify provider matches
    if connection["provider"].lower() != provider.lower():
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Provider mismatch",
        code=ErrorCode.INVALID_INPUT,
      )

    # Handle provider-specific OAuth completion
    if provider.lower() == "quickbooks":
      from robosystems.operations.providers.quickbooks_provider import (
        quickbooks_oauth_handler,
        quickbooks_oauth_provider,
      )

      # Exchange code for tokens
      tokens = await quickbooks_oauth_handler.exchange_code_for_tokens(
        request.code, redirect_uri
      )

      # Extract provider data
      provider_data = quickbooks_oauth_provider.extract_provider_data(
        {"realmId": request.realm_id}
      )

      # Store tokens
      quickbooks_oauth_handler.store_tokens(connection_id, tokens, provider_data, db)

      # Update connection metadata
      metadata = connection["metadata"]
      metadata.update(
        {
          "status": "connected",
          "realm_id": provider_data.get("realm_id"),
          "last_auth": datetime.now(timezone.utc).isoformat(),
        }
      )

      # Update connection in database
      await ConnectionService.update(
        connection_id=connection_id,
        user_id=str(current_user.id),
        metadata=metadata,
        status="connected",
        graph_id=graph_id,
        db_session=db,
      )

      # Validate connection
      is_valid = await quickbooks_oauth_provider.validate_connection(
        tokens["access_token"], provider_data.get("realm_id")
      )

      if is_valid:
        # Optionally trigger initial sync
        auto_sync = True  # Always auto-sync on connect
        task_id = None

        if auto_sync:
          task_id = await provider_registry.sync_connection(
            "quickbooks", connection, None, graph_id
          )
          logger.info(
            f"Auto-sync initiated for QuickBooks connection: task_id={task_id}"
          )

        return {
          "success": True,
          "message": "QuickBooks connection established successfully",
          "connection_id": connection_id,
          "auto_sync_task_id": task_id,
        }
      else:
        raise create_error_response(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Failed to validate QuickBooks connection",
          code=ErrorCode.PROVIDER_ERROR,
        )
    else:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"OAuth callback not implemented for provider: {provider}",
        code=ErrorCode.PROVIDER_ERROR,
      )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"OAuth callback failed: {e}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"OAuth callback failed: {str(e)}",
      code=ErrorCode.INTERNAL_ERROR,
    )
