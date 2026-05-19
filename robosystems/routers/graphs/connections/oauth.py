"""
OAuth endpoints for connection authentication.
"""

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import (
  RESOURCE_ERROR_RESPONSES,
  ErrorCode,
  create_error_response,
)
from robosystems.models.api.oauth import (
  OAuthCallbackRequest,
  OAuthInitRequest,
  OAuthInitResponse,
)
from robosystems.models.core import User
from robosystems.operations.connection_service import ConnectionService

from .utils import provider_registry

router = APIRouter()


@router.post(
  "/oauth/init",
  operation_id="initOAuth",
  summary="Initialize OAuth Flow",
  response_model=OAuthInitResponse,
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def init_oauth(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  request: OAuthInitRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> OAuthInitResponse:
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
      expires_at=datetime.now(UTC) + timedelta(minutes=10),
    )

  except HTTPException:
    raise
  except Exception:
    logger.error("OAuth initialization failed", exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="OAuth initialization failed",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.post(
  "/oauth/callback/{provider}",
  summary="OAuth Callback",
  description="Completes the OAuth authorization flow after provider redirect. Exchanges the authorization code for tokens, stores them, and triggers an initial sync. This is a redirect target — not typically called directly.",
  operation_id="oauthCallback",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def oauth_callback(
  provider: str = Path(..., description="OAuth provider name"),
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  request: OAuthCallbackRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
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
      returned_realm_id = provider_data.get("realm_id")

      # Phase 3 B6 reuse-on-re-OAuth: if a soft-deleted connection exists
      # for this graph+provider+realm, revive it in place rather than
      # leaving the pending connection as a brand-new row. Preserves
      # connection_id so the tenant-side events / agents / elements
      # scoped to it stay attached (avoids the orphan-data footgun the
      # old hard-delete path created).
      revived_id: str | None = None
      if returned_realm_id:
        from robosystems.models.core.connection.connection import Connection

        prior = Connection.find_soft_deleted_for_realm(
          graph_id=graph_id,
          provider="quickbooks",
          realm_id=returned_realm_id,
          session=db,
        )
        if prior is not None and prior.id != connection_id:
          logger.info(
            "Re-OAuth reuse: reviving soft-deleted connection %s for "
            "realm %s; discarding freshly-created pending %s",
            prior.id,
            returned_realm_id,
            connection_id,
          )
          # Hard-delete the pending Connection — it has no tenant data
          # attached and no credentials stored yet (store_tokens fires
          # after this branch).
          pending = Connection.get_by_id(connection_id, db)
          if pending is not None:
            pending.delete(db)
          # Revive the prior connection.
          prior.restore(db)
          revived_id = str(prior.id)
          # Pull the refreshed dict for downstream auto-sync.
          connection = await ConnectionService.get_connection(
            revived_id, current_user.id, db_session=db
          )

      # Route subsequent writes at the revived id if reuse happened,
      # else at the freshly-created pending id.
      target_connection_id = revived_id or connection_id

      # Store tokens
      quickbooks_oauth_handler.store_tokens(
        target_connection_id, tokens, provider_data, db, user_id=str(current_user.id)
      )

      # Update connection metadata
      metadata = (connection or {}).get("metadata") or {}
      metadata.update(
        {
          "status": "connected",
          "realm_id": returned_realm_id,
          "last_auth": datetime.now(UTC).isoformat(),
        }
      )

      # Update connection in database
      await ConnectionService.update(
        connection_id=target_connection_id,
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
          # First sync after a fresh OAuth has no prior data to be
          # incremental against — default to full_rebuild so the user
          # sees their full history. Existing connections (already
          # synced once) re-trigger with the default 60-day incremental
          # window. Detected via ``last_sync`` being None.
          # ``connection`` is the dict from ConnectionService.get_connection
          # → Connection.to_dict(), which nests ``last_sync`` under
          # ``metadata`` (alongside realm_id, item_id, etc.).
          is_first_sync = (connection.get("metadata") or {}).get("last_sync") is None
          sync_options = {"full_rebuild": True} if is_first_sync else None

          task_id = await provider_registry.sync_connection(
            "quickbooks", connection, sync_options, graph_id
          )
          logger.info(
            "Auto-sync initiated for QuickBooks connection: task_id=%s "
            "(first_sync=%s, full_rebuild=%s)",
            task_id,
            is_first_sync,
            is_first_sync,
          )

        return {
          "success": True,
          "message": "QuickBooks connection established successfully",
          "connection_id": target_connection_id,
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
  except Exception:
    logger.error("OAuth callback failed", exc_info=True)
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="OAuth callback failed",
      code=ErrorCode.INTERNAL_ERROR,
    )
