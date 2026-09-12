"""
OAuth endpoints for connection authentication.
"""

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, status
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
  create_error_response,
)
from robosystems.models.api.oauth import (
  OAuthCallbackRequest,
  OAuthCallbackResponse,
  OAuthInitRequest,
  OAuthInitResponse,
)
from robosystems.models.core import User
from robosystems.operations.connection_service import (
  ConnectionService,
  ProviderConflictError,
  assert_provider_compatible,
)

from .utils import provider_registry

router = APIRouter()

# Providers that authorize over OAuth. The handler is looked up per call so a
# test can patch the provider module's singleton.
OAUTH_PROVIDERS = frozenset({"quickbooks", "mercury"})


def _oauth_handler_for(provider: str):
  if provider == "mercury":
    from robosystems.operations.providers.mercury_provider import (
      mercury_oauth_handler,
    )

    return mercury_oauth_handler
  from robosystems.operations.providers.quickbooks_provider import (
    quickbooks_oauth_handler,
  )

  return quickbooks_oauth_handler


async def _complete_mercury_oauth(
  *,
  graph_id: str,
  connection: dict,
  connection_id: str,
  code: str,
  redirect_uri: str,
  current_user: User,
  db: Session,
) -> dict:
  """Finish a Mercury consent: exchange, store, record, validate, sync.

  Mercury has no realm and no revival path — a disconnected feed is purged
  (the partnership's deletion protocol) and a reconnect is a new row. The
  connect-time sync config was parked in the pending row's credential
  bundle; it rides into the token bundle as provider data. The consent is
  written to the security audit log: the record the data agreement asks
  for (who connected which organization, over which scope, when).
  """
  from robosystems.models.core import ConnectionCredentials
  from robosystems.operations.providers.mercury_provider import (
    mercury_oauth_handler,
    mercury_oauth_provider,
    record_bank_feed_consent,
  )

  tokens = await mercury_oauth_handler.exchange_code_for_tokens(code, redirect_uri)

  existing = ConnectionCredentials.get_by_connection_id(connection_id, db)
  parked = existing.get_credentials() if existing else {}
  provider_data = mercury_oauth_provider.extract_provider_data(
    {"sync_config": parked.get("sync_config") or {}}
  )
  mercury_oauth_handler.store_tokens(
    connection_id, tokens, provider_data, db, user_id=str(current_user.id)
  )

  info = await mercury_oauth_provider.get_entity_info(tokens["access_token"])
  if not info:
    raise create_error_response(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Failed to validate Mercury connection",
      code=ErrorCode.PROVIDER_ERROR,
    )
  organization = info.get("legal_business_name")

  metadata = connection.get("metadata") or {}
  metadata.update(
    {
      "status": "connected",
      "entity_name": organization,
      "institution_name": "Mercury",
      "last_auth": datetime.now(UTC).isoformat(),
    }
  )
  await ConnectionService.update(
    connection_id=connection_id,
    user_id=str(current_user.id),
    metadata=metadata,
    status="connected",
    graph_id=graph_id,
    db_session=db,
  )

  record_bank_feed_consent(
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=str(current_user.id),
    auth_mode="oauth",
    scope=tokens.get("scope"),
    organization=organization,
  )

  # The first sync after consent backfills from the connect-time start date;
  # a later re-consent on the same row keeps the incremental window.
  is_first_sync = (connection.get("metadata") or {}).get("last_sync") is None
  outcome = await provider_registry.sync_connection(
    "mercury", connection, {"full_rebuild": True} if is_first_sync else None, graph_id
  )
  logger.info(
    "Auto-sync initiated for Mercury connection: task_id=%s (first_sync=%s)",
    outcome.task_id,
    is_first_sync,
  )
  return {
    "success": True,
    "message": "Mercury connection established successfully",
    "connection_id": connection_id,
    "auto_sync_task_id": outcome.task_id,
  }


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
  # Completing OAuth stores credentials and starts a full-rebuild sync —
  # a write to the graph, so the write role is required from the start.
  require_graph_write_role(str(current_user.id), graph_id)

  try:
    # Get connection to verify it exists and get provider
    connection = await ConnectionService.get_connection(
      request.connection_id, current_user.id, graph_id=graph_id
    )

    if not connection:
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Connection not found",
        code=ErrorCode.NOT_FOUND,
      )

    provider = connection["provider"].lower()

    if provider not in OAUTH_PROVIDERS:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"OAuth not supported for provider: {provider}",
        code=ErrorCode.PROVIDER_ERROR,
      )

    # The callback can revive a soft-deleted connection, so the books guard
    # runs here too (specs/ledger/native-accounting-cutover.md §2).
    try:
      assert_provider_compatible(graph_id, provider, db)
    except ProviderConflictError as conflict:
      raise create_error_response(
        status_code=status.HTTP_409_CONFLICT,
        detail=conflict.message,
        code=conflict.code,
      )

    # Generate authorization URL
    auth_url, state = _oauth_handler_for(provider).get_authorization_url(
      connection_id=request.connection_id,
      user_id=str(current_user.id),
      redirect_uri=request.redirect_uri,
    )

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
  response_model=OAuthCallbackResponse,
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
  # The callback stores tokens, revives soft-deleted connections and kicks
  # off the initial sync: a write to the graph.
  require_graph_write_role(str(current_user.id), graph_id)

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

    connection = await ConnectionService.get_connection(
      connection_id, current_user.id, graph_id=graph_id
    )

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

      tokens = await quickbooks_oauth_handler.exchange_code_for_tokens(
        request.code, redirect_uri
      )

      provider_data = quickbooks_oauth_provider.extract_provider_data(
        {"realmId": request.realm_id}
      )
      returned_realm_id = provider_data.get("realm_id")

      # Reuse-on-re-OAuth: if a soft-deleted connection exists
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
          # Order matters — `restore` commits before `delete` commits.
          # If the process crashes between, BOTH connections are alive
          # (a retry sees the restored prior and skips the no-op
          # delete on the pending). Reversing this order would leave
          # the pending deleted but the prior still soft-deleted on
          # crash — neither would be visible to the user and recovery
          # would require manual intervention.
          prior.restore(db)
          revived_id = str(prior.id)
          # Hard-delete the pending Connection — it has no tenant data
          # attached and no credentials stored yet (store_tokens fires
          # after this branch).
          pending = Connection.get_by_id(connection_id, db)
          if pending is not None:
            pending.delete(db)
          # Pull the refreshed dict for downstream auto-sync.
          connection = await ConnectionService.get_connection(
            revived_id, current_user.id, graph_id=graph_id, db_session=db
          )

      # Route subsequent writes at the revived id if reuse happened,
      # else at the freshly-created pending id.
      target_connection_id = revived_id or connection_id

      quickbooks_oauth_handler.store_tokens(
        target_connection_id, tokens, provider_data, db, user_id=str(current_user.id)
      )

      if connection is None:
        # Tokens are stored on the revived row already; the sync below
        # needs the connection dict, so this is a hard stop, not a
        # crash — surfacing an actionable error beats an AttributeError
        # after the credential write.
        raise create_error_response(
          status_code=status.HTTP_404_NOT_FOUND,
          detail="Connection not found",
          code=ErrorCode.NOT_FOUND,
        )

      # Update connection metadata
      metadata = connection.get("metadata") or {}
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

          outcome = await provider_registry.sync_connection(
            "quickbooks", connection, sync_options, graph_id
          )
          # `auto_sync_task_id` is the run id, not the outcome object.
          task_id = outcome.task_id
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
    elif provider.lower() == "mercury":
      return await _complete_mercury_oauth(
        graph_id=graph_id,
        connection=connection,
        connection_id=connection_id,
        code=request.code,
        redirect_uri=redirect_uri,
        current_user=current_user,
        db=db,
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
