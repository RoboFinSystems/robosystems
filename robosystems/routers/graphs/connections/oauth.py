"""OAuth endpoints for connection authentication."""

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
  dispatch_first_sync,
)

router = APIRouter()

# QuickBooks and Mercury redirect (OAuth 2.0); Plaid authorizes in the
# embedded Link widget: init returns a link_token, and Link's public_token
# comes back through the callback as ``code``. Handlers are looked up per call
# so tests can patch the provider module's singleton.
OAUTH_PROVIDERS = frozenset({"quickbooks", "mercury", "plaid"})


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

  No realm and no revival path: a disconnected feed is purged and a reconnect
  is a new row. The consent is written to the security audit log (who
  connected which organization, over which scope, when), as the data
  agreement requires.
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
  task_id = await dispatch_first_sync(
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=str(current_user.id),
    full_rebuild=is_first_sync,
  )
  logger.info(
    "Auto-sync initiated for Mercury connection: task_id=%s (first_sync=%s)",
    task_id,
    is_first_sync,
  )
  return {
    "success": True,
    "message": "Mercury connection established successfully",
    "connection_id": connection_id,
    "auto_sync_task_id": task_id,
  }


async def _init_plaid_link(
  *, connection_id: str, user_id: str, redirect_uri: str | None, db: Session
) -> OAuthInitResponse:
  """A Link token for the connection, and the state its callback redeems.

  Update mode when the connection already holds an Item (a login to repair);
  a new Item otherwise. The state outlives the default OAuth window: the
  bank's own multi-factor step happens inside Link.
  """
  from robosystems.adapters.plaid.client import PlaidError
  from robosystems.operations.providers.oauth_handler import OAuthState
  from robosystems.operations.providers.plaid_provider import (
    LINK_STATE_TTL_SECONDS,
    create_link_token,
  )

  try:
    link = await create_link_token(connection_id, user_id, db)
  except PlaidError as exc:
    logger.warning(
      "Plaid refused a Link token for connection %s: %s (request %s)",
      connection_id,
      exc.code,
      exc.request_id,
    )
    raise create_error_response(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Plaid could not start Link for this connection.",
      code=ErrorCode.PROVIDER_ERROR,
    )
  state = OAuthState.create(
    connection_id, user_id, redirect_uri or "", ttl_seconds=LINK_STATE_TTL_SECONDS
  )
  return OAuthInitResponse(
    link_token=str(link["link_token"]),
    state=state,
    expires_at=datetime.now(UTC) + timedelta(seconds=LINK_STATE_TTL_SECONDS),
  )


async def _complete_plaid_link(
  *,
  graph_id: str,
  connection: dict,
  connection_id: str,
  public_token: str,
  current_user: User,
  db: Session,
) -> dict:
  """Finish Link. A bank the graph already has connected is refused (409) and
  the fresh pending row withdrawn."""
  from robosystems.adapters.plaid.client import PlaidError
  from robosystems.operations.providers.plaid_provider import (
    DuplicateBankConnectionError,
    complete_plaid_link,
  )

  try:
    return await complete_plaid_link(
      graph_id=graph_id,
      connection=connection,
      connection_id=connection_id,
      public_token=public_token,
      user_id=str(current_user.id),
      db=db,
    )
  except DuplicateBankConnectionError as duplicate:
    if connection.get("status") == "pending_oauth":
      await ConnectionService.delete_connection(
        connection_id, str(current_user.id), graph_id=graph_id, db_session=db
      )
    raise create_error_response(
      status_code=status.HTTP_409_CONFLICT,
      detail=str(duplicate),
      code="DUPLICATE_BANK_CONNECTION",
    )
  except PlaidError as exc:
    logger.warning(
      "Plaid Link completion failed for connection %s: %s (request %s)",
      connection_id,
      exc.code,
      exc.request_id,
    )
    raise create_error_response(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Plaid could not complete the bank connection. Start Link again.",
      code=ErrorCode.PROVIDER_ERROR,
    )


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
  # Completing OAuth stores credentials and starts a sync: a graph write.
  require_graph_write_role(str(current_user.id), graph_id)

  try:
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
    # runs here too.
    try:
      assert_provider_compatible(graph_id, provider, db)
    except ProviderConflictError as conflict:
      raise create_error_response(
        status_code=status.HTTP_409_CONFLICT,
        detail=conflict.message,
        code=conflict.code,
      )

    if provider == "plaid":
      return await _init_plaid_link(
        connection_id=request.connection_id,
        user_id=str(current_user.id),
        redirect_uri=request.redirect_uri,
        db=db,
      )

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
  description="Completes the OAuth authorization flow after provider redirect. Exchanges the authorization code for tokens, stores them, and triggers an initial sync. This is a redirect target — not typically called directly. Plaid: pass Link's public_token as `code`; a bank already connected to the graph is refused (409 DUPLICATE_BANK_CONNECTION).",
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
  # Stores tokens, revives connections and starts a sync: a graph write.
  require_graph_write_role(str(current_user.id), graph_id)

  try:
    if request.error:
      logger.error(f"OAuth error: {request.error} - {request.error_description}")
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"OAuth authorization failed: {request.error_description or request.error}",
        code=ErrorCode.PROVIDER_ERROR,
      )

    from robosystems.operations.providers.oauth_handler import OAuthState

    state_data = OAuthState.validate(request.state)
    if not state_data:
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Invalid or expired OAuth state",
        code=ErrorCode.INVALID_INPUT,
      )

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

    if connection["provider"].lower() != provider.lower():
      raise create_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Provider mismatch",
        code=ErrorCode.INVALID_INPUT,
      )

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

      # Must not move to another company: the ledger holds the first
      # company's books and the next incremental sync would write over them.
      stored_realm_id = (connection.get("metadata") or {}).get("realm_id")
      if stored_realm_id and returned_realm_id and stored_realm_id != returned_realm_id:
        raise create_error_response(
          status_code=status.HTTP_409_CONFLICT,
          detail=(
            "This connection belongs to a different QuickBooks company. "
            "Reconnect the same company, or add the other one as a new connection."
          ),
          code=ErrorCode.INVALID_INPUT,
        )
      is_pending = connection.get("status") == "pending_oauth"

      # Revive a soft-deleted connection for this graph+provider+realm in
      # place, keeping its connection_id so tenant-side data scoped to it
      # stays attached.
      revived_id: str | None = None
      if returned_realm_id:
        from robosystems.models.core.connection.connection import Connection

        prior = Connection.find_soft_deleted_for_realm(
          graph_id=graph_id,
          provider="quickbooks",
          realm_id=returned_realm_id,
          session=db,
        )
        if prior is not None and prior.id != connection_id and is_pending:
          logger.info(
            "Re-OAuth reuse: reviving soft-deleted connection %s for "
            "realm %s; discarding freshly-created pending %s",
            prior.id,
            returned_realm_id,
            connection_id,
          )
          # Restore commits before delete: a crash between leaves both alive
          # (recoverable on retry); the reverse would leave neither visible.
          prior.restore(db)
          revived_id = str(prior.id)
          # The pending row has no tenant data and no credentials yet.
          pending = Connection.get_by_id(connection_id, db)
          if pending is not None:
            pending.delete(db)
          connection = await ConnectionService.get_connection(
            revived_id, current_user.id, graph_id=graph_id, db_session=db
          )

      target_connection_id = revived_id or connection_id

      quickbooks_oauth_handler.store_tokens(
        target_connection_id, tokens, provider_data, db, user_id=str(current_user.id)
      )

      if connection is None:
        # Tokens are already stored; fail with an actionable error rather
        # than an AttributeError in the sync below.
        raise create_error_response(
          status_code=status.HTTP_404_NOT_FOUND,
          detail="Connection not found",
          code=ErrorCode.NOT_FOUND,
        )

      metadata = connection.get("metadata") or {}
      metadata.update(
        {
          "status": "connected",
          "realm_id": returned_realm_id,
          "last_auth": datetime.now(UTC).isoformat(),
        }
      )

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
        # A fresh connection has nothing to be incremental against, so its
        # first sync is a full rebuild. Dispatched under the per-connection
        # lock, like the sync endpoint.
        is_first_sync = (connection.get("metadata") or {}).get("last_sync") is None
        task_id = await dispatch_first_sync(
          graph_id=graph_id,
          connection_id=target_connection_id,
          user_id=str(current_user.id),
          full_rebuild=is_first_sync,
        )
        logger.info(
          "Auto-sync initiated for QuickBooks connection: task_id=%s (first_sync=%s)",
          task_id,
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
    elif provider.lower() == "plaid":
      return await _complete_plaid_link(
        graph_id=graph_id,
        connection=connection,
        connection_id=connection_id,
        public_token=request.code,
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
