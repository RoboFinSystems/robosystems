"""Mercury through the connection OAuth endpoints: init and the callback."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.providers.types import SyncOutcome
from robosystems.routers.graphs.connections.oauth import init_oauth, oauth_callback

OAUTH_MODULE = "robosystems.routers.graphs.connections.oauth"
PROVIDER_MODULE = "robosystems.operations.providers.mercury_provider"

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"
CONNECTION_ID = "conn_mercury456"


def _user():
  user = MagicMock()
  user.id = USER_ID
  return user


def _connection(last_sync=None) -> dict:
  return {
    "connection_id": CONNECTION_ID,
    "provider": "mercury",
    "entity_id": GRAPH_ID,
    "status": "pending_oauth",
    "created_at": datetime.now(UTC),
    "updated_at": datetime.now(UTC),
    "metadata": {"last_sync": last_sync, "realm_id": None},
  }


@pytest.fixture(autouse=True)
def _bypass_gates():
  with (
    patch(f"{OAUTH_MODULE}.require_graph_write_role"),
    patch(f"{OAUTH_MODULE}.assert_provider_compatible"),
  ):
    yield


@pytest.mark.unit
@pytest.mark.asyncio
async def test_init_oauth_uses_the_mercury_handler():
  from robosystems.models.api.oauth import OAuthInitRequest

  handler = MagicMock()
  handler.get_authorization_url.return_value = (
    "https://oauth2-sandbox.mercury.com/oauth2/auth?client_id=x",
    "state_1",
  )
  with (
    patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=_connection(),
    ),
    patch(f"{PROVIDER_MODULE}.mercury_oauth_handler", handler),
  ):
    result = await init_oauth(
      graph_id=GRAPH_ID,
      request=OAuthInitRequest(
        connection_id=CONNECTION_ID,
        redirect_uri="http://localhost:3001/connections/mercury-callback",
      ),
      current_user=_user(),
      db=MagicMock(),
      _rate_limit=None,
    )
  assert result.auth_url.startswith("https://oauth2-sandbox.mercury.com")
  assert result.state == "state_1"
  handler.get_authorization_url.assert_called_once_with(
    connection_id=CONNECTION_ID,
    user_id=USER_ID,
    redirect_uri="http://localhost:3001/connections/mercury-callback",
  )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_callback_completes_consent_records_it_and_syncs():
  from robosystems.models.api.oauth import OAuthCallbackRequest

  tokens = {
    "access_token": "acc",
    "refresh_token": "ref",
    "scope": "read offline_access",
    "expires_at": datetime.now(UTC),
  }
  handler = MagicMock()
  handler.exchange_code_for_tokens = AsyncMock(return_value=tokens)
  provider = MagicMock()
  provider.extract_provider_data.return_value = {
    "auth_mode": "oauth",
    "sync_config": {"since_date": "2026-01-01"},
  }
  provider.get_entity_info = AsyncMock(
    return_value={"legal_business_name": "Cascade Books LLC", "account_count": 3}
  )
  parked = MagicMock()
  parked.get_credentials.return_value = {
    "auth_mode": "oauth",
    "sync_config": {"since_date": "2026-01-01"},
  }
  registry = MagicMock()
  registry.sync_connection = AsyncMock(
    return_value=SyncOutcome(status="dispatched", task_id="run_9")
  )

  with (
    patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      return_value={
        "user_id": USER_ID,
        "connection_id": CONNECTION_ID,
        "redirect_uri": "http://localhost:3001/connections/mercury-callback",
      },
    ),
    patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=_connection(),
    ),
    patch(f"{OAUTH_MODULE}.ConnectionService.update", new_callable=AsyncMock) as update,
    patch(
      "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
      return_value=parked,
    ),
    patch(f"{PROVIDER_MODULE}.mercury_oauth_handler", handler),
    patch(f"{PROVIDER_MODULE}.mercury_oauth_provider", provider),
    patch(f"{PROVIDER_MODULE}.record_bank_feed_consent") as consent,
    patch(f"{OAUTH_MODULE}.provider_registry", registry),
  ):
    result = await oauth_callback(
      provider="mercury",
      graph_id=GRAPH_ID,
      request=OAuthCallbackRequest(code="code_1", state="state_1"),
      current_user=_user(),
      db=MagicMock(),
      _rate_limit=None,
    )

  assert result == {
    "success": True,
    "message": "Mercury connection established successfully",
    "connection_id": CONNECTION_ID,
    "auto_sync_task_id": "run_9",
  }
  handler.exchange_code_for_tokens.assert_awaited_once_with(
    "code_1", "http://localhost:3001/connections/mercury-callback"
  )
  # The parked sync config rides into the token bundle.
  provider.extract_provider_data.assert_called_once_with(
    {"sync_config": {"since_date": "2026-01-01"}}
  )
  store_args = handler.store_tokens.call_args
  assert store_args.args[0] == CONNECTION_ID
  assert store_args.args[2] == {
    "auth_mode": "oauth",
    "sync_config": {"since_date": "2026-01-01"},
  }
  assert store_args.kwargs["user_id"] == USER_ID
  # The row is connected and named after the organization.
  metadata = update.call_args.kwargs["metadata"]
  assert metadata["status"] == "connected"
  assert metadata["entity_name"] == "Cascade Books LLC"
  assert metadata["institution_name"] == "Mercury"
  # The DAA consent record.
  consent.assert_called_once()
  assert consent.call_args.kwargs["organization"] == "Cascade Books LLC"
  assert consent.call_args.kwargs["scope"] == "read offline_access"
  # First consent → full backfill.
  registry.sync_connection.assert_awaited_once()
  assert registry.sync_connection.call_args.args[2] == {"full_rebuild": True}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_callback_with_no_readable_accounts_is_400():
  from fastapi import HTTPException

  from robosystems.models.api.oauth import OAuthCallbackRequest

  handler = MagicMock()
  handler.exchange_code_for_tokens = AsyncMock(return_value={"access_token": "acc"})
  provider = MagicMock()
  provider.extract_provider_data.return_value = {
    "auth_mode": "oauth",
    "sync_config": {},
  }
  provider.get_entity_info = AsyncMock(return_value={})

  with (
    patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      return_value={
        "user_id": USER_ID,
        "connection_id": CONNECTION_ID,
        "redirect_uri": "http://localhost:3001/connections/mercury-callback",
      },
    ),
    patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=_connection(),
    ),
    patch(
      "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
      return_value=None,
    ),
    patch(f"{PROVIDER_MODULE}.mercury_oauth_handler", handler),
    patch(f"{PROVIDER_MODULE}.mercury_oauth_provider", provider),
    patch(f"{PROVIDER_MODULE}.record_bank_feed_consent") as consent,
  ):
    with pytest.raises(HTTPException) as excinfo:
      await oauth_callback(
        provider="mercury",
        graph_id=GRAPH_ID,
        request=OAuthCallbackRequest(code="code_1", state="state_1"),
        current_user=_user(),
        db=MagicMock(),
        _rate_limit=None,
      )
  assert excinfo.value.status_code == 400
  consent.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_callback_reconsent_keeps_the_incremental_window():
  from robosystems.models.api.oauth import OAuthCallbackRequest

  handler = MagicMock()
  handler.exchange_code_for_tokens = AsyncMock(return_value={"access_token": "acc"})
  provider = MagicMock()
  provider.extract_provider_data.return_value = {
    "auth_mode": "oauth",
    "sync_config": {},
  }
  provider.get_entity_info = AsyncMock(return_value={"legal_business_name": "X"})
  registry = MagicMock()
  registry.sync_connection = AsyncMock(
    return_value=SyncOutcome(status="dispatched", task_id="run_2")
  )
  with (
    patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      return_value={
        "user_id": USER_ID,
        "connection_id": CONNECTION_ID,
        "redirect_uri": "http://localhost:3001/connections/mercury-callback",
      },
    ),
    patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=_connection(last_sync="2026-09-01T00:00:00"),
    ),
    patch(f"{OAUTH_MODULE}.ConnectionService.update", new_callable=AsyncMock),
    patch(
      "robosystems.models.core.ConnectionCredentials.get_by_connection_id",
      return_value=None,
    ),
    patch(f"{PROVIDER_MODULE}.mercury_oauth_handler", handler),
    patch(f"{PROVIDER_MODULE}.mercury_oauth_provider", provider),
    patch(f"{PROVIDER_MODULE}.record_bank_feed_consent"),
    patch(f"{OAUTH_MODULE}.provider_registry", registry),
  ):
    await oauth_callback(
      provider="mercury",
      graph_id=GRAPH_ID,
      request=OAuthCallbackRequest(code="code_1", state="state_1"),
      current_user=_user(),
      db=MagicMock(),
      _rate_limit=None,
    )
  assert registry.sync_connection.call_args.args[2] is None
