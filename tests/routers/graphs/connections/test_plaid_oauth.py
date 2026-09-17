"""Plaid through the connection OAuth endpoints: a Link token, then Link's token back."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.adapters.plaid.client import PlaidError
from robosystems.models.api.oauth import OAuthCallbackRequest, OAuthInitRequest
from robosystems.routers.graphs.connections.oauth import init_oauth, oauth_callback

OAUTH_MODULE = "robosystems.routers.graphs.connections.oauth"
PROVIDER_MODULE = "robosystems.operations.providers.plaid_provider"

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"
CONNECTION_ID = "conn_plaid456"


def _user():
  user = MagicMock()
  user.id = USER_ID
  return user


def _connection(status="pending_oauth") -> dict:
  return {
    "connection_id": CONNECTION_ID,
    "provider": "plaid",
    "entity_id": GRAPH_ID,
    "status": status,
    "created_at": datetime.now(UTC),
    "metadata": {"last_sync": None},
  }


@pytest.fixture(autouse=True)
def _bypass_gates():
  with (
    patch(f"{OAUTH_MODULE}.require_graph_write_role"),
    patch(f"{OAUTH_MODULE}.assert_provider_compatible"),
    patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=_connection(),
    ),
  ):
    yield


async def _init():
  return await init_oauth(
    graph_id=GRAPH_ID,
    request=OAuthInitRequest(connection_id=CONNECTION_ID),
    current_user=_user(),
    db=MagicMock(),
    _rate_limit=None,
  )


async def _callback():
  return await oauth_callback(
    provider="plaid",
    graph_id=GRAPH_ID,
    request=OAuthCallbackRequest(code="public-sandbox-1", state="state_1"),
    current_user=_user(),
    db=MagicMock(),
    _rate_limit=None,
  )


def _valid_state():
  return patch(
    "robosystems.operations.providers.oauth_handler.OAuthState.validate",
    return_value={
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "",
    },
  )


@pytest.mark.unit
@pytest.mark.asyncio
class TestInit:
  async def test_returns_a_link_token_and_a_long_lived_state(self):
    with (
      patch(
        f"{PROVIDER_MODULE}.create_link_token",
        new_callable=AsyncMock,
        return_value={"link_token": "link-sandbox-1"},
      ) as create,
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.create",
        return_value="state_1",
      ) as state,
    ):
      result = await _init()
    assert result.link_token == "link-sandbox-1"
    assert result.auth_url is None
    assert result.state == "state_1"
    assert create.await_args.args[:2] == (CONNECTION_ID, USER_ID)
    assert state.call_args.kwargs["ttl_seconds"] == 1800

  async def test_a_plaid_refusal_is_a_provider_error(self):
    with patch(
      f"{PROVIDER_MODULE}.create_link_token",
      new_callable=AsyncMock,
      side_effect=PlaidError("nope", code="INVALID_FIELD"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await _init()
    assert exc_info.value.status_code == 400


@pytest.mark.unit
@pytest.mark.asyncio
class TestCallback:
  async def test_completes_link_with_the_public_token(self):
    outcome = {
      "success": True,
      "message": "Harborline Bank connected through Plaid",
      "connection_id": CONNECTION_ID,
      "auto_sync_task_id": "run_1",
    }
    with (
      _valid_state(),
      patch(
        f"{PROVIDER_MODULE}.complete_plaid_link",
        new_callable=AsyncMock,
        return_value=outcome,
      ) as complete,
    ):
      assert await _callback() == outcome
    kwargs = complete.await_args.kwargs
    assert kwargs["public_token"] == "public-sandbox-1"
    assert kwargs["user_id"] == USER_ID

  async def test_a_duplicate_is_refused_and_the_pending_row_withdrawn(self):
    from robosystems.operations.providers.plaid_provider import (
      DuplicateBankConnectionError,
    )

    with (
      _valid_state(),
      patch(
        f"{PROVIDER_MODULE}.complete_plaid_link",
        new_callable=AsyncMock,
        side_effect=DuplicateBankConnectionError("conn_other", "Harborline Bank"),
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.delete_connection", new_callable=AsyncMock
      ) as delete,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await _callback()
    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "DUPLICATE_BANK_CONNECTION"
    delete.assert_awaited_once()

  async def test_a_plaid_failure_is_a_provider_error(self):
    with (
      _valid_state(),
      patch(
        f"{PROVIDER_MODULE}.complete_plaid_link",
        new_callable=AsyncMock,
        side_effect=PlaidError("expired", code="INVALID_PUBLIC_TOKEN"),
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await _callback()
    assert exc_info.value.status_code == 400
