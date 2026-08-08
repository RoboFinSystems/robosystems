# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportOperatorIssue=false, reportOptionalMemberAccess=false
"""Tests for OAuthHandler and OAuthState."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.operations.providers.oauth_handler import (
  STATE_TTL_SECONDS,
  OAuthHandler,
  OAuthState,
)

MODULE = "robosystems.operations.providers.oauth_handler"


class MockProvider:
  """Test-only OAuth provider implementation."""

  @property
  def name(self):
    return "test_provider"

  @property
  def client_id(self):
    return "test_client_id"

  @property
  def client_secret(self):
    return "test_client_secret"

  @property
  def authorize_url(self):
    return "https://auth.example.com/oauth2"

  @property
  def token_url(self):
    return "https://auth.example.com/token"

  @property
  def scopes(self):
    return ["read", "write"]

  def get_additional_auth_params(self):
    return {"access_type": "offline"}

  def extract_provider_data(self, callback_data):
    return {"extra": callback_data.get("extra", "")}

  def get_refresh_params(self):
    return {}


class FakeValkey:
  """Minimal Valkey stand-in with real TTL and GETDEL semantics.

  A plain Mock would let the store's contract drift silently — single-use
  redemption and expiry are the two properties under test, so they have to
  actually happen here.
  """

  def __init__(self):
    self.store: dict[str, tuple[str, float]] = {}
    self.now = 1000.0

  def setex(self, key: str, ttl_seconds: int, value: str) -> None:
    self.store[key] = (value, self.now + ttl_seconds)

  def getdel(self, key: str) -> str | None:
    entry = self.store.pop(key, None)
    if entry is None:
      return None
    value, expires_at = entry
    return None if self.now >= expires_at else value

  def advance(self, seconds: float) -> None:
    self.now += seconds


@pytest.fixture(autouse=True)
def fake_valkey():
  """Back OAuthState with an in-test Valkey for the whole module."""
  fake = FakeValkey()
  with patch(f"{MODULE}.create_redis_client", return_value=fake):
    yield fake


@pytest.mark.unit
class TestOAuthState:
  def test_create_returns_state_string(self):
    state = OAuthState.create("conn_1", "usr_1", "https://example.com/callback")

    assert isinstance(state, str)
    assert len(state) > 20

  def test_validate_returns_state_data(self):
    state = OAuthState.create("conn_1", "usr_1", "https://example.com/callback")

    data = OAuthState.validate(state)

    assert data is not None
    assert data["connection_id"] == "conn_1"
    assert data["user_id"] == "usr_1"
    assert data["redirect_uri"] == "https://example.com/callback"

  def test_raw_state_is_not_the_storage_key(self, fake_valkey):
    """The token is hashed at rest, so a store read can't replay a flow."""
    state = OAuthState.create("conn_1", "usr_1", "https://example.com/callback")

    assert state not in fake_valkey.store
    assert len(fake_valkey.store) == 1

  def test_state_survives_across_processes(self, fake_valkey):
    """The whole point: a callback served by another task still validates.

    Reloading the module gives a genuinely fresh class — the stand-in for a
    second API task, which has none of the first one's in-process memory.
    Only the shared store carries over. This is the assertion that fails
    against a per-process dict, where the reloaded class starts empty and
    every cross-task callback 400s.
    """
    import importlib

    state = OAuthState.create("conn_1", "usr_1", "https://example.com/callback")

    second_task = importlib.reload(
      importlib.import_module("robosystems.operations.providers.oauth_handler")
    )
    with patch.object(second_task, "create_redis_client", return_value=fake_valkey):
      data = second_task.OAuthState.validate(state)

    assert data is not None
    assert data["connection_id"] == "conn_1"

  def test_validate_one_time_use(self):
    state = OAuthState.create("conn_1", "usr_1", "https://example.com/callback")

    first = OAuthState.validate(state)
    second = OAuthState.validate(state)

    assert first is not None
    assert second is None

  def test_validate_invalid_state(self):
    result = OAuthState.validate("totally_bogus_state")

    assert result is None

  def test_validate_expired_state(self, fake_valkey):
    state = OAuthState.create("conn_1", "usr_1", "https://example.com/callback")

    fake_valkey.advance(STATE_TTL_SECONDS + 1)

    assert OAuthState.validate(state) is None

  def test_abandoned_states_evict_on_ttl(self, fake_valkey):
    """Abandoned flows expire on their own — the old dict grew forever."""
    OAuthState.create("conn_1", "usr_1", "https://example.com/callback")
    live = OAuthState.create("conn_2", "usr_2", "https://example.com/callback")

    assert fake_valkey.store  # both persisted with a TTL...
    for _, (_, expires_at) in fake_valkey.store.items():
      assert expires_at == fake_valkey.now + STATE_TTL_SECONDS

    data = OAuthState.validate(live)
    assert data is not None
    assert data["connection_id"] == "conn_2"

  def test_validate_fails_closed_when_store_unreachable(self):
    """An outage must read as an invalid state, never as a pass."""
    with patch(f"{MODULE}.create_redis_client", side_effect=RuntimeError("down")):
      assert OAuthState.validate("any_state") is None

  def test_validate_discards_malformed_payload(self, fake_valkey):
    fake_valkey.store[OAuthState._key("s")] = ("not json", fake_valkey.now + 600)

    assert OAuthState.validate("s") is None

  def test_create_raises_when_store_unreachable(self):
    """Fail at mint time, not silently three redirects later."""
    with patch(f"{MODULE}.create_redis_client", side_effect=RuntimeError("down")):
      with pytest.raises(HTTPException) as exc_info:
        OAuthState.create("conn_1", "usr_1", "https://example.com/callback")

    assert exc_info.value.status_code == 503


@pytest.mark.unit
class TestOAuthHandlerAuthorizationUrl:
  @patch(f"{MODULE}.env")
  def test_generates_url_with_redirect(self, mock_env):
    mock_env.get_main_cors_origins.return_value = ["https://myapp.com"]
    handler = OAuthHandler(MockProvider())

    url, state = handler.get_authorization_url(
      "conn_1", "usr_1", redirect_uri="https://myapp.com/callback"
    )

    assert "https://auth.example.com/oauth2?" in url
    assert "client_id=test_client_id" in url
    assert "response_type=code" in url
    assert "redirect_uri=https%3A%2F%2Fmyapp.com%2Fcallback" in url
    assert "scope=read+write" in url
    assert "access_type=offline" in url
    assert f"state={state}" in url
    assert isinstance(state, str)

  @patch(f"{MODULE}.env")
  def test_rejects_redirect_uri_outside_allowlist(self, mock_env, fake_valkey):
    """A client-supplied redirect_uri outside the trusted origins is rejected
    (open-redirect / auth-code-theft guard)."""
    mock_env.get_main_cors_origins.return_value = ["https://roboledger.ai"]
    handler = OAuthHandler(MockProvider())

    with pytest.raises(HTTPException) as exc_info:
      handler.get_authorization_url(
        "conn_1", "usr_1", redirect_uri="https://attacker.example.com/callback"
      )

    assert exc_info.value.status_code == 400
    # The flow never started — no state was persisted.
    assert fake_valkey.store == {}

  @patch(f"{MODULE}.env")
  def test_generates_url_with_default_redirect(self, mock_env):
    mock_env.ROBOSYSTEMS_API_URL = "https://api.robosystems.io"
    handler = OAuthHandler(MockProvider())

    url, state = handler.get_authorization_url("conn_1", "usr_1")

    assert (
      "redirect_uri=https%3A%2F%2Fapi.robosystems.io%2Fv1%2Foauth%2Fcallback%2Ftest_provider"
      in url
    )


@pytest.mark.unit
class TestOAuthHandlerExchangeCode:
  @pytest.mark.asyncio
  async def test_success(self):
    handler = OAuthHandler(MockProvider())
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "access_token": "access_tok",
      "refresh_token": "refresh_tok",
      "token_type": "Bearer",
      "expires_in": 3600,
    }

    with patch("httpx.AsyncClient") as MockClient:
      mock_client = AsyncMock()
      mock_client.post.return_value = mock_response
      MockClient.return_value.__aenter__.return_value = mock_client

      tokens = await handler.exchange_code_for_tokens(
        "auth_code_123", "https://example.com/callback"
      )

    assert tokens["access_token"] == "access_tok"
    assert tokens["refresh_token"] == "refresh_tok"
    assert "expires_at" in tokens

  @pytest.mark.asyncio
  async def test_without_expires_in(self):
    handler = OAuthHandler(MockProvider())
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "access_token": "access_tok",
      "token_type": "Bearer",
    }

    with patch("httpx.AsyncClient") as MockClient:
      mock_client = AsyncMock()
      mock_client.post.return_value = mock_response
      MockClient.return_value.__aenter__.return_value = mock_client

      tokens = await handler.exchange_code_for_tokens(
        "auth_code_123", "https://example.com/callback"
      )

    assert tokens["access_token"] == "access_tok"
    assert "expires_at" not in tokens

  @pytest.mark.asyncio
  async def test_failure_raises_http_exception(self):
    handler = OAuthHandler(MockProvider())
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "invalid_grant"

    with patch("httpx.AsyncClient") as MockClient:
      mock_client = AsyncMock()
      mock_client.post.return_value = mock_response
      MockClient.return_value.__aenter__.return_value = mock_client

      with pytest.raises(HTTPException) as exc_info:
        await handler.exchange_code_for_tokens(
          "bad_code", "https://example.com/callback"
        )

    assert exc_info.value.status_code == 400


@pytest.mark.unit
class TestOAuthHandlerRefreshTokens:
  @pytest.mark.asyncio
  async def test_success(self):
    handler = OAuthHandler(MockProvider())
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "access_token": "new_access",
      "refresh_token": "new_refresh",
      "expires_in": 7200,
    }

    with patch("httpx.AsyncClient") as MockClient:
      mock_client = AsyncMock()
      mock_client.post.return_value = mock_response
      MockClient.return_value.__aenter__.return_value = mock_client

      tokens = await handler.refresh_tokens("old_refresh_token")

    assert tokens["access_token"] == "new_access"
    assert "expires_at" in tokens

  @pytest.mark.asyncio
  async def test_failure_raises_http_exception(self):
    handler = OAuthHandler(MockProvider())
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.text = "invalid_refresh_token"

    with patch("httpx.AsyncClient") as MockClient:
      mock_client = AsyncMock()
      mock_client.post.return_value = mock_response
      MockClient.return_value.__aenter__.return_value = mock_client

      with pytest.raises(HTTPException) as exc_info:
        await handler.refresh_tokens("bad_refresh")

    assert exc_info.value.status_code == 400


@pytest.mark.unit
class TestOAuthHandlerStoreTokens:
  def test_creates_new_credentials(self):
    handler = OAuthHandler(MockProvider())
    session = MagicMock()

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = None

      handler.store_tokens(
        connection_id="conn_1",
        tokens={
          "access_token": "tok_abc",
          "refresh_token": "ref_xyz",
          "token_type": "Bearer",
          "scope": "read write",
        },
        provider_data={"realm_id": "r123"},
        db=session,
        user_id="usr_1",
      )

    MockCreds.create.assert_called_once()
    call_kwargs = MockCreds.create.call_args.kwargs
    assert call_kwargs["connection_id"] == "conn_1"
    assert call_kwargs["provider"] == "test_provider"
    assert call_kwargs["user_id"] == "usr_1"
    creds = call_kwargs["credentials"]
    assert creds["access_token"] == "tok_abc"
    assert creds["realm_id"] == "r123"

  def test_updates_existing_credentials(self):
    handler = OAuthHandler(MockProvider())
    session = MagicMock()
    mock_existing = MagicMock()

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = mock_existing

      handler.store_tokens(
        connection_id="conn_1",
        tokens={
          "access_token": "new_tok",
          "refresh_token": "new_ref",
          "expires_at": datetime.now(UTC) + timedelta(hours=1),
        },
        provider_data={},
        db=session,
      )

    mock_existing.update_credentials.assert_called_once()
    mock_existing.update_expiry.assert_called_once()

  def test_updates_without_expires_at(self):
    handler = OAuthHandler(MockProvider())
    session = MagicMock()
    mock_existing = MagicMock()

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = mock_existing

      handler.store_tokens(
        connection_id="conn_1",
        tokens={"access_token": "tok"},
        provider_data={},
        db=session,
      )

    mock_existing.update_credentials.assert_called_once()
    mock_existing.update_expiry.assert_not_called()


@pytest.mark.unit
class TestOAuthHandlerValidateConnection:
  @pytest.mark.asyncio
  async def test_returns_true_with_token(self):
    handler = OAuthHandler(MockProvider())

    result = await handler.validate_connection("some_access_token")

    assert result is True

  @pytest.mark.asyncio
  async def test_returns_false_without_token(self):
    handler = OAuthHandler(MockProvider())

    result = await handler.validate_connection("")

    assert result is False
