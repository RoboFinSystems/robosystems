"""Unit tests for OAuth API models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from robosystems.models.api.oauth import (
  OAuthCallbackRequest,
  OAuthCallbackResponse,
  OAuthConnectionUpdate,
  OAuthInitRequest,
  OAuthInitResponse,
  OAuthProvider,
  OAuthTokens,
)


@pytest.mark.unit
class TestOAuthProvider:
  def test_valid_provider(self):
    model = OAuthProvider(
      name="quickbooks",
      client_id="client_123",
      client_secret="secret_456",
      authorize_url="https://auth.example.com/authorize",
      token_url="https://auth.example.com/token",
      scopes=["read", "write"],
      redirect_uri="http://localhost:8000/callback",
    )
    assert model.name == "quickbooks"
    assert len(model.scopes) == 2

  def test_all_fields_required(self):
    with pytest.raises(ValidationError):
      OAuthProvider(name="quickbooks")  # type: ignore[call-arg]


@pytest.mark.unit
class TestOAuthInitRequest:
  def test_valid_request(self):
    model = OAuthInitRequest(connection_id="conn_123")
    assert model.connection_id == "conn_123"
    assert model.redirect_uri is None
    assert model.additional_params is None

  def test_with_optional_fields(self):
    model = OAuthInitRequest(
      connection_id="conn_123",
      redirect_uri="http://localhost:3001/callback",
      additional_params={"scope": "extended"},
    )
    assert model.redirect_uri == "http://localhost:3001/callback"


@pytest.mark.unit
class TestOAuthInitResponse:
  def test_valid_response(self):
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    model = OAuthInitResponse(
      auth_url="https://auth.example.com/authorize?state=abc",
      state="abc",
      expires_at=ts,
    )
    assert model.auth_url.startswith("https://")
    assert model.state == "abc"


@pytest.mark.unit
class TestOAuthCallbackRequest:
  def test_valid_callback(self):
    model = OAuthCallbackRequest(
      code="auth_code_123",
      state="state_abc",
    )
    assert model.code == "auth_code_123"
    assert model.realm_id is None
    assert model.error is None

  def test_quickbooks_callback_with_realm(self):
    model = OAuthCallbackRequest(
      code="auth_code_123",
      state="state_abc",
      realm_id="12345678",
    )
    assert model.realm_id == "12345678"

  def test_error_callback(self):
    model = OAuthCallbackRequest(
      code="",
      state="state_abc",
      error="access_denied",
      error_description="User denied access",
    )
    assert model.error == "access_denied"
    assert model.error_description == "User denied access"


@pytest.mark.unit
class TestOAuthTokens:
  def test_minimal_tokens(self):
    model = OAuthTokens(access_token="access_123")
    assert model.access_token == "access_123"
    assert model.refresh_token is None
    assert model.token_type == "Bearer"
    assert model.expires_in is None
    assert model.expires_at is None
    assert model.scope is None

  def test_full_tokens(self):
    ts = datetime(2024, 6, 1, 1, 0, 0, tzinfo=UTC)
    model = OAuthTokens(
      access_token="access_123",
      refresh_token="refresh_456",
      token_type="Bearer",
      expires_in=3600,
      expires_at=ts,
      scope="read write",
    )
    assert model.refresh_token == "refresh_456"
    assert model.expires_in == 3600

  def test_access_token_required(self):
    with pytest.raises(ValidationError):
      OAuthTokens()  # type: ignore[call-arg]


@pytest.mark.unit
class TestOAuthConnectionUpdate:
  def test_valid_connected_update(self):
    tokens = OAuthTokens(access_token="access_123")
    model = OAuthConnectionUpdate(
      tokens=tokens,
      provider_data={"realm_id": "12345"},
      status="connected",
    )
    assert model.status == "connected"

  def test_valid_error_update(self):
    tokens = OAuthTokens(access_token="")
    model = OAuthConnectionUpdate(
      tokens=tokens,
      provider_data={"error": "token_expired"},
      status="error",
    )
    assert model.status == "error"

  def test_invalid_status(self):
    tokens = OAuthTokens(access_token="access_123")
    with pytest.raises(ValidationError) as exc_info:
      OAuthConnectionUpdate(
        tokens=tokens,
        provider_data={},
        status="pending",  # type: ignore[arg-type]
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("status",) for e in errors)

  def test_tokens_required(self):
    with pytest.raises(ValidationError):
      OAuthConnectionUpdate(
        provider_data={},
        status="connected",
      )  # type: ignore[call-arg]


@pytest.mark.unit
class TestOAuthCallbackResponse:
  """The callback's success payload is a published contract: the OpenAPI 200
  for oauthCallback is generated from this model, and the TypeScript client
  casts against it."""

  def test_valid_response(self):
    model = OAuthCallbackResponse(
      success=True,
      message="QuickBooks connection established successfully",
      connection_id="conn_123",
      auto_sync_task_id="task_456",
    )
    assert model.success is True
    assert model.connection_id == "conn_123"
    assert model.auto_sync_task_id == "task_456"

  def test_auto_sync_task_id_is_optional(self):
    # No sync is started when the connection is revived rather than created.
    model = OAuthCallbackResponse(
      success=True,
      message="QuickBooks connection established successfully",
      connection_id="conn_123",
    )
    assert model.auto_sync_task_id is None

  def test_requires_connection_id(self):
    with pytest.raises(ValidationError):
      OAuthCallbackResponse(success=True, message="ok")  # type: ignore[call-arg]

  def test_serializes_the_shape_the_router_returns(self):
    # Mirrors the literal dict returned by oauth_callback so a drift between
    # the two shows up here rather than as an `unknown` in the clients.
    payload = {
      "success": True,
      "message": "QuickBooks connection established successfully",
      "connection_id": "conn_123",
      "auto_sync_task_id": None,
    }
    assert OAuthCallbackResponse(**payload).model_dump() == payload
