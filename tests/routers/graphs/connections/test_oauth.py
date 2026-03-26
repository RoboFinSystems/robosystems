"""
Unit tests for connection OAuth endpoints.

Tests init_oauth and oauth_callback.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.connections.oauth import init_oauth, oauth_callback

OAUTH_MODULE = "robosystems.routers.graphs.connections.oauth"

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"
CONNECTION_ID = "conn_test456"


def _make_mock_user(user_id: str = USER_ID):
  user = MagicMock()
  user.id = user_id
  user.email = "test@example.com"
  return user


def _make_connection_dict(
  connection_id: str = CONNECTION_ID,
  provider: str = "quickbooks",
  entity_id: str = GRAPH_ID,
  status: str = "pending_oauth",
) -> dict:
  return {
    "connection_id": connection_id,
    "provider": provider,
    "entity_id": entity_id,
    "status": status,
    "created_at": datetime.now(UTC),
    "updated_at": datetime.now(UTC),
    "metadata": {
      "last_sync": None,
      "realm_id": None,
    },
  }


def _make_oauth_init_request(
  connection_id: str = CONNECTION_ID,
  redirect_uri: str | None = "http://localhost:3001/connections/qb-callback",
):
  from robosystems.models.api.oauth import OAuthInitRequest

  return OAuthInitRequest(
    connection_id=connection_id,
    redirect_uri=redirect_uri,
  )


def _make_oauth_callback_request(
  code: str = "auth_code_abc123",
  state: str = "valid_state_token",
  realm_id: str | None = "9341452700148642",
  error: str | None = None,
  error_description: str | None = None,
):
  from robosystems.models.api.oauth import OAuthCallbackRequest

  return OAuthCallbackRequest(
    code=code,
    state=state,
    realm_id=realm_id,
    error=error,
    error_description=error_description,
  )


# ---------------------------------------------------------------------------
# init_oauth
# ---------------------------------------------------------------------------


class TestInitOAuth:
  """Tests for the init_oauth endpoint."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_init_oauth_success_quickbooks(self):
    """Happy path: returns auth_url and state for QuickBooks OAuth."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    connection_dict = _make_connection_dict(provider="quickbooks")

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.get_authorization_url = MagicMock(
      return_value=(
        "https://appcenter.intuit.com/connect/oauth2?client_id=...",
        "state_token_xyz",
      )
    )

    with (
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
    ):
      result = await init_oauth(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.auth_url.startswith("https://")
    assert result.state == "state_token_xyz"
    assert result.expires_at > datetime.now(UTC)

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_init_oauth_connection_not_found_raises_404(self):
    """Connection not found → 404 Not Found."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()

    with patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=None,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await init_oauth(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_init_oauth_non_quickbooks_provider_raises_400(self):
    """Unsupported provider for OAuth raises 400 Bad Request."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    # SEC does not support OAuth
    connection_dict = _make_connection_dict(provider="sec")

    with patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=connection_dict,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await init_oauth(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 400

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_init_oauth_sec_provider_raises_400(self):
    """SEC provider raises 400 since only QuickBooks is supported for OAuth."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    connection_dict = _make_connection_dict(provider="sec")

    with patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=connection_dict,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await init_oauth(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 400

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_init_oauth_service_error_raises_500(self):
    """Unexpected exception raises 500."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()

    with patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      side_effect=RuntimeError("DB failure"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await init_oauth(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_init_oauth_http_exception_propagates(self):
    """HTTPException raised internally is re-raised as-is."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    original_exc = HTTPException(status_code=403, detail="No access")

    with patch(
      f"{OAUTH_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      side_effect=original_exc,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await init_oauth(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_init_oauth_expires_at_is_10_minutes_from_now(self):
    """OAuthInitResponse expires_at is approximately 10 minutes from now."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    connection_dict = _make_connection_dict(provider="quickbooks")

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.get_authorization_url = MagicMock(
      return_value=("https://example.com/auth", "state_abc")
    )

    before = datetime.now(UTC)

    with (
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
    ):
      result = await init_oauth(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    after = datetime.now(UTC)
    # expires_at should be between 9m59s and 10m1s from now
    from datetime import timedelta

    assert result.expires_at >= before + timedelta(minutes=9, seconds=59)
    assert result.expires_at <= after + timedelta(minutes=10, seconds=1)


# ---------------------------------------------------------------------------
# oauth_callback
# ---------------------------------------------------------------------------


class TestOAuthCallback:
  """Tests for the oauth_callback endpoint."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_success_quickbooks(self):
    """Happy path: QuickBooks callback completes the OAuth flow."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    connection_dict = _make_connection_dict(provider="quickbooks")

    state_data = {
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }
    tokens = {
      "access_token": "access_abc",
      "refresh_token": "refresh_xyz",
    }

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.exchange_code_for_tokens = AsyncMock(return_value=tokens)
    mock_oauth_handler.store_tokens = MagicMock()

    mock_oauth_provider = MagicMock()
    mock_oauth_provider.extract_provider_data = MagicMock(
      return_value={"realm_id": "9341452700148642"}
    )
    mock_oauth_provider.validate_connection = AsyncMock(return_value=True)

    mock_provider_registry = MagicMock()
    mock_provider_registry.sync_connection = AsyncMock(return_value="task_123")

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.update",
        new_callable=AsyncMock,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.provider_registry", mock_provider_registry),
    ):
      result = await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result["success"] is True
    assert "QuickBooks" in result["message"]
    assert result["connection_id"] == CONNECTION_ID
    assert result["auto_sync_task_id"] == "task_123"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_error_in_request_raises_400(self):
    """Error field in callback request → 400 Bad Request."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request(
      error="access_denied",
      error_description="User denied access",
    )

    with pytest.raises(HTTPException) as exc_info:
      await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 400

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_invalid_state_raises_400(self):
    """Invalid OAuth state → 400 Bad Request."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()

    with patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      return_value=None,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="quickbooks",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 400

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_user_mismatch_raises_403(self):
    """State user_id doesn't match current user → 403 Forbidden."""
    mock_user = _make_mock_user(user_id="usr_different")
    mock_db = MagicMock()
    request = _make_oauth_callback_request()

    state_data = {
      "user_id": USER_ID,  # Different from mock_user.id
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }

    with patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      return_value=state_data,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="quickbooks",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_connection_not_found_raises_404(self):
    """Connection not found after state validation → 404 Not Found."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()

    state_data = {
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=None,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="quickbooks",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_provider_mismatch_raises_400(self):
    """Connection provider doesn't match URL provider → 400 Bad Request."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    # Connection is for SEC but callback says quickbooks
    connection_dict = _make_connection_dict(provider="sec")

    state_data = {
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="quickbooks",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 400

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_unsupported_provider_raises_400(self):
    """Non-QuickBooks provider in URL → 400 Bad Request."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    connection_dict = _make_connection_dict(provider="sec")

    state_data = {
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="sec",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 400

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_validation_fails_raises_400(self):
    """QuickBooks validate_connection returns False → 400 Bad Request."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    connection_dict = _make_connection_dict(provider="quickbooks")

    state_data = {
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }
    tokens = {"access_token": "access_abc", "refresh_token": "refresh_xyz"}

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.exchange_code_for_tokens = AsyncMock(return_value=tokens)
    mock_oauth_handler.store_tokens = MagicMock()

    mock_oauth_provider = MagicMock()
    mock_oauth_provider.extract_provider_data = MagicMock(
      return_value={"realm_id": "9341452700148642"}
    )
    mock_oauth_provider.validate_connection = AsyncMock(return_value=False)

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.update",
        new_callable=AsyncMock,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="quickbooks",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 400

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_service_error_raises_500(self):
    """Unexpected exception raises 500."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()

    with patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      side_effect=RuntimeError("Unexpected error"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="quickbooks",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_http_exception_propagates(self):
    """HTTPException raised internally is re-raised as-is."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    original_exc = HTTPException(status_code=409, detail="Conflict")

    with patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      side_effect=original_exc,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await oauth_callback(
          provider="quickbooks",
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 409

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_auto_sync_triggered(self):
    """Auto-sync is triggered after successful OAuth and connection validation."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    connection_dict = _make_connection_dict(provider="quickbooks")

    state_data = {
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }
    tokens = {"access_token": "access_abc", "refresh_token": "refresh_xyz"}

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.exchange_code_for_tokens = AsyncMock(return_value=tokens)
    mock_oauth_handler.store_tokens = MagicMock()

    mock_oauth_provider = MagicMock()
    mock_oauth_provider.extract_provider_data = MagicMock(
      return_value={"realm_id": "9341452700148642"}
    )
    mock_oauth_provider.validate_connection = AsyncMock(return_value=True)

    mock_provider_registry = MagicMock()
    sync_mock = AsyncMock(return_value="task_sync_999")
    mock_provider_registry.sync_connection = sync_mock

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.update",
        new_callable=AsyncMock,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.provider_registry", mock_provider_registry),
    ):
      result = await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    sync_mock.assert_awaited_once_with("quickbooks", connection_dict, None, GRAPH_ID)
    assert result["auto_sync_task_id"] == "task_sync_999"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_error_description_used_in_message(self):
    """error_description is preferred over error in the 400 detail message."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request(
      error="access_denied",
      error_description="The user clicked deny",
    )

    with pytest.raises(HTTPException) as exc_info:
      await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 400
    detail = exc_info.value.detail
    # detail may be a dict or string depending on create_error_response
    detail_str = str(detail)
    assert "The user clicked deny" in detail_str

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_stores_tokens_with_user_id(self):
    """store_tokens is called with the authenticated user's ID."""
    mock_user = _make_mock_user(user_id="usr_specific_789")
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    connection_dict = _make_connection_dict(provider="quickbooks")

    state_data = {
      "user_id": "usr_specific_789",
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }
    tokens = {"access_token": "tok_abc", "refresh_token": "ref_xyz"}

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.exchange_code_for_tokens = AsyncMock(return_value=tokens)
    mock_oauth_handler.store_tokens = MagicMock()

    mock_oauth_provider = MagicMock()
    mock_oauth_provider.extract_provider_data = MagicMock(
      return_value={"realm_id": "9341452700148642"}
    )
    mock_oauth_provider.validate_connection = AsyncMock(return_value=True)

    mock_provider_registry = MagicMock()
    mock_provider_registry.sync_connection = AsyncMock(return_value="task_abc")

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.update",
        new_callable=AsyncMock,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.provider_registry", mock_provider_registry),
    ):
      await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    mock_oauth_handler.store_tokens.assert_called_once()
    call_kwargs = mock_oauth_handler.store_tokens.call_args
    # user_id should be passed as keyword arg
    assert call_kwargs.kwargs.get("user_id") == "usr_specific_789"
