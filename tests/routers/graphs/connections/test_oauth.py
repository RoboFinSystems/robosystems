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
  last_sync: str | None = None,
) -> dict:
  return {
    "connection_id": connection_id,
    "provider": provider,
    "entity_id": entity_id,
    "status": status,
    "created_at": datetime.now(UTC),
    "updated_at": datetime.now(UTC),
    "metadata": {
      "last_sync": last_sync,
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

  @pytest.fixture(autouse=True)
  def _bypass_write_role(self):
    """These tests use synthetic users with no GraphUser row, so the write-role
    + lifecycle gate would 403 before reaching handler logic. The deny path is
    covered in tests/routers/graphs/test_write_role_gates.py; bypass it here."""
    with (
      patch(f"{OAUTH_MODULE}.require_graph_write_role"),
      # The books guard opens an extensions session; its matrix lives in
      # tests/operations/test_connection_service.py, its 409 below.
      patch(f"{OAUTH_MODULE}.assert_provider_compatible"),
    ):
      yield

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

  @pytest.fixture(autouse=True)
  def _bypass_write_role(self):
    """These tests use synthetic users with no GraphUser row, so the write-role
    + lifecycle gate would 403 before reaching handler logic. The deny path is
    covered in tests/routers/graphs/test_write_role_gates.py; bypass it here."""
    with (
      patch(f"{OAUTH_MODULE}.require_graph_write_role"),
      # The same-company guard reads the tenant's books; it runs against
      # real SQL in TestOnlyTheBuildingCompanyReconnects below.
      patch(f"{OAUTH_MODULE}.assert_same_company"),
    ):
      yield

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

    sync_mock = AsyncMock(return_value="task_123")

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
      # The OAuth callback checks for a soft-deleted prior connection
      # matching the returned realm. Happy path: no prior match, proceed
      # with the freshly-created connection_id.
      patch(
        "robosystems.models.core.connection.connection.Connection."
        "find_soft_deleted_for_realm",
        return_value=None,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync_mock),
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
  async def test_oauth_callback_revives_soft_deleted_connection(self):
    """Re-OAuthing to a realm that has a soft-deleted prior connection
    revives the prior in place rather than minting a new connection_id.
    Preserves the tenant-side events/agents/elements."""
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

    # Soft-deleted prior connection with a different ID that we'll
    # revive on this OAuth callback.
    prior_conn = MagicMock()
    prior_conn.id = "conn_prior_soft_deleted"
    prior_conn.restore = MagicMock()

    # The pending freshly-created connection that will be hard-deleted.
    pending_conn = MagicMock()
    pending_conn.id = CONNECTION_ID
    pending_conn.delete = MagicMock()

    revived_connection_dict = _make_connection_dict(provider="quickbooks")
    revived_connection_dict["connection_id"] = "conn_prior_soft_deleted"

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.exchange_code_for_tokens = AsyncMock(return_value=tokens)
    mock_oauth_handler.store_tokens = MagicMock()

    mock_oauth_provider = MagicMock()
    mock_oauth_provider.extract_provider_data = MagicMock(
      return_value={"realm_id": "9341452700148642"}
    )
    mock_oauth_provider.validate_connection = AsyncMock(return_value=True)

    sync_mock = AsyncMock(return_value="task_revived")

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        side_effect=[connection_dict, revived_connection_dict],
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.update",
        new_callable=AsyncMock,
      ),
      patch(
        "robosystems.models.core.connection.connection.Connection."
        "find_soft_deleted_for_realm",
        return_value=prior_conn,
      ),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=pending_conn,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync_mock),
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
    # Result reports the revived connection_id, not the discarded pending one.
    assert result["connection_id"] == "conn_prior_soft_deleted"
    # Prior soft-deleted row was restored.
    prior_conn.restore.assert_called_once_with(mock_db)
    # Freshly-created pending row was hard-deleted (it had no tenant data).
    pending_conn.delete.assert_called_once_with(mock_db)
    # store_tokens routed at the revived id.
    mock_oauth_handler.store_tokens.assert_called_once()
    assert (
      mock_oauth_handler.store_tokens.call_args.args[0] == "conn_prior_soft_deleted"
    )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_refuses_a_different_company(self):
    """A connected connection re-authorized against another QuickBooks company
    is refused before any token is stored or data synced."""
    from fastapi import HTTPException

    connection_dict = _make_connection_dict(status="connected")
    connection_dict["metadata"]["realm_id"] = "1111111111"
    state_data = {
      "user_id": USER_ID,
      "connection_id": CONNECTION_ID,
      "redirect_uri": "http://localhost:3001/connections/qb-callback",
    }
    mock_oauth_handler = MagicMock()
    mock_oauth_handler.exchange_code_for_tokens = AsyncMock(
      return_value={"access_token": "a", "refresh_token": "r"}
    )
    mock_oauth_provider = MagicMock()
    mock_oauth_provider.extract_provider_data = MagicMock(
      return_value={"realm_id": "2222222222"}
    )
    sync_mock = AsyncMock()

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
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync_mock),
      pytest.raises(HTTPException) as excinfo,
    ):
      await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=_make_oauth_callback_request(realm_id="2222222222"),
        current_user=_make_mock_user(),
        db=MagicMock(),
        _rate_limit=None,
      )

    assert excinfo.value.status_code == 409
    mock_oauth_handler.store_tokens.assert_not_called()
    sync_mock.assert_not_called()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_revived_lookup_miss_is_404_not_crash(self):
    """If the revived connection cannot be re-read after the prior row was
    restored (it is stored, tokens are written), the callback answers a 404
    rather than an AttributeError on `None.get(...)`."""
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

    prior_conn = MagicMock()
    prior_conn.id = "conn_prior_soft_deleted"
    pending_conn = MagicMock()
    pending_conn.id = CONNECTION_ID

    mock_oauth_handler = MagicMock()
    mock_oauth_handler.exchange_code_for_tokens = AsyncMock(return_value=tokens)
    mock_oauth_provider = MagicMock()
    mock_oauth_provider.extract_provider_data = MagicMock(
      return_value={"realm_id": "9341452700148642"}
    )
    sync_mock = AsyncMock()

    with (
      patch(
        "robosystems.operations.providers.oauth_handler.OAuthState.validate",
        return_value=state_data,
      ),
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        side_effect=[connection_dict, None],
      ),
      patch(f"{OAUTH_MODULE}.ConnectionService.update", new_callable=AsyncMock),
      patch(
        "robosystems.models.core.connection.connection.Connection."
        "find_soft_deleted_for_realm",
        return_value=prior_conn,
      ),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        return_value=pending_conn,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync_mock),
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
    sync_mock.assert_not_called()

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
    """Auto-sync is triggered after successful OAuth and connection validation.

    First sync (``last_sync=None``) requests a full rebuild — there's no
    prior data to be incremental against, so a 60-day window would miss
    most of the user's history.
    """
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

    sync_mock = AsyncMock(return_value="task_sync_999")

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
        "robosystems.models.core.connection.connection.Connection."
        "find_soft_deleted_for_realm",
        return_value=None,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync_mock),
    ):
      result = await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    sync_mock.assert_awaited_once_with(
      graph_id=GRAPH_ID,
      connection_id=CONNECTION_ID,
      user_id=USER_ID,
      full_rebuild=True,
    )
    assert result["auto_sync_task_id"] == "task_sync_999"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_oauth_callback_subsequent_sync_uses_incremental_default(self):
    """An OAuth re-flow on an existing connection (``last_sync`` already
    populated) uses the incremental default — no `full_rebuild` override.

    Use case: user re-authorizes after token expiry. The connection
    already has months of synced data; a `full_rebuild` would re-pull
    everything for no benefit. Pass ``None`` so the provider falls
    through to ``lookback_days=60``.
    """
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_callback_request()
    # last_sync set → not a first sync.
    connection_dict = _make_connection_dict(
      provider="quickbooks",
      last_sync="2026-04-15T10:00:00",
    )

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

    sync_mock = AsyncMock(return_value="task_sync_resync")

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
        "robosystems.models.core.connection.connection.Connection."
        "find_soft_deleted_for_realm",
        return_value=None,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_provider",
        mock_oauth_provider,
      ),
      patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync_mock),
    ):
      await oauth_callback(
        provider="quickbooks",
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    # No `full_rebuild` override — subsequent syncs use the default
    # incremental window.
    sync_mock.assert_awaited_once_with(
      graph_id=GRAPH_ID,
      connection_id=CONNECTION_ID,
      user_id=USER_ID,
      full_rebuild=False,
    )

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

    sync_mock = AsyncMock(return_value="task_abc")

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
      patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync_mock),
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


class TestInitOAuthBooksGuard:
  """The callback can revive a soft-deleted connection, so the books guard
  also runs at init — a QuickBooks re-OAuth over natively-kept books is
  refused before any authorize URL is minted."""

  @pytest.fixture(autouse=True)
  def _bypass_write_role(self):
    with patch(f"{OAUTH_MODULE}.require_graph_write_role"):
      yield

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_conflict_is_409_before_the_authorize_url(self):
    from robosystems.operations.connection_service import ProviderConflictError

    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    mock_oauth_handler = MagicMock()

    with (
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{OAUTH_MODULE}.assert_provider_compatible",
        side_effect=ProviderConflictError("NATIVE_BOOKS_PRESENT", "native books"),
      ) as guard,
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
      pytest.raises(HTTPException) as exc_info,
    ):
      await init_oauth(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 409
    assert exc_info.value.detail["code"] == "NATIVE_BOOKS_PRESENT"
    guard.assert_called_once_with(
      GRAPH_ID, "quickbooks", mock_db, entity_id=None, resuming=False, realm_id=None
    )
    mock_oauth_handler.get_authorization_url.assert_not_called()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_a_resumed_link_is_judged_by_its_stored_entity(self):
    """A pending feed carries the entity it was connected for; the guard
    judges that entity, and one the graph no longer has is a 404, the same
    answer the create route gives."""
    from robosystems.operations.connection_service import ProviderConflictError

    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    connection_dict = _make_connection_dict(provider="plaid")
    connection_dict["credentials"] = {"sync_config": {"entity_id": "gone"}}

    with (
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{OAUTH_MODULE}.assert_provider_compatible",
        side_effect=ProviderConflictError("ENTITY_NOT_FOUND", "no such entity"),
      ) as guard,
      pytest.raises(HTTPException) as exc_info,
    ):
      await init_oauth(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail["code"] == "ENTITY_NOT_FOUND"
    guard.assert_called_once_with(
      GRAPH_ID, "plaid", mock_db, entity_id="gone", resuming=False, realm_id=None
    )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_a_reconnect_is_judged_as_one(self):
    """A live QuickBooks row past its first OAuth is re-authorizing the books
    it already keeps; the guard is told so and the authorize URL is minted."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_oauth_init_request()
    connection_dict = _make_connection_dict(
      provider="quickbooks", status="needs_reauth"
    )
    mock_oauth_handler = MagicMock()
    mock_oauth_handler.get_authorization_url.return_value = ("https://qb", "state")

    with (
      patch(
        f"{OAUTH_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{OAUTH_MODULE}.assert_provider_compatible") as guard,
      patch(
        "robosystems.operations.providers.quickbooks_provider.quickbooks_oauth_handler",
        mock_oauth_handler,
      ),
    ):
      await init_oauth(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    guard.assert_called_once_with(
      GRAPH_ID, "quickbooks", mock_db, entity_id=None, resuming=True, realm_id=None
    )
    mock_oauth_handler.get_authorization_url.assert_called_once()


# ---------------------------------------------------------------------------
# Only the company that built the books reconnects to them — real SQL
# ---------------------------------------------------------------------------

REALM_A = "9130000000000000001"
REALM_B = "9130000000000000002"
QB_REDIRECT = "http://localhost:3001/connections/qb-callback"


@pytest.fixture()
def tenant_books():
  """An extensions schema on the test database standing in for the graph's
  tenant: every books probe opens its session here."""
  import os
  import uuid
  from contextlib import contextmanager

  from sqlalchemy import create_engine, text
  from sqlalchemy.orm import sessionmaker

  import robosystems.models.extensions  # noqa: F401  (register models)
  from robosystems.db.extensions import ExtensionsBase

  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_oauth_{uuid.uuid4().hex[:12]}"
  admin = create_engine(database_url)
  with admin.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  engine = create_engine(
    database_url, connect_args={"options": f"-csearch_path={schema}"}
  )
  ExtensionsBase.metadata.create_all(bind=engine)
  make_session = sessionmaker(bind=engine)

  @contextmanager
  def tenant_session(graph_id, **_kwargs):
    session = make_session()
    try:
      yield session
    finally:
      session.close()

  try:
    with patch("robosystems.db.extensions.extensions_session", tenant_session):
      yield make_session
  finally:
    engine.dispose()
    with admin.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    admin.dispose()


def _seed_books_built_by(session, connection_id: str, *, tag: str) -> None:
  """The chart and a posted entry a QuickBooks sync of ``connection_id``
  leaves behind, on the group parent it renamed."""
  from datetime import date

  from robosystems.models.extensions import (
    Element,
    Entity,
    EntityTaxonomy,
    LineItem,
    Taxonomy,
  )
  from robosystems.models.extensions.roboledger.entry import Entry

  parent = session.query(Entity).filter(Entity.is_parent.is_(True)).first()
  if parent is None:
    parent = Entity(id="ent_parent", name="Company A", is_parent=True, created_by="t")
    session.add(parent)
    chart = Taxonomy(
      id="tax_qb",
      name="Quickbooks Chart of Accounts",
      taxonomy_type="chart_of_accounts",
      is_active=True,
      created_by="t",
    )
    session.add(chart)
    session.flush()
    session.add(
      EntityTaxonomy(
        entity_id=parent.id,
        taxonomy_id=chart.id,
        is_primary=True,
        basis="chart_of_accounts",
        adoption_context="voluntary",
      )
    )
  parent.source = "quickbooks"
  parent.connection_id = connection_id
  accounts = []
  for i, (name, balance) in enumerate([("Checking", "debit"), ("Sales", "credit")]):
    element = Element(
      id=f"elem_{tag}_{i}",
      code=f"{tag}{i}",
      name=name,
      qname=f"qb:{tag}{i}",
      balance_type=balance,
      source="quickbooks",
      external_source="quickbooks",
      external_id=str(i + 1),
      connection_id=connection_id,
      taxonomy_id="tax_qb",
      element_type="concept",
      period_type="instant" if balance == "debit" else "duration",
      is_abstract=False,
      is_monetary=True,
      item_type="monetary",
    )
    session.add(element)
    accounts.append(element)
  session.flush()
  entry = Entry(
    id=f"je_{tag}",
    entity_id=parent.id,
    type="standard",
    status="posted",
    posting_date=date(2026, 7, 15),
    memo=f"{tag} invoice",
    created_by="t",
  )
  session.add(entry)
  session.flush()
  session.add_all(
    [
      LineItem(
        id=f"li_{tag}_dr",
        entry_id=entry.id,
        element_id=accounts[0].id,
        debit_amount=100,
        credit_amount=0,
      ),
      LineItem(
        id=f"li_{tag}_cr",
        entry_id=entry.id,
        element_id=accounts[1].id,
        debit_amount=0,
        credit_amount=100,
      ),
    ]
  )
  session.commit()


def _disconnected_quickbooks(test_db, graph_id: str, user_id: str, realm: str):
  """A QuickBooks connection after the app's default Disconnect: soft-deleted,
  revivable by the same realm."""
  from robosystems.models.core.connection.connection import Connection

  conn = Connection.create(
    graph_id=graph_id,
    user_id=user_id,
    provider="quickbooks",
    session=test_db,
    status="connected",
    realm_id=realm,
    write_policy="native",
  )
  conn.soft_delete(test_db)
  return conn


def _pending_quickbooks(test_db, graph_id: str, user_id: str, realm=None):
  from robosystems.models.core.connection.connection import Connection

  return Connection.create(
    graph_id=graph_id,
    user_id=user_id,
    provider="quickbooks",
    session=test_db,
    realm_id=realm,
  )


async def _quickbooks_callback(db, user, graph_id: str, connection_id: str, realm):
  """Run the callback for ``realm`` with Intuit stubbed out. Returns the
  result, or the HTTPException, with the token-exchange and sync mocks."""
  from robosystems.operations.providers.quickbooks_provider import (
    quickbooks_oauth_handler,
    quickbooks_oauth_provider,
  )

  exchange = AsyncMock(
    return_value={"access_token": "access_new", "refresh_token": "refresh_new"}
  )
  sync = AsyncMock(return_value="run_1")
  with (
    patch(f"{OAUTH_MODULE}.require_graph_write_role"),
    patch(
      "robosystems.operations.providers.oauth_handler.OAuthState.validate",
      return_value={
        "user_id": str(user.id),
        "connection_id": connection_id,
        "redirect_uri": QB_REDIRECT,
      },
    ),
    patch.object(quickbooks_oauth_handler, "exchange_code_for_tokens", exchange),
    patch.object(
      quickbooks_oauth_provider, "validate_connection", AsyncMock(return_value=True)
    ),
    patch(f"{OAUTH_MODULE}.dispatch_first_sync", sync),
  ):
    try:
      outcome = await oauth_callback(
        provider="quickbooks",
        graph_id=graph_id,
        request=_make_oauth_callback_request(realm_id=realm),
        current_user=user,
        db=db,
        _rate_limit=None,
      )
    except HTTPException as refused:
      outcome = refused
  return outcome, exchange, sync


class TestOnlyTheBuildingCompanyReconnects:
  """After a plain disconnect of QuickBooks company A, the chart and books A
  built stay on the graph. Company B, picked in Intuit's chooser, would be
  merged into them by its first full sync — so only A may reattach."""

  @pytest.fixture(autouse=True)
  def _credential_key(self):
    """Tokens are stored for real here; the suite's key is not a Fernet key."""
    from cryptography.fernet import Fernet

    from robosystems.config import env

    with patch.object(
      env, "CONNECTION_CREDENTIALS_KEY", Fernet.generate_key().decode()
    ):
      yield

  @pytest.mark.unit
  @pytest.mark.parametrize(
    "path",
    ["callback_fresh_pending", "callback_pending_named_realm", "init_named_realm"],
  )
  async def test_another_company_is_refused(
    self, path, test_db, test_user, sample_graph, tenant_books
  ):
    from robosystems.models.core.connection.connection import Connection
    from robosystems.models.core.connection.connection_credentials import (
      ConnectionCredentials,
    )

    graph_id = sample_graph.graph_id
    with tenant_books() as ext:
      conn_a = _disconnected_quickbooks(test_db, graph_id, test_user.id, REALM_A)
      _seed_books_built_by(ext, conn_a.id, tag="a")
    named = None if path == "callback_fresh_pending" else REALM_B
    pending = _pending_quickbooks(test_db, graph_id, test_user.id, realm=named)
    pending_id = pending.id

    if path == "init_named_realm":
      authorize = MagicMock(return_value=("https://intuit", "state"))
      with (
        patch(f"{OAUTH_MODULE}.require_graph_write_role"),
        patch(
          "robosystems.operations.providers.quickbooks_provider."
          "quickbooks_oauth_handler.get_authorization_url",
          authorize,
        ),
        pytest.raises(HTTPException) as refused,
      ):
        await init_oauth(
          graph_id=graph_id,
          request=_make_oauth_init_request(connection_id=pending_id),
          current_user=test_user,
          db=test_db,
          _rate_limit=None,
        )
      assert refused.value.status_code == 409
      assert refused.value.detail["code"] == "OTHER_COMPANY_BOOKS"
      authorize.assert_not_called()
      return

    outcome, exchange, sync = await _quickbooks_callback(
      test_db, test_user, graph_id, pending_id, REALM_B
    )

    assert isinstance(outcome, HTTPException), outcome
    assert outcome.status_code == 409
    assert outcome.detail["code"] == "OTHER_COMPANY_BOOKS"
    assert "different QuickBooks company" in outcome.detail["detail"]
    # Refused before Intuit mints a token: nothing to store or revoke.
    exchange.assert_not_called()
    sync.assert_not_called()
    test_db.expire_all()
    assert ConnectionCredentials.get_by_connection_id(pending_id, test_db) is None
    assert Connection.get_by_id(pending_id, test_db, include_deleted=True) is None
    assert Connection.get_by_id(conn_a.id, test_db) is None  # still disconnected
    with tenant_books() as ext:
      from robosystems.models.extensions import Element, Entity

      parent = ext.query(Entity).filter(Entity.is_parent.is_(True)).one()
      assert (parent.name, parent.connection_id) == ("Company A", conn_a.id)
      assert {e.connection_id for e in ext.query(Element)} == {conn_a.id}

  @pytest.mark.unit
  async def test_the_same_company_revives_its_connection(
    self, test_db, test_user, sample_graph, tenant_books
  ):
    from robosystems.models.core.connection.connection import Connection
    from robosystems.models.core.connection.connection_credentials import (
      ConnectionCredentials,
    )

    graph_id = sample_graph.graph_id
    with tenant_books() as ext:
      conn_a = _disconnected_quickbooks(test_db, graph_id, test_user.id, REALM_A)
      _seed_books_built_by(ext, conn_a.id, tag="a")
    pending_id = _pending_quickbooks(test_db, graph_id, test_user.id).id

    outcome, exchange, sync = await _quickbooks_callback(
      test_db, test_user, graph_id, pending_id, REALM_A
    )

    assert not isinstance(outcome, HTTPException), outcome.detail
    assert outcome["connection_id"] == conn_a.id
    exchange.assert_called_once()
    assert sync.call_args.kwargs["connection_id"] == conn_a.id
    test_db.expire_all()
    revived = Connection.get_by_id(conn_a.id, test_db)
    assert revived is not None and revived.status == "connected"
    assert Connection.get_by_id(pending_id, test_db, include_deleted=True) is None
    assert ConnectionCredentials.get_by_connection_id(conn_a.id, test_db) is not None

  @pytest.mark.unit
  async def test_a_revival_over_books_another_company_also_built_is_refused(
    self, test_db, test_user, sample_graph, tenant_books
  ):
    """Books already holding a second company's accounts are no longer the
    first company's alone; reviving it would sync over a mixed chart."""
    graph_id = sample_graph.graph_id
    with tenant_books() as ext:
      conn_a = _disconnected_quickbooks(test_db, graph_id, test_user.id, REALM_A)
      _seed_books_built_by(ext, conn_a.id, tag="a")
      conn_b = _disconnected_quickbooks(test_db, graph_id, test_user.id, REALM_B)
      _seed_books_built_by(ext, conn_b.id, tag="b")
    pending_id = _pending_quickbooks(test_db, graph_id, test_user.id).id

    outcome, exchange, sync = await _quickbooks_callback(
      test_db, test_user, graph_id, pending_id, REALM_A
    )

    assert isinstance(outcome, HTTPException), outcome
    assert outcome.status_code == 409
    exchange.assert_not_called()
    sync.assert_not_called()

  @pytest.mark.unit
  def test_a_connection_naming_another_company_at_create_is_refused(
    self, test_db, test_user, sample_graph, tenant_books
  ):
    from robosystems.operations.connection_service import (
      ProviderConflictError,
      assert_provider_compatible,
    )

    graph_id = sample_graph.graph_id
    with tenant_books() as ext:
      conn_a = _disconnected_quickbooks(test_db, graph_id, test_user.id, REALM_A)
      _seed_books_built_by(ext, conn_a.id, tag="a")

    with pytest.raises(ProviderConflictError) as refused:
      assert_provider_compatible(graph_id, "quickbooks", test_db, realm_id=REALM_B)
    assert refused.value.code == "OTHER_COMPANY_BOOKS"
    assert_provider_compatible(graph_id, "quickbooks", test_db, realm_id=REALM_A)
    assert_provider_compatible(graph_id, "quickbooks", test_db)
