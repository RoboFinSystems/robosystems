"""Test suite for ConnectionService (PostgreSQL-only).

Tests connection CRUD operations using mocked SQLAlchemy models.
No graph database dependencies.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.connection_service import SYSTEM_USER_ID, ConnectionService

MODULE = "robosystems.operations.connection_service"


def _make_mock_connection(**overrides):
  """Create a mock Connection model instance."""
  conn = MagicMock()
  conn.id = overrides.get("id", "conn_test123")
  conn.graph_id = overrides.get("graph_id", "kg_test")
  conn.user_id = overrides.get("user_id", "usr_123")
  conn.provider = overrides.get("provider", "quickbooks")
  conn.status = overrides.get("status", "connected")
  conn.realm_id = overrides.get("realm_id", "9341452700148642")
  conn.item_id = overrides.get("item_id")
  conn.cik = overrides.get("cik")
  conn.entity_name = overrides.get("entity_name")
  conn.institution_name = overrides.get("institution_name")
  conn.auto_sync_enabled = overrides.get("auto_sync_enabled", True)
  conn.last_sync = overrides.get("last_sync")
  conn.created_at = overrides.get("created_at", datetime.now(UTC))
  conn.updated_at = overrides.get("updated_at", datetime.now(UTC))
  conn.to_dict.return_value = {
    "connection_id": conn.id,
    "provider": conn.provider,
    "status": conn.status,
    "entity_id": conn.graph_id,
    "graph_id": conn.graph_id,
    "user_id": conn.user_id,
    "metadata": {
      "realm_id": conn.realm_id,
      "item_id": conn.item_id,
      "cik": conn.cik,
      "entity_name": conn.entity_name,
      "institution_name": conn.institution_name,
      "auto_sync_enabled": conn.auto_sync_enabled,
      "last_sync": None,
    },
    "created_at": conn.created_at,
    "updated_at": conn.updated_at,
  }
  return conn


def _make_mock_credentials(**overrides):
  """Create a mock ConnectionCredentials instance."""
  cred = MagicMock()
  cred.connection_id = overrides.get("connection_id", "conn_test123")
  cred.user_id = overrides.get("user_id", "usr_123")
  cred.provider = overrides.get("provider", "quickbooks")
  cred.get_credentials.return_value = overrides.get(
    "credentials", {"access_token": "tok_123", "refresh_token": "ref_456"}
  )
  cred.is_expired.return_value = overrides.get("is_expired", False)
  cred.expires_at = overrides.get("expires_at")
  return cred


class TestCreateConnection:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_connection_in_postgres(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.create.return_value = mock_conn

      result = await ConnectionService.create_connection(
        entity_id="kg_test",
        provider="quickbooks",
        user_id="usr_123",
        credentials={"access_token": "tok"},
        metadata={"realm_id": "9341452700148642", "status": "pending_oauth"},
        db_session=mock_session,
      )

    assert result["connection_id"] == "conn_test123"
    assert result["provider"] == "quickbooks"
    MockConn.create.assert_called_once()
    MockCreds.create.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_without_credentials(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(status="pending_oauth")

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.create.return_value = mock_conn

      result = await ConnectionService.create_connection(
        entity_id="kg_test",
        provider="quickbooks",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result["connection_id"] == "conn_test123"
    MockConn.create.assert_called_once()
    MockCreds.create.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_uses_graph_id_over_entity_id(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(graph_id="kg_explicit")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.create.return_value = mock_conn

      await ConnectionService.create_connection(
        entity_id="kg_fallback",
        provider="quickbooks",
        user_id="usr_123",
        graph_id="kg_explicit",
        db_session=mock_session,
      )

    call_kwargs = MockConn.create.call_args
    assert call_kwargs.kwargs["graph_id"] == "kg_explicit"


class TestGetConnection:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_returns_connection_with_credentials(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()
    mock_cred = _make_mock_credentials()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.get_connection(
        connection_id="conn_test123",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result["connection_id"] == "conn_test123"
    assert result["credentials"] == {
      "access_token": "tok_123",
      "refresh_token": "ref_456",
    }
    assert result["is_expired"] is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_none(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None

      result = await ConnectionService.get_connection(
        connection_id="nonexistent",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_wrong_user_returns_none(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(user_id="usr_other")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.get_connection(
        connection_id="conn_test123",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_wrong_graph_returns_none(self):
    """A connection in a different graph must not be reachable by guessing
    its id from another graph's scope (IDOR guard)."""
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(graph_id="kg_other", user_id="usr_123")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.get_connection(
        connection_id="conn_test123",
        user_id="usr_123",
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert result is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_system_user_bypasses_access_check(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(user_id="usr_other")
    mock_cred = _make_mock_credentials()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.get_connection(
        connection_id="conn_test123",
        user_id=SYSTEM_USER_ID,
        db_session=mock_session,
      )

    assert result is not None
    assert result["connection_id"] == "conn_test123"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_credentials_returns_empty_dict(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = None

      result = await ConnectionService.get_connection(
        connection_id="conn_test123",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result["credentials"] == {}
    assert result["is_expired"] is False


class TestListConnections:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_returns_all_for_graph(self):
    mock_session = MagicMock()
    mock_conns = [
      _make_mock_connection(id="conn_1", provider="quickbooks"),
      _make_mock_connection(id="conn_2", provider="sec"),
    ]

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.list_filtered.return_value = mock_conns
      MockCreds.get_by_connection_id.return_value = None

      result = await ConnectionService.list_connections(
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert len(result) == 2
    MockConn.list_filtered.assert_called_once_with(
      session=mock_session,
      graph_id="kg_test",
      user_id=None,
      provider=None,
    )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_filters_by_provider(self):
    mock_session = MagicMock()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials"),
    ):
      MockConn.list_filtered.return_value = []

      await ConnectionService.list_connections(
        graph_id="kg_test",
        provider="quickbooks",
        db_session=mock_session,
      )

    MockConn.list_filtered.assert_called_once_with(
      session=mock_session,
      graph_id="kg_test",
      user_id=None,
      provider="quickbooks",
    )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_system_user_sees_all(self):
    mock_session = MagicMock()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials"),
    ):
      MockConn.list_filtered.return_value = []

      await ConnectionService.list_connections(
        graph_id="kg_test",
        user_id=SYSTEM_USER_ID,
        db_session=mock_session,
      )

    call_kwargs = MockConn.list_filtered.call_args.kwargs
    assert call_kwargs["user_id"] is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_empty_graph_returns_empty(self):
    mock_session = MagicMock()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials"),
    ):
      MockConn.list_filtered.return_value = []

      result = await ConnectionService.list_connections(
        graph_id="kg_empty",
        db_session=mock_session,
      )

    assert result == []


class TestUpdateConnectionCredentials:
  @pytest.mark.unit
  def test_updates_existing_credentials(self):
    mock_session = MagicMock()
    mock_cred = _make_mock_credentials()

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = ConnectionService.update_connection_credentials(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"new_token": "abc"},
        db_session=mock_session,
      )

    assert result is True
    mock_cred.update_credentials.assert_called_once()

  @pytest.mark.unit
  def test_creates_if_not_exists(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = None

      result = ConnectionService.update_connection_credentials(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"new_token": "abc"},
        db_session=mock_session,
      )

    assert result is True
    MockCreds.create.assert_called_once()


class TestDeleteConnection:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_soft_deletes_connection_and_deactivates_creds(self):
    """delete_connection soft-deletes (preserves the row with deleted_at
    stamped) rather than hard-deleting. Tenant-side events/agents/elements
    scoped to the connection_id stay attached."""
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()
    mock_cred = _make_mock_credentials()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.delete_connection(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is True
    mock_cred.deactivate.assert_called_once()
    mock_conn.soft_delete.assert_called_once()
    # Hard-delete is NOT called — that would orphan tenant data.
    mock_conn.delete.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_wrong_graph_returns_false(self):
    """A connection in a different graph must not be deletable by guessing
    its id from another graph's scope (IDOR guard)."""
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(graph_id="kg_other", user_id="usr_123")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.delete_connection(
        connection_id="conn_1",
        user_id="usr_123",
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert result is False
    mock_conn.soft_delete.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None

      result = await ConnectionService.delete_connection(
        connection_id="nonexistent",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is False


class TestMarkConnectionStatus:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_error(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.mark_connection_error(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_status.assert_called_once_with("error", mock_session)

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_connected(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.mark_connection_connected(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_status.assert_called_once_with("connected", mock_session)

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None

      result = await ConnectionService.mark_connection_error(
        connection_id="nonexistent",
        db_session=mock_session,
      )

    assert result is False


class TestUpdateLastSync:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_success(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.update_last_sync(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_last_sync.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None

      result = await ConnectionService.update_last_sync(
        connection_id="nonexistent",
        db_session=mock_session,
      )

    assert result is False


class TestUpdateConnection:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_updates_metadata_and_credentials(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()
    mock_cred = _make_mock_credentials()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        metadata={"realm_id": "new_realm", "entity_name": "Updated Corp"},
        credentials={"token": "new_tok"},
        status="connected",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_metadata.assert_called_once()
    mock_cred.update_credentials.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None

      result = await ConnectionService.update(
        connection_id="nonexistent",
        user_id="usr_123",
        status="error",
        db_session=mock_session,
      )

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_status_only_update(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        status="error",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_metadata.assert_called_once_with(mock_session, status="error")

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_credentials_when_none_exist(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(provider="quickbooks")

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = None

      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"token": "new_tok"},
        db_session=mock_session,
      )

    assert result is True
    MockCreds.create.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_metadata_no_status_skips_update_metadata(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_metadata.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_all_metadata_fields(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        metadata={
          "realm_id": "r1",
          "item_id": "i1",
          "cik": "0001234567",
          "entity_name": "Acme",
          "institution_name": "Chase",
          "auto_sync_enabled": False,
        },
        db_session=mock_session,
      )

    assert result is True
    call_kwargs = mock_conn.update_metadata.call_args.kwargs
    assert call_kwargs["realm_id"] == "r1"
    assert call_kwargs["item_id"] == "i1"
    assert call_kwargs["cik"] == "0001234567"
    assert call_kwargs["entity_name"] == "Acme"
    assert call_kwargs["institution_name"] == "Chase"
    assert call_kwargs["auto_sync_enabled"] is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_exception_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.side_effect = Exception("db error")

      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        status="error",
        db_session=mock_session,
      )

    assert result is False


class TestConnectionServiceErrorHandling:
  """Test error paths and session management across all methods."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_connection_exception(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.create.side_effect = Exception("create failed")

      with pytest.raises(Exception, match="create failed"):
        await ConnectionService.create_connection(
          entity_id="kg_test",
          provider="quickbooks",
          user_id="usr_123",
          db_session=mock_session,
        )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_get_connection_exception_returns_none(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.side_effect = Exception("db error")

      result = await ConnectionService.get_connection(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_get_connection_expired_credential(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()
    mock_cred = _make_mock_credentials()
    mock_cred.is_expired.side_effect = Exception("timezone mismatch")

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.get_connection(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result["is_expired"] is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_list_connections_exception_returns_empty(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.list_filtered.side_effect = Exception("db error")

      result = await ConnectionService.list_connections(
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert result == []

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_list_connections_entity_id_fallback(self):
    mock_session = MagicMock()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials"),
    ):
      MockConn.list_filtered.return_value = []

      await ConnectionService.list_connections(
        entity_id="kg_via_entity",
        db_session=mock_session,
      )

    call_kwargs = MockConn.list_filtered.call_args.kwargs
    assert call_kwargs["graph_id"] == "kg_via_entity"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_list_connections_credential_expiry_exception(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()
    mock_cred = _make_mock_credentials()
    mock_cred.is_expired.side_effect = Exception("tz mismatch")

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.list_filtered.return_value = [mock_conn]
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.list_connections(
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert len(result) == 1
    assert result[0]["is_expired"] is False

  @pytest.mark.unit
  def test_update_credentials_exception_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.side_effect = Exception("db error")

      result = ConnectionService.update_connection_credentials(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"token": "tok"},
        db_session=mock_session,
      )

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_update_last_sync_exception_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.side_effect = Exception("db error")

      result = await ConnectionService.update_last_sync(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_connection_exception_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.side_effect = Exception("db error")

      result = await ConnectionService.delete_connection(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_delete_connection_without_credentials(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection()

    with (
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockConn.get_by_id.return_value = mock_conn
      MockCreds.get_by_connection_id.return_value = None

      result = await ConnectionService.delete_connection(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.soft_delete.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_error_exception_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.side_effect = Exception("db error")

      result = await ConnectionService.mark_connection_error(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_connected_exception_returns_false(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.side_effect = Exception("db error")

      result = await ConnectionService.mark_connection_connected(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_connected_not_found(self):
    mock_session = MagicMock()

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None

      result = await ConnectionService.mark_connection_connected(
        connection_id="nonexistent",
        db_session=mock_session,
      )

    assert result is False


class TestConnectionServiceSessionManagement:
  """Test that sessions are created/closed properly when not provided."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_creates_session_when_none_provided(self):
    mock_session = MagicMock()

    with (
      patch(f"{MODULE}.SessionFactory", return_value=mock_session),
      patch(f"{MODULE}.Connection") as MockConn,
      patch(f"{MODULE}.ConnectionCredentials"),
    ):
      MockConn.get_by_id.return_value = None

      await ConnectionService.get_connection(
        connection_id="conn_1",
        user_id="usr_123",
      )

    mock_session.close.assert_called_once()

  @pytest.mark.unit
  def test_update_credentials_creates_session(self):
    mock_session = MagicMock()

    with (
      patch(f"{MODULE}.SessionFactory", return_value=mock_session),
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      MockCreds.get_by_connection_id.return_value = None

      ConnectionService.update_connection_credentials(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"token": "tok"},
      )

    mock_session.close.assert_called_once()


class TestMarkNeedsReauthSync:
  """Sync helper flips the connection to needs_reauth."""

  @pytest.mark.unit
  def test_helper_flips_status(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(status="connected")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = ConnectionService.mark_connection_needs_reauth_sync(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_status.assert_called_once_with("needs_reauth", mock_session)

  @pytest.mark.unit
  def test_helper_idempotent_when_already_needs_reauth(self):
    """Second call when status is already needs_reauth is a no-op."""
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(status="needs_reauth")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = ConnectionService.mark_connection_needs_reauth_sync(
        connection_id="conn_1",
        db_session=mock_session,
      )

    assert result is True
    mock_conn.update_status.assert_not_called()

  @pytest.mark.unit
  def test_helper_returns_false_when_connection_missing(self):
    mock_session = MagicMock()
    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None
      result = ConnectionService.mark_connection_needs_reauth_sync(
        connection_id="missing",
        db_session=mock_session,
      )
    assert result is False


class TestSetWritePolicy:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_sets_policy_for_matching_graph(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(graph_id="kg_test", user_id="usr_123")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn

      result = await ConnectionService.set_write_policy(
        connection_id="conn_test123",
        write_policy="qb_authoritative",
        user_id="usr_123",
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert result is not None
    assert result["connection_id"] == "conn_test123"
    mock_conn.set_write_policy.assert_called_once_with(mock_session, "qb_authoritative")

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_none(self):
    mock_session = MagicMock()
    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = None
      result = await ConnectionService.set_write_policy(
        connection_id="missing",
        write_policy="native",
        user_id="usr_123",
        graph_id="kg_test",
        db_session=mock_session,
      )
    assert result is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_wrong_graph_returns_none(self):
    """A connection in a different graph must not be reachable by guessing
    its id from another graph's scope (IDOR guard)."""
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(graph_id="kg_other", user_id="usr_123")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn
      result = await ConnectionService.set_write_policy(
        connection_id="conn_test123",
        write_policy="qb_authoritative",
        user_id="usr_123",
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert result is None
    mock_conn.set_write_policy.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_wrong_user_returns_none(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(graph_id="kg_test", user_id="usr_other")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn
      result = await ConnectionService.set_write_policy(
        connection_id="conn_test123",
        write_policy="native",
        user_id="usr_123",
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert result is None
    mock_conn.set_write_policy.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_system_user_bypasses_user_check(self):
    mock_session = MagicMock()
    mock_conn = _make_mock_connection(graph_id="kg_test", user_id="usr_other")

    with patch(f"{MODULE}.Connection") as MockConn:
      MockConn.get_by_id.return_value = mock_conn
      result = await ConnectionService.set_write_policy(
        connection_id="conn_test123",
        write_policy="qb_authoritative",
        user_id=SYSTEM_USER_ID,
        graph_id="kg_test",
        db_session=mock_session,
      )

    assert result is not None
    mock_conn.set_write_policy.assert_called_once_with(mock_session, "qb_authoritative")


# ---------------------------------------------------------------------------
# Connection-sync dispatch kernel (resolve_sync_connection /
# dispatch_connection_sync) — shared by the REST endpoint and the
# `sync-connection` MCP tool.
# ---------------------------------------------------------------------------

REGISTRY_MODULE = "robosystems.operations.providers.registry"
LOCK_MODULE = "robosystems.middleware.auth.distributed_lock"
VALKEY_MODULE = "robosystems.config.valkey_registry"


def _conn_dict(connection_id="conn_1", provider="quickbooks", status="connected"):
  return {
    "connection_id": connection_id,
    "provider": provider,
    "status": status,
    "last_sync": "2026-07-31T00:00:00+00:00",
    "metadata": {"realm_id": "123456"},
  }


class TestResolveSyncConnection:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_connections_raises(self):
    from robosystems.operations.connection_service import (
      NoSyncConnectionError,
      resolve_sync_connection,
    )

    with (
      patch(
        f"{MODULE}.ConnectionService.list_connections",
        new=AsyncMock(return_value=[]),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.is_enabled.return_value = True
      with pytest.raises(NoSyncConnectionError):
        await resolve_sync_connection("kg_test", "usr_123")

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_single_connection_resolves(self):
    from robosystems.operations.connection_service import resolve_sync_connection

    with (
      patch(
        f"{MODULE}.ConnectionService.list_connections",
        new=AsyncMock(return_value=[_conn_dict()]),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.is_enabled.return_value = True
      result = await resolve_sync_connection("kg_test", "usr_123")

    assert result["connection_id"] == "conn_1"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_disabled_provider_filtered_out(self):
    from robosystems.operations.connection_service import (
      NoSyncConnectionError,
      resolve_sync_connection,
    )

    with (
      patch(
        f"{MODULE}.ConnectionService.list_connections",
        new=AsyncMock(return_value=[_conn_dict(provider="quickbooks")]),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.is_enabled.return_value = False
      with pytest.raises(NoSyncConnectionError):
        await resolve_sync_connection("kg_test", "usr_123")

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_prefers_connected_over_stale_rows(self):
    from robosystems.operations.connection_service import resolve_sync_connection

    rows = [
      _conn_dict(connection_id="conn_old", status="error"),
      _conn_dict(connection_id="conn_live", status="connected"),
    ]
    with (
      patch(
        f"{MODULE}.ConnectionService.list_connections",
        new=AsyncMock(return_value=rows),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.is_enabled.return_value = True
      result = await resolve_sync_connection("kg_test", "usr_123")

    assert result["connection_id"] == "conn_live"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_multiple_live_candidates_raise_ambiguous(self):
    from robosystems.operations.connection_service import (
      AmbiguousSyncConnectionError,
      resolve_sync_connection,
    )

    rows = [
      _conn_dict(connection_id="conn_1", provider="quickbooks"),
      _conn_dict(connection_id="conn_2", provider="sec"),
    ]
    with (
      patch(
        f"{MODULE}.ConnectionService.list_connections",
        new=AsyncMock(return_value=rows),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.is_enabled.return_value = True
      with pytest.raises(AmbiguousSyncConnectionError) as exc:
        await resolve_sync_connection("kg_test", "usr_123")

    ids = [c["connection_id"] for c in exc.value.candidates]
    assert ids == ["conn_1", "conn_2"]


class TestDispatchConnectionSync:
  def _lock_patches(self, acquired=True, holder_id=None, ttl_remaining=None):
    lock_result = MagicMock()
    lock_result.acquired = acquired
    lock_result.holder_id = holder_id
    lock_result.ttl_remaining = ttl_remaining
    lock_result.lock_id = "lock_abc" if acquired else None
    lock_instance = MagicMock()
    lock_instance.acquire.return_value = lock_result
    return (
      patch(f"{LOCK_MODULE}.DistributedLock", MagicMock(return_value=lock_instance)),
      patch(f"{VALKEY_MODULE}.create_redis_client", MagicMock()),
    )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_connection_not_found_raises(self):
    from robosystems.operations.connection_service import (
      ConnectionNotFoundError,
      dispatch_connection_sync,
    )

    with patch(
      f"{MODULE}.ConnectionService.get_connection",
      new=AsyncMock(return_value=None),
    ):
      with pytest.raises(ConnectionNotFoundError):
        await dispatch_connection_sync(
          graph_id="kg_test", connection_id="conn_x", user_id="usr_123"
        )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_disabled_provider_raises_provider_unavailable(self):
    from robosystems.operations.connection_service import (
      ProviderUnavailableError,
      dispatch_connection_sync,
    )

    with (
      patch(
        f"{MODULE}.ConnectionService.get_connection",
        new=AsyncMock(return_value=_conn_dict()),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider.side_effect = ValueError("disabled")
      with pytest.raises(ProviderUnavailableError):
        await dispatch_connection_sync(
          graph_id="kg_test", connection_id="conn_1", user_id="usr_123"
        )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_lock_contention_raises_sync_in_progress(self):
    from robosystems.operations.connection_service import (
      SyncInProgressError,
      dispatch_connection_sync,
    )

    lock_patch, valkey_patch = self._lock_patches(
      acquired=False, holder_id="holder_xyz", ttl_remaining=900
    )
    with (
      patch(
        f"{MODULE}.ConnectionService.get_connection",
        new=AsyncMock(return_value=_conn_dict()),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
      lock_patch,
      valkey_patch,
    ):
      mock_registry.get_provider.return_value = MagicMock()
      with pytest.raises(SyncInProgressError) as exc:
        await dispatch_connection_sync(
          graph_id="kg_test", connection_id="conn_1", user_id="usr_123"
        )

    assert exc.value.holder_id == "holder_xyz"
    assert exc.value.ttl_remaining == 900

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_happy_path_builds_options_and_returns_dispatch_info(self):
    from datetime import date

    from robosystems.operations.connection_service import dispatch_connection_sync

    lock_patch, valkey_patch = self._lock_patches(acquired=True)
    with (
      patch(
        f"{MODULE}.ConnectionService.get_connection",
        new=AsyncMock(return_value=_conn_dict()),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
      lock_patch,
      valkey_patch,
    ):
      mock_registry.get_provider.return_value = MagicMock()
      mock_registry.sync_connection = AsyncMock(return_value="run_777")

      result = await dispatch_connection_sync(
        graph_id="kg_test",
        connection_id="conn_1",
        user_id="usr_123",
        full_rebuild=True,
        since_date=date(2026, 7, 1),
        sync_options={"lookback_days": 90},
      )

    assert result["task_id"] == "run_777"
    assert result["provider"] == "quickbooks"
    assert result["last_sync_before_dispatch"] == "2026-07-31T00:00:00+00:00"

    options = mock_registry.sync_connection.call_args.args[2]
    assert options["full_rebuild"] is True
    assert options["since_date"] == "2026-07-01"
    assert options["sync_lock_id"] == "lock_abc"
    assert options["lookback_days"] == 90

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_valkey_degraded_proceeds_without_lock(self):
    from robosystems.operations.connection_service import dispatch_connection_sync

    with (
      patch(
        f"{MODULE}.ConnectionService.get_connection",
        new=AsyncMock(return_value=_conn_dict()),
      ),
      patch(f"{REGISTRY_MODULE}.provider_registry") as mock_registry,
      patch(
        f"{VALKEY_MODULE}.create_redis_client",
        MagicMock(side_effect=RuntimeError("valkey down")),
      ),
    ):
      mock_registry.get_provider.return_value = MagicMock()
      mock_registry.sync_connection = AsyncMock(return_value="run_888")

      result = await dispatch_connection_sync(
        graph_id="kg_test", connection_id="conn_1", user_id="usr_123"
      )

    assert result["task_id"] == "run_888"
    options = mock_registry.sync_connection.call_args.args[2]
    assert options is None or "sync_lock_id" not in options
