"""Comprehensive test suite for connection service operations.

Tests connection management across graph database and PostgreSQL.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from robosystems.operations.connection_service import (
  ConnectionService,
  CredentialsNotFoundError,
  UserAccessDeniedError,
  _safe_datetime_conversion,
)

MODULE = "robosystems.operations.connection_service"


class TestSafeDatetimeConversion:
  """Test the _safe_datetime_conversion utility function."""

  def test_none_returns_none(self):
    """Test that None input returns None."""
    assert _safe_datetime_conversion(None) is None

  def test_datetime_passthrough(self):
    """Test that datetime objects are returned as-is."""
    dt = datetime.now(UTC)
    assert _safe_datetime_conversion(dt) == dt

  def test_datetime_attribute_extraction(self):
    """Test extraction of datetime from objects with datetime attribute."""
    mock_obj = Mock()
    dt = datetime.now(UTC)
    mock_obj.datetime = dt
    assert _safe_datetime_conversion(mock_obj) == dt

  def test_iso_string_parsing(self):
    """Test parsing of ISO format strings."""
    iso_string = "2024-01-01T12:00:00Z"
    result = _safe_datetime_conversion(iso_string)
    assert isinstance(result, datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 1
    assert result.hour == 12

  def test_iso_string_with_timezone(self):
    """Test parsing of ISO strings with timezone info."""
    iso_string = "2024-01-01T12:00:00+05:00"
    result = _safe_datetime_conversion(iso_string)
    assert isinstance(result, datetime)
    assert result.tzinfo is not None

  def test_invalid_string_returns_none(self):
    """Test that invalid date strings return None."""
    assert _safe_datetime_conversion("not a date") is None

  def test_unix_timestamp_float(self):
    """Test conversion of Unix timestamp as float."""
    timestamp = 1704110400.0  # 2024-01-01 12:00:00 UTC
    result = _safe_datetime_conversion(timestamp)
    assert isinstance(result, datetime)
    assert result.year == 2024

  def test_unix_timestamp_int(self):
    """Test conversion of Unix timestamp as integer."""
    timestamp = 1704110400  # 2024-01-01 12:00:00 UTC
    result = _safe_datetime_conversion(timestamp)
    assert isinstance(result, datetime)
    assert result.year == 2024

  def test_invalid_timestamp_returns_none(self):
    """Test that invalid timestamps return None."""
    assert _safe_datetime_conversion(-99999999999999) is None

  def test_unsupported_type_returns_none(self):
    """Test that unsupported types return None."""
    assert _safe_datetime_conversion([1, 2, 3]) is None
    assert _safe_datetime_conversion({"key": "value"}) is None

  def test_isoformat_object(self):
    """Test objects with isoformat method are handled."""
    mock_obj = Mock()
    mock_obj.isoformat = Mock(return_value="2024-01-01T12:00:00Z")
    # Ensure datetime attribute doesn't exist to test isoformat path
    mock_obj.datetime = None

    # First check if it has datetime attribute (which is None)
    # Then check if it has isoformat
    result = _safe_datetime_conversion(mock_obj)
    # Since datetime attribute exists but is None, it returns None
    assert result is None

    # Now test without datetime attribute at all
    mock_obj2 = Mock(spec=["isoformat"])
    mock_obj2.isoformat = Mock(return_value="2024-01-01T12:00:00Z")
    result2 = _safe_datetime_conversion(mock_obj2)
    assert result2 == mock_obj2


class TestConnectionServiceAsync:
  """Test async ConnectionService methods with proper mocking."""

  @pytest.mark.asyncio
  async def test_list_connections_mock(self):
    """Test that we can mock ConnectionService list_connections."""

    # Create a mock that returns an async result
    mock_service = Mock(spec=ConnectionService)
    mock_service.list_connections = AsyncMock(
      return_value=[{"id": "conn1", "provider": "quickbooks", "status": "active"}]
    )

    # Call the mocked method
    result = await mock_service.list_connections("entity123")

    # Verify the result
    assert len(result) == 1
    assert result[0]["id"] == "conn1"
    assert result[0]["provider"] == "quickbooks"

  @pytest.mark.asyncio
  async def test_get_connection_mock(self):
    """Test that we can mock ConnectionService get_connection."""

    mock_service = Mock(spec=ConnectionService)
    mock_service.get_connection = AsyncMock(
      return_value={
        "id": "conn1",
        "provider": "quickbooks",
        "status": "active",
        "metadata": {"company": "Test Corp"},
      }
    )

    result = await mock_service.get_connection("conn1", "user123")

    assert result["id"] == "conn1"
    assert result["metadata"]["company"] == "Test Corp"

  @pytest.mark.asyncio
  async def test_create_connection_mock(self):
    """Test that we can mock ConnectionService create_connection."""

    mock_service = Mock(spec=ConnectionService)
    mock_service.create_connection = AsyncMock(
      return_value={"id": "new_conn", "provider": "plaid", "status": "pending"}
    )

    result = await mock_service.create_connection(
      entity_id="entity123", provider="plaid", credentials={"client_id": "test"}
    )

    assert result["id"] == "new_conn"
    assert result["status"] == "pending"


def _make_graph_repo_mock(**execute_single_kwargs):
  """Helper to create a mock graph repository with context manager support."""
  repo = MagicMock()
  repo.__enter__ = MagicMock(return_value=repo)
  repo.__exit__ = MagicMock(return_value=False)
  if execute_single_kwargs:
    repo.execute_single = MagicMock(**execute_single_kwargs)
  return repo


class TestCreateConnectionActual:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_happy_path(self):
    mock_repo = _make_graph_repo_mock(
      return_value={"identifier": "NVDA", "name": "NVIDIA"}
    )
    mock_repo.execute_query.return_value = None
    mock_pg_session = MagicMock()

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      mock_mt.is_multitenant_mode.return_value = False
      mock_mt.get_database_name.return_value = "test_db"
      MockCreds.get_by_connection_id.return_value = None

      result = await ConnectionService.create_connection(
        entity_id="NVDA",
        provider="SEC",
        user_id="usr_123",
        credentials={"api_key": "test"},
        db_session=mock_pg_session,
      )

    assert result["provider"] == "SEC"
    assert result["status"] == "connected"
    assert result["entity_id"] == "NVDA"
    assert result["connection_id"] == "sec_NVDA_usr_123"
    MockCreds.create.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_entity_not_found_raises(self):
    mock_repo = _make_graph_repo_mock(return_value=None)
    mock_pg_session = MagicMock()

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
    ):
      mock_mt.is_multitenant_mode.return_value = False
      mock_mt.get_database_name.return_value = "test_db"

      with pytest.raises(ValueError, match=r"Entity .* not found"):
        await ConnectionService.create_connection(
          entity_id="INVALID",
          provider="SEC",
          user_id="usr_123",
          credentials={"api_key": "test"},
          db_session=mock_pg_session,
        )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_deactivates_existing_credentials(self):
    mock_repo = _make_graph_repo_mock(
      return_value={"identifier": "NVDA", "name": "NVIDIA"}
    )
    mock_repo.execute_query.return_value = None
    mock_pg_session = MagicMock()
    mock_existing_cred = MagicMock()

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      mock_mt.is_multitenant_mode.return_value = False
      mock_mt.get_database_name.return_value = "test_db"
      MockCreds.get_by_connection_id.return_value = mock_existing_cred

      await ConnectionService.create_connection(
        entity_id="NVDA",
        provider="SEC",
        user_id="usr_123",
        credentials={"api_key": "test"},
        db_session=mock_pg_session,
      )

    mock_existing_cred.deactivate.assert_called_once()


class TestGetConnectionActual:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_happy_path(self):
    conn_props = {
      "provider": "SEC",
      "status": "connected",
      "realm_id": None,
      "item_id": None,
      "cik": "0001045810",
      "entity_name": "NVIDIA",
      "institution_name": None,
      "auto_sync_enabled": True,
      "last_sync": None,
      "created_at": None,
    }
    mock_repo = _make_graph_repo_mock(return_value={"conn": conn_props})
    mock_pg_session = MagicMock()

    mock_pg_cred = MagicMock()
    mock_pg_cred.user_id = "usr_123"
    mock_pg_cred.get_credentials.return_value = {"api_key": "secret"}
    mock_pg_cred.expires_at = None
    mock_pg_cred.is_expired.return_value = False

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      mock_mt.get_database_name.return_value = "test_db"
      MockCreds.get_by_connection_id.return_value = mock_pg_cred

      result = await ConnectionService.get_connection(
        connection_id="sec_NVDA_usr_123",
        user_id="usr_123",
        db_session=mock_pg_session,
      )

    assert result["provider"] == "SEC"
    assert result["credentials"] == {"api_key": "secret"}
    assert result["is_expired"] is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_none(self):
    mock_repo = _make_graph_repo_mock(return_value=None)
    mock_repo.execute_query.return_value = []

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
    ):
      mock_mt.get_database_name.return_value = "test_db"

      result = await ConnectionService.get_connection(
        connection_id="nonexistent",
        user_id="usr_123",
      )

    assert result is None

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_credentials_not_found_raises(self):
    mock_repo = _make_graph_repo_mock(return_value={"conn": {"provider": "SEC"}})
    mock_pg_session = MagicMock()

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      mock_mt.get_database_name.return_value = "test_db"
      MockCreds.get_by_connection_id.return_value = None

      with pytest.raises(CredentialsNotFoundError):
        await ConnectionService.get_connection(
          connection_id="sec_NVDA_usr_123",
          user_id="usr_123",
          db_session=mock_pg_session,
        )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_access_denied_raises(self):
    mock_repo = _make_graph_repo_mock(return_value={"conn": {"provider": "SEC"}})
    mock_pg_session = MagicMock()
    mock_pg_cred = MagicMock()
    mock_pg_cred.user_id = "usr_other"

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      mock_mt.get_database_name.return_value = "test_db"
      MockCreds.get_by_connection_id.return_value = mock_pg_cred

      with pytest.raises(UserAccessDeniedError):
        await ConnectionService.get_connection(
          connection_id="sec_NVDA_usr_123",
          user_id="usr_123",
          db_session=mock_pg_session,
        )


class TestUpdateConnectionCredentials:
  @pytest.mark.unit
  def test_happy_path(self):
    mock_pg_session = MagicMock()
    mock_cred = MagicMock()
    mock_cred.user_id = "usr_123"

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = ConnectionService.update_connection_credentials(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"new_token": "abc"},
        db_session=mock_pg_session,
      )

    assert result is True
    mock_cred.update_credentials.assert_called_once()

  @pytest.mark.unit
  def test_not_found_returns_false(self):
    mock_pg_session = MagicMock()

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = None

      result = ConnectionService.update_connection_credentials(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"new_token": "abc"},
        db_session=mock_pg_session,
      )

    assert result is False

  @pytest.mark.unit
  def test_wrong_user_returns_false(self):
    mock_pg_session = MagicMock()
    mock_cred = MagicMock()
    mock_cred.user_id = "usr_other"

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = ConnectionService.update_connection_credentials(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"new_token": "abc"},
        db_session=mock_pg_session,
      )

    assert result is False


class TestDeleteConnection:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_happy_path(self):
    mock_repo = _make_graph_repo_mock()
    mock_pg_session = MagicMock()
    mock_cred = MagicMock()
    mock_cred.user_id = "usr_123"

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
      patch(f"{MODULE}.ConnectionCredentials") as MockCreds,
    ):
      mock_mt.get_database_name.return_value = "test_db"
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.delete_connection(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_pg_session,
      )

    assert result is True
    mock_cred.deactivate.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_wrong_user_returns_false(self):
    mock_pg_session = MagicMock()
    mock_cred = MagicMock()
    mock_cred.user_id = "usr_other"

    with patch(f"{MODULE}.ConnectionCredentials") as MockCreds:
      MockCreds.get_by_connection_id.return_value = mock_cred

      result = await ConnectionService.delete_connection(
        connection_id="conn_1",
        user_id="usr_123",
        db_session=mock_pg_session,
      )

    assert result is False


class TestMarkConnectionStatus:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_error(self):
    mock_repo = _make_graph_repo_mock(return_value={"conn": {}})

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
    ):
      mock_mt.get_database_name.return_value = "test_db"
      result = await ConnectionService.mark_connection_error("conn_1")

    assert result is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_connected(self):
    mock_repo = _make_graph_repo_mock(return_value={"conn": {}})

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
    ):
      mock_mt.get_database_name.return_value = "test_db"
      result = await ConnectionService.mark_connection_connected("conn_1")

    assert result is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_mark_error_not_found(self):
    mock_repo = _make_graph_repo_mock(return_value=None)

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
    ):
      mock_mt.get_database_name.return_value = "test_db"
      result = await ConnectionService.mark_connection_error("conn_1")

    assert result is False


class TestUpdateLastSync:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_success(self):
    mock_repo = _make_graph_repo_mock(return_value={"conn": {}})

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
    ):
      mock_mt.get_database_name.return_value = "test_db"
      result = await ConnectionService.update_last_sync("conn_1")

    assert result is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_not_found_returns_false(self):
    mock_repo = _make_graph_repo_mock(return_value=None)

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
    ):
      mock_mt.get_database_name.return_value = "test_db"
      result = await ConnectionService.update_last_sync("conn_1")

    assert result is False


class TestUpdateConnection:
  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_with_credentials_and_metadata(self):
    mock_repo = _make_graph_repo_mock(return_value={"conn": {}})

    with (
      patch(f"{MODULE}.MultiTenantUtils") as mock_mt,
      patch(
        f"{MODULE}.get_graph_repository",
        new_callable=AsyncMock,
        return_value=mock_repo,
      ),
      patch.object(
        ConnectionService,
        "update_connection_credentials",
        return_value=True,
      ),
    ):
      mock_mt.get_database_name.return_value = "test_db"

      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"token": "new"},
        metadata={"entity_name": "Updated Corp"},
        status="connected",
      )

    assert result is True

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_credential_update_failure_returns_false(self):
    with patch.object(
      ConnectionService,
      "update_connection_credentials",
      return_value=False,
    ):
      result = await ConnectionService.update(
        connection_id="conn_1",
        user_id="usr_123",
        credentials={"token": "new"},
      )

    assert result is False

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_no_changes_returns_true(self):
    result = await ConnectionService.update(
      connection_id="conn_1",
      user_id="usr_123",
    )

    assert result is True
