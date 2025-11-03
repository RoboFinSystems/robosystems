"""Tests for Plaid data sync tasks."""

import pytest
from unittest.mock import patch, AsyncMock

from robosystems.tasks.data_sync.plaid import (
  sync_plaid_data,
  get_last_cursor,
  save_cursor,
  plaid_account_element_uri,
  plaid_account_qname,
  plaid_transaction_uri,
  plaid_line_item_uri,
)


class TestSyncPlaidDataTask:
  """Test cases for Plaid data sync Celery task."""

  @patch("robosystems.tasks.data_sync.plaid.asyncio")
  def test_successful_sync(self, mock_asyncio):
    """Test successful Plaid data sync execution."""
    mock_asyncio.run.return_value = True

    result = sync_plaid_data("entity1", "item1")  # type: ignore[call-arg]

    mock_asyncio.run.assert_called_once()
    assert result is None

  @patch("robosystems.tasks.data_sync.plaid.asyncio")
  def test_sync_with_asyncio_error(self, mock_asyncio):
    """Test handling of asyncio errors."""
    mock_asyncio.run.side_effect = RuntimeError("Async error")

    with pytest.raises(RuntimeError) as exc_info:
      sync_plaid_data.apply(kwargs={"entity_id": "entity1", "item_id": "item1"}).get()  # type: ignore[attr-defined]

    assert "Async error" in str(exc_info.value)


class TestGetLastCursor:
  """Test cases for cursor retrieval function."""

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_successful_cursor_retrieval(self, mock_connection_service):
    """Test successful cursor retrieval from connection metadata."""
    mock_connections = [
      {
        "metadata": {
          "item_id": "item1",
          "last_cursor": "cursor_abc123",
        }
      }
    ]
    mock_connection_service.list_connections.return_value = mock_connections

    cursor = get_last_cursor("entity1", "item1")

    assert cursor == "cursor_abc123"
    mock_connection_service.list_connections.assert_called_once_with(
      entity_id="entity1", provider="Plaid", user_id=""
    )

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_cursor_not_found(self, mock_connection_service):
    """Test when cursor is not found in metadata."""
    mock_connections = [{"metadata": {"item_id": "item1"}}]
    mock_connection_service.list_connections.return_value = mock_connections

    cursor = get_last_cursor("entity1", "item1")

    assert cursor is None

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_no_matching_item(self, mock_connection_service):
    """Test when no connection matches the item_id."""
    mock_connections = [{"metadata": {"item_id": "other_item"}}]
    mock_connection_service.list_connections.return_value = mock_connections

    cursor = get_last_cursor("entity1", "item1")

    assert cursor is None

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_error_during_retrieval(self, mock_connection_service):
    """Test error handling during cursor retrieval."""
    mock_connection_service.list_connections.side_effect = RuntimeError(
      "Connection error"
    )

    cursor = get_last_cursor("entity1", "item1")

    assert cursor is None

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_empty_connections_list(self, mock_connection_service):
    """Test when no connections are found."""
    mock_connection_service.list_connections.return_value = []

    cursor = get_last_cursor("entity1", "item1")

    assert cursor is None


class TestSaveCursor:
  """Test cases for cursor saving function."""

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_cursor_saving_warning(self, mock_connection_service):
    """Test that cursor saving logs a warning (not implemented)."""
    mock_connections = [
      {
        "connection_id": "conn1",
        "metadata": {"item_id": "item1"},
      }
    ]
    mock_connection_service.list_connections.return_value = mock_connections

    save_cursor("entity1", "item1", "new_cursor")

    mock_connection_service.list_connections.assert_called_once_with(
      entity_id="entity1", provider="Plaid", user_id=""
    )

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_connection_not_found(self, mock_connection_service):
    """Test when connection is not found for cursor saving."""
    mock_connections = []
    mock_connection_service.list_connections.return_value = mock_connections

    save_cursor("entity1", "item1", "new_cursor")

    mock_connection_service.list_connections.assert_called_once()

  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  def test_error_during_save(self, mock_connection_service):
    """Test error handling during cursor save."""
    mock_connection_service.list_connections.side_effect = RuntimeError(
      "Connection error"
    )

    save_cursor("entity1", "item1", "new_cursor")


class TestPlaidURIGeneration:
  """Test cases for Plaid URI generation functions."""

  def test_plaid_account_element_uri(self):
    """Test account element URI generation."""
    uri = plaid_account_element_uri("acc123")
    assert uri == "https://plaid.com/account/acc123#element"

  def test_plaid_transaction_uri(self):
    """Test transaction URI generation."""
    uri = plaid_transaction_uri("txn456")
    assert uri == "https://plaid.com/transaction/txn456"

  def test_plaid_line_item_uri(self):
    """Test line item URI generation."""
    uri = plaid_line_item_uri("txn789", 1)
    assert uri == "https://plaid.com/transaction/txn789/line-item/1"


class TestPlaidAccountQName:
  """Test cases for Plaid account QName generation."""

  def test_simple_account_name(self):
    """Test QName generation with simple account name."""
    qname = plaid_account_qname("Checking", "depository", "checking")
    assert qname == "Checking_depository_checking"

  def test_account_name_with_spaces(self):
    """Test QName generation removes spaces."""
    qname = plaid_account_qname("My Checking Account", "depository", "checking")
    assert " " not in qname
    assert qname == "MyCheckingAccount_depository_checking"

  def test_account_name_with_special_characters(self):
    """Test QName generation removes special characters."""
    qname = plaid_account_qname("Account #123 (Main)", "depository", "savings")
    assert "#" not in qname
    assert "(" not in qname
    assert ")" not in qname
    assert qname == "Account123Main_depository_savings"

  def test_account_without_subtype(self):
    """Test QName generation without subtype."""
    qname = plaid_account_qname("Checking", "depository", "")
    assert qname == "Checking_depository"

  def test_account_with_all_special_chars(self):
    """Test QName generation removes most special characters."""
    special_chars = "Test!@#$%^&*()_+{}|:<>?~`[];',./\\"
    qname = plaid_account_qname(special_chars, "depository", "checking")
    assert qname == "Test_{}[]'_depository_checking"

  def test_account_with_dots_and_commas(self):
    """Test QName generation removes dots and commas."""
    qname = plaid_account_qname("Account.Name,Test", "credit", "card")
    assert "." not in qname
    assert "," not in qname
    assert qname == "AccountNameTest_credit_card"


class TestPlaidAsyncIntegration:
  """Test cases for async Plaid integration functions."""

  @patch("robosystems.tasks.data_sync.plaid.session")
  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  @patch("robosystems.tasks.data_sync.plaid.plaid_api")
  @patch("robosystems.tasks.data_sync.plaid.ApiClient")
  @patch("robosystems.tasks.data_sync.plaid.Configuration")
  @patch("robosystems.tasks.data_sync.plaid.process_plaid_data_to_graph")
  @patch("robosystems.tasks.data_sync.plaid.save_cursor")
  @patch("robosystems.tasks.data_sync.plaid.get_last_cursor")
  async def test_sync_with_credentials_error(
    self,
    mock_get_cursor,
    mock_save_cursor,
    mock_process_data,
    mock_configuration,
    mock_api_client,
    mock_plaid_api,
    mock_connection_service,
    mock_session,
  ):
    """Test handling of credentials retrieval errors."""
    from robosystems.tasks.data_sync.plaid import _sync_plaid_data_async

    mock_connection_service.list_connections = AsyncMock(
      side_effect=RuntimeError("Auth error")
    )

    result = await _sync_plaid_data_async("entity1", "item1")

    assert result is None
    mock_session.remove.assert_called()

  @patch("robosystems.tasks.data_sync.plaid.session")
  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  async def test_sync_with_no_matching_connection(
    self, mock_connection_service, mock_session
  ):
    """Test handling when no matching connection is found."""
    from robosystems.tasks.data_sync.plaid import _sync_plaid_data_async

    mock_connection_service.list_connections = AsyncMock(
      return_value=[{"metadata": {"item_id": "other_item"}}]
    )

    result = await _sync_plaid_data_async("entity1", "item1")

    assert result is None
    mock_session.remove.assert_called()

  @patch("robosystems.tasks.data_sync.plaid.session")
  @patch("robosystems.tasks.data_sync.plaid.ConnectionService")
  async def test_sync_with_missing_credentials(
    self, mock_connection_service, mock_session
  ):
    """Test handling when credentials are missing from connection."""
    from robosystems.tasks.data_sync.plaid import _sync_plaid_data_async

    mock_connection = {"metadata": {"item_id": "item1"}, "connection_id": "conn1"}
    mock_connection_service.list_connections = AsyncMock(return_value=[mock_connection])
    mock_connection_service.get_connection = AsyncMock(return_value=None)

    result = await _sync_plaid_data_async("entity1", "item1")

    assert result is None
    mock_session.remove.assert_called()
