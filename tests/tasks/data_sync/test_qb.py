"""Tests for QuickBooks data sync tasks."""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from decimal import Decimal

from robosystems.tasks.data_sync.qb import (
  sync_task,
  sync_task_sse,
)


class TestSyncTask:
  """Test cases for QuickBooks sync Celery task."""

  @patch("robosystems.tasks.data_sync.qb.asyncio")
  def test_successful_sync_one_arg(self, mock_asyncio):
    """Test successful QB sync with one argument (backward compatibility)."""
    mock_asyncio.run.return_value = None

    result = sync_task("entity1")  # type: ignore[call-arg]

    mock_asyncio.run.assert_called_once()
    assert result is None

  @patch("robosystems.tasks.data_sync.qb.asyncio")
  def test_successful_sync_two_args(self, mock_asyncio):
    """Test successful QB sync with two arguments."""
    mock_asyncio.run.return_value = None

    result = sync_task("entity1", "graph1")  # type: ignore[call-arg]

    mock_asyncio.run.assert_called_once()
    assert result is None

  @patch("robosystems.tasks.data_sync.qb.asyncio")
  def test_sync_with_asyncio_error(self, mock_asyncio):
    """Test handling of asyncio errors."""
    mock_asyncio.run.side_effect = RuntimeError("Async error")

    with pytest.raises(RuntimeError) as exc_info:
      sync_task.apply(args=("entity1",)).get()  # type: ignore[attr-defined]

    assert "Async error" in str(exc_info.value)


class TestSyncTaskAsync:
  """Test cases for async QB sync implementation."""

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_sync_with_invalid_args(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service,
    mock_get_session,
  ):
    """Test handling of invalid number of arguments."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    result = await _sync_task_async("arg1", "arg2", "arg3")

    assert result is None

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_sync_with_missing_entity(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service,
    mock_get_session,
  ):
    """Test handling when entity is not found."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    mock_utils.get_database_name.return_value = "db1"
    mock_utils.log_database_operation.return_value = None

    mock_repository = MagicMock()
    mock_repository.__aenter__ = AsyncMock(return_value=mock_repository)
    mock_repository.__aexit__ = AsyncMock(return_value=None)
    mock_repository.execute_single = AsyncMock(return_value=None)
    mock_repo.return_value = mock_repository

    result = await _sync_task_async("entity1", "graph1")

    assert result is None
    mock_repository.execute_single.assert_called_once()

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_sync_with_no_connections(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service,
    mock_get_session,
  ):
    """Test handling when no QB connections are found."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    mock_utils.get_database_name.return_value = "db1"
    mock_utils.log_database_operation.return_value = None

    mock_repository = MagicMock()
    mock_repository.__aenter__ = AsyncMock(return_value=mock_repository)
    mock_repository.__aexit__ = AsyncMock(return_value=None)
    mock_repository.execute_single = AsyncMock(
      return_value={"identifier": "entity1", "name": "Test Entity"}
    )
    mock_repo.return_value = mock_repository

    mock_conn_service.list_connections = AsyncMock(return_value=[])

    result = await _sync_task_async("entity1", "graph1")

    assert result is None
    mock_conn_service.list_connections.assert_called_once()

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_sync_with_missing_credentials(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service,
    mock_get_session,
  ):
    """Test handling when credentials are missing."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    mock_utils.get_database_name.return_value = "db1"
    mock_utils.log_database_operation.return_value = None

    mock_repository = MagicMock()
    mock_repository.__aenter__ = AsyncMock(return_value=mock_repository)
    mock_repository.__aexit__ = AsyncMock(return_value=None)
    mock_repository.execute_single = AsyncMock(
      return_value={"identifier": "entity1", "name": "Test Entity"}
    )
    mock_repo.return_value = mock_repository

    mock_conn_service.list_connections = AsyncMock(
      return_value=[{"connection_id": "conn1"}]
    )
    mock_conn_service.get_connection = AsyncMock(return_value=None)

    result = await _sync_task_async("entity1", "graph1")

    assert result is None

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_sync_with_connection_error(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service,
    mock_get_session,
  ):
    """Test handling of connection service errors."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    mock_utils.get_database_name.return_value = "db1"
    mock_utils.log_database_operation.return_value = None

    mock_repository = MagicMock()
    mock_repository.__aenter__ = AsyncMock(return_value=mock_repository)
    mock_repository.__aexit__ = AsyncMock(return_value=None)
    mock_repository.execute_single = AsyncMock(
      return_value={"identifier": "entity1", "name": "Test Entity"}
    )
    mock_repo.return_value = mock_repository

    mock_conn_service.list_connections = AsyncMock(
      side_effect=RuntimeError("Connection error")
    )

    result = await _sync_task_async("entity1", "graph1")

    assert result is None

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_successful_sync_with_credits(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service_class,
    mock_get_session,
  ):
    """Test successful sync with credit consumption."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    mock_utils.get_database_name.return_value = "db1"
    mock_utils.log_database_operation.return_value = None

    mock_repository = MagicMock()
    mock_repository.__aenter__ = AsyncMock(return_value=mock_repository)
    mock_repository.__aexit__ = AsyncMock(return_value=None)
    mock_repository.execute_single = AsyncMock(
      return_value={"identifier": "entity1", "name": "Test Entity"}
    )
    mock_repo.return_value = mock_repository

    mock_conn_service.list_connections = AsyncMock(
      return_value=[{"connection_id": "conn1"}]
    )
    mock_conn_service.get_connection = AsyncMock(
      return_value={
        "credentials": {"access_token": "token"},
        "metadata": {"realm_id": "realm1"},
      }
    )

    mock_qb = MagicMock()
    mock_qb.sync = MagicMock()
    mock_qb_processor.return_value = mock_qb

    mock_db = MagicMock()
    mock_db.__enter__ = MagicMock(return_value=mock_db)
    mock_db.__exit__ = MagicMock(return_value=False)
    mock_get_session.return_value = iter([mock_db])

    mock_credit_service = MagicMock()
    mock_credit_service.consume_credits = MagicMock()
    mock_credit_service_class.return_value = mock_credit_service

    result = await _sync_task_async("entity1", "graph1")

    assert result is None
    mock_qb.sync.assert_called_once()
    mock_credit_service.consume_credits.assert_called_once_with(
      graph_id="graph1",
      operation_type="connection_sync",
      base_cost=Decimal("10.0"),
      metadata={
        "provider": "QuickBooks",
        "entity_id": "entity1",
        "sync_type": "full",
      },
    )

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_sync_without_graph_id_no_credits(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service_class,
    mock_get_session,
  ):
    """Test sync without graph_id doesn't consume credits."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    mock_utils.get_database_name.return_value = "db1"
    mock_utils.log_database_operation.return_value = None

    mock_repository = MagicMock()
    mock_repository.__aenter__ = AsyncMock(return_value=mock_repository)
    mock_repository.__aexit__ = AsyncMock(return_value=None)
    mock_repository.execute_single = AsyncMock(
      return_value={"identifier": "entity1", "name": "Test Entity"}
    )
    mock_repo.return_value = mock_repository

    mock_conn_service.list_connections = AsyncMock(
      return_value=[{"connection_id": "conn1"}]
    )
    mock_conn_service.get_connection = AsyncMock(
      return_value={
        "credentials": {"access_token": "token"},
        "metadata": {"realm_id": "realm1"},
      }
    )

    mock_qb = MagicMock()
    mock_qb.sync = MagicMock()
    mock_qb_processor.return_value = mock_qb

    mock_db = MagicMock()
    mock_db.__enter__ = MagicMock(return_value=mock_db)
    mock_db.__exit__ = MagicMock(return_value=False)
    mock_get_session.return_value = iter([mock_db])

    mock_credit_service = MagicMock()
    mock_credit_service.consume_credits = MagicMock()
    mock_credit_service_class.return_value = mock_credit_service

    result = await _sync_task_async("entity1")

    assert result is None
    mock_qb.sync.assert_called_once()
    mock_credit_service.consume_credits.assert_not_called()

  @patch("robosystems.tasks.data_sync.qb.get_db_session")
  @patch("robosystems.tasks.data_sync.qb.CreditService")
  @patch("robosystems.tasks.data_sync.qb.QBTransactionsProcessor")
  @patch("robosystems.tasks.data_sync.qb.ConnectionService")
  @patch("robosystems.tasks.data_sync.qb.get_graph_repository")
  @patch("robosystems.tasks.data_sync.qb.MultiTenantUtils")
  async def test_sync_with_credit_failure(
    self,
    mock_utils,
    mock_repo,
    mock_conn_service,
    mock_qb_processor,
    mock_credit_service_class,
    mock_get_session,
  ):
    """Test that credit consumption failure doesn't fail the sync."""
    from robosystems.tasks.data_sync.qb import _sync_task_async

    mock_utils.get_database_name.return_value = "db1"
    mock_utils.log_database_operation.return_value = None

    mock_repository = MagicMock()
    mock_repository.__aenter__ = AsyncMock(return_value=mock_repository)
    mock_repository.__aexit__ = AsyncMock(return_value=None)
    mock_repository.execute_single = AsyncMock(
      return_value={"identifier": "entity1", "name": "Test Entity"}
    )
    mock_repo.return_value = mock_repository

    mock_conn_service.list_connections = AsyncMock(
      return_value=[{"connection_id": "conn1"}]
    )
    mock_conn_service.get_connection = AsyncMock(
      return_value={
        "credentials": {"access_token": "token"},
        "metadata": {"realm_id": "realm1"},
      }
    )

    mock_qb = MagicMock()
    mock_qb.sync = MagicMock()
    mock_qb_processor.return_value = mock_qb

    mock_db = MagicMock()
    mock_db.__enter__ = MagicMock(return_value=mock_db)
    mock_db.__exit__ = MagicMock(return_value=False)
    mock_get_session.return_value = iter([mock_db])

    mock_credit_service = MagicMock()
    mock_credit_service.consume_credits = MagicMock(
      side_effect=RuntimeError("Credit error")
    )
    mock_credit_service_class.return_value = mock_credit_service

    result = await _sync_task_async("entity1", "graph1")

    assert result is None
    mock_qb.sync.assert_called_once()


class TestSyncTaskSSE:
  """Test cases for SSE-compatible QuickBooks sync task."""

  def test_sse_sync_returns_result(self):
    """Test SSE sync returns completion result."""
    result = sync_task_sse("entity1", "graph1", "op1")  # type: ignore[call-arg]

    assert result["status"] == "completed"
    assert result["entity_id"] == "entity1"
    assert result["graph_id"] == "graph1"

  def test_sse_sync_without_operation_id(self):
    """Test SSE sync without operation_id."""
    result = sync_task_sse("entity1", "graph1")  # type: ignore[call-arg]

    assert result["status"] == "completed"
    assert result["entity_id"] == "entity1"
    assert result["graph_id"] == "graph1"
