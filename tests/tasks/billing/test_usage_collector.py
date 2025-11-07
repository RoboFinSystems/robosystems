"""Tests for graph usage collection tasks."""

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from decimal import Decimal

from robosystems.tasks.billing.usage_collector import (
  graph_usage_collector,
)


class TestGraphUsageCollectorTask:
  """Test cases for graph usage collection Celery task."""

  @patch("robosystems.tasks.billing.usage_collector.StorageCalculator")
  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_successful_collection(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
    mock_storage_calc_class,
  ):
    """Test successful usage metrics collection with storage breakdown."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_graphs = [
      {
        "graph_id": "graph1",
        "user_id": "user1",
        "graph_tier": "standard",
      },
      {
        "graph_id": "graph2",
        "user_id": "user2",
        "graph_tier": "enterprise",
      },
    ]
    mock_get_graphs.return_value = mock_graphs

    # Mock StorageCalculator
    mock_storage_calc = MagicMock()
    mock_storage_calc_class.return_value = mock_storage_calc

    # Mock storage breakdown
    mock_storage_breakdown = {
      "total_gb": Decimal("10.5"),
      "files_gb": Decimal("2.5"),
      "tables_gb": Decimal("3.0"),
      "graphs_gb": Decimal("4.0"),
      "subgraphs_gb": Decimal("1.0"),
      "total_bytes": 11274289152,
    }
    mock_storage_calc.calculate_graph_storage.return_value = mock_storage_breakdown

    # Mock async metrics
    mock_metrics = {
      "size_bytes": 1024000,
      "node_count": 100,
      "relationship_count": 200,
      "instance_id": "i-12345",
      "region": "us-east-1",
    }
    mock_asyncio.run.return_value = mock_metrics

    mock_deleted = {
      "deleted_records": 5,
      "oldest_deleted": datetime.now(timezone.utc),
    }
    mock_usage_tracking.cleanup_old_records.return_value = mock_deleted

    result = graph_usage_collector()  # type: ignore[misc]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 2
    assert result["records_created"] == 2
    assert result["failed_graphs"] == []
    assert result["old_records_deleted"]["deleted_records"] == 5

    mock_get_graphs.assert_called_once_with(mock_db)
    assert mock_asyncio.run.call_count == 2
    assert mock_storage_calc.calculate_graph_storage.call_count == 2
    assert mock_usage_tracking.record_storage_usage.call_count == 2
    mock_db.commit.assert_called_once()

  @patch("robosystems.tasks.billing.usage_collector.StorageCalculator")
  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_no_graphs_to_process(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
    mock_storage_calc_class,
  ):
    """Test collection when no graphs exist."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_get_graphs.return_value = []

    mock_deleted = {"deleted_records": 0, "oldest_deleted": None}
    mock_usage_tracking.cleanup_old_records.return_value = mock_deleted

    result = graph_usage_collector()  # type: ignore[misc]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 0
    assert result["records_created"] == 0
    assert result["failed_graphs"] == []

    mock_asyncio.run.assert_not_called()
    mock_usage_tracking.record_storage_usage.assert_not_called()

  @patch("robosystems.tasks.billing.usage_collector.StorageCalculator")
  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_partial_failure(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
    mock_storage_calc_class,
  ):
    """Test collection with some graphs failing."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_graphs = [
      {"graph_id": "graph1", "user_id": "user1", "graph_tier": "standard"},
      {"graph_id": "graph2", "user_id": "user2", "graph_tier": "enterprise"},
      {"graph_id": "graph3", "user_id": "user3", "graph_tier": "premium"},
    ]
    mock_get_graphs.return_value = mock_graphs

    # Mock StorageCalculator
    mock_storage_calc = MagicMock()
    mock_storage_calc_class.return_value = mock_storage_calc

    # First and third succeed, second fails
    def storage_side_effect(graph_id, user_id):
      if graph_id == "graph2":
        raise RuntimeError("Storage calculation failed")
      return {
        "total_gb": Decimal("10.0"),
        "files_gb": Decimal("2.0"),
        "tables_gb": Decimal("3.0"),
        "graphs_gb": Decimal("4.0"),
        "subgraphs_gb": Decimal("1.0"),
        "total_bytes": 10737418240,
      }

    mock_storage_calc.calculate_graph_storage.side_effect = storage_side_effect

    mock_success_metrics = {
      "size_bytes": 1024000,
      "instance_id": "i-12345",
      "region": "us-east-1",
    }
    mock_asyncio.run.return_value = mock_success_metrics

    mock_deleted = {"deleted_records": 0}
    mock_usage_tracking.cleanup_old_records.return_value = mock_deleted

    result = graph_usage_collector()  # type: ignore[misc]

    assert result["status"] == "success"
    assert result["graphs_processed"] == 3
    assert result["records_created"] == 2
    assert "graph2" in result["failed_graphs"]
    assert len(result["failed_graphs"]) == 1

  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_database_initialization_failure(self, mock_session, mock_get_graphs):
    """Test handling of database initialization failure."""
    mock_session.side_effect = RuntimeError("Database connection failed")

    result = graph_usage_collector()  # type: ignore[misc]

    assert result["status"] == "error"
    assert "Database connection failed" in result["error"]
    assert "timestamp" in result

    mock_get_graphs.assert_not_called()

  @patch("robosystems.tasks.billing.usage_collector.StorageCalculator")
  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_database_commit_failure(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
    mock_storage_calc_class,
  ):
    """Test handling of database commit failure."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_graphs = [{"graph_id": "graph1", "user_id": "user1", "graph_tier": "standard"}]
    mock_get_graphs.return_value = mock_graphs

    # Mock StorageCalculator
    mock_storage_calc = MagicMock()
    mock_storage_calc_class.return_value = mock_storage_calc
    mock_storage_calc.calculate_graph_storage.return_value = {
      "total_gb": Decimal("10.0"),
      "files_gb": Decimal("2.0"),
      "tables_gb": Decimal("3.0"),
      "graphs_gb": Decimal("4.0"),
      "subgraphs_gb": Decimal("1.0"),
      "total_bytes": 10737418240,
    }

    mock_metrics = {"size_bytes": 1024000, "instance_id": "i-12345"}
    mock_asyncio.run.return_value = mock_metrics

    mock_db.commit.side_effect = RuntimeError("Commit failed")

    result = graph_usage_collector()  # type: ignore[misc]

    assert result["status"] == "error"
    assert "Commit failed" in result["error"]

    mock_db.rollback.assert_called_once()

  @patch("robosystems.tasks.billing.usage_collector.StorageCalculator")
  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_cleanup_old_records(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
    mock_storage_calc_class,
  ):
    """Test that old records are cleaned up."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_graphs = [{"graph_id": "graph1", "user_id": "user1", "graph_tier": "standard"}]
    mock_get_graphs.return_value = mock_graphs

    # Mock StorageCalculator
    mock_storage_calc = MagicMock()
    mock_storage_calc_class.return_value = mock_storage_calc
    mock_storage_calc.calculate_graph_storage.return_value = {
      "total_gb": Decimal("10.0"),
      "files_gb": Decimal("2.0"),
      "tables_gb": Decimal("3.0"),
      "graphs_gb": Decimal("4.0"),
      "subgraphs_gb": Decimal("1.0"),
      "total_bytes": 10737418240,
    }

    mock_metrics = {"size_bytes": 1024000}
    mock_asyncio.run.return_value = mock_metrics

    mock_deleted = {
      "deleted_records": 150,
      "oldest_deleted": datetime.now(timezone.utc),
    }
    mock_usage_tracking.cleanup_old_records.return_value = mock_deleted

    result = graph_usage_collector()  # type: ignore[misc]

    assert result["status"] == "success"
    assert result["old_records_deleted"]["deleted_records"] == 150

    mock_usage_tracking.cleanup_old_records.assert_called_once_with(
      mock_db, older_than_days=365
    )

  @patch("robosystems.tasks.billing.usage_collector.StorageCalculator")
  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_session_closed_on_success(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
    mock_storage_calc_class,
  ):
    """Test that database session is properly closed on success."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_graphs = []
    mock_get_graphs.return_value = mock_graphs

    mock_deleted = {"deleted_records": 0}
    mock_usage_tracking.cleanup_old_records.return_value = mock_deleted

    graph_usage_collector()  # type: ignore[misc]

    mock_db.close.assert_called_once()

  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_session_closed_on_error(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
  ):
    """Test that database session is properly closed on error."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_get_graphs.side_effect = RuntimeError("Query failed")

    graph_usage_collector()  # type: ignore[misc]

    mock_db.close.assert_called_once()
    mock_db.rollback.assert_called_once()

  @patch("robosystems.tasks.billing.usage_collector.logger")
  @patch("robosystems.tasks.billing.usage_collector.StorageCalculator")
  @patch("robosystems.tasks.billing.usage_collector.GraphUsageTracking")
  @patch("robosystems.tasks.billing.usage_collector.asyncio")
  @patch("robosystems.tasks.billing.usage_collector.get_user_graphs_with_details")
  @patch("robosystems.database.session")
  def test_logging_on_success(
    self,
    mock_session,
    mock_get_graphs,
    mock_asyncio,
    mock_usage_tracking,
    mock_storage_calc_class,
    mock_logger,
  ):
    """Test that success is logged appropriately."""
    mock_db = MagicMock()
    mock_session.return_value = mock_db

    mock_graphs = [{"graph_id": "graph1", "user_id": "user1", "graph_tier": "standard"}]
    mock_get_graphs.return_value = mock_graphs

    # Mock StorageCalculator
    mock_storage_calc = MagicMock()
    mock_storage_calc_class.return_value = mock_storage_calc
    mock_storage_calc.calculate_graph_storage.return_value = {
      "total_gb": Decimal("10.0"),
      "files_gb": Decimal("2.0"),
      "tables_gb": Decimal("3.0"),
      "graphs_gb": Decimal("4.0"),
      "subgraphs_gb": Decimal("1.0"),
      "total_bytes": 10737418240,
    }

    mock_metrics = {"size_bytes": 1024000}
    mock_asyncio.run.return_value = mock_metrics

    mock_deleted = {"deleted_records": 0}
    mock_usage_tracking.cleanup_old_records.return_value = mock_deleted

    graph_usage_collector()  # type: ignore[misc]

    mock_logger.info.assert_any_call("Starting graph usage collection task")
    assert any(
      "Usage collection completed" in str(call)
      for call in mock_logger.info.call_args_list
    )
