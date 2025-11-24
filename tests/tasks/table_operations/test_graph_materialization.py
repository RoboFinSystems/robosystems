"""Tests for graph materialization Celery task."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from robosystems.tasks.table_operations.graph_materialization import (
  materialize_file_to_graph,
)


class TestMaterializeFileToGraph:
  """Integration tests for materialize_file_to_graph Celery task."""

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_successful_materialization(self, mock_file_class, mock_client_factory):
    """Test successful file materialization to graph."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.duckdb_status = "staged"
    mock_file.mark_graph_ingested = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.materialize_table = AsyncMock(
      return_value={"rows_ingested": 250, "execution_time_ms": 1500.0}
    )
    mock_client_factory.return_value = mock_client

    result = materialize_file_to_graph.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_name": "customers",
      }
    ).get()

    assert result["status"] == "success"
    assert result["file_id"] == "file-123"
    assert result["graph_id"] == "kg123456"
    assert result["table_name"] == "customers"
    assert result["rows_ingested"] == 250
    assert result["graph_status"] == "ingested"
    assert "execution_time_seconds" in result

    mock_file.mark_graph_ingested.assert_called_once()
    mock_client.materialize_table.assert_called_once()

    # Verify selective materialization with file_id filter
    call_kwargs = mock_client.materialize_table.call_args[1]
    assert call_kwargs["graph_id"] == "kg123456"
    assert call_kwargs["table_name"] == "customers"
    assert call_kwargs["file_ids"] == ["file-123"]
    assert call_kwargs["ignore_errors"] is True

  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_file_not_found_error(self, mock_file_class):
    """Test error when file doesn't exist."""
    mock_file_class.get_by_id.return_value = None

    with pytest.raises(Exception) as exc_info:
      materialize_file_to_graph.apply(
        kwargs={
          "file_id": "file-nonexistent",
          "graph_id": "kg123456",
          "table_name": "customers",
        }
      ).get()

    assert "not found" in str(exc_info.value).lower()

  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_file_not_staged_skipped(self, mock_file_class):
    """Test skipped status when file not staged in DuckDB."""
    # Mock GraphFile not staged
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.duckdb_status = "pending"
    mock_file_class.get_by_id.return_value = mock_file

    result = materialize_file_to_graph.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_name": "customers",
      }
    ).get()

    assert result["status"] == "skipped"
    assert "not staged" in result["message"].lower()
    assert result["file_id"] == "file-123"

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_graph_client_creation_failure(self, mock_file_class, mock_client_factory):
    """Test failure when Graph API client can't be created."""
    # Mock GraphFile staged
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.duckdb_status = "staged"
    mock_file_class.get_by_id.return_value = mock_file

    # Mock client creation failure
    mock_client_factory.side_effect = Exception("Graph API unreachable")

    with pytest.raises(Exception) as exc_info:
      materialize_file_to_graph.apply(
        kwargs={
          "file_id": "file-123",
          "graph_id": "kg123456",
          "table_name": "customers",
        }
      ).get()

    assert "Graph API unreachable" in str(exc_info.value)

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_materialization_error(self, mock_file_class, mock_client_factory):
    """Test error during materialization operation."""
    # Mock GraphFile staged
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.duckdb_status = "staged"
    mock_file_class.get_by_id.return_value = mock_file

    # Mock client with materialization error
    mock_client = MagicMock()
    mock_client.materialize_table = AsyncMock(
      side_effect=Exception("Constraint violation: duplicate key")
    )
    mock_client_factory.return_value = mock_client

    with pytest.raises(Exception) as exc_info:
      materialize_file_to_graph.apply(
        kwargs={
          "file_id": "file-123",
          "graph_id": "kg123456",
          "table_name": "customers",
        }
      ).get()

    assert "Constraint violation" in str(exc_info.value)

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_zero_rows_ingested(self, mock_file_class, mock_client_factory):
    """Test successful completion with zero rows ingested."""
    # Mock GraphFile staged
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.duckdb_status = "staged"
    mock_file.mark_graph_ingested = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file

    # Mock client with zero rows
    mock_client = MagicMock()
    mock_client.materialize_table = AsyncMock(
      return_value={"rows_ingested": 0, "execution_time_ms": 100.0}
    )
    mock_client_factory.return_value = mock_client

    result = materialize_file_to_graph.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_name": "empty_table",
      }
    ).get()

    assert result["status"] == "success"
    assert result["rows_ingested"] == 0
    mock_file.mark_graph_ingested.assert_called_once()

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_large_batch_ingestion(self, mock_file_class, mock_client_factory):
    """Test successful ingestion of large batch."""
    # Mock GraphFile staged
    mock_file = MagicMock()
    mock_file.id = "file-large"
    mock_file.duckdb_status = "staged"
    mock_file.mark_graph_ingested = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file

    # Mock client with large batch
    mock_client = MagicMock()
    mock_client.materialize_table = AsyncMock(
      return_value={"rows_ingested": 1000000, "execution_time_ms": 60000.0}
    )
    mock_client_factory.return_value = mock_client

    result = materialize_file_to_graph.apply(
      kwargs={
        "file_id": "file-large",
        "graph_id": "kg123456",
        "table_name": "large_table",
      }
    ).get()

    assert result["status"] == "success"
    assert result["rows_ingested"] == 1000000

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_different_duckdb_statuses(self, mock_file_class, mock_client_factory):
    """Test various DuckDB status values."""
    for status in ["pending", "uploading", "uploaded", "error"]:
      # Mock GraphFile with different status
      mock_file = MagicMock()
      mock_file.id = f"file-{status}"
      mock_file.duckdb_status = status
      mock_file_class.get_by_id.return_value = mock_file

      result = materialize_file_to_graph.apply(
        kwargs={
          "file_id": f"file-{status}",
          "graph_id": "kg123456",
          "table_name": "test_table",
        }
      ).get()

      assert result["status"] == "skipped"
      assert status in result["message"]

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.graph_materialization.GraphFile")
  def test_result_missing_fields_handled(self, mock_file_class, mock_client_factory):
    """Test handling when API result is missing expected fields."""
    # Mock GraphFile staged
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.duckdb_status = "staged"
    mock_file.mark_graph_ingested = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file

    # Mock client with minimal result (missing fields)
    mock_client = MagicMock()
    mock_client.materialize_table = AsyncMock(
      return_value={}  # Empty result
    )
    mock_client_factory.return_value = mock_client

    result = materialize_file_to_graph.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_name": "minimal_table",
      }
    ).get()

    assert result["status"] == "success"
    assert result["rows_ingested"] == 0  # Should default to 0
    mock_file.mark_graph_ingested.assert_called_once()
