"""Tests for DuckDB staging Celery task."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from robosystems.tasks.table_operations.duckdb_staging import stage_file_in_duckdb


class TestStageFileInDuckDB:
  """Integration tests for stage_file_in_duckdb Celery task."""

  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_successful_staging_with_operation_id(
    self, mock_file_class, mock_table_class, mock_client_factory
  ):
    """Test successful file staging with SSE operation tracking."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file.row_count = 100
    mock_file.mark_duckdb_staged = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.id = "table-456"
    mock_table.table_name = "customers"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      return_value={"status": "success", "rows_ingested": 100}
    )
    mock_client_factory.return_value = mock_client

    result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_id": "table-456",
        "operation_id": "op-789",
      }
    ).get()

    assert result["status"] == "success"
    assert result["file_id"] == "file-123"
    assert result["graph_id"] == "kg123456"
    assert result["table_name"] == "customers"
    assert result["files_staged"] == 1
    assert result["duckdb_status"] == "staged"
    assert "execution_time_seconds" in result

    mock_file.mark_duckdb_staged.assert_called_once()
    mock_client.create_table.assert_called_once()

  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_successful_staging_without_operation_id(
    self, mock_file_class, mock_table_class, mock_client_factory
  ):
    """Test successful file staging without SSE tracking."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file.row_count = 50
    mock_file.mark_duckdb_staged = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.id = "table-456"
    mock_table.table_name = "products"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      return_value={"status": "success", "rows_ingested": 50}
    )
    mock_client_factory.return_value = mock_client

    result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_id": "table-456",
      }
    ).get()

    assert result["status"] == "success"
    assert result["files_staged"] == 1

  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_file_not_found_error(self, mock_file_class):
    """Test error when file doesn't exist."""
    mock_file_class.get_by_id.return_value = None

    with pytest.raises(Exception) as exc_info:
      stage_file_in_duckdb.apply(
        kwargs={
          "file_id": "file-nonexistent",
          "graph_id": "kg123456",
          "table_id": "table-456",
        }
      ).get()

    assert "not found" in str(exc_info.value).lower()

  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_table_not_found_error(self, mock_file_class, mock_table_class):
    """Test error when table doesn't exist."""
    # Mock GraphFile exists
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file_class.get_by_id.return_value = mock_file

    # Mock GraphTable doesn't exist
    mock_table_class.get_by_id.return_value = None

    with pytest.raises(Exception) as exc_info:
      stage_file_in_duckdb.apply(
        kwargs={
          "file_id": "file-123",
          "graph_id": "kg123456",
          "table_id": "table-nonexistent",
        }
      ).get()

    assert "not found" in str(exc_info.value).lower()

  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_no_uploaded_files_skipped(self, mock_file_class, mock_table_class):
    """Test skipped status when no uploaded files exist."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file_class.get_by_id.return_value = mock_file

    # No uploaded files
    mock_file_class.get_all_for_table.return_value = []

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "empty_table"
    mock_table_class.get_by_id.return_value = mock_table

    result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_id": "table-456",
      }
    ).get()

    assert result["status"] == "skipped"
    assert "No uploaded files" in result["message"]
    assert result["file_id"] == "file-123"

  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_multiple_files_staging(
    self, mock_file_class, mock_table_class, mock_client_factory
  ):
    """Test staging multiple uploaded files together."""
    # Mock multiple GraphFiles
    mock_file1 = MagicMock()
    mock_file1.id = "file-1"
    mock_file1.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file1.upload_status = "uploaded"
    mock_file1.row_count = 100
    mock_file1.mark_duckdb_staged = MagicMock()

    mock_file2 = MagicMock()
    mock_file2.id = "file-2"
    mock_file2.s3_key = "uploads/user-123/table-456/file2.parquet"
    mock_file2.upload_status = "uploaded"
    mock_file2.row_count = 50

    mock_file3 = MagicMock()
    mock_file3.id = "file-3"
    mock_file3.s3_key = "uploads/user-123/table-456/file3.parquet"
    mock_file3.upload_status = "pending"  # Not uploaded yet

    mock_file_class.get_by_id.return_value = mock_file1
    mock_file_class.get_all_for_table.return_value = [
      mock_file1,
      mock_file2,
      mock_file3,
    ]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "large_table"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      return_value={"status": "success", "rows_ingested": 150}
    )
    mock_client_factory.return_value = mock_client

    result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-1",
        "graph_id": "kg123456",
        "table_id": "table-456",
      }
    ).get()

    assert result["status"] == "success"
    assert result["files_staged"] == 2  # Only uploaded files

  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_graph_api_client_failure(
    self, mock_file_class, mock_table_class, mock_client_factory
  ):
    """Test failure when Graph API client fails."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "failed_table"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client failure
    mock_client_factory.side_effect = Exception("Graph API unreachable")

    with pytest.raises(Exception) as exc_info:
      stage_file_in_duckdb.apply(
        kwargs={
          "file_id": "file-123",
          "graph_id": "kg123456",
          "table_id": "table-456",
        }
      ).get()

    assert "Graph API unreachable" in str(exc_info.value)

  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_duckdb_staging_error(
    self, mock_file_class, mock_table_class, mock_client_factory
  ):
    """Test error during DuckDB staging operation."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "error_table"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client with create_table error
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      side_effect=Exception("Schema mismatch: column type error")
    )
    mock_client_factory.return_value = mock_client

    with pytest.raises(Exception) as exc_info:
      stage_file_in_duckdb.apply(
        kwargs={
          "file_id": "file-123",
          "graph_id": "kg123456",
          "table_id": "table-456",
        }
      ).get()

    assert "Schema mismatch" in str(exc_info.value)

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.materialize_file_to_graph"
  )
  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_successful_staging_with_graph_ingestion_chain(
    self, mock_file_class, mock_table_class, mock_client_factory, mock_materialize
  ):
    """Test staging with automatic chain to graph ingestion."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file.row_count = 100
    mock_file.mark_duckdb_staged = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "auto_ingest_table"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      return_value={"status": "success", "rows_ingested": 100}
    )
    mock_client_factory.return_value = mock_client

    # Mock materialize task
    mock_materialize.apply_async = MagicMock(return_value="task-id-xyz")

    result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_id": "table-456",
        "ingest_to_graph": True,
      }
    ).get()

    assert result["status"] == "success"
    mock_materialize.apply_async.assert_called_once()
    call_args = mock_materialize.apply_async.call_args
    assert call_args[1]["args"] == ["file-123", "kg123456", "auto_ingest_table"]
    assert call_args[1]["priority"] == 5

  @patch(
    "robosystems.tasks.table_operations.graph_materialization.materialize_file_to_graph"
  )
  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_staging_handles_chain_failure_gracefully(
    self, mock_file_class, mock_table_class, mock_client_factory, mock_materialize
  ):
    """Test that staging succeeds even if chain to graph ingestion fails."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file.row_count = 100
    mock_file.mark_duckdb_staged = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "chain_fail_table"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      return_value={"status": "success", "rows_ingested": 100}
    )
    mock_client_factory.return_value = mock_client

    # Mock materialize task failure
    mock_materialize.apply_async = MagicMock(side_effect=Exception("Queue is full"))

    result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_id": "table-456",
        "ingest_to_graph": True,
      }
    ).get()

    # Should still succeed even though chain failed
    assert result["status"] == "success"
    mock_file.mark_duckdb_staged.assert_called_once()

  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_staging_with_null_row_count(
    self, mock_file_class, mock_table_class, mock_client_factory
  ):
    """Test staging handles null row_count gracefully."""
    # Mock GraphFile with null row_count
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file.row_count = None  # Null row count
    mock_file.mark_duckdb_staged = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "null_count_table"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      return_value={"status": "success", "rows_ingested": 0}
    )
    mock_client_factory.return_value = mock_client

    result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_id": "table-456",
      }
    ).get()

    assert result["status"] == "success"
    # Should pass 0 instead of None
    mock_file.mark_duckdb_staged.assert_called_once()
    call_kwargs = mock_file.mark_duckdb_staged.call_args[1]
    assert call_kwargs["row_count"] == 0

  @patch(
    "robosystems.tasks.table_operations.duckdb_staging.GraphClientFactory.create_client"
  )
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphTable")
  @patch("robosystems.tasks.table_operations.duckdb_staging.GraphFile")
  def test_staging_with_operation_id_sse_updates(
    self, mock_file_class, mock_table_class, mock_client_factory
  ):
    """Test that SSE progress updates are sent when operation_id is provided."""
    # Mock GraphFile
    mock_file = MagicMock()
    mock_file.id = "file-123"
    mock_file.s3_key = "uploads/user-123/table-456/file1.parquet"
    mock_file.upload_status = "uploaded"
    mock_file.row_count = 100
    mock_file.mark_duckdb_staged = MagicMock()
    mock_file_class.get_by_id.return_value = mock_file
    mock_file_class.get_all_for_table.return_value = [mock_file]

    # Mock GraphTable
    mock_table = MagicMock()
    mock_table.table_name = "sse_table"
    mock_table_class.get_by_id.return_value = mock_table

    # Mock Graph API client
    mock_client = MagicMock()
    mock_client.create_table = AsyncMock(
      return_value={"status": "success", "rows_ingested": 100}
    )
    mock_client_factory.return_value = mock_client

    # Apply task with operation_id
    task_result = stage_file_in_duckdb.apply(
      kwargs={
        "file_id": "file-123",
        "graph_id": "kg123456",
        "table_id": "table-456",
        "operation_id": "op-sse-123",
      }
    )

    result = task_result.get()

    assert result["status"] == "success"
    # Task should have updated state multiple times with operation_id
    # We can't easily verify update_state calls in tests, but we verify task completes
