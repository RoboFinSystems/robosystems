"""Tests for graph_api databases/metrics router."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock
import pytest
from fastapi import HTTPException, status

from robosystems.graph_api.routers.databases.metrics import get_database_metrics
from robosystems.middleware.graph.types import NodeType


class TestDatabaseMetricsRouter:
  """Test cases for database metrics endpoints."""

  @pytest.fixture
  def mock_cluster_service(self):
    """Create a mock cluster service."""
    service = MagicMock()
    service.node_id = "test-instance-01"
    service.node_type = NodeType.WRITER
    service.db_manager = MagicMock()
    return service

  @pytest.mark.asyncio
  async def test_get_database_metrics_success(self, mock_cluster_service):
    """Test successful retrieval of database metrics."""
    # Setup mocks
    mock_cluster_service.db_manager.list_databases.return_value = [
      "test-db",
      "other-db",
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "test-db"
      db_path.mkdir()

      # Create some test files to simulate database size
      (db_path / "data.db").write_bytes(b"x" * 1024)  # 1KB file
      (db_path / "index.db").write_bytes(b"x" * 2048)  # 2KB file

      mock_cluster_service.db_manager.get_database_path.return_value = str(db_path)

      # Mock database connection for stats
      mock_conn = MagicMock()
      mock_result_nodes = MagicMock()
      mock_result_nodes.has_next.return_value = True
      mock_result_nodes.get_next.return_value = [100]  # 100 nodes

      mock_result_rels = MagicMock()
      mock_result_rels.has_next.return_value = True
      mock_result_rels.get_next.return_value = [250]  # 250 relationships

      mock_conn.execute.side_effect = [mock_result_nodes, mock_result_rels]
      mock_cluster_service.db_manager.get_connection.return_value.__enter__.return_value = mock_conn

      # Call the function
      result = await get_database_metrics("test-db", mock_cluster_service)

      # Verify results
      assert result["graph_id"] == "test-db"
      assert result["database_name"] == "test-db"
      assert result["size_bytes"] == 3072  # 1KB + 2KB
      assert result["size_mb"] == 0.0  # 3KB rounds to 0.00 MB
      assert result["node_count"] == 100
      assert result["relationship_count"] == 250
      assert result["last_modified"] is not None
      assert result["instance_id"] == "test-instance-01"
      assert result["node_type"] == "writer"

  @pytest.mark.asyncio
  async def test_get_database_metrics_not_found(self, mock_cluster_service):
    """Test metrics retrieval for non-existent database."""
    mock_cluster_service.db_manager.list_databases.return_value = ["other-db"]

    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("non-existent-db", mock_cluster_service)

    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    assert "not found" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_get_database_metrics_single_file_database(self, mock_cluster_service):
    """Test metrics for single-file database."""
    mock_cluster_service.db_manager.list_databases.return_value = ["test-db"]

    with tempfile.NamedTemporaryFile(suffix=".db") as tmpfile:
      # Write some data to the file
      tmpfile.write(b"x" * 5000)
      tmpfile.flush()

      mock_cluster_service.db_manager.get_database_path.return_value = tmpfile.name

      # Mock empty database (no nodes/relationships)
      mock_conn = MagicMock()
      mock_result = MagicMock()
      mock_result.has_next.return_value = False
      mock_conn.execute.return_value = mock_result
      mock_cluster_service.db_manager.get_connection.return_value.__enter__.return_value = mock_conn

      result = await get_database_metrics("test-db", mock_cluster_service)

      assert result["size_bytes"] == 5000
      assert result["size_mb"] == 0.0  # 5KB rounds to 0.00 MB
      assert result["node_count"] == 0
      assert result["relationship_count"] == 0

  @pytest.mark.asyncio
  async def test_get_database_metrics_connection_error(self, mock_cluster_service):
    """Test metrics retrieval when database connection fails."""
    mock_cluster_service.db_manager.list_databases.return_value = ["test-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "test-db"
      db_path.mkdir()
      (db_path / "data.db").write_bytes(b"x" * 1000)

      mock_cluster_service.db_manager.get_database_path.return_value = str(db_path)

      # Mock connection failure
      mock_cluster_service.db_manager.get_connection.side_effect = Exception(
        "Connection failed"
      )

      # Should still return metrics without counts
      result = await get_database_metrics("test-db", mock_cluster_service)

      assert result["graph_id"] == "test-db"
      assert result["size_bytes"] == 1000
      assert result["node_count"] == 0  # Default when connection fails
      assert result["relationship_count"] == 0  # Default when connection fails

  @pytest.mark.asyncio
  async def test_get_database_metrics_invalid_name(self, mock_cluster_service):
    """Test metrics retrieval with invalid database name."""
    # Names with invalid characters should raise exception
    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("test/db", mock_cluster_service)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

  @pytest.mark.asyncio
  async def test_get_database_metrics_nonexistent_path(self, mock_cluster_service):
    """Test metrics when database path doesn't exist."""
    mock_cluster_service.db_manager.list_databases.return_value = ["test-db"]
    mock_cluster_service.db_manager.get_database_path.return_value = (
      "/non/existent/path"
    )

    # Mock connection
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.has_next.return_value = False
    mock_conn.execute.return_value = mock_result
    mock_cluster_service.db_manager.get_connection.return_value.__enter__.return_value = mock_conn

    result = await get_database_metrics("test-db", mock_cluster_service)

    assert result["size_bytes"] == 0
    assert result["last_modified"] is None

  @pytest.mark.asyncio
  async def test_get_database_metrics_large_database(self, mock_cluster_service):
    """Test metrics for large database with many files."""
    mock_cluster_service.db_manager.list_databases.return_value = ["large-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "large-db"
      db_path.mkdir()

      # Create nested structure with multiple files
      (db_path / "data").mkdir()
      (db_path / "data" / "file1.db").write_bytes(b"x" * 1024 * 1024)  # 1MB
      (db_path / "data" / "file2.db").write_bytes(b"x" * 512 * 1024)  # 512KB
      (db_path / "index").mkdir()
      (db_path / "index" / "idx1.db").write_bytes(b"x" * 256 * 1024)  # 256KB

      mock_cluster_service.db_manager.get_database_path.return_value = str(db_path)

      # Mock large counts
      mock_conn = MagicMock()
      mock_result_nodes = MagicMock()
      mock_result_nodes.has_next.return_value = True
      mock_result_nodes.get_next.return_value = [1000000]  # 1M nodes

      mock_result_rels = MagicMock()
      mock_result_rels.has_next.return_value = True
      mock_result_rels.get_next.return_value = [5000000]  # 5M relationships

      mock_conn.execute.side_effect = [mock_result_nodes, mock_result_rels]
      mock_cluster_service.db_manager.get_connection.return_value.__enter__.return_value = mock_conn

      result = await get_database_metrics("large-db", mock_cluster_service)

      expected_size = 1024 * 1024 + 512 * 1024 + 256 * 1024  # 1.75MB
      assert result["size_bytes"] == expected_size
      assert result["size_mb"] == 1.75
      assert result["node_count"] == 1000000
      assert result["relationship_count"] == 5000000

  @pytest.mark.asyncio
  async def test_get_database_metrics_unexpected_error(self, mock_cluster_service):
    """Test handling of unexpected errors during metrics retrieval."""
    mock_cluster_service.db_manager.list_databases.side_effect = Exception(
      "Unexpected error"
    )

    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("test-db", mock_cluster_service)

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to retrieve database metrics" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_get_database_metrics_shared_node_type(self, mock_cluster_service):
    """Test metrics retrieval on shared node type."""
    mock_cluster_service.node_type = NodeType.SHARED_MASTER
    mock_cluster_service.db_manager.list_databases.return_value = ["shared-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "shared-db"
      db_path.mkdir()

      mock_cluster_service.db_manager.get_database_path.return_value = str(db_path)

      # Mock connection
      mock_conn = MagicMock()
      mock_result = MagicMock()
      mock_result.has_next.return_value = False
      mock_conn.execute.return_value = mock_result
      mock_cluster_service.db_manager.get_connection.return_value.__enter__.return_value = mock_conn

      result = await get_database_metrics("shared-db", mock_cluster_service)

      assert result["node_type"] == "shared_master"

  @pytest.mark.asyncio
  async def test_get_database_metrics_query_partial_failure(self, mock_cluster_service):
    """Test when node count succeeds but relationship count fails."""
    mock_cluster_service.db_manager.list_databases.return_value = ["test-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "test-db"
      db_path.mkdir()

      mock_cluster_service.db_manager.get_database_path.return_value = str(db_path)

      # Mock connection where second query fails
      mock_conn = MagicMock()
      mock_result_nodes = MagicMock()
      mock_result_nodes.has_next.return_value = True
      mock_result_nodes.get_next.return_value = [50]

      mock_conn.execute.side_effect = [mock_result_nodes, Exception("Query failed")]
      mock_cluster_service.db_manager.get_connection.return_value.__enter__.return_value = mock_conn

      # Should handle partial failure gracefully
      result = await get_database_metrics("test-db", mock_cluster_service)

      # The implementation keeps partial results - node_count is set before exception
      # but relationship_count remains 0 since it never gets set
      assert result["node_count"] == 50  # Successfully retrieved before exception
      assert result["relationship_count"] == 0  # Remains default due to exception
