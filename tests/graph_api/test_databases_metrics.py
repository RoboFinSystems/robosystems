"""Tests for graph_api databases/metrics router."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
import pytest
from fastapi import HTTPException, status

from robosystems.graph_api.routers.databases.metrics import get_database_metrics
from robosystems.graph_api.backends.base import DatabaseInfo
from robosystems.middleware.graph.types import NodeType


class TestDatabaseMetricsRouter:
  """Test cases for database metrics endpoints."""

  @pytest.fixture
  def mock_backend(self):
    """Create a mock backend."""
    backend = AsyncMock()
    backend.__class__.__name__ = "LadybugBackend"
    return backend

  @pytest.fixture
  def mock_service(self):
    """Create a mock service."""
    service = MagicMock()
    service.node_id = "test-instance-01"
    service.node_type = NodeType.WRITER
    return service

  @pytest.mark.asyncio
  async def test_get_database_metrics_success(self, mock_backend, mock_service):
    """Test successful retrieval of database metrics."""
    # Setup backend mocks
    mock_backend.list_databases.return_value = ["test-db", "other-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "test-db.lbug"
      db_path.write_bytes(b"x" * 3072)  # 3KB file

      # Set data_path attribute for LadybugDB backend
      mock_backend.data_path = tmpdir

      # Mock database info
      db_info = DatabaseInfo(
        name="test-db",
        size_bytes=3072,
        node_count=100,
        relationship_count=250,
      )
      mock_backend.get_database_info.return_value = db_info

      # Call the function
      result = await get_database_metrics("test-db", mock_backend, mock_service)

      # Verify results
      assert result["graph_id"] == "test-db"
      assert result["database_name"] == "test-db"
      assert result["size_bytes"] == 3072
      assert result["size_mb"] == 0.0  # 3KB rounds to 0.00 MB
      assert result["node_count"] == 100
      assert result["relationship_count"] == 250
      assert result["last_modified"] is not None
      assert result["backend_type"] == "LadybugBackend"
      assert result["instance_id"] == "test-instance-01"
      assert result["node_type"] == "writer"

  @pytest.mark.asyncio
  async def test_get_database_metrics_not_found(self, mock_backend, mock_service):
    """Test metrics retrieval for non-existent database."""
    mock_backend.list_databases.return_value = ["other-db"]

    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("non-existent-db", mock_backend, mock_service)

    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    assert "not found" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_get_database_metrics_single_file_database(
    self, mock_backend, mock_service
  ):
    """Test metrics for single-file database."""
    mock_backend.list_databases.return_value = ["test-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "test-db.lbug"
      db_path.write_bytes(b"x" * 5000)

      mock_backend.data_path = tmpdir

      db_info = DatabaseInfo(
        name="test-db",
        size_bytes=5000,
        node_count=0,
        relationship_count=0,
      )
      mock_backend.get_database_info.return_value = db_info

      result = await get_database_metrics("test-db", mock_backend, mock_service)

      assert result["size_bytes"] == 5000
      assert result["size_mb"] == 0.0  # 5KB rounds to 0.00 MB
      assert result["node_count"] == 0
      assert result["relationship_count"] == 0

  @pytest.mark.asyncio
  async def test_get_database_metrics_invalid_name(self, mock_backend, mock_service):
    """Test metrics retrieval with invalid database name."""
    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("test/db", mock_backend, mock_service)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

  @pytest.mark.asyncio
  async def test_get_database_metrics_nonexistent_path(
    self, mock_backend, mock_service
  ):
    """Test metrics when database path doesn't exist."""
    mock_backend.list_databases.return_value = ["test-db"]

    db_info = DatabaseInfo(
      name="test-db",
      size_bytes=0,
      node_count=0,
      relationship_count=0,
    )
    mock_backend.get_database_info.return_value = db_info

    result = await get_database_metrics("test-db", mock_backend, mock_service)

    assert result["size_bytes"] == 0
    assert result["last_modified"] is None

  @pytest.mark.asyncio
  async def test_get_database_metrics_large_database(self, mock_backend, mock_service):
    """Test metrics for large database with many files."""
    mock_backend.list_databases.return_value = ["large-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "large-db.lbug"
      expected_size = 1024 * 1024 + 512 * 1024 + 256 * 1024  # 1.75MB
      db_path.write_bytes(b"x" * expected_size)

      mock_backend.data_path = tmpdir

      db_info = DatabaseInfo(
        name="large-db",
        size_bytes=expected_size,
        node_count=1000000,
        relationship_count=5000000,
      )
      mock_backend.get_database_info.return_value = db_info

      result = await get_database_metrics("large-db", mock_backend, mock_service)

      assert result["size_bytes"] == expected_size
      assert result["size_mb"] == 1.75
      assert result["node_count"] == 1000000
      assert result["relationship_count"] == 5000000

  @pytest.mark.asyncio
  async def test_get_database_metrics_unexpected_error(
    self, mock_backend, mock_service
  ):
    """Test handling of unexpected errors during metrics retrieval."""
    mock_backend.list_databases.side_effect = Exception("Unexpected error")

    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("test-db", mock_backend, mock_service)

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to retrieve database metrics" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_get_database_metrics_shared_node_type(
    self, mock_backend, mock_service
  ):
    """Test metrics retrieval on shared node type."""
    mock_service.node_type = NodeType.SHARED_MASTER
    mock_backend.list_databases.return_value = ["shared-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "shared-db.lbug"
      db_path.write_bytes(b"x" * 1000)

      mock_backend.data_path = tmpdir

      db_info = DatabaseInfo(
        name="shared-db",
        size_bytes=1000,
        node_count=0,
        relationship_count=0,
      )
      mock_backend.get_database_info.return_value = db_info

      result = await get_database_metrics("shared-db", mock_backend, mock_service)

      assert result["node_type"] == "shared_master"
