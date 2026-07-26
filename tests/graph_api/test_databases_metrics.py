"""Tests for graph_api databases/metrics router."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, status

from robosystems.graph_api.models.database import DatabaseInfo
from robosystems.graph_api.routers.databases.metrics import get_database_metrics
from robosystems.middleware.graph.types import NodeType


class TestDatabaseMetricsRouter:
  """Test cases for database metrics endpoints."""

  @pytest.fixture
  def mock_service(self):
    """Create a mock LadybugDB service with db_manager."""
    service = MagicMock()
    service.node_id = "test-instance-01"
    service.node_type = NodeType.WRITER
    service.db_manager = MagicMock()
    return service

  @pytest.mark.asyncio
  async def test_get_database_metrics_success(self, mock_service):
    """Test successful retrieval of database metrics."""
    mock_service.db_manager.list_databases.return_value = ["test-db", "other-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "test-db.lbug"
      db_path.write_bytes(b"x" * 3072)  # 3KB file

      mock_service.db_manager.base_path = Path(tmpdir)

      db_info = DatabaseInfo(
        graph_id="test-db",
        database_path=str(db_path),
        created_at="2024-01-01T00:00:00",
        size_bytes=3072,
        read_only=False,
        is_healthy=True,
      )
      mock_service.db_manager.get_database_info.return_value = db_info

      # Mock connection pool for node/relationship counts
      mock_conn = MagicMock()
      mock_result_nodes = MagicMock()
      mock_result_nodes.has_next.return_value = True
      mock_result_nodes.get_next.return_value = [100]
      mock_result_rels = MagicMock()
      mock_result_rels.has_next.return_value = True
      mock_result_rels.get_next.return_value = [250]
      mock_conn.execute.side_effect = [mock_result_nodes, mock_result_rels]

      mock_pool = MagicMock()
      mock_pool.get_connection.return_value.__enter__ = MagicMock(
        return_value=mock_conn
      )
      mock_pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)

      with patch(
        "robosystems.graph_api.core.ladybug.get_connection_pool",
        return_value=mock_pool,
      ):
        result = await get_database_metrics("test-db", service=mock_service)

      assert result["graph_id"] == "test-db"
      assert result["database_name"] == "test-db"
      assert result["size_bytes"] == 3072
      assert result["size_mb"] == 0.0
      assert result["node_count"] == 100
      assert result["relationship_count"] == 250
      assert result["last_modified"] is not None
      assert result["backend_type"] == "LadybugDB"
      assert result["instance_id"] == "test-instance-01"
      assert result["node_type"] == "writer"

  def _stub_counts(self, tmpdir, mock_service, size=3072):
    """Wire a service whose count queries would succeed if they were run."""
    db_path = Path(tmpdir) / "test-db.lbug"
    db_path.write_bytes(b"x" * size)
    mock_service.db_manager.base_path = Path(tmpdir)
    mock_service.db_manager.list_databases.return_value = ["test-db"]
    mock_service.db_manager.get_database_info.return_value = DatabaseInfo(
      graph_id="test-db",
      database_path=str(db_path),
      created_at="2024-01-01T00:00:00",
      size_bytes=size,
      read_only=False,
      is_healthy=True,
    )

    mock_conn = MagicMock()
    node_result = MagicMock()
    node_result.has_next.return_value = True
    node_result.get_next.return_value = [100]
    rel_result = MagicMock()
    rel_result.has_next.return_value = True
    rel_result.get_next.return_value = [250]
    mock_conn.execute.side_effect = [node_result, rel_result]

    pool = MagicMock()
    pool.get_connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
    pool.get_connection.return_value.__exit__ = MagicMock(return_value=False)
    return pool, mock_conn

  @pytest.mark.asyncio
  async def test_counts_are_skipped_unless_requested(self, mock_service):
    """The scans must not run by default.

    `MATCH (n) RETURN count(n)` has no index to lean on; on the SEC
    repository it exceeded 30s and timed out `/health`, which is why the
    cost is opt-in.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
      pool, mock_conn = self._stub_counts(tmpdir, mock_service)

      with patch(
        "robosystems.graph_api.core.ladybug.get_connection_pool", return_value=pool
      ):
        result = await get_database_metrics(
          "test-db", include_counts=False, service=mock_service
        )

      assert result["node_count"] is None
      assert result["relationship_count"] is None
      assert result["size_bytes"] == 3072
      # The expensive part is not merely discarded — it never runs.
      mock_conn.execute.assert_not_called()

  @pytest.mark.asyncio
  async def test_counts_computed_when_requested(self, mock_service):
    with tempfile.TemporaryDirectory() as tmpdir:
      pool, _ = self._stub_counts(tmpdir, mock_service)

      with patch(
        "robosystems.graph_api.core.ladybug.get_connection_pool", return_value=pool
      ):
        result = await get_database_metrics(
          "test-db", include_counts=True, service=mock_service
        )

      assert result["node_count"] == 100
      assert result["relationship_count"] == 250

  def test_openapi_default_is_off(self):
    """Guard the default through FastAPI, not just the Python signature."""
    from robosystems.graph_api.routers.databases.metrics import router

    route = next(
      r
      for r in router.routes
      if getattr(r, "path", "") == "/databases/{graph_id}/metrics"
    )
    param = next(p for p in route.dependant.query_params if p.name == "include_counts")
    assert param.default is False

  @pytest.mark.asyncio
  async def test_get_database_metrics_not_found(self, mock_service):
    """Test metrics retrieval for non-existent database."""
    mock_service.db_manager.list_databases.return_value = ["other-db"]

    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("non-existent-db", service=mock_service)

    assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
    assert "not found" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_get_database_metrics_invalid_name(self, mock_service):
    """Test metrics retrieval with invalid database name."""
    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("test/db", service=mock_service)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

  @pytest.mark.asyncio
  async def test_get_database_metrics_unexpected_error(self, mock_service):
    """Test handling of unexpected errors during metrics retrieval."""
    mock_service.db_manager.list_databases.side_effect = Exception("Unexpected error")

    with pytest.raises(HTTPException) as exc_info:
      await get_database_metrics("test-db", service=mock_service)

    assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "Failed to retrieve database metrics" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_get_database_metrics_shared_node_type(self, mock_service):
    """Test metrics retrieval on shared node type."""
    mock_service.node_type = NodeType.SHARED_MASTER
    mock_service.db_manager.list_databases.return_value = ["shared-db"]

    with tempfile.TemporaryDirectory() as tmpdir:
      db_path = Path(tmpdir) / "shared-db.lbug"
      db_path.write_bytes(b"x" * 1000)

      mock_service.db_manager.base_path = Path(tmpdir)

      db_info = DatabaseInfo(
        graph_id="shared-db",
        database_path=str(db_path),
        created_at="2024-01-01T00:00:00",
        size_bytes=1000,
        read_only=False,
        is_healthy=True,
      )
      mock_service.db_manager.get_database_info.return_value = db_info

      with patch(
        "robosystems.graph_api.core.ladybug.get_connection_pool",
      ) as mock_get_pool:
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.has_next.return_value = True
        mock_result.get_next.return_value = [0]
        mock_conn.execute.return_value = mock_result
        mock_get_pool.return_value.get_connection.return_value.__enter__ = MagicMock(
          return_value=mock_conn
        )
        mock_get_pool.return_value.get_connection.return_value.__exit__ = MagicMock(
          return_value=False
        )

        result = await get_database_metrics("shared-db", service=mock_service)

      assert result["node_type"] == "shared_master"
