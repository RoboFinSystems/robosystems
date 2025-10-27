"""Tests for BackendClusterService coordinating backend operations."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.backend_cluster_manager import BackendClusterService
from robosystems.graph_api.models.database import QueryRequest


@pytest.fixture
def service_with_backend(monkeypatch):
  """Create a BackendClusterService with a mocked backend."""
  backend = AsyncMock()
  backend.execute_query = AsyncMock(return_value=[{"result": 1}])
  backend.health_check = AsyncMock(return_value=True)
  backend.list_databases = AsyncMock(return_value=["graph-a", "graph-b"])
  backend.get_cluster_topology = AsyncMock(return_value={"mode": "single"})

  monkeypatch.setattr(
    "robosystems.graph_api.core.backend_cluster_manager.get_backend",
    lambda: backend,
  )

  service = BackendClusterService()
  return service, backend


@pytest.mark.asyncio
async def test_execute_query_success(service_with_backend):
  """Happy-path query execution returns data and metrics."""
  service, backend = service_with_backend
  request = QueryRequest(database="graph-a", cypher="RETURN 1", parameters={})

  response = await service.execute_query(request)

  backend.execute_query.assert_awaited_once_with(
    graph_id="graph-a", cypher="RETURN 1", parameters={}
  )
  assert response.row_count == 1
  assert response.columns == ["result"]
  assert response.data == [{"result": 1}]


@pytest.mark.asyncio
async def test_execute_query_failure_raises_http_exception(service_with_backend):
  """Backend failures should be surfaced as 500 errors."""
  service, backend = service_with_backend
  backend.execute_query.side_effect = RuntimeError("boom")
  request = QueryRequest(database="graph-a", cypher="RETURN 1", parameters=None)

  with pytest.raises(HTTPException) as exc:
    await service.execute_query(request)

  assert exc.value.status_code == 500
  assert "Query execution failed" in exc.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize(
  "health_return,cpu_percent,mem_percent,expected_status",
  [
    (False, 10.0, 10.0, "unhealthy"),
    (True, 95.0, 50.0, "critical"),
    (True, 60.0, 92.0, "critical"),
    (True, 80.0, 60.0, "warning"),
    (True, 50.0, 40.0, "healthy"),
  ],
)
async def test_get_cluster_health_classifies_status(
  service_with_backend,
  monkeypatch,
  health_return,
  cpu_percent,
  mem_percent,
  expected_status,
):
  """Health endpoint should downgrade status based on resource usage."""
  service, backend = service_with_backend
  backend.health_check.return_value = health_return

  monkeypatch.setattr(
    "robosystems.graph_api.core.backend_cluster_manager.psutil.cpu_percent",
    lambda interval=0.1: cpu_percent,
  )
  monkeypatch.setattr(
    "robosystems.graph_api.core.backend_cluster_manager.psutil.virtual_memory",
    lambda: SimpleNamespace(percent=mem_percent),
  )

  response = await service.get_cluster_health()

  assert response.status == expected_status
  assert response.node_type == "backend"
  assert response.read_only is False


@pytest.mark.asyncio
async def test_get_cluster_info_returns_backend_metadata(service_with_backend):
  """Cluster info should include database list and backend identifier."""
  service, backend = service_with_backend
  backend.list_databases.return_value = ["graph-a", "graph-b"]

  response = await service.get_cluster_info()

  backend.get_cluster_topology.assert_awaited_once()
  backend.list_databases.assert_awaited_once()
  assert response.databases == ["graph-a", "graph-b"]
  assert response.node_id.startswith("backend-")
  assert response.node_type == "backend"


@pytest.mark.asyncio
async def test_get_cluster_info_propagates_errors(service_with_backend):
  """Failures fetching topology should raise HTTP 500."""
  service, backend = service_with_backend
  backend.get_cluster_topology.side_effect = RuntimeError("offline")

  with pytest.raises(HTTPException) as exc:
    await service.get_cluster_info()

  assert exc.value.status_code == 500
  assert "Failed to get cluster info" in exc.value.detail
