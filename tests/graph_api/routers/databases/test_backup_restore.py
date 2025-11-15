"""Tests for database backup and restore routes."""

import io
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from robosystems.graph_api.routers.databases import backup as backup_module
from robosystems.graph_api.routers.databases import restore as restore_module
from robosystems.graph_api.core.cluster_manager import get_cluster_service


class FakeDBManager:
  def __init__(self, databases):
    self._databases = databases
    self.connection_pool = None

  def list_databases(self):
    return list(self._databases)


class FakeClusterService:
  def __init__(self, read_only=False, databases=None):
    self.read_only = read_only
    self.db_manager = FakeDBManager(databases or [])


@pytest.fixture
def backup_client(monkeypatch):
  """Create TestClient with backup router and patched task manager."""
  app = FastAPI()
  app.include_router(backup_module.router)

  task_manager = SimpleNamespace(
    create_task=AsyncMock(return_value="task-123"),
    fail_task=AsyncMock(),
    update_task=AsyncMock(),
    complete_task=AsyncMock(),
  )
  monkeypatch.setattr(backup_module, "backup_task_manager", task_manager)

  # Mock the background task function so it doesn't actually execute
  perform_backup_mock = AsyncMock()
  monkeypatch.setattr(backup_module, "perform_backup", perform_backup_mock)

  def _factory(cluster_service):
    app.dependency_overrides[get_cluster_service] = lambda: cluster_service
    client = TestClient(app)
    client.task_manager = task_manager  # attach for assertions
    client.perform_backup_mock = perform_backup_mock  # attach for assertions
    return client

  return _factory


@pytest.fixture
def restore_client(monkeypatch):
  """Create TestClient with restore router and patched task manager."""
  app = FastAPI()
  app.include_router(restore_module.router)

  task_manager = SimpleNamespace(
    create_task=AsyncMock(return_value="task-456"),
    fail_task=AsyncMock(),
    update_task=AsyncMock(),
    complete_task=AsyncMock(),
  )
  monkeypatch.setattr(restore_module, "restore_task_manager", task_manager)

  # Mock the background task function so it doesn't actually execute
  perform_restore_mock = AsyncMock()
  monkeypatch.setattr(restore_module, "perform_restore", perform_restore_mock)

  def _factory(cluster_service):
    app.dependency_overrides[get_cluster_service] = lambda: cluster_service
    client = TestClient(app)
    client.task_manager = task_manager
    client.perform_restore_mock = perform_restore_mock
    return client

  return _factory


def test_create_backup_rejects_on_read_only(backup_client):
  cluster = FakeClusterService(read_only=True, databases=["graph1"])
  client = backup_client(cluster)

  response = client.post(
    "/databases/graph1/backup",
    json={
      "backup_format": "full_dump",
      "include_metadata": True,
      "compression": True,
      "encryption": False,
    },
  )

  assert response.status_code == 403
  assert "not allowed" in response.json()["detail"]
  client.task_manager.create_task.assert_not_awaited()


def test_create_backup_initiates_task(backup_client):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  client = backup_client(cluster)

  response = client.post(
    "/databases/graph1/backup",
    json={
      "backup_format": "full_dump",
      "include_metadata": True,
      "compression": True,
      "encryption": False,
    },
  )

  assert response.status_code == 200
  data = response.json()
  assert data["task_id"] == "task-123"
  assert data["database"] == "graph1"
  assert data["status"] == "initiated"

  client.task_manager.create_task.assert_awaited_once()
  _, kwargs = client.task_manager.create_task.await_args
  assert kwargs["task_type"] == "backup"
  assert kwargs["metadata"]["database"] == "graph1"

  # Backup is now processed in background task, not immediately failed
  # Note: Background task execution is not tested here (would need async test)


def test_restore_rejects_existing_database_without_force(restore_client):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  client = restore_client(cluster)

  response = client.post(
    "/databases/graph1/restore",
    data={
      "s3_bucket": "test-bucket",
      "s3_key": "backups/graph1.zip",
      "create_system_backup": "true",
      "force_overwrite": "false",
    },
  )

  assert response.status_code == 409
  assert "already exists" in response.json()["detail"]
  client.task_manager.create_task.assert_not_awaited()


def test_restore_initiates_task_with_metadata(restore_client):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  client = restore_client(cluster)

  response = client.post(
    "/databases/graph1/restore",
    data={
      "s3_bucket": "test-bucket",
      "s3_key": "backups/graph1.zip",
      "create_system_backup": "true",
      "force_overwrite": "true",
    },
  )

  assert response.status_code == 200
  payload = response.json()
  assert payload["task_id"] == "task-456"
  assert payload["database"] == "graph1"
  assert payload["status"] == "initiated"
  assert payload["system_backup_created"] is True

  client.task_manager.create_task.assert_awaited_once()
  _, kwargs = client.task_manager.create_task.await_args
  assert kwargs["task_type"] == "restore"
  assert kwargs["metadata"]["database"] == "graph1"
  assert kwargs["metadata"]["s3_bucket"] == "test-bucket"
  assert kwargs["metadata"]["s3_key"] == "backups/graph1.zip"
  assert kwargs["metadata"]["create_system_backup"] is True
  assert kwargs["metadata"]["force_overwrite"] is True

  client.task_manager.fail_task.assert_not_awaited()


@pytest.mark.asyncio
async def test_download_backup_returns_zip_for_file_db(monkeypatch, tmp_path):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  db_file = tmp_path / "graph1.kuzu"
  db_file.write_bytes(b"neo4j-data")

  monkeypatch.setattr(
    "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.get_database_path_for_graph",
    lambda graph_id: str(db_file),
  )

  response = await restore_module.download_backup(
    graph_id="graph1", cluster_service=cluster
  )

  assert response.headers["X-Database"] == "graph1"
  assert response.headers["X-Backup-Format"] == "full_dump"
  assert int(response.headers["X-Backup-Size"]) > 0

  backup_bytes = response.body
  assert isinstance(backup_bytes, bytes)

  with zipfile.ZipFile(io.BytesIO(backup_bytes), "r") as zf:
    # Should contain the single .kuzu file with matching contents
    namelist = zf.namelist()
    assert namelist == ["graph1.kuzu"]
    extracted = zf.read("graph1.kuzu")
    assert extracted == b"neo4j-data"


@pytest.mark.asyncio
async def test_download_backup_missing_path_returns_404(monkeypatch):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  monkeypatch.setattr(
    "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.get_database_path_for_graph",
    lambda graph_id: "/nonexistent/path/graph1.kuzu",
  )

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="graph1", cluster_service=cluster)

  assert exc.value.status_code == 404
  assert "not found" in exc.value.detail.lower()


@pytest.mark.asyncio
async def test_download_backup_rejects_read_only():
  cluster = FakeClusterService(read_only=True, databases=["graph1"])

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="graph1", cluster_service=cluster)

  assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_download_backup_missing_database_returns_404():
  cluster = FakeClusterService(read_only=False, databases=[])

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="graph1", cluster_service=cluster)

  assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_download_backup_directory_database(monkeypatch, tmp_path):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])

  db_dir = tmp_path / "graph1"
  db_dir.mkdir()
  (db_dir / "nodes").write_bytes(b"node-data")
  nested_dir = db_dir / "logs"
  nested_dir.mkdir()
  (nested_dir / "metrics.log").write_text("metrics")

  monkeypatch.setattr(
    "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.get_database_path_for_graph",
    lambda graph_id: str(db_dir),
  )

  response = await restore_module.download_backup(
    graph_id="graph1", cluster_service=cluster
  )

  backup_bytes = response.body
  with zipfile.ZipFile(io.BytesIO(backup_bytes), "r") as zf:
    names = sorted(zf.namelist())
    assert names == ["graph1/logs/metrics.log", "graph1/nodes"]
    assert zf.read("graph1/nodes") == b"node-data"
    assert zf.read("graph1/logs/metrics.log") == b"metrics"
