"""Tests for database backup and restore routes."""

import io
import zipfile
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.routers.databases import backup as backup_module
from robosystems.graph_api.routers.databases import restore as restore_module


class FakeConnection:
  def __init__(self, recorder, fail=False):
    self._recorder = recorder
    self._fail = fail

  def execute(self, sql):
    if self._fail:
      raise RuntimeError("checkpoint boom")
    self._recorder.append(sql)


class FakeDBManager:
  def __init__(self, databases, checkpoint_fails=False):
    self._databases = databases
    self.connection_pool = None
    self.executed: list[str] = []
    self.opened_read_only: list[bool] = []
    self._checkpoint_fails = checkpoint_fails

  def list_databases(self):
    return list(self._databases)

  @contextmanager
  def get_connection(self, graph_id, read_only=True):
    self.opened_read_only.append(read_only)
    yield FakeConnection(self.executed, fail=self._checkpoint_fails)


class FakeClusterService:
  def __init__(self, read_only=False, databases=None, checkpoint_fails=False):
    self.read_only = read_only
    self.db_manager = FakeDBManager(databases or [], checkpoint_fails)


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
    app.dependency_overrides[get_ladybug_service] = lambda: cluster_service
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
    app.dependency_overrides[get_ladybug_service] = lambda: cluster_service
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


def test_restore_passes_system_backup_choice_to_background_task(restore_client):
  """The caller's create_system_backup choice must reach `perform_restore`.

  It rides RestoreJob into `_import_full_dump`, which is the only thing that
  snapshots the database before overwriting it. Dropping the flag here would
  silently force a snapshot the caller opted out of.
  """
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  client = restore_client(cluster)

  response = client.post(
    "/databases/graph1/restore",
    data={
      "s3_bucket": "test-bucket",
      "s3_key": "backups/graph1.zip",
      "create_system_backup": "false",
      "force_overwrite": "true",
    },
  )

  assert response.status_code == 200
  assert response.json()["system_backup_created"] is False

  _, kwargs = client.perform_restore_mock.call_args
  assert kwargs["create_system_backup"] is False


@pytest.mark.asyncio
async def test_download_backup_returns_zip_for_file_db(monkeypatch, tmp_path):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  db_file = tmp_path / "graph1.lbug"
  db_file.write_bytes(b"lbug-data")

  monkeypatch.setattr(
    "robosystems.middleware.graph.utils.MultiTenantUtils.get_database_path_for_graph",
    lambda graph_id: str(db_file),
  )

  response = await restore_module.download_backup(
    graph_id="graph1", ladybug_service=cluster
  )

  assert response.headers["X-Database"] == "graph1"
  assert response.headers["X-Backup-Format"] == "full_dump"
  assert int(response.headers["X-Backup-Size"]) > 0

  backup_bytes = response.body
  assert isinstance(backup_bytes, bytes)

  with zipfile.ZipFile(io.BytesIO(backup_bytes), "r") as zf:
    # Should contain the single .lbug file with matching contents
    namelist = zf.namelist()
    assert namelist == ["graph1.lbug"]
    extracted = zf.read("graph1.lbug")
    assert extracted == b"lbug-data"

  # The WAL must be folded into the main file before it is copied — without
  # this the backup captures the database as of its last pool eviction, which
  # for a graph written to since then is stale and for a young one is empty.
  assert cluster.db_manager.executed == ["CHECKPOINT"]
  assert cluster.db_manager.opened_read_only == [False]


@pytest.mark.asyncio
async def test_download_backup_aborts_when_checkpoint_fails(monkeypatch, tmp_path):
  """A failed checkpoint must fail the backup, not fall through to a stale copy.

  Falling through is the whole defect class: a backup that reports success over
  incomplete data is worse than one that never ran.
  """
  cluster = FakeClusterService(
    read_only=False, databases=["graph1"], checkpoint_fails=True
  )
  db_file = tmp_path / "graph1.lbug"
  db_file.write_bytes(b"lbug-data")

  monkeypatch.setattr(
    "robosystems.middleware.graph.utils.MultiTenantUtils.get_database_path_for_graph",
    lambda graph_id: str(db_file),
  )

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="graph1", ladybug_service=cluster)

  assert exc.value.status_code == 500
  assert "checkpoint" in exc.value.detail.lower()


@pytest.mark.asyncio
async def test_download_backup_checkpoints_only_known_databases():
  """The membership check must precede the checkpoint.

  LadybugDB creates a database when the path is absent, so checkpointing an
  unknown graph would mint an empty one and satisfy the existence guard.
  """
  cluster = FakeClusterService(read_only=False, databases=["graph1"])

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="ghost", ladybug_service=cluster)

  assert exc.value.status_code == 404
  assert cluster.db_manager.executed == []


@pytest.mark.asyncio
async def test_download_backup_missing_path_returns_404(monkeypatch):
  cluster = FakeClusterService(read_only=False, databases=["graph1"])
  monkeypatch.setattr(
    "robosystems.middleware.graph.utils.MultiTenantUtils.get_database_path_for_graph",
    lambda graph_id: "/nonexistent/path/graph1.lbug",
  )

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="graph1", ladybug_service=cluster)

  assert exc.value.status_code == 404
  assert "not found" in exc.value.detail.lower()


@pytest.mark.asyncio
async def test_download_backup_rejects_read_only():
  cluster = FakeClusterService(read_only=True, databases=["graph1"])

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="graph1", ladybug_service=cluster)

  assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_download_backup_missing_database_returns_404():
  cluster = FakeClusterService(read_only=False, databases=[])

  with pytest.raises(HTTPException) as exc:
    await restore_module.download_backup(graph_id="graph1", ladybug_service=cluster)

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
    "robosystems.middleware.graph.utils.MultiTenantUtils.get_database_path_for_graph",
    lambda graph_id: str(db_dir),
  )

  response = await restore_module.download_backup(
    graph_id="graph1", ladybug_service=cluster
  )

  backup_bytes = response.body
  with zipfile.ZipFile(io.BytesIO(backup_bytes), "r") as zf:
    names = sorted(zf.namelist())
    assert names == ["graph1/logs/metrics.log", "graph1/nodes"]
    assert zf.read("graph1/nodes") == b"node-data"
    assert zf.read("graph1/logs/metrics.log") == b"metrics"
