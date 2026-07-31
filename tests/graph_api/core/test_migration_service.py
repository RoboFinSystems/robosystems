"""Tests for the LadybugDB version-migration service.

Focus is the state machine that protects customer databases during an
engine upgrade: the export/import mutual-exclusion guards, the disk-space
precondition, node-count verification, and the rollback that restores
`.pre-migration` files when an import fails partway through.
"""

import json
import os
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.graph_api.core import migration_service as ms
from robosystems.graph_api.core.migration_service import MigrationService

TASK_MODULE = "robosystems.graph_api.core.migration_service"


@pytest.fixture(autouse=True)
def reset_migration_state():
  """Module-level flags are process globals -- reset around every test."""
  ms._migration_in_progress = False
  ms._export_in_progress = False
  ms._active_task_id = None
  yield
  ms._migration_in_progress = False
  ms._export_in_progress = False
  ms._active_task_id = None


@pytest.fixture
def service(tmp_path):
  return MigrationService(base_path=tmp_path)


@pytest.fixture
def task_manager():
  """Stub the redis-backed migration task manager."""
  tm = MagicMock()
  tm.update_task = AsyncMock()
  tm.complete_task = AsyncMock()
  tm.fail_task = AsyncMock()
  with patch(f"{TASK_MODULE}.migration_task_manager", tm):
    yield tm


def _fake_conn(tables, counts=None, failing_tables=()):
  """Build a stand-in LadybugDB connection.

  `tables` is a list of (name, type) pairs as `show_tables()` would yield;
  `counts` maps node-table name to its node count.
  """
  counts = counts or {}

  def execute(query):
    if "show_tables" in query:
      rows = [(i, name, ttype) for i, (name, ttype) in enumerate(tables)]
      return _FakeResult(rows)
    for name in counts:
      if f"(n:{name})" in query:
        if name in failing_tables:
          raise RuntimeError(f"table {name} unreadable")
        return _FakeResult([(counts[name],)])
    if any(f"(n:{t})" in query for t, _ in tables):
      raise RuntimeError("unreadable table")
    return _FakeResult([])

  conn = MagicMock()
  conn.execute.side_effect = execute
  return conn


def _importing_database():
  """Stand-in for `lbug.Database` that materialises the file it opens.

  The real IMPORT creates the `.lbug` on disk; the import path then stats it
  for the result payload, so a bare MagicMock leaves a missing-file error.
  """

  def factory(path, *args, **kwargs):
    Path(path).write_bytes(b"imported-database")
    return MagicMock()

  return factory


class _FakeResult:
  def __init__(self, rows):
    self._rows = list(rows)

  def has_next(self):
    return bool(self._rows)

  def get_next(self):
    return self._rows.pop(0)


class TestMigrationStateGuards:
  """The export/import flags are the only thing preventing two migrations
  from operating on the same database files concurrently."""

  def test_export_guard_is_exclusive(self):
    assert ms._try_start_export() is True
    assert ms._try_start_export() is False, "second caller must not acquire"

    ms._clear_export_in_progress()
    assert ms._try_start_export() is True, "flag must be reusable after clear"

  def test_import_guard_is_exclusive_and_records_task(self):
    assert ms._try_start_import("task-1") is True
    assert ms.is_migration_in_progress() is True
    assert ms.get_active_task_id() == "task-1"

    # A second import must not acquire, and must not steal the task id.
    assert ms._try_start_import("task-2") is False
    assert ms.get_active_task_id() == "task-1"

  def test_clearing_import_state_drops_task_id(self):
    ms._try_start_import("task-1")
    ms._set_migration_in_progress(False)

    assert ms.is_migration_in_progress() is False
    assert ms.get_active_task_id() is None

  def test_export_and_import_guards_are_independent(self):
    """An in-flight export must not block an import (they run on opposite
    sides of a container swap), and vice versa."""
    assert ms._try_start_export() is True
    assert ms._try_start_import("task-1") is True

  def test_only_one_thread_wins_the_export_guard(self):
    """The guard is a check-then-set under a lock; if the lock were dropped,
    concurrent callers could both proceed and corrupt the export directory."""
    acquired = []
    barrier = threading.Barrier(8)

    def contend():
      barrier.wait()
      if ms._try_start_export():
        acquired.append(threading.current_thread().name)

    threads = [threading.Thread(target=contend) for _ in range(8)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert len(acquired) == 1, f"expected exactly one winner, got {acquired}"

  def test_only_one_thread_wins_the_import_guard(self):
    acquired = []
    barrier = threading.Barrier(8)

    def contend(n):
      barrier.wait()
      if ms._try_start_import(f"task-{n}"):
        acquired.append(n)

    threads = [threading.Thread(target=contend, args=(i,)) for i in range(8)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert len(acquired) == 1
    assert ms.get_active_task_id() == f"task-{acquired[0]}"


class TestDatabaseDiscovery:
  def test_returns_empty_when_base_path_missing(self, tmp_path):
    service = MigrationService(base_path=tmp_path / "does-not-exist")
    assert service._discover_databases() == []

  def test_finds_only_lbug_files_sorted(self, service, tmp_path):
    (tmp_path / "kg_b.lbug").write_bytes(b"x")
    (tmp_path / "kg_a.lbug").write_bytes(b"x")
    (tmp_path / "notes.txt").write_text("ignore me")
    (tmp_path / "kg_a.lbug.wal").write_bytes(b"x")
    (tmp_path / "subdir.lbug").mkdir()

    assert service._discover_databases() == ["kg_a", "kg_b"]


class TestDiskSpacePrecondition:
  def test_raises_when_available_space_is_short(self, service, tmp_path):
    (tmp_path / "kg_a.lbug").write_bytes(b"x" * 2048)

    stat = MagicMock(f_bavail=1, f_frsize=512)  # 512 bytes available
    with patch(f"{TASK_MODULE}.os.statvfs", return_value=stat):
      with pytest.raises(RuntimeError, match="Insufficient disk space"):
        service._check_disk_space(["kg_a"])

  def test_passes_when_space_is_sufficient(self, service, tmp_path):
    (tmp_path / "kg_a.lbug").write_bytes(b"x" * 1024)

    stat = MagicMock(f_bavail=1024, f_frsize=1024)  # 1 MB available
    with patch(f"{TASK_MODULE}.os.statvfs", return_value=stat):
      service._check_disk_space(["kg_a"])  # must not raise

  def test_ignores_databases_with_no_file_on_disk(self, service):
    stat = MagicMock(f_bavail=1024, f_frsize=1024)
    with patch(f"{TASK_MODULE}.os.statvfs", return_value=stat):
      service._check_disk_space(["missing_db"])  # must not raise


class TestCounts:
  def test_node_count_sums_node_tables_only(self, service):
    conn = _fake_conn(
      tables=[("Entity", "NODE"), ("Fact", "NODE"), ("HAS_FACT", "REL")],
      counts={"Entity": 7, "Fact": 35},
    )
    assert service._get_node_count(conn) == 42

  def test_node_count_tolerates_an_unreadable_table(self, service):
    """One bad table must not abort the whole count -- the export still
    proceeds, it just verifies against a lower number."""
    conn = _fake_conn(
      tables=[("Entity", "NODE"), ("Broken", "NODE")],
      counts={"Entity": 7, "Broken": 5},
      failing_tables=("Broken",),
    )
    assert service._get_node_count(conn) == 7

  def test_rel_table_count_counts_only_rel(self, service):
    conn = _fake_conn(
      tables=[("Entity", "NODE"), ("HAS_FACT", "REL"), ("IN_PERIOD", "REL")]
    )
    assert service._get_rel_table_count(conn) == 2


class TestSystemBackupUpload:
  def test_uploads_with_migration_metadata(self, service, tmp_path):
    db_file = tmp_path / "kg_a.lbug"
    db_file.write_bytes(b"x" * 100)

    s3 = MagicMock()
    with patch.object(service, "_get_s3_client", return_value=s3):
      key = service._upload_system_backup(
        db_file, "kg_a", "0.18.1", "0.19.0", "test-bucket"
      )

    s3.upload_file.assert_called_once()
    args, kwargs = s3.upload_file.call_args
    assert args[1] == "test-bucket"
    assert args[2] == key
    metadata = kwargs["ExtraArgs"]["Metadata"]
    assert metadata["backup_type"] == "system"
    assert metadata["source_version"] == "0.18.1"
    assert metadata["target_version"] == "0.19.0"
    assert metadata["original_size"] == "100"


class TestStatusReporting:
  def test_reports_no_pending_migration_on_clean_instance(self, service):
    status = service.get_status()

    assert status["migration_pending"] is False
    assert status["migration_in_progress"] is False
    assert status["manifest"] is None
    assert status["pre_migration_files"] == []
    assert status["active_task_id"] is None

  def test_reports_pending_migration_and_active_task(self, service, tmp_path):
    manifest = {"source_version": "0.18.1", "target_version": "0.19.0"}
    (tmp_path / "migration.json").write_text(json.dumps(manifest))
    (tmp_path / "kg_a.lbug.pre-migration").write_bytes(b"x")
    ms._try_start_import("task-9")

    status = service.get_status()

    assert status["migration_pending"] is True
    assert status["migration_in_progress"] is True
    assert status["manifest"] == manifest
    assert status["pre_migration_files"] == ["kg_a.lbug.pre-migration"]
    assert status["active_task_id"] == "task-9"

  def test_corrupt_manifest_is_reported_as_no_manifest(self, service, tmp_path):
    """A truncated manifest must not crash the status endpoint -- operators
    need it most when a migration has gone wrong."""
    (tmp_path / "migration.json").write_text("{not valid json")

    status = service.get_status()

    assert status["manifest"] is None
    assert status["migration_pending"] is False


class TestPreMigrationCleanup:
  def test_deletes_pre_migration_files_and_reports_bytes_freed(self, service, tmp_path):
    (tmp_path / "kg_a.lbug.pre-migration").write_bytes(b"x" * 100)
    (tmp_path / "kg_b.lbug.pre-migration").write_bytes(b"x" * 50)
    (tmp_path / "kg_a.lbug").write_bytes(b"keep me")

    result = service.cleanup_pre_migration_files()

    assert sorted(result["files_deleted"]) == [
      "kg_a.lbug.pre-migration",
      "kg_b.lbug.pre-migration",
    ]
    assert result["total_freed_bytes"] == 150
    assert (tmp_path / "kg_a.lbug").exists(), "live database must survive cleanup"

  def test_noop_when_base_path_missing(self, tmp_path):
    service = MigrationService(base_path=tmp_path / "gone")
    assert service.cleanup_pre_migration_files() == {
      "files_deleted": [],
      "total_freed_bytes": 0,
    }


class TestExportAllDatabases:
  @pytest.mark.asyncio
  async def test_refuses_when_an_export_is_already_running(self, service, task_manager):
    ms._try_start_export()

    await service.export_all_databases("task-1", "0.18.1", "0.19.0", "bucket")

    task_manager.fail_task.assert_awaited_once_with(
      "task-1", "Export already in progress"
    )

  @pytest.mark.asyncio
  async def test_completes_early_when_no_databases_present(self, service, task_manager):
    await service.export_all_databases("task-1", "0.18.1", "0.19.0", "bucket")

    task_manager.complete_task.assert_awaited_once()
    result = task_manager.complete_task.await_args.kwargs["result"]
    assert result["databases"] == []
    assert ms._export_in_progress is False, "guard must be released"

  @pytest.mark.asyncio
  async def test_releases_guard_when_export_raises(
    self, service, task_manager, tmp_path
  ):
    """The finally-block release is what lets a retry succeed after a failed
    export; without it the instance is wedged until restart."""
    (tmp_path / "kg_a.lbug").write_bytes(b"x")

    with patch.object(
      service, "_check_disk_space", side_effect=RuntimeError("disk full")
    ):
      await service.export_all_databases("task-1", "0.18.1", "0.19.0", "bucket")

    task_manager.fail_task.assert_awaited_once_with("task-1", "disk full")
    assert ms._export_in_progress is False


class TestImportAllDatabases:
  @pytest.mark.asyncio
  async def test_refuses_when_an_import_is_already_running(self, service, task_manager):
    ms._try_start_import("other-task")

    await service.import_all_databases("task-1")

    task_manager.fail_task.assert_awaited_once_with(
      "task-1", "Import already in progress"
    )

  @pytest.mark.asyncio
  async def test_completes_and_clears_flag_when_no_manifest(
    self, service, task_manager
  ):
    await service.import_all_databases("task-1")

    task_manager.complete_task.assert_awaited_once()
    assert task_manager.complete_task.await_args.kwargs["result"] == {
      "message": "No migration pending"
    }
    assert ms.is_migration_in_progress() is False, "health must not stay at 503"

  @pytest.mark.asyncio
  async def test_completes_and_clears_flag_when_manifest_has_no_databases(
    self, service, task_manager, tmp_path
  ):
    (tmp_path / "migration.json").write_text(json.dumps({"databases": []}))

    await service.import_all_databases("task-1")

    assert task_manager.complete_task.await_args.kwargs["result"] == {
      "message": "Manifest has no databases"
    }
    assert ms.is_migration_in_progress() is False

  @pytest.mark.asyncio
  async def test_missing_export_directory_fails_and_clears_flag(
    self, service, task_manager, tmp_path
  ):
    (tmp_path / "migration.json").write_text(
      json.dumps(
        {
          "source_version": "0.18.1",
          "target_version": "0.19.0",
          "databases": [
            {"graph_id": "kg_a", "export_path": "exports/kg_a", "node_count": 1}
          ],
        }
      )
    )

    await service.import_all_databases("task-1")

    task_manager.fail_task.assert_awaited_once()
    assert "Export directory missing" in task_manager.fail_task.await_args.args[1]
    assert ms.is_migration_in_progress() is False

  @pytest.mark.asyncio
  async def test_rolls_back_renamed_database_when_import_fails(
    self, service, task_manager, tmp_path
  ):
    """The rollback is the whole safety story: if IMPORT fails, the original
    .lbug must come back or the customer's graph is gone."""
    (tmp_path / "migration.json").write_text(
      json.dumps(
        {
          "source_version": "0.18.1",
          "target_version": "0.19.0",
          "databases": [
            {"graph_id": "kg_a", "export_path": "exports/kg_a", "node_count": 10}
          ],
        }
      )
    )
    export_dir = tmp_path / "exports" / "kg_a"
    export_dir.mkdir(parents=True)
    db_file = tmp_path / "kg_a.lbug"
    db_file.write_bytes(b"original-database")

    with patch(f"{TASK_MODULE}.lbug.Database", side_effect=RuntimeError("bad engine")):
      await service.import_all_databases("task-1")

    task_manager.fail_task.assert_awaited_once()
    assert db_file.exists(), "original database must be restored"
    assert db_file.read_bytes() == b"original-database"
    assert not Path(f"{db_file}.pre-migration").exists()
    assert ms.is_migration_in_progress() is False

  @pytest.mark.asyncio
  async def test_node_count_mismatch_beyond_tolerance_fails_the_import(
    self, service, task_manager, tmp_path
  ):
    (tmp_path / "migration.json").write_text(
      json.dumps(
        {
          "source_version": "0.18.1",
          "target_version": "0.19.0",
          "databases": [
            {"graph_id": "kg_a", "export_path": "exports/kg_a", "node_count": 1000}
          ],
        }
      )
    )
    (tmp_path / "exports" / "kg_a").mkdir(parents=True)
    (tmp_path / "kg_a.lbug").write_bytes(b"original")

    with (
      patch(f"{TASK_MODULE}.lbug.Database"),
      patch(f"{TASK_MODULE}.lbug.Connection"),
      patch.object(service, "_get_node_count", return_value=400),
    ):
      await service.import_all_databases("task-1")

    task_manager.fail_task.assert_awaited_once()
    assert "Node count mismatch" in task_manager.fail_task.await_args.args[1]

  @pytest.mark.asyncio
  async def test_small_node_count_drift_is_tolerated(
    self, service, task_manager, tmp_path
  ):
    """Writes can land between export and import; a 0.1% drift is accepted
    rather than failing the upgrade."""
    (tmp_path / "migration.json").write_text(
      json.dumps(
        {
          "source_version": "0.18.1",
          "target_version": "0.19.0",
          "databases": [
            {"graph_id": "kg_a", "export_path": "exports/kg_a", "node_count": 10000}
          ],
        }
      )
    )
    (tmp_path / "exports" / "kg_a").mkdir(parents=True)
    (tmp_path / "kg_a.lbug").write_bytes(b"original")

    with (
      patch(f"{TASK_MODULE}.lbug.Database", side_effect=_importing_database()),
      patch(f"{TASK_MODULE}.lbug.Connection"),
      patch.object(service, "_get_node_count", return_value=10005),
    ):
      await service.import_all_databases("task-1")

    task_manager.complete_task.assert_awaited_once()
    result = task_manager.complete_task.await_args.kwargs["result"]
    assert result["databases_imported"] == 1
    assert result["results"][0]["verified"] is False
    assert ms.is_migration_in_progress() is False

  @pytest.mark.asyncio
  async def test_successful_import_cleans_up_manifest_and_exports(
    self, service, task_manager, tmp_path
  ):
    (tmp_path / "migration.json").write_text(
      json.dumps(
        {
          "source_version": "0.18.1",
          "target_version": "0.19.0",
          "databases": [
            {"graph_id": "kg_a", "export_path": "exports/kg_a", "node_count": 10}
          ],
        }
      )
    )
    (tmp_path / "exports" / "kg_a").mkdir(parents=True)
    (tmp_path / "kg_a.lbug").write_bytes(b"original")

    with (
      patch(f"{TASK_MODULE}.lbug.Database", side_effect=_importing_database()),
      patch(f"{TASK_MODULE}.lbug.Connection"),
      patch.object(service, "_get_node_count", return_value=10),
    ):
      await service.import_all_databases("task-1")

    task_manager.complete_task.assert_awaited_once()
    assert not (tmp_path / "migration.json").exists()
    assert not (tmp_path / "exports").exists()
    assert ms.is_migration_in_progress() is False

  @pytest.mark.asyncio
  async def test_parents_are_imported_before_subgraphs(
    self, service, task_manager, tmp_path
  ):
    """Subgraph ids extend their parent's, so shorter ids must import first."""
    (tmp_path / "migration.json").write_text(
      json.dumps(
        {
          "source_version": "0.18.1",
          "target_version": "0.19.0",
          "databases": [
            {"graph_id": "kg1a2b3c_dev", "export_path": "exports/sub", "node_count": 0},
            {"graph_id": "kg1a2b3c", "export_path": "exports/parent", "node_count": 0},
          ],
        }
      )
    )
    (tmp_path / "exports" / "sub").mkdir(parents=True)
    (tmp_path / "exports" / "parent").mkdir(parents=True)

    with (
      patch(f"{TASK_MODULE}.lbug.Database", side_effect=_importing_database()),
      patch(f"{TASK_MODULE}.lbug.Connection"),
      patch.object(service, "_get_node_count", return_value=0),
    ):
      await service.import_all_databases("task-1")

    result = task_manager.complete_task.await_args.kwargs["result"]
    order = [r["graph_id"] for r in result["results"]]
    assert order == ["kg1a2b3c", "kg1a2b3c_dev"]


class TestServicePaths:
  def test_derives_export_and_manifest_paths_from_base(self, tmp_path):
    service = MigrationService(base_path=tmp_path)

    assert service.exports_path == tmp_path / "exports"
    assert service.manifest_path == tmp_path / "migration.json"

  def test_defaults_base_path_to_configured_database_path(self):
    with patch(f"{TASK_MODULE}.env") as mock_env:
      mock_env.LBUG_DATABASE_PATH = "/data/lbug-dbs"
      service = MigrationService()

    assert service.base_path == Path("/data/lbug-dbs")


class TestS3ClientConstruction:
  def test_passes_endpoint_and_credentials_when_configured(self, service):
    config = {
      "region_name": "us-east-1",
      "endpoint_url": "http://localstack:4566",
      "aws_access_key_id": "key",
      "aws_secret_access_key": "secret",
    }
    with (
      patch(f"{TASK_MODULE}.env.get_s3_config", return_value=config),
      patch(f"{TASK_MODULE}.boto3.client") as mock_client,
    ):
      service._get_s3_client()

    kwargs = mock_client.call_args.kwargs
    assert kwargs["endpoint_url"] == "http://localstack:4566"
    assert kwargs["aws_access_key_id"] == "key"
    assert kwargs["aws_secret_access_key"] == "secret"

  def test_omits_endpoint_and_credentials_when_using_instance_role(self, service):
    """On EC2 the instance profile supplies credentials; passing empty
    strings through would break the default provider chain."""
    with (
      patch(
        f"{TASK_MODULE}.env.get_s3_config", return_value={"region_name": "us-east-1"}
      ),
      patch(f"{TASK_MODULE}.boto3.client") as mock_client,
    ):
      service._get_s3_client()

    kwargs = mock_client.call_args.kwargs
    assert "endpoint_url" not in kwargs
    assert "aws_access_key_id" not in kwargs
    assert kwargs["region_name"] == "us-east-1"


class TestCheckpointBeforeExport:
  def test_checkpoint_runs_against_a_writable_connection(self, service):
    """CHECKPOINT must not use a read-only handle, or the WAL never drains
    and the upgraded engine hits a corrupt WAL tail."""
    conn = MagicMock()
    ladybug_service = MagicMock()
    ladybug_service.db_manager.get_connection.return_value.__enter__.return_value = conn

    with patch(
      "robosystems.graph_api.core.ladybug.get_ladybug_service",
      return_value=ladybug_service,
    ):
      service._checkpoint_database("kg_a")

    ladybug_service.db_manager.get_connection.assert_called_once_with(
      "kg_a", read_only=False
    )
    conn.execute.assert_called_once_with("CHECKPOINT")


def test_os_import_is_module_level():
  """Guards the patch targets used above."""
  assert ms.os is os
