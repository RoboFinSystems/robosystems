"""Tests for OnInstanceBackupService.

Covers checkpoint, path resolution, temp directory cleanup,
and execute_backup orchestration (replica, shared_repository, error paths).
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

MODULE = "robosystems.graph_api.core.backup_service"


class TestCleanupStaleTempDirs:
  """Tests for the static _cleanup_stale_temp_dirs method."""

  @pytest.mark.unit
  def test_removes_old_dirs(self, tmp_path):
    """Old directories (beyond max_age_hours) are removed."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    old_dir = tmp_path / "stale_backup"
    old_dir.mkdir()

    # Set mtime to 48 hours ago
    old_time = time.time() - (48 * 3600)
    os.utime(old_dir, (old_time, old_time))

    OnInstanceBackupService._cleanup_stale_temp_dirs(tmp_path, max_age_hours=24)
    assert not old_dir.exists()

  @pytest.mark.unit
  def test_keeps_recent_dirs(self, tmp_path):
    """Recent directories within max_age_hours are preserved."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    recent_dir = tmp_path / "recent_backup"
    recent_dir.mkdir()
    # mtime is now (just created), well within 24 hours

    OnInstanceBackupService._cleanup_stale_temp_dirs(tmp_path, max_age_hours=24)
    assert recent_dir.exists()

  @pytest.mark.unit
  def test_handles_empty_dir_gracefully(self, tmp_path):
    """An empty parent directory does not raise."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    empty_dir = tmp_path / "empty_parent"
    empty_dir.mkdir()

    # Should not raise when iterating an empty directory
    OnInstanceBackupService._cleanup_stale_temp_dirs(empty_dir, max_age_hours=24)


class TestCheckpoint:
  """Tests for _checkpoint and _duckdb_checkpoint methods."""

  @pytest.mark.unit
  def test_checkpoint_calls_execute(self):
    """_checkpoint opens a connection and runs CHECKPOINT."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    mock_conn = MagicMock()
    mock_db_manager = MagicMock()
    mock_db_manager.get_connection.return_value.__enter__ = MagicMock(
      return_value=mock_conn
    )
    mock_db_manager.get_connection.return_value.__exit__ = MagicMock(return_value=False)

    service = OnInstanceBackupService(
      db_manager=mock_db_manager,
      task_manager=MagicMock(),
    )
    service._checkpoint("test_graph")

    mock_db_manager.get_connection.assert_called_once_with(
      "test_graph", read_only=False
    )
    mock_conn.execute.assert_called_once_with("CHECKPOINT")

  @pytest.mark.unit
  def test_duckdb_checkpoint_no_pool_raises(self):
    """_duckdb_checkpoint raises RuntimeError when duckdb_pool is None."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    service = OnInstanceBackupService(
      db_manager=MagicMock(),
      task_manager=MagicMock(),
      duckdb_pool=None,
    )
    with pytest.raises(RuntimeError, match="DuckDB pool not provided"):
      service._duckdb_checkpoint("test_graph")


class TestResolvePaths:
  """Tests for _resolve_db_path and _resolve_duckdb_path."""

  @pytest.mark.unit
  @patch(f"{MODULE}.get_lbug_database_path", create=True)
  def test_resolve_db_path(self, mock_get_path):
    """_resolve_db_path delegates to get_lbug_database_path."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    expected = Path("/data/test.lbug")

    with patch(
      "robosystems.operations.lbug.path_utils.get_lbug_database_path",
      return_value=expected,
    ):
      service = OnInstanceBackupService(
        db_manager=MagicMock(),
        task_manager=MagicMock(),
      )
      result = service._resolve_db_path("test_graph")
      assert result == expected

  @pytest.mark.unit
  def test_resolve_duckdb_path(self):
    """_resolve_duckdb_path delegates to get_staging_duckdb_path."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    with patch(
      "robosystems.config.storage.shared.get_staging_duckdb_path",
      return_value="/data/test.duckdb",
    ):
      service = OnInstanceBackupService(
        db_manager=MagicMock(),
        task_manager=MagicMock(),
      )
      result = service._resolve_duckdb_path("test_graph")
      assert result == Path("/data/test.duckdb")


class TestExecuteBackup:
  """Tests for the execute_backup orchestration method."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_replica_backup_happy_path(self):
    """Replica backup resolves path, uploads, and returns result metadata."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    mock_task_manager = AsyncMock()

    # Create a real temp file so db_path.exists() and stat() work
    with tempfile.NamedTemporaryFile(suffix=".lbug", delete=False) as f:
      f.write(b"test data")
      temp_path = Path(f.name)

    try:
      upload_result = {
        "status": "success",
        "s3_uri": "s3://bucket/key",
        "s3_bucket": "bucket",
        "s3_key": "key",
        "original_size_bytes": 9,
        "uploaded_size_bytes": 9,
        "uploaded_at": "2025-01-01T00:00:00+00:00",
      }

      service = OnInstanceBackupService(
        db_manager=MagicMock(),
        task_manager=mock_task_manager,
      )

      with (
        patch.object(service, "_checkpoint"),
        patch.object(service, "_resolve_db_path", return_value=temp_path),
        patch.object(service, "_upload_replica", return_value=upload_result),
      ):
        result = await service.execute_backup(
          task_id="task-1",
          graph_id="test_graph",
          backup_type="replica",
          s3_destination={"bucket": "bucket", "key": "key"},
        )

      assert result["status"] == "success"
      assert result["graph_id"] == "test_graph"
      assert result["backup_type"] == "replica"
      assert "duration_seconds" in result
      assert mock_task_manager.update_task.call_count >= 1
    finally:
      temp_path.unlink(missing_ok=True)

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_db_not_found_raises(self):
    """FileNotFoundError when resolved database path does not exist."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    mock_task_manager = AsyncMock()
    service = OnInstanceBackupService(
      db_manager=MagicMock(),
      task_manager=mock_task_manager,
    )

    nonexistent = Path("/tmp/does_not_exist_abc123.lbug")

    with (
      patch.object(service, "_checkpoint"),
      patch.object(service, "_resolve_db_path", return_value=nonexistent),
      pytest.raises(FileNotFoundError, match="Database not found"),
    ):
      await service.execute_backup(
        task_id="task-2",
        graph_id="test_graph",
        backup_type="replica",
        s3_destination={"bucket": "bucket", "key": "key"},
      )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_unsupported_backup_type_raises(self):
    """ValueError for unrecognised backup type."""
    from robosystems.graph_api.core.backup_service import OnInstanceBackupService

    mock_task_manager = AsyncMock()

    with tempfile.NamedTemporaryFile(suffix=".lbug", delete=False) as f:
      f.write(b"test data")
      temp_path = Path(f.name)

    try:
      service = OnInstanceBackupService(
        db_manager=MagicMock(),
        task_manager=mock_task_manager,
      )

      with (
        patch.object(service, "_checkpoint"),
        patch.object(service, "_resolve_db_path", return_value=temp_path),
        pytest.raises(ValueError, match="Unsupported on-instance backup type"),
      ):
        await service.execute_backup(
          task_id="task-3",
          graph_id="test_graph",
          backup_type="unknown",
          s3_destination={"bucket": "bucket", "key": "key"},
        )
    finally:
      temp_path.unlink(missing_ok=True)
