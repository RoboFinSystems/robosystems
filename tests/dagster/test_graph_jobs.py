"""Tests for Dagster graph operation jobs.

Note: TestWaitAndCreateGraphConfig and TestWaitAndCreate removed —
wait-for-capacity graph creation replaced by worker task + sensor retry.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.dagster.jobs.graph import (
  _emit_completion_sync,
  _emit_failure_sync,
  _emit_progress_sync,
)


class TestSyncSSEHelpers:
  """Test sync SSE emission helpers."""

  def test_emit_progress_sync(self):
    """Test progress emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_progress_sync("op123", "Creating graph...", 50.0)

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert call_args.args[2]["message"] == "Creating graph..."
      assert call_args.args[2]["progress_percent"] == 50.0

  def test_emit_failure_sync(self):
    """Test failure emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_failure_sync("op123", "Timeout exceeded")

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert "Timeout exceeded" in call_args.args[2]["error"]

  def test_emit_completion_sync(self):
    """Test completion emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_completion_sync("op123", {"graph_id": "kg123"})

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert call_args.args[2]["result"]["graph_id"] == "kg123"

  def test_emit_progress_handles_errors(self):
    """Test that SSE emission errors are swallowed."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      MockStorage.return_value.store_event_sync.side_effect = Exception("Redis down")

      # Should not raise
      _emit_progress_sync("op123", "test", 50)


class TestRestoreBackupOpFailsClosed:
  """A restore overwrites a database irreversibly, so every failure on the
  path must stop the job rather than return a "completed" envelope over it.

  `restore_with_sse` reports failure by returning `{"status": "failed"}`, and
  the safety snapshot is the only rollback the operation has.
  """

  @staticmethod
  def _run(restore_status="completed", backup_raises=False, create_system_backup=True):
    from dagster import build_op_context

    from robosystems.dagster.jobs.graph import RestoreGraphConfig, restore_backup

    backup_record = MagicMock()
    backup_record.graph_id = "kg123"
    backup_record.s3_bucket = "bucket"
    backup_record.s3_key = "key"
    backup_record.compression_enabled = True

    session = MagicMock()
    db = MagicMock()
    db.get_session.return_value.__enter__ = lambda *_: session
    db.get_session.return_value.__exit__ = lambda *_: False

    client = MagicMock()

    async def _restore(**_kwargs):
      return {"status": restore_status, "error": "boom"}

    async def _close():
      return None

    client.restore_with_sse = _restore
    client.close = _close

    async def _create_client(*_a, **_k):
      return client

    manager = MagicMock()

    async def _create_backup(_job):
      if backup_raises:
        raise RuntimeError("s3 unavailable")
      return MagicMock(s3_key="safety.zip")

    manager.create_backup = _create_backup

    with (
      patch(
        "robosystems.models.core.GraphBackup.get_by_id", return_value=backup_record
      ),
      patch(
        "robosystems.graph_api.client.factory.GraphClientFactory.create_client",
        _create_client,
      ),
      patch(
        "robosystems.operations.graph.engine.backup_manager.create_backup_manager",
        return_value=manager,
      ),
    ):
      return restore_backup(
        build_op_context(),
        db,
        MagicMock(),
        RestoreGraphConfig(
          graph_id="kg123",
          backup_id="backup1",
          create_system_backup=create_system_backup,
        ),
      )

  @pytest.mark.unit
  def test_completed_restore_returns_completed(self):
    result = self._run()

    assert result["status"] == "completed"
    assert result["verification_status"] == "verified"

  @pytest.mark.unit
  def test_failed_restore_raises(self):
    """The graph has already been overwritten by this point — reporting
    success would hide that from the operator and from the SSE envelope."""
    from dagster import Failure

    with pytest.raises(Failure, match="did not complete"):
      self._run(restore_status="failed")

  @pytest.mark.unit
  def test_safety_backup_failure_aborts_before_restore(self):
    """`create_system_backup=False` is passed downstream on the strength of
    this snapshot, so continuing without it leaves no rollback anywhere."""
    from dagster import Failure

    with pytest.raises(Failure, match="Safety backup failed"):
      self._run(backup_raises=True)
