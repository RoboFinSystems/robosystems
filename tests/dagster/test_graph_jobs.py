"""Tests for Dagster graph operation jobs.

Note: TestWaitAndCreateGraphConfig and TestWaitAndCreate removed —
wait-for-capacity graph creation replaced by worker task + sensor retry.
"""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import Failure, build_op_context

from robosystems.dagster.jobs.graph import (
  StageFileConfig,
  _emit_completion_sync,
  _emit_failure_sync,
  _emit_progress_sync,
  stage_file_in_duckdb,
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


class TestDeprovisionSuspendedGraphsSessionIsolation:
  """One graph's DBAPI failure must not poison the shared session for the
  graphs after it. The op rolls the session back per-failure so each graph is
  independent; the failed one is left stranded for the sensor to retry."""

  @pytest.mark.unit
  def test_a_failed_graph_rolls_back_and_the_next_graph_still_runs(self):
    from dagster import build_op_context

    from robosystems.dagster.jobs.graph_lifecycle import (
      DeprovisionGraphsConfig,
      deprovision_suspended_graphs,
    )

    session = MagicMock()
    db = MagicMock()
    db.get_session.return_value.__enter__ = lambda *_: session
    db.get_session.return_value.__exit__ = lambda *_: False

    ok = MagicMock()
    ok.status = "success"
    ok.errors = []

    async def _deprovision(graph_id, _sess, create_backup=True):
      if graph_id == "kg_poison":
        raise RuntimeError("PG delete blew up mid-transaction")
      return ok

    with patch(
      "robosystems.operations.graph.deprovision_service.GraphDeprovisionService"
    ) as svc_cls:
      svc_cls.return_value.deprovision_graph = _deprovision

      result = deprovision_suspended_graphs(
        build_op_context(),
        db,
        DeprovisionGraphsConfig(graph_ids=["kg_poison", "kg_ok"]),
      )

    # The poison graph failed → the session was reset before the next graph,
    # and the next graph was still deprovisioned.
    session.rollback.assert_called_once()
    assert result["deprovisioned_count"] == 1
    assert any("kg_poison" in e for e in result["errors"])


# stage_file_in_duckdb must not mark a file staged when staging failed.
def _run_stage_op(graph_file, staging_result):
  client = MagicMock(create_table=AsyncMock(return_value=staging_result))

  @contextmanager
  def _session_cm():
    yield MagicMock()

  db = MagicMock()
  db.get_session = _session_cm

  with (
    patch("robosystems.models.core.GraphFile") as file_cls,
    patch("robosystems.models.core.GraphTable") as table_cls,
    patch(
      "robosystems.graph_api.client.factory.GraphClientFactory.create_client",
      AsyncMock(return_value=client),
    ),
  ):
    file_cls.get_by_id.return_value = graph_file
    file_cls.get_all_for_table.return_value = [graph_file]
    table_cls.get_by_id.return_value = MagicMock(table_name="Entity")
    config = StageFileConfig(file_id="gf_1", graph_id="kg1", table_id="gt_1")
    return stage_file_in_duckdb(build_op_context(), db, MagicMock(), config)


def _uploaded_file():
  return MagicMock(id="gf_1", s3_key="k/file.parquet", upload_status="uploaded")


@pytest.mark.unit
def test_failed_staging_fails_the_op():
  graph_file = _uploaded_file()
  with pytest.raises(Failure, match="S3 read failed"):
    _run_stage_op(graph_file, {"status": "failed", "error": "S3 read failed"})
  graph_file.mark_duckdb_staged.assert_not_called()


@pytest.mark.unit
def test_completed_staging_marks_file_staged():
  graph_file = _uploaded_file()
  result = _run_stage_op(graph_file, {"status": "completed"})
  assert result["duckdb_status"] == "staged"
  graph_file.mark_duckdb_staged.assert_called_once()
