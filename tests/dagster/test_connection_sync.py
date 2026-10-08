"""The scheduled connection sync: which connections a tick picks up, how the
sweep treats each dispatch, and the job's shape and registration."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import JobDefinition, build_op_context

from robosystems.dagster.jobs.connection_sync import (
  SYNCABLE_STATUSES,
  due_connections,
  scheduled_connection_sync_job,
  scheduled_connection_sync_schedule,
  sweep_connection_syncs,
)
from robosystems.models.core.connection.connection import Connection

MODULE = "robosystems.dagster.jobs.connection_sync"
SERVICE = "robosystems.operations.connection_service"


@pytest.mark.unit
class TestDefinition:
  def test_job_has_one_op(self):
    assert isinstance(scheduled_connection_sync_job, JobDefinition)
    assert {n.name for n in scheduled_connection_sync_job.all_node_defs} == {
      "sweep_connection_syncs"
    }

  def test_job_retries_like_the_other_platform_sweeps(self):
    assert scheduled_connection_sync_job.tags.get("dagster/max_retries") == "3"
    assert scheduled_connection_sync_job.tags.get("dagster/priority") == "2"

  def test_ticks_hourly_at_a_quarter_past(self):
    assert scheduled_connection_sync_schedule.cron_schedule == "15 * * * *"
    assert (
      scheduled_connection_sync_schedule.job_name == scheduled_connection_sync_job.name
    )

  def test_registered_in_definitions(self):
    from robosystems.dagster.definitions import all_jobs, all_schedules

    assert scheduled_connection_sync_job in all_jobs
    assert scheduled_connection_sync_schedule in all_schedules

  def test_a_revoked_login_is_not_a_syncable_status(self):
    assert "needs_reauth" not in SYNCABLE_STATUSES
    assert "pending_oauth" not in SYNCABLE_STATUSES


NOW = datetime(2026, 10, 8, 3, 15, tzinfo=UTC)
DAY = timedelta(hours=24)


@pytest.mark.unit
class TestDueConnections:
  """Against the shared test database: the selection is the whole point."""

  def _connection(self, db, graph_id, user_id, provider="quickbooks", **fields):
    status = fields.pop("status", "connected")
    last_sync = fields.pop("last_sync", None)
    deleted = fields.pop("deleted", False)
    conn = Connection.create(
      graph_id,
      user_id,
      provider,
      db,
      status=status,
      auto_sync_enabled=fields.pop("auto_sync_enabled", True),
    )
    conn.last_sync = last_sync
    if deleted:
      conn.deleted_at = NOW
    db.commit()
    return conn

  def test_picks_the_stale_and_never_synced_and_leaves_the_rest(
    self, test_db, test_user, sample_graph
  ):
    from robosystems.operations.providers.registry import provider_registry

    g, u = sample_graph.graph_id, test_user.id
    made = []
    try:
      never = self._connection(test_db, g, u, last_sync=None)
      stale = self._connection(test_db, g, u, last_sync=NOW - timedelta(hours=30))
      failed = self._connection(
        test_db, g, u, status="error", last_sync=NOW - timedelta(hours=48)
      )
      fresh = self._connection(test_db, g, u, last_sync=NOW - timedelta(hours=2))
      off = self._connection(
        test_db, g, u, auto_sync_enabled=False, last_sync=NOW - timedelta(days=9)
      )
      reauth = self._connection(
        test_db, g, u, status="needs_reauth", last_sync=NOW - timedelta(days=9)
      )
      pending = self._connection(test_db, g, u, status="pending_oauth")
      gone = self._connection(
        test_db, g, u, deleted=True, last_sync=NOW - timedelta(days=9)
      )
      dark = self._connection(
        test_db, g, u, provider="mercury", last_sync=NOW - timedelta(days=9)
      )
      made = [never, stale, failed, fresh, off, reauth, pending, gone, dark]

      with patch.object(
        provider_registry, "is_enabled", side_effect=lambda p: p != "mercury"
      ):
        due = due_connections(test_db, now=NOW, interval=DAY)

      ids = [c.id for c in due if c.graph_id == g]
      # Never synced first, then the oldest sync.
      assert ids == [never.id, failed.id, stale.id]
    finally:
      for conn in made:
        test_db.delete(conn)
      test_db.commit()


def _db_with(session) -> MagicMock:
  @contextmanager
  def get_session():
    yield session

  db = MagicMock()
  db.get_session = get_session
  return db


def _due(*ids):
  return [SimpleNamespace(graph_id=f"kg_{i}", id=i, provider="quickbooks") for i in ids]


@pytest.mark.unit
class TestSweep:
  def _run(self, due, dispatch, *, flag="true"):
    with (
      patch(f"{MODULE}.due_connections", return_value=due),
      patch(
        "robosystems.config.parameter_store.get_parameter_value",
        return_value=flag,
      ),
      patch(f"{SERVICE}.dispatch_connection_sync", new=dispatch),
      patch(f"{MODULE}.env.CONNECTION_SYNC_INTERVAL_HOURS", 24),
    ):
      return sweep_connection_syncs(build_op_context(), _db_with(MagicMock()))

  def test_dispatches_each_due_connection_as_the_platform(self):
    dispatch = AsyncMock(
      return_value={"dispatched": True, "task_id": "run_1", "message": None}
    )
    counts = self._run(_due("conn_a", "conn_b"), dispatch)
    assert counts == {"due": 2, "dispatched": 2, "in_progress": 0, "failed": 0}
    dispatch.assert_any_await(
      graph_id="kg_conn_a", connection_id="conn_a", user_id="system"
    )

  def test_a_sync_already_running_is_the_expected_collision(self):
    from robosystems.operations.connection_service import SyncInProgressError

    dispatch = AsyncMock(side_effect=SyncInProgressError("conn_a", "holder", 900))
    counts = self._run(_due("conn_a"), dispatch)
    assert counts == {"due": 1, "dispatched": 0, "in_progress": 1, "failed": 0}

  def test_one_failure_does_not_stop_the_sweep(self):
    dispatch = AsyncMock(
      side_effect=[
        RuntimeError("provider down"),
        {"dispatched": True, "task_id": "run_2", "message": None},
      ]
    )
    counts = self._run(_due("conn_a", "conn_b"), dispatch)
    assert counts == {"due": 2, "dispatched": 1, "in_progress": 0, "failed": 1}

  def test_a_no_op_dispatch_is_not_a_run(self):
    dispatch = AsyncMock(
      return_value={"dispatched": False, "task_id": None, "message": "nothing"}
    )
    counts = self._run(_due("conn_a"), dispatch)
    assert counts["dispatched"] == 0

  def test_the_kill_switch_skips_the_sweep(self):
    dispatch = AsyncMock()
    result = self._run(_due("conn_a"), dispatch, flag="false")
    assert result["skipped"] is True
    dispatch.assert_not_awaited()
