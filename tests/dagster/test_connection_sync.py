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
  describe_failure,
  due_connections,
  recently_attempted,
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

  def test_job_retries_a_dead_worker_only(self):
    tags = scheduled_connection_sync_job.tags
    assert tags.get("dagster/max_retries") == "3"
    assert tags.get("dagster/priority") == "2"
    # A sweep that failed on purpose (nothing dispatched) is not re-run.
    assert tags.get("dagster/retry_on_asset_or_op_failure") == "false"

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
    failed_at = fields.pop("failed_at", None)
    conn = Connection.create(
      graph_id,
      user_id,
      provider,
      db,
      status=status,
      auto_sync_enabled=fields.pop("auto_sync_enabled", True),
    )
    conn.last_sync = last_sync
    if failed_at is not None:
      conn.last_sync_result = {
        "status": "failed",
        "synced_at": failed_at.isoformat(),
        **({"stage": fields.pop("stage")} if "stage" in fields else {}),
      }
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
      # Failing every attempt: tried once per cadence, not every tick.
      just_failed = self._connection(
        test_db,
        g,
        u,
        last_sync=NOW - timedelta(days=3),
        failed_at=NOW - timedelta(hours=2),
      )
      failed_a_while_ago = self._connection(
        test_db,
        g,
        u,
        last_sync=NOW - timedelta(days=3),
        failed_at=NOW - timedelta(hours=25),
      )
      # A dispatch that failed started nothing: tried again within the hour.
      dispatch_failed = self._connection(
        test_db,
        g,
        u,
        last_sync=NOW - timedelta(days=4),
        failed_at=NOW - timedelta(hours=2),
        stage="dispatch",
      )
      dispatch_just_failed = self._connection(
        test_db,
        g,
        u,
        last_sync=NOW - timedelta(days=4),
        failed_at=NOW - timedelta(minutes=10),
        stage="dispatch",
      )
      # Push-only: nothing to pull, never due.
      pushed = self._connection(
        test_db, g, u, provider="external", last_sync=NOW - timedelta(days=9)
      )
      made = [
        dispatch_failed,
        dispatch_just_failed,
        pushed,
        never,
        stale,
        failed,
        fresh,
        off,
        reauth,
        pending,
        gone,
        dark,
        just_failed,
        failed_a_while_ago,
      ]

      with patch.object(
        provider_registry, "is_enabled", side_effect=lambda p: p != "mercury"
      ):
        due = due_connections(test_db, now=NOW, interval=DAY)

      ids = [c.id for c in due if c.graph_id == g]
      # Never synced first, then the oldest sync.
      assert ids == [
        never.id,
        dispatch_failed.id,
        failed_a_while_ago.id,
        failed.id,
        stale.id,
      ]
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
class TestRecentlyAttempted:
  """The run store is the record of attempts: a run tagged for the
  connection, in any state, started since the cutoff."""

  def test_a_run_started_since_the_cutoff_is_an_attempt(self):
    from dagster import DagsterInstance

    instance = DagsterInstance.ephemeral()
    instance.create_run_for_job(
      job_def=scheduled_connection_sync_job, tags={"connection_id": "conn_x"}
    )
    started = datetime.now(UTC)
    assert recently_attempted(
      instance, ["conn_x", "conn_y"], cutoff=started - timedelta(hours=1)
    ) == {"conn_x"}
    # Older than the cadence: tried again.
    assert (
      recently_attempted(instance, ["conn_x"], cutoff=started + timedelta(seconds=1))
      == set()
    )


def _counts(**overrides):
  base = {
    "due": 0,
    "attempted": 0,
    "dispatched": 0,
    "in_progress": 0,
    "no_op": 0,
    "failed": 0,
  }
  return {**base, **overrides}


@pytest.mark.unit
class TestSweep:
  def _run(self, due, dispatch, *, flag="true", attempted=None, session=None):
    with (
      patch(f"{MODULE}.due_connections", return_value=due),
      patch(f"{MODULE}.recently_attempted", return_value=attempted or set()),
      patch(
        "robosystems.config.parameter_store.get_parameter_value",
        return_value=flag,
      ),
      patch(f"{SERVICE}.dispatch_connection_sync", new=dispatch),
      patch(f"{MODULE}.env.CONNECTION_SYNC_INTERVAL_HOURS", 24),
    ):
      return sweep_connection_syncs(
        build_op_context(), _db_with(session or MagicMock())
      )

  def test_dispatches_each_due_connection_as_the_platform_unattended(self):
    dispatch = AsyncMock(
      return_value={"dispatched": True, "task_id": "run_1", "message": None}
    )
    counts = self._run(_due("conn_a", "conn_b"), dispatch)
    assert counts == _counts(due=2, dispatched=2)
    dispatch.assert_any_await(
      graph_id="kg_conn_a",
      connection_id="conn_a",
      user_id="system",
      sync_options={"unattended": True},
    )

  def test_a_connection_attempted_within_the_cadence_is_left_alone(self):
    dispatch = AsyncMock(
      return_value={"dispatched": True, "task_id": "run_1", "message": None}
    )
    counts = self._run(_due("conn_a", "conn_b"), dispatch, attempted={"conn_a"})
    assert counts == _counts(due=1, attempted=1, dispatched=1)
    assert dispatch.await_count == 1
    assert dispatch.await_args.kwargs["connection_id"] == "conn_b"

  def test_a_sync_already_running_is_the_expected_collision(self):
    from robosystems.operations.connection_service import SyncInProgressError

    dispatch = AsyncMock(side_effect=SyncInProgressError("conn_a", "holder", 900))
    counts = self._run(_due("conn_a"), dispatch)
    assert counts == _counts(due=1, in_progress=1)

  def test_one_failure_does_not_stop_the_sweep(self):
    dispatch = AsyncMock(
      side_effect=[
        RuntimeError("provider down"),
        {"dispatched": True, "task_id": "run_2", "message": None},
      ]
    )
    session = MagicMock()
    counts = self._run(_due("conn_a", "conn_b"), dispatch, session=session)
    assert counts == _counts(due=2, dispatched=1, failed=1)
    # The failed dispatch is recorded on its connection, as the clock.
    recorded = session.get.return_value.record_sync_result.call_args.args[1]
    assert recorded["status"] == "failed" and recorded["stage"] == "dispatch"
    assert recorded["error"] == {
      "code": "RuntimeError",
      "message": "RuntimeError: provider down",
    }

  def test_a_sweep_that_dispatches_nothing_fails_the_run(self):
    from dagster import Failure

    dispatch = AsyncMock(side_effect=RuntimeError("webserver unreachable"))
    session = MagicMock()
    with pytest.raises(Failure) as raised:
      self._run(_due("conn_a", "conn_b"), dispatch, session=session)
    assert "2 due, 0 dispatched" in str(raised.value.description)
    assert session.get.return_value.record_sync_result.call_count == 2

  def test_a_no_op_dispatch_is_not_a_run(self):
    dispatch = AsyncMock(
      return_value={"dispatched": False, "task_id": None, "message": "nothing"}
    )
    counts = self._run(_due("conn_a"), dispatch)
    assert counts == _counts(due=1, no_op=1)

  def test_a_no_op_cannot_mask_a_sweep_that_dispatched_nothing(self):
    from dagster import Failure

    dispatch = AsyncMock(
      side_effect=[
        {"dispatched": False, "task_id": None, "message": "nothing"},
        RuntimeError("webserver unreachable"),
      ]
    )
    with pytest.raises(Failure):
      self._run(_due("conn_a", "conn_b"), dispatch)

  def test_the_kill_switch_skips_the_sweep(self):
    dispatch = AsyncMock()
    result = self._run(_due("conn_a"), dispatch, flag="false")
    assert result["skipped"] is True
    dispatch.assert_not_awaited()


@pytest.mark.unit
class TestDescribeFailure:
  def test_names_the_cause_behind_a_wrapped_client_error(self):
    try:
      try:
        raise ConnectionError("Name or service not known: dagster-webserver")
      except ConnectionError as inner:
        raise RuntimeError("Exception occured during execution of query ...") from inner
    except RuntimeError as exc:
      text = describe_failure(exc)
    assert text.startswith("RuntimeError: Exception occured")
    assert text.endswith(
      "ConnectionError: Name or service not known: dagster-webserver"
    )

  def test_a_bare_exception_is_just_itself(self):
    assert describe_failure(ValueError("x")) == "ValueError: x"
