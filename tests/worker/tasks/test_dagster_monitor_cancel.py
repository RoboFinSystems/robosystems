"""Cancelling a monitored Dagster run releases its lock only once the run has
actually stopped; a live writer must never lose its lock."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.worker.tasks.dagster_monitoring import DagsterJobMonitorTask


def _task():
  task = DagsterJobMonitorTask(
    task_id="op_test",
    graph_id="kg0000000000000001",
    user_id="usr_test",
    params={"job_name": "materialize", "lock_key": "lock:materialize:kg1"},
    manager=MagicMock(),
  )
  task.is_cancelled = AsyncMock(return_value=True)
  task.report_progress = AsyncMock()
  task.release_lock = MagicMock()
  return task


def _monitor(statuses):
  monitor = MagicMock()
  monitor.poll_interval = 0.01
  monitor.submit_job.return_value = "run_1"
  monitor.get_run_status.side_effect = [{"status": s} for s in statuses]
  return monitor


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_cancelled_run_that_stops_releases_its_lock():
  task = _task()
  monitor = _monitor(["running", "cancelled"])
  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=monitor,
  ):
    result = await task.execute()

  assert result["status"] == "cancelled"
  monitor.terminate_run.assert_called_once_with("run_1")
  task.release_lock.assert_called_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_run_still_writing_keeps_its_lock():
  task = _task()
  task.CANCEL_SETTLE_SECONDS = 0.05
  monitor = _monitor(["running"] * 50)
  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=monitor,
  ):
    await task.execute()

  monitor.terminate_run.assert_called_once_with("run_1")
  task.release_lock.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_run_whose_state_cannot_be_read_keeps_its_lock():
  task = _task()
  task.CANCEL_SETTLE_SECONDS = 0.05
  monitor = _monitor([])
  monitor.get_run_status.side_effect = ConnectionError("webserver unreachable")
  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=monitor,
  ):
    result = await task.execute()

  assert result["status"] == "cancelled"
  task.release_lock.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_status_read_that_recovers_still_releases_the_lock():
  task = _task()
  monitor = _monitor([])
  monitor.get_run_status.side_effect = [
    ConnectionError("blip"),
    {"status": "cancelled"},
  ]
  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=monitor,
  ):
    await task.execute()

  task.release_lock.assert_called_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_the_operation_id_reaches_a_job_that_asks_for_it():
  """The materialize job reports its result (tables, rows) to the operation;
  without its id the SDK saw an empty result for every Dagster-path run."""
  task = DagsterJobMonitorTask(
    task_id="op_test",
    graph_id="kg0000000000000001",
    user_id="usr_test",
    params={
      "job_name": "materialize_graph_job",
      "run_config": {
        "ops": {"materialize_graph_tables": {"config": {"graph_id": "g"}}}
      },
      "pass_operation_id": True,
    },
    manager=MagicMock(),
  )
  task.is_cancelled = AsyncMock(return_value=False)
  task.report_progress = AsyncMock()
  task.release_lock = MagicMock()
  monitor = _monitor(["completed"])
  monitor.emit_completion = AsyncMock()
  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=monitor,
  ):
    await task.execute()

  run_config = monitor.submit_job.call_args.args[1]
  assert (
    run_config["ops"]["materialize_graph_tables"]["config"]["operation_id"] == "op_test"
  )
