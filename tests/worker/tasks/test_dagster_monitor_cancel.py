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
