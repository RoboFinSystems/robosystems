"""A monitored materialize run keeps its lock until Dagster reports it stopped.

Against real Valkey: the lock the API takes, the worker's compare-and-delete
release, and a Dagster client that stops answering mid-run.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
from robosystems.worker.tasks.dagster_monitoring import DagsterJobMonitorTask

pytestmark = pytest.mark.integration


@pytest.fixture
def held_lock():
  client = create_redis_client(ValkeyDatabase.LOCKS)
  lock_key = f"graph_materialize:kgtest{uuid.uuid4().hex[:10]}"
  lock_id = uuid.uuid4().hex
  client.set(f"lock:{lock_key}", lock_id, ex=300)
  yield client, lock_key, lock_id
  client.delete(f"lock:{lock_key}")
  client.close()


def _task(lock_key, lock_id):
  task = DagsterJobMonitorTask(
    task_id="op_test",
    graph_id="kg0000000000000001",
    user_id="usr_test",
    params={
      "job_name": "materialize_graph_job",
      "lock_key": lock_key,
      "lock_id": lock_id,
    },
    manager=MagicMock(),
  )
  task.is_cancelled = AsyncMock(return_value=False)
  task.report_progress = AsyncMock()
  return task


def _monitor(side_effect):
  monitor = MagicMock()
  monitor.poll_interval = 0.01
  monitor.submit_job.return_value = "run_1"
  monitor.get_run_status.side_effect = side_effect
  monitor.emit_completion = AsyncMock()
  monitor.emit_error = AsyncMock()
  return monitor


@pytest.mark.asyncio
async def test_a_run_that_stops_answering_keeps_its_lock(held_lock):
  client, lock_key, lock_id = held_lock
  task = _task(lock_key, lock_id)
  task.STATUS_READ_GRACE_SECONDS = 0.05

  def status(_run_id):
    status.calls += 1
    if status.calls == 1:
      return {"status": "running"}
    raise ConnectionError("webserver restarting")

  status.calls = 0
  with (
    patch(
      "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
      return_value=_monitor(status),
    ),
    pytest.raises(ConnectionError),
  ):
    await task.execute()

  assert client.get(f"lock:{lock_key}") == lock_id


@pytest.mark.asyncio
async def test_a_blip_then_completion_releases_the_lock(held_lock):
  client, lock_key, lock_id = held_lock
  task = _task(lock_key, lock_id)
  monitor = _monitor(
    [
      {"status": "running"},
      ConnectionError("webserver restarting"),
      {"status": "completed"},
    ]
  )
  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=monitor,
  ):
    result = await task.execute()

  assert result["status"] == "completed"
  assert client.get(f"lock:{lock_key}") is None
