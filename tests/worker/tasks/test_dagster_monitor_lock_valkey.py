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
    task_id=f"op_test{uuid.uuid4().hex[:10]}",
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


@pytest.fixture
def run_record():
  client = create_redis_client(ValkeyDatabase.WORKER_QUEUE)
  keys: list[str] = []

  def put(task, value):
    key = f"worker:dagster_run:{task.task_id}"
    keys.append(key)
    client.set(key, value, ex=300)

  yield put
  if keys:
    client.delete(*keys)
  client.close()


@pytest.mark.asyncio
async def test_a_requeued_monitor_reattaches_instead_of_resubmitting(
  held_lock, run_record
):
  client, lock_key, lock_id = held_lock
  task = _task(lock_key, lock_id)
  run_record(task, "run_first")
  monitor = _monitor([{"status": "running"}, {"status": "completed"}])
  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=monitor,
  ):
    await task.execute()

  monitor.submit_job.assert_not_called()
  assert [c.args[0] for c in monitor.get_run_status.call_args_list] == [
    "run_first",
    "run_first",
  ]


@pytest.mark.asyncio
async def test_a_monitor_that_died_mid_submit_never_submits_again(
  held_lock, run_record
):
  client, lock_key, lock_id = held_lock
  task = _task(lock_key, lock_id)
  run_record(task, DagsterJobMonitorTask.SUBMITTING)
  monitor = _monitor([{"status": "completed"}])
  with (
    patch(
      "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
      return_value=monitor,
    ),
    pytest.raises(RuntimeError),
  ):
    await task.execute()

  monitor.submit_job.assert_not_called()
  assert client.get(f"lock:{lock_key}") == lock_id


@pytest.mark.asyncio
async def test_a_long_run_keeps_its_lock_fresh(held_lock, run_record):
  client, lock_key, lock_id = held_lock
  client.expire(f"lock:{lock_key}", 5)
  task = _task(lock_key, lock_id)
  run_record(task, "run_long")
  task.LOCK_EXTEND_INTERVAL_SECONDS = 0
  ttls: list[int] = []

  def status(_run_id):
    ttls.append(client.ttl(f"lock:{lock_key}"))
    return {"status": "running" if len(ttls) < 3 else "completed"}

  with patch(
    "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
    return_value=_monitor(status),
  ):
    await task.execute()

  assert ttls[0] <= 5
  assert ttls[-1] > 5
