"""The reaper takes inflight tasks only from workers that have stopped beating.

Against real Valkey, on spare databases so the local worker never consumes a
requeued test task. A task's age is measured from its enqueue, so a long queue
wait or a handler's grace past its budget made a live task look stale; a
requeue then ran it twice alongside itself.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
import redis
from dagster import build_sensor_context

from robosystems.config.valkey_registry import ValkeyDatabase, ValkeyURLBuilder
from robosystems.dagster.sensors.worker_reaper import (
  SSE_META_PREFIX,
  worker_inflight_reaper_sensor,
)
from robosystems.worker.constants import worker_heartbeat_key

pytestmark = pytest.mark.integration

_SCRATCH = {ValkeyDatabase.WORKER_QUEUE: 14, ValkeyDatabase.SSE: 15}


def _scratch_client(database, decode_responses=True, **kwargs):
  url = ValkeyURLBuilder.build_authenticated_url(database, include_ssl_params=False)
  url = f"{url.rsplit('/', 1)[0]}/{_SCRATCH[database]}"
  return redis.Redis.from_url(url, decode_responses=decode_responses, **kwargs)


@pytest.fixture
def valkey():
  queue = _scratch_client(ValkeyDatabase.WORKER_QUEUE)
  sse = _scratch_client(ValkeyDatabase.SSE)
  worker_id = f"worker-test-{uuid.uuid4().hex[:8]}"
  task_id = f"op_{uuid.uuid4().hex[:12]}"
  keys = (f"worker:inflight:{worker_id}", worker_heartbeat_key(worker_id))
  yield queue, sse, worker_id, task_id
  queue.delete(*keys, "worker:tasks", "worker:dlq")
  sse.delete(f"{SSE_META_PREFIX}{task_id}")
  queue.close()
  sse.close()


def _stale_task(queue, sse, worker_id, task_id):
  task = {
    "task_id": task_id,
    "task_type": "dagster_job_monitor",
    "graph_id": "kg0000000000000001",
    "user_id": "usr_test",
    "params": {},
    "attempt": 1,
  }
  queue.lpush(f"worker:inflight:{worker_id}", json.dumps(task))
  created = datetime.now(UTC) - timedelta(hours=3)
  sse.set(
    f"{SSE_META_PREFIX}{task_id}",
    json.dumps({"status": "running", "created_at": created.isoformat()}),
  )


def _reap():
  with patch("robosystems.config.valkey_registry.create_redis_client", _scratch_client):
    worker_inflight_reaper_sensor(build_sensor_context())


def test_a_live_workers_task_is_never_requeued(valkey):
  queue, sse, worker_id, task_id = valkey
  _stale_task(queue, sse, worker_id, task_id)
  queue.set(worker_heartbeat_key(worker_id), "1", ex=90)

  _reap()

  assert queue.llen("worker:tasks") == 0
  assert queue.llen(f"worker:inflight:{worker_id}") == 1


def test_a_dead_workers_stale_task_is_requeued(valkey):
  queue, sse, worker_id, task_id = valkey
  _stale_task(queue, sse, worker_id, task_id)

  _reap()

  requeued = [json.loads(t) for t in queue.lrange("worker:tasks", 0, -1)]
  assert [(t["task_id"], t["attempt"]) for t in requeued] == [(task_id, 2)]
  assert queue.llen(f"worker:inflight:{worker_id}") == 0


@pytest.mark.asyncio
async def test_the_consumer_keeps_its_heartbeat_alive(valkey):
  import asyncio

  import redis.asyncio as redis_async

  from robosystems.worker.consumer import _heartbeat

  queue, _, worker_id, _ = valkey
  url = queue.connection_pool.connection_kwargs
  client = redis_async.Redis(
    host=url["host"], port=url["port"], db=14, password=url.get("password")
  )
  beating = asyncio.create_task(_heartbeat(client, worker_id))
  try:
    await asyncio.sleep(0.2)
    assert 0 < queue.ttl(worker_heartbeat_key(worker_id)) <= 90
  finally:
    beating.cancel()
    await client.aclose()
