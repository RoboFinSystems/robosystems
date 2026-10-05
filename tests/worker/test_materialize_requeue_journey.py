"""Journey: a materialize whose worker dies mid-run is requeued and finishes once.

Submit (per-graph lock + enqueue) -> a real worker process picks the task up
and is SIGKILLed mid-copy -> the reaper leaves it alone while the heartbeat is
live, requeues it once the heartbeat lapses -> a fresh worker process finishes
it. Real Valkey throughout (queue and SSE on spare databases so the local
worker never consumes the test task, the lock on the real locks database);
only the graph write itself is faked. Pins the three second-order bugs this
lifecycle has produced: a lock released under a live writer, a requeued task
run twice, and a lock never released after a crash.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  ValkeyURLBuilder,
  create_redis_client,
)

pytestmark = pytest.mark.integration

_SCRATCH = {ValkeyDatabase.WORKER_QUEUE: 9, ValkeyDatabase.SSE: 10}
_REPO_ROOT = Path(__file__).resolve().parents[2]
_original_url = ValkeyURLBuilder.build_authenticated_url


def _scratch_url(
  database: ValkeyDatabase = ValkeyDatabase.AUTH,
  base_url: str | None = None,
  include_ssl_params: bool = True,
) -> str:
  url = _original_url(database, base_url, include_ssl_params)
  scratch = _SCRATCH.get(database)
  if scratch is None:
    return url
  head, _, tail = url.rpartition("/")
  query = tail.partition("?")[2]
  return f"{head}/{scratch}" + (f"?{query}" if query else "")


def _run_worker(mode: str, events_key: str) -> None:
  """Child-process entry: the real consumer loop with only the graph write
  faked. ``mode="hang"`` blocks mid-copy until the process is killed."""
  patches = pytest.MonkeyPatch()
  patches.setattr(
    ValkeyURLBuilder, "build_authenticated_url", staticmethod(_scratch_url)
  )

  import robosystems.worker  # noqa: F401  (registers the task handlers)
  from robosystems.graph_api.core.ladybug import materialization_lock
  from robosystems.operations.extensions.materialize import (
    ExtensionsMaterializer,
    MaterializeResult,
  )
  from robosystems.worker import consumer

  events = create_redis_client(ValkeyDatabase.WORKER_QUEUE)
  locks = create_redis_client(ValkeyDatabase.LOCKS)
  pid = os.getpid()

  async def fake_materialize(
    self: Any,
    graph_id: str,
    entity_id: str | None = None,
    rebuild: bool = True,
    lock_token: str | None = None,
  ) -> MaterializeResult:
    # The real adoption: the token must still be the holder's, and taking it
    # over renews the TTL for this attempt.
    assert lock_token is not None
    await self._adopt_lock(graph_id, lock_token)
    ttl = locks.ttl(materialization_lock.lock_key_for(graph_id))
    events.rpush(events_key, f"start:{pid}:{ttl}")
    if mode == "hang":
      await asyncio.sleep(3600)
    events.rpush(events_key, f"end:{pid}")
    return MaterializeResult(graph_id=graph_id, tables_materialized=["Entity"])

  real_release = materialization_lock.release_token

  def recording_release(client: Any, graph_id: str, token: str) -> bool:
    released = real_release(client, graph_id, token)
    events.rpush(events_key, f"release:{pid}:{released}")
    return released

  class _NoPublisher:
    def start(self) -> None: ...
    def stop(self) -> None: ...

  patches.setattr(ExtensionsMaterializer, "materialize", fake_materialize)
  patches.setattr(materialization_lock, "release_token", recording_release)
  patches.setattr(consumer, "QueueDepthPublisher", _NoPublisher)
  # A short heartbeat so the death is observable in seconds, not 90.
  patches.setattr(consumer, "WORKER_HEARTBEAT_INTERVAL", 1)
  patches.setattr(consumer, "WORKER_HEARTBEAT_TTL", 2)

  asyncio.run(consumer.run())


def _spawn_worker(mode: str, events_key: str, log: Path) -> subprocess.Popen[bytes]:
  code = (
    "from tests.worker.test_materialize_requeue_journey import _run_worker; "
    f"_run_worker({mode!r}, {events_key!r})"
  )
  # The inherited database URLs are already this xdist worker's own; importing
  # `tests` with the worker id still set would suffix them a second time.
  env = {k: v for k, v in os.environ.items() if k != "PYTEST_XDIST_WORKER"}
  with log.open("wb") as out:
    return subprocess.Popen(
      [sys.executable, "-c", code], cwd=_REPO_ROOT, env=env, stdout=out, stderr=out
    )


def _task_ids(queue: Any, key: str) -> set[str]:
  return {json.loads(entry)["task_id"] for entry in queue.lrange(key, 0, -1)}


def _wait_for(predicate: Any, what: str, log: Path, timeout: float = 90) -> None:
  deadline = time.monotonic() + timeout
  while time.monotonic() < deadline:
    if predicate():
      return
    time.sleep(0.2)
  pytest.fail(f"Timed out waiting for {what}; worker log:\n{log.read_text()[-4000:]}")


def _reap(monkeypatch: pytest.MonkeyPatch, skew: float) -> None:
  """One sensor tick, with the reaper's clock ``skew`` seconds ahead."""
  from dagster import build_sensor_context

  from robosystems.dagster.sensors import worker_reaper

  real_time = time.time
  monkeypatch.setattr(
    worker_reaper, "time", SimpleNamespace(time=lambda: real_time() + skew)
  )
  worker_reaper.worker_inflight_reaper_sensor(build_sensor_context())


@pytest.fixture
def scratch(monkeypatch: pytest.MonkeyPatch):
  from robosystems.middleware.sse import event_storage, operation_manager

  monkeypatch.setattr(
    ValkeyURLBuilder, "build_authenticated_url", staticmethod(_scratch_url)
  )
  # Both singletons cache a client bound to this test's loop and scratch
  # database; setting them here restores the previous ones at teardown.
  monkeypatch.setattr(operation_manager, "_operation_manager", None)
  monkeypatch.setattr(event_storage, "_event_storage", None)
  queue = create_redis_client(ValkeyDatabase.WORKER_QUEUE)
  sse = create_redis_client(ValkeyDatabase.SSE)
  locks = create_redis_client(ValkeyDatabase.LOCKS)
  children: list[subprocess.Popen[bytes]] = []
  task_ids: list[str] = []
  graph_id = f"kg{uuid.uuid4().hex[:18]}"
  events_key = f"journey:materialize:{graph_id}"
  yield SimpleNamespace(
    queue=queue,
    sse=sse,
    locks=locks,
    children=children,
    graph_id=graph_id,
    events_key=events_key,
    task_ids=task_ids,
  )
  for child in children:
    if child.poll() is None:
      child.kill()
      child.wait()
  locks.delete(f"materialize_lock:{graph_id}")
  queue.delete(events_key, "worker:tasks", "worker:dlq")
  for key in [
    *queue.scan_iter(match="worker:inflight:worker-*"),
    *queue.scan_iter(match=f"worker:dedup:*:{graph_id}:*"),
  ]:
    if key.startswith("worker:dedup:") or set(task_ids) & _task_ids(queue, key):
      queue.delete(key)
  for task_id in task_ids:
    sse.delete(
      *(f"sse:operation:{kind}:{task_id}" for kind in ("meta", "events", "seq"))
    )
  for client in (queue, sse, locks):
    client.close()


@pytest.mark.timeout(240)
async def test_a_killed_materialize_is_requeued_and_finishes_exactly_once(
  scratch: SimpleNamespace,
  monkeypatch: pytest.MonkeyPatch,
  tmp_path: Path,
  test_db: Any,  # the worker processes read platform tables from this database
) -> None:
  from robosystems.dagster.sensors.worker_reaper import (
    SSE_META_PREFIX,
    STALE_GRACE_SECONDS,
  )
  from robosystems.operations.graph.commands.materialize import (
    acquire_materialize_lock,
  )
  from robosystems.worker.client import enqueue_task
  from robosystems.worker.constants import TASK_TIMEOUTS, worker_heartbeat_key

  queue, sse, locks = scratch.queue, scratch.sse, scratch.locks
  graph_id = scratch.graph_id
  lock_key = f"materialize_lock:{graph_id}"

  # Submit: the same lock + payload materialize_cmd produces for an entity graph.
  lock = await acquire_materialize_lock(graph_id)
  response = await enqueue_task(
    task_type="extensions_materialize",
    graph_id=graph_id,
    user_id="usr_journey",
    params={"rebuild": True, "materialization_lock_token": lock.token},
  )
  task_id = response["operation_id"]
  scratch.task_ids.append(task_id)

  def status() -> str | None:
    meta = sse.get(f"{SSE_META_PREFIX}{task_id}")
    return json.loads(meta)["status"] if meta else None

  def events() -> list[str]:
    return queue.lrange(scratch.events_key, 0, -1)

  # Attempt 1 starts writing, then the worker dies without running any finally.
  log_a = tmp_path / "worker_a.log"
  worker_a = _spawn_worker("hang", scratch.events_key, log_a)
  scratch.children.append(worker_a)
  _wait_for(
    lambda: [e.rsplit(":", 1)[0] for e in events()] == [f"start:{worker_a.pid}"],
    "attempt 1 to start",
    log_a,
  )
  [inflight_key] = [
    key
    for key in queue.scan_iter(match="worker:inflight:*")
    if task_id in _task_ids(queue, key)
  ]
  worker_id = inflight_key.removeprefix("worker:inflight:")
  assert status() == "running"

  worker_a.send_signal(signal.SIGKILL)
  worker_a.wait()
  assert locks.get(lock_key) == lock.token, "a crash must not free the lock"
  assert queue.llen(inflight_key) == 1

  # The reaper waits out the full budget, measured from the enqueue.
  skew = TASK_TIMEOUTS["extensions_materialize"] + STALE_GRACE_SECONDS + 1

  # Right after the kill the heartbeat is still live: hands off.
  assert queue.exists(worker_heartbeat_key(worker_id))
  _reap(monkeypatch, skew)
  assert queue.llen("worker:tasks") == 0, "requeued while the heartbeat was live"
  assert queue.llen(inflight_key) == 1

  _wait_for(
    lambda: not queue.exists(worker_heartbeat_key(worker_id)),
    "the dead worker's heartbeat to lapse",
    log_a,
    timeout=10,
  )
  # The same wall-clock time has passed for the lock the task still runs under.
  locks.pexpire(lock_key, locks.pttl(lock_key) - skew * 1000)
  _reap(monkeypatch, skew)

  requeued = [json.loads(t) for t in queue.lrange("worker:tasks", 0, -1)]
  assert [(t["task_id"], t["attempt"]) for t in requeued] == [(task_id, 2)]
  assert not queue.exists(inflight_key)

  # Attempt 2 on a fresh worker process runs to completion.
  log_b = tmp_path / "worker_b.log"
  worker_b = _spawn_worker("finish", scratch.events_key, log_b)
  scratch.children.append(worker_b)
  _wait_for(lambda: status() == "completed", "attempt 2 to complete", log_b)
  _wait_for(
    lambda: f"release:{worker_b.pid}:True" in events(), "the lock release", log_b
  )
  worker_b.send_signal(signal.SIGTERM)
  assert worker_b.wait(timeout=30) == 0

  # One body started per attempt, one ran to its end, and the single release
  # came from that attempt after its write: never from the dead one, never twice.
  started_a, started_b, *rest = events()
  assert started_a.startswith(f"start:{worker_a.pid}:")
  assert started_b.startswith(f"start:{worker_b.pid}:")
  assert rest == [f"end:{worker_b.pid}", f"release:{worker_b.pid}:True"]
  # The requeued attempt ran under a renewed lock, not the remainder the
  # first attempt left: its budget fits inside the lock it holds.
  assert int(started_b.rsplit(":", 1)[1]) > TASK_TIMEOUTS["extensions_materialize"]
  assert locks.get(lock_key) is None

  completed = [
    raw
    for raw in sse.zrange(f"sse:operation:events:{task_id}", 0, -1)
    if json.loads(raw)["event_type"] == "operation_completed"
  ]
  assert len(completed) == 1

  # Nothing left for a later tick to pick up and run a third time.
  _reap(monkeypatch, skew)
  assert queue.llen("worker:tasks") == 0
  assert queue.llen("worker:dlq") == 0
  assert not any(
    task_id in _task_ids(queue, key)
    for key in queue.scan_iter(match="worker:inflight:*")
  )


@pytest.mark.unit
async def test_a_requeued_attempt_whose_lock_lapsed_refuses_to_run() -> None:
  """An attempt that reaches the lock after its TTL ran out (or after another
  run took it) must not write unlocked: adoption fails and the run errors."""
  from unittest.mock import AsyncMock, patch

  from robosystems.operations.extensions.materialize import (
    ExtensionsMaterializer,
    MaterializationLockError,
  )

  redis = AsyncMock()
  redis.eval = AsyncMock(return_value=0)
  with (
    patch(
      "robosystems.config.valkey_registry.create_async_redis_client",
      return_value=redis,
    ),
    pytest.raises(MaterializationLockError, match="no longer held"),
  ):
    await ExtensionsMaterializer()._adopt_lock("kg1", "tok-from-submit")
