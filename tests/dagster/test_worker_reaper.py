"""The reaper's DLQ path writes the terminal status straight into the SSE
metadata key, bypassing the store's eviction hook — so it must evict the
idempotency envelope itself, and only when its own write is the one that
made the operation terminal."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import redis

from robosystems.dagster.sensors.worker_reaper import _fail_operation_sync


class _FakePipeline:
  """A WATCH/MULTI/EXEC pipeline over one in-memory key.

  ``interleave`` runs between the reaper's read and its EXEC, standing in for a
  worker that finishes the task in that window — the store then raises
  ``WatchError`` exactly as redis-py does when a watched key changed.
  """

  def __init__(self, store: dict, interleave=None):
    self.store = store
    self.interleave = interleave
    self.queued: list = []
    self.watching = False
    self.executed = False

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    return False

  def watch(self, key):
    self.watching = key

  def unwatch(self):
    self.watching = False

  def get(self, key):
    return self.store.get(key)

  def multi(self):
    self.queued = []

  def set(self, key, value, keepttl=False):
    self.queued.append((key, value))

  def execute(self):
    if self.interleave is not None:
      self.interleave(self.store)
      raise redis.WatchError("watched key changed")
    for key, value in self.queued:
      self.store[key] = value
    self.executed = True


def _client(store: dict, interleave=None) -> MagicMock:
  sse = MagicMock()
  pipeline = _FakePipeline(store, interleave)
  sse.pipeline.return_value = pipeline
  sse._pipeline = pipeline
  return sse


def _key(task_id: str) -> str:
  from robosystems.dagster.sensors.worker_reaper import SSE_META_PREFIX

  return f"{SSE_META_PREFIX}{task_id}"


def test_dlq_failure_evicts_the_idempotency_envelope():
  store = {_key("op_123"): json.dumps({"status": "running"})}
  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(_client(store), "op_123", attempts=3)
  assert json.loads(store[_key("op_123")])["status"] == "failed"
  evict.assert_called_once_with("op_123")


def test_missing_metadata_evicts_nothing():
  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(_client({}), "op_missing", attempts=3)
  evict.assert_not_called()


def test_an_operation_that_already_completed_is_left_alone():
  """The staleness decision came from an earlier snapshot; by the time the
  DLQ write runs the worker may have finished. Completed wins — no flip to
  failed, no eviction, or a retry under the same key would run it again."""
  store = {_key("op_done"): json.dumps({"status": "completed"})}
  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(_client(store), "op_done", attempts=3)
  assert json.loads(store[_key("op_done")])["status"] == "completed"
  evict.assert_not_called()


def test_worker_completing_between_read_and_write_wins():
  """Same race, one step later: the worker's completion lands after the
  reaper read `running` but before its EXEC. The WATCH fails the reaper's
  write; nothing is flipped and nothing is evicted."""
  store = {_key("op_race"): json.dumps({"status": "running"})}

  def worker_finishes(s):
    s[_key("op_race")] = json.dumps({"status": "completed"})

  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(_client(store, worker_finishes), "op_race", attempts=3)
  assert json.loads(store[_key("op_race")])["status"] == "completed"
  evict.assert_not_called()


def test_metadata_write_failure_does_not_evict():
  """If the status write fails the operation is not terminal yet; leaving the
  envelope in place is the honest state."""
  store = {_key("op_x"): json.dumps({"status": "running"})}
  sse = _client(store)
  sse._pipeline.execute = MagicMock(side_effect=RuntimeError("valkey down"))
  with patch(
    "robosystems.middleware.operations.invalidate_operation_idempotency_sync"
  ) as evict:
    _fail_operation_sync(sse, "op_x", attempts=3)
  evict.assert_not_called()
