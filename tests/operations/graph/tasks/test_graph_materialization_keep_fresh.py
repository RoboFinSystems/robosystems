"""The direct copy's lock refresher: a blip is retried, a lost lock stops the copy."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.graph.tasks.graph_materialization import (
  GraphMaterializationTask,
)

pytestmark = pytest.mark.unit


def _task() -> GraphMaterializationTask:
  task = GraphMaterializationTask(
    task_id="op_test",
    graph_id="kg0000000000000001",
    user_id="usr_test",
    params={"materialization_lock_token": "tok"},
    manager=MagicMock(),
  )
  task.LOCK_EXTEND_INTERVAL_SECONDS = 0.01
  return task


async def _execute(task, copy_seconds: float, extend_results):
  def db_gen():
    yield MagicMock()

  async def slow_copy(**_kwargs):
    await asyncio.sleep(copy_seconds)
    return {"status": "success"}

  with (
    patch("robosystems.database.get_db_session", side_effect=lambda: db_gen()),
    patch(
      "robosystems.operations.graph.engine.direct_materialization.materialize_graph_directly",
      slow_copy,
    ),
    patch.object(task, "_extend_lock", side_effect=extend_results) as extend,
    patch.object(task, "release_lock") as release,
  ):
    try:
      outcome = await task.execute()
    except Exception as e:
      outcome = e
  return outcome, extend, release


async def test_a_valkey_blip_is_retried_and_the_copy_finishes():
  """An error building the client or reaching Valkey must not end the
  refresher: the copy would then run past the TTL unrefreshed."""
  task = _task()
  results = [ConnectionError("blip"), ConnectionError("blip")] + [True] * 100

  outcome, extend, release = await _execute(task, 0.1, results)

  assert outcome == {"status": "success"}
  assert extend.call_count > 2
  release.assert_called_once()


async def test_a_lost_lock_stops_the_copy_and_keeps_its_hands_off_the_lock():
  """Another run may hold the lock now; the copy stops before its next write
  rather than carry on unlocked, and does not release a lock it lost."""
  task = _task()

  outcome, _, release = await _execute(task, 5, [False])

  assert isinstance(outcome, RuntimeError)
  assert "lost mid-copy" in str(outcome)
  release.assert_not_called()
