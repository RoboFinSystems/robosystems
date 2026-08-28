"""Tests for the BaseTask class."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.sse.event_storage import OperationStatus
from robosystems.worker.tasks.base import BaseTask


class ConcreteTask(BaseTask):
  async def execute(self) -> dict[str, Any]:
    return {"result": "done"}


@pytest.fixture
def mock_manager():
  manager = AsyncMock()
  manager.emit_progress = AsyncMock()
  manager.get_operation_status = AsyncMock(return_value=OperationStatus.RUNNING)
  return manager


@pytest.fixture
def task(mock_manager):
  return ConcreteTask(
    task_id="op_01TEST",
    graph_id="kg0123456789abcdef",
    user_id="user_01TEST",
    params={"key": "value"},
    manager=mock_manager,
  )


@pytest.mark.asyncio
async def test_execute(task):
  result = await task.execute()
  assert result == {"result": "done"}


@pytest.mark.asyncio
async def test_report_progress(task, mock_manager):
  await task.report_progress("Working...", percent=50, details={"step": 1})

  mock_manager.emit_progress.assert_called_once_with(
    "op_01TEST",
    message="Working...",
    progress_percent=50,
    details={"step": 1},
  )


@pytest.mark.asyncio
async def test_report_progress_no_optional_args(task, mock_manager):
  await task.report_progress("Starting...")

  mock_manager.emit_progress.assert_called_once_with(
    "op_01TEST",
    message="Starting...",
    progress_percent=None,
    details=None,
  )


@pytest.mark.asyncio
async def test_is_cancelled_false(task, mock_manager):
  mock_manager.get_operation_status.return_value = OperationStatus.RUNNING
  assert await task.is_cancelled() is False


@pytest.mark.asyncio
async def test_is_cancelled_true(task, mock_manager):
  mock_manager.get_operation_status.return_value = OperationStatus.CANCELLED
  assert await task.is_cancelled() is True


def test_task_attributes(task):
  assert task.task_id == "op_01TEST"
  assert task.graph_id == "kg0123456789abcdef"
  assert task.user_id == "user_01TEST"
  assert task.params == {"key": "value"}


class TestReleaseLock:
  """``release_lock`` must be a compare-and-delete when the enqueuing API
  passed its lock_id: a task finishing after the lock's TTL lapsed must not
  strip the lock a successor has since acquired."""

  def _task(self, mock_manager, params):
    return ConcreteTask(
      task_id="op_01TEST",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
      params=params,
      manager=mock_manager,
    )

  def test_none_lock_key_is_noop(self, mock_manager):
    task = self._task(mock_manager, {})
    with patch(
      "robosystems.config.valkey_registry.create_redis_client"
    ) as create_client:
      task.release_lock(None)
    create_client.assert_not_called()

  def test_release_with_lock_id_uses_compare_and_delete(self, mock_manager):
    task = self._task(mock_manager, {"lock_id": "abc"})
    redis_client = MagicMock()
    with (
      patch(
        "robosystems.config.valkey_registry.create_redis_client",
        return_value=redis_client,
      ),
      patch(
        "robosystems.middleware.auth.distributed_lock.release_lock_by_id",
        return_value=True,
      ) as release_by_id,
    ):
      task.release_lock("graph_materialize:kg1")

    release_by_id.assert_called_once_with(redis_client, "graph_materialize:kg1", "abc")
    redis_client.delete.assert_not_called()
    redis_client.close.assert_called_once()

  def test_explicit_lock_id_wins_over_params(self, mock_manager):
    task = self._task(mock_manager, {"lock_id": "from-params"})
    redis_client = MagicMock()
    with (
      patch(
        "robosystems.config.valkey_registry.create_redis_client",
        return_value=redis_client,
      ),
      patch(
        "robosystems.middleware.auth.distributed_lock.release_lock_by_id",
        return_value=True,
      ) as release_by_id,
    ):
      task.release_lock("graph_materialize:kg1", lock_id="explicit")

    release_by_id.assert_called_once_with(
      redis_client, "graph_materialize:kg1", "explicit"
    )

  def test_release_without_lock_id_falls_back_to_delete(self, mock_manager):
    """Tasks enqueued before the API passed lock_id carry only the key; they
    still release unconditionally so their lock is not stranded for the TTL."""
    task = self._task(mock_manager, {})
    redis_client = MagicMock()
    with (
      patch(
        "robosystems.config.valkey_registry.create_redis_client",
        return_value=redis_client,
      ),
      patch(
        "robosystems.middleware.auth.distributed_lock.release_lock_by_id",
      ) as release_by_id,
    ):
      task.release_lock("graph_materialize:kg1")

    release_by_id.assert_not_called()
    redis_client.delete.assert_called_once_with("lock:graph_materialize:kg1")

  def test_release_lock_by_id_is_compare_and_delete(self):
    """Pin the helper the worker relies on: a mismatched lock_id leaves the
    key alone."""
    from robosystems.middleware.auth.distributed_lock import release_lock_by_id

    redis_client = MagicMock()
    redis_client.eval.return_value = 0

    assert release_lock_by_id(redis_client, "graph_materialize:kg1", "stale") is False
    args = redis_client.eval.call_args.args
    assert args[1] == 1
    assert args[2] == "lock:graph_materialize:kg1"
    assert args[3] == "stale"
    redis_client.delete.assert_not_called()


class TestRunBlocking:
  """The consumer's budget cancels the coroutine and never the thread. What
  happens in the gap decides whether a close that lands is reported as one
  that failed."""

  def _task(self, mock_manager, work):
    class BlockingTask(BaseTask):
      async def execute(self) -> dict[str, Any]:
        return await self.run_blocking(work)

    return BlockingTask(
      task_id="op_01TEST",
      graph_id="kg0123456789abcdef",
      user_id="user_01TEST",
      params={},
      manager=mock_manager,
    )

  @pytest.mark.asyncio
  async def test_a_thread_that_lands_inside_the_grace_is_the_result(self, mock_manager):
    """The budget expires at 50ms; the thread needs 200ms; the grace is a
    second. The outcome is the thread's, and `wait_for` returns it rather
    than raising — the stdlib contract for a task that absorbs its cancel."""
    import time

    def work():
      time.sleep(0.2)
      return {"landed": True}

    task = self._task(mock_manager, work)
    with patch("robosystems.worker.tasks.base.DEFAULT_TASK_TIMEOUT", 1):
      import asyncio

      result = await asyncio.wait_for(task.execute(), timeout=0.05)

    assert result == {"landed": True}
    assert task.abandoned_work == []

  @pytest.mark.asyncio
  async def test_a_thread_that_fails_inside_the_grace_raises_its_own_error(
    self, mock_manager
  ):
    """A fault after the budget is still that fault, not a timeout — the
    consumer records its type, and a timeout would hide it."""
    import asyncio
    import time

    def work():
      time.sleep(0.1)
      raise RuntimeError("stamp failed")

    task = self._task(mock_manager, work)
    with patch("robosystems.worker.tasks.base.DEFAULT_TASK_TIMEOUT", 1):
      with pytest.raises(RuntimeError, match="stamp failed"):
        await asyncio.wait_for(task.execute(), timeout=0.02)

  @pytest.mark.asyncio
  async def test_a_thread_that_outlives_the_grace_is_abandoned_and_the_timeout_stands(
    self, mock_manager
  ):
    """Past budget + grace the worker must move on — a hung QuickBooks call
    would otherwise wedge the only worker. The thread is tracked so the
    consumer can say 'still running', and its late outcome is logged rather
    than dropped as never retrieved."""
    import asyncio
    import threading

    release = threading.Event()

    def work():
      release.wait(5)
      return {"late": True}

    task = self._task(mock_manager, work)
    try:
      with patch("robosystems.worker.tasks.base.DEFAULT_TASK_TIMEOUT", 0.05):
        with pytest.raises(TimeoutError):
          await asyncio.wait_for(task.execute(), timeout=0.05)
      assert len(task.abandoned_work) == 1
    finally:
      release.set()

    with patch("robosystems.worker.tasks.base.logger") as log:
      await asyncio.wait(task._abandoned, timeout=2)
      # Give the done-callback its turn on the loop.
      await asyncio.sleep(0)
    assert task.abandoned_work == []
    assert log.warning.called
    assert "abandoned blocking work finished" in log.warning.call_args.args[0]


class TestBudget:
  def test_register_task_stamps_the_type_the_budget_is_looked_up_by(self):
    from robosystems.worker.tasks import TASK_REGISTRY, register_task

    original = dict(TASK_REGISTRY)
    try:

      @register_task("test_budgeted")
      class Budgeted(ConcreteTask):
        pass

      assert Budgeted.task_type == "test_budgeted"
      task = Budgeted("op", None, "u", {}, MagicMock())
      with patch("robosystems.worker.tasks.base.TASK_TIMEOUTS", {"test_budgeted": 7}):
        assert task.budget_seconds == 7
    finally:
      TASK_REGISTRY.clear()
      TASK_REGISTRY.update(original)

  def test_an_unregistered_task_falls_back_to_the_default_budget(self, task):
    assert task.task_type is None
    with patch("robosystems.worker.tasks.base.DEFAULT_TASK_TIMEOUT", 42):
      assert task.budget_seconds == 42


@pytest.mark.asyncio
async def test_pause_for_input_records_the_request_and_unwinds(task, mock_manager):
  from robosystems.worker.tasks.base import TaskPaused

  mock_manager.await_input = AsyncMock()
  task.task_type = "operator"

  with pytest.raises(TaskPaused) as exc_info:
    await task.pause_for_input(
      "Post the close?", checkpoint={"step": 3}, details={"period": "2026-07"}
    )

  assert exc_info.value.prompt == "Post the close?"
  mock_manager.await_input.assert_awaited_once_with(
    "op_01TEST",
    prompt="Post the close?",
    checkpoint={"step": 3},
    details={"period": "2026-07"},
    task={
      "task_type": "operator",
      "graph_id": "kg0123456789abcdef",
      "user_id": "user_01TEST",
      "params": {"key": "value"},
    },
  )


@pytest.mark.asyncio
async def test_pause_strips_a_previous_resume_from_the_recorded_params(mock_manager):
  """A second pause must not carry the first answer back into the payload."""
  mock_manager.await_input = AsyncMock()
  task = ConcreteTask(
    task_id="op_01TEST",
    graph_id="kg0123456789abcdef",
    user_id="user_01TEST",
    params={"key": "value", "resume": {"checkpoint": {"step": 1}, "input": {}}},
    manager=mock_manager,
  )

  assert task.resume == {"checkpoint": {"step": 1}, "input": {}}

  from robosystems.worker.tasks.base import TaskPaused

  with pytest.raises(TaskPaused):
    await task.pause_for_input("Again?")

  recorded = mock_manager.await_input.call_args.kwargs["task"]["params"]
  assert recorded == {"key": "value"}


def test_resume_is_none_on_a_first_run(task):
  assert task.resume is None
