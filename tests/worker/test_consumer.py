"""Tests for the worker consumer loop."""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.worker.consumer import _process_task
from robosystems.worker.tasks import TASK_REGISTRY, register_task
from robosystems.worker.tasks.base import BaseTask


class SuccessTask(BaseTask):
  async def execute(self) -> dict[str, Any]:
    return {"mapped": 10, "coverage": 100}


class FailingTask(BaseTask):
  async def execute(self) -> dict[str, Any]:
    raise ValueError("Something went wrong")


@pytest.fixture(autouse=True)
def _clean_registry():
  original = dict(TASK_REGISTRY)
  register_task("test_success")(SuccessTask)
  register_task("test_failure")(FailingTask)
  yield
  TASK_REGISTRY.clear()
  TASK_REGISTRY.update(original)


@pytest.fixture
def mock_manager():
  manager = AsyncMock()
  manager.mark_running = AsyncMock()
  manager.emit_progress = AsyncMock()
  manager.complete_operation = AsyncMock()
  manager.fail_operation = AsyncMock()
  return manager


@pytest.fixture
def mock_queue():
  return AsyncMock()


def _make_task_data(task_type="test_success", **overrides):
  data = {
    "task_id": "op_01TEST",
    "task_type": task_type,
    "graph_id": "kg0123456789abcdef",
    "user_id": "user_01TEST",
    "params": {},
  }
  data.update(overrides)
  return data


async def _call_process_task(
  task_data, mock_queue, mock_manager, worker_id="worker-test-1", protection=None
):
  """Helper to call _process_task with the correct signature."""
  task_json = json.dumps(task_data)
  inflight_key = f"worker:inflight:{worker_id}"
  protection = protection or AsyncMock()
  await _process_task(
    task_data, task_json, mock_queue, inflight_key, mock_manager, worker_id, protection
  )
  return protection


@pytest.mark.asyncio
async def test_protects_and_unprotects_around_task(mock_queue, mock_manager):
  """A protected task wraps execution in protect()/unprotect()."""
  protection = AsyncMock()
  await _call_process_task(
    _make_task_data(), mock_queue, mock_manager, protection=protection
  )
  protection.protect.assert_awaited_once()
  protection.unprotect.assert_awaited_once()


@pytest.mark.asyncio
async def test_short_task_skips_protection(mock_queue, mock_manager):
  """A task below the protection threshold makes no ECS protection calls."""
  protection = AsyncMock()
  with (
    patch("robosystems.worker.consumer.TASK_TIMEOUTS", {}),
    patch("robosystems.worker.consumer.DEFAULT_TASK_TIMEOUT", 60),
    patch("robosystems.worker.consumer.PROTECT_MIN_TIMEOUT_SECONDS", 120),
  ):
    await _call_process_task(
      _make_task_data(), mock_queue, mock_manager, protection=protection
    )
  protection.protect.assert_not_awaited()
  protection.unprotect.assert_not_awaited()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_happy_path(mock_tracer, mock_cleanup, mock_manager, mock_queue):
  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()

  task_data = _make_task_data()
  await _call_process_task(task_data, mock_queue, mock_manager)

  # The pickup moves the operation from PENDING (queued) to RUNNING
  mock_manager.mark_running.assert_called_once_with("op_01TEST")

  # Operation completed with result
  mock_manager.complete_operation.assert_called_once()
  call_args = mock_manager.complete_operation.call_args
  assert call_args[0][0] == "op_01TEST"
  assert call_args[1]["result"] == {"mapped": 10, "coverage": 100}

  # Cleanup always called
  mock_cleanup.assert_called_once()

  # Inflight entry removed
  mock_queue.lrem.assert_called_once()

  # fail_operation NOT called
  mock_manager.fail_operation.assert_not_called()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_error_path(mock_tracer, mock_cleanup, mock_manager, mock_queue):
  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()

  task_data = _make_task_data(task_type="test_failure")
  await _call_process_task(task_data, mock_queue, mock_manager)

  # fail_operation called with error
  mock_manager.fail_operation.assert_called_once()
  call_args = mock_manager.fail_operation.call_args
  assert call_args[0][0] == "op_01TEST"
  assert "Something went wrong" in call_args[1]["error"]
  assert call_args[1]["error_details"]["error_type"] == "ValueError"

  # complete_operation NOT called
  mock_manager.complete_operation.assert_not_called()

  # Cleanup STILL called
  mock_cleanup.assert_called_once()

  # Inflight entry removed even on failure
  mock_queue.lrem.assert_called_once()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_a_raising_fail_operation_does_not_escape(
  mock_tracer, mock_cleanup, mock_manager, mock_queue
):
  """`fail_operation` raises when the operation's SSE metadata has expired
  (a task outliving its TTL). Raising from inside the except handler used to
  escape `_process_task` and, with nothing above it in the run loop, exit the
  worker — so one long task could kill the process. It must be logged and
  swallowed; cleanup and the inflight removal must still happen."""
  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()

  # The real failure shape: the handler raises, then recording the failure
  # raises too because the metadata key is gone.
  mock_manager.fail_operation = AsyncMock(
    side_effect=ValueError("Operation op_01TEST not found (metadata expired)")
  )

  task_data = _make_task_data(task_type="test_failure")
  # Must not raise.
  await _call_process_task(task_data, mock_queue, mock_manager)

  mock_manager.fail_operation.assert_called_once()
  mock_cleanup.assert_called_once()
  mock_queue.lrem.assert_called_once()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
async def test_unknown_task_type(mock_cleanup, mock_manager, mock_queue):
  task_data = _make_task_data(task_type="nonexistent_type")
  await _call_process_task(task_data, mock_queue, mock_manager)

  # fail_operation called for unknown type
  mock_manager.fail_operation.assert_called_once()
  assert "Unknown task type" in mock_manager.fail_operation.call_args[1]["error"]

  # Inflight entry removed
  mock_queue.lrem.assert_called_once()

  # No cleanup needed (handler never ran)
  mock_cleanup.assert_not_called()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_timeout_marks_failed(
  mock_tracer, mock_cleanup, mock_manager, mock_queue
):
  """Task that exceeds its timeout is marked as failed."""
  import asyncio

  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()

  class SlowTask(BaseTask):
    async def execute(self) -> dict[str, Any]:
      await asyncio.sleep(999)
      return {}

  TASK_REGISTRY["test_slow"] = SlowTask

  task_data = _make_task_data(task_type="test_slow")

  # Patch DEFAULT_TASK_TIMEOUT to something tiny for the test
  with (
    patch("robosystems.worker.consumer.TASK_TIMEOUTS", {}),
    patch("robosystems.worker.consumer.DEFAULT_TASK_TIMEOUT", 0.01),
  ):
    await _call_process_task(task_data, mock_queue, mock_manager)

  # Should be marked as failed with timeout error
  mock_manager.fail_operation.assert_called_once()
  call_args = mock_manager.fail_operation.call_args
  assert "timed out" in call_args[1]["error"]
  assert call_args[1]["error_details"]["error_type"] == "TimeoutError"

  # complete_operation NOT called
  mock_manager.complete_operation.assert_not_called()

  # Inflight entry removed
  mock_queue.lrem.assert_called_once()

  # Cleanup still called
  mock_cleanup.assert_called_once()


class ThreadBackedTask(BaseTask):
  """A task whose work runs where the budget cannot cancel it."""

  release: Any = None

  async def execute(self) -> dict[str, Any]:
    return await self.run_blocking(self._work)

  def _work(self) -> dict[str, Any]:
    import time

    if self.release is not None:
      self.release.wait(5)
    else:
      time.sleep(0.2)
    return {"landed": True}


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_a_thread_that_lands_past_its_budget_is_completed_not_failed(
  mock_tracer, mock_cleanup, mock_manager, mock_queue
):
  """The failure mode named in TASK_TIMEOUTS: a close whose thread keeps
  running after the budget cancels the coroutine, and then commits. The
  operation must report what the thread produced, not FAILED — and cleanup
  must run only once the thread has joined."""
  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()
  TASK_REGISTRY["test_thread"] = ThreadBackedTask

  with (
    patch("robosystems.worker.consumer.TASK_TIMEOUTS", {}),
    patch("robosystems.worker.consumer.DEFAULT_TASK_TIMEOUT", 0.05),
    patch("robosystems.worker.tasks.base.DEFAULT_TASK_TIMEOUT", 1),
  ):
    await _call_process_task(_make_task_data("test_thread"), mock_queue, mock_manager)

  mock_manager.complete_operation.assert_called_once()
  assert mock_manager.complete_operation.call_args[1]["result"] == {"landed": True}
  mock_manager.fail_operation.assert_not_called()
  mock_cleanup.assert_called_once()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_a_thread_abandoned_past_its_grace_is_reported_as_still_running(
  mock_tracer, mock_cleanup, mock_manager, mock_queue
):
  """'Timed out' alone reads as 'did not happen' and invites a retry of a
  write that may be about to land. The stored failure has to say the work is
  still in flight."""
  import asyncio
  import threading

  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()

  release = threading.Event()

  class StuckTask(ThreadBackedTask):
    pass

  StuckTask.release = release
  TASK_REGISTRY["test_stuck"] = StuckTask

  try:
    with (
      patch("robosystems.worker.consumer.TASK_TIMEOUTS", {}),
      patch("robosystems.worker.consumer.DEFAULT_TASK_TIMEOUT", 0.05),
      patch("robosystems.worker.tasks.base.DEFAULT_TASK_TIMEOUT", 0.05),
    ):
      await _call_process_task(_make_task_data("test_stuck"), mock_queue, mock_manager)
  finally:
    release.set()

  mock_manager.fail_operation.assert_called_once()
  call_args = mock_manager.fail_operation.call_args
  assert "may still complete" in call_args[1]["error"]
  assert call_args[1]["error_details"]["still_running"] is True
  mock_manager.complete_operation.assert_not_called()
  mock_cleanup.assert_called_once()
  # Let the released thread finish before the loop closes.
  await asyncio.sleep(0.1)


class PausingTask(BaseTask):
  async def execute(self) -> dict[str, Any]:
    from robosystems.worker.tasks.base import TaskPaused

    raise TaskPaused("Need a decision")


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_paused_task_is_neither_completed_nor_failed(
  mock_tracer, mock_cleanup, mock_manager, mock_queue
):
  """A pause leaves the operation AWAITING_INPUT for the resume endpoint."""
  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()
  register_task("test_pausing")(PausingTask)

  await _call_process_task(_make_task_data("test_pausing"), mock_queue, mock_manager)

  mock_manager.complete_operation.assert_not_called()
  mock_manager.fail_operation.assert_not_called()
  # It still leaves the inflight list and the worker is cleaned up
  mock_queue.lrem.assert_called_once()
  mock_cleanup.assert_called_once()
