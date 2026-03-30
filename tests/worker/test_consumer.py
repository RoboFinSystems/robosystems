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
  manager.emit_progress = AsyncMock()
  manager.complete_operation = AsyncMock()
  manager.fail_operation = AsyncMock()
  return manager


@pytest.fixture
def mock_queue():
  return AsyncMock()


def _make_task_data(task_type="test_success", **overrides):
  data = {
    "task_id": "task_01TEST",
    "task_type": task_type,
    "graph_id": "kg0123456789abcdef",
    "user_id": "user_01TEST",
    "params": {},
  }
  data.update(overrides)
  return data


async def _call_process_task(
  task_data, mock_queue, mock_manager, worker_id="worker-test-1"
):
  """Helper to call _process_task with the correct signature."""
  task_json = json.dumps(task_data)
  inflight_key = f"worker:inflight:{worker_id}"
  await _process_task(
    task_data, task_json, mock_queue, inflight_key, mock_manager, worker_id
  )


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.report_task_to_dagster", new_callable=AsyncMock)
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_happy_path(
  mock_tracer, mock_cleanup, mock_dagster, mock_manager, mock_queue
):
  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()

  task_data = _make_task_data()
  await _call_process_task(task_data, mock_queue, mock_manager)

  # Progress emitted at start
  mock_manager.emit_progress.assert_called_once_with(
    "task_01TEST", "Starting...", progress_percent=0
  )

  # Operation completed with result
  mock_manager.complete_operation.assert_called_once()
  call_args = mock_manager.complete_operation.call_args
  assert call_args[0][0] == "task_01TEST"
  assert call_args[1]["result"] == {"mapped": 10, "coverage": 100}

  # Dagster reporting called
  mock_dagster.assert_called_once()

  # Cleanup always called
  mock_cleanup.assert_called_once()

  # Inflight entry removed
  mock_queue.lrem.assert_called_once()

  # fail_operation NOT called
  mock_manager.fail_operation.assert_not_called()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.report_task_to_dagster", new_callable=AsyncMock)
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_error_path(
  mock_tracer, mock_cleanup, mock_dagster, mock_manager, mock_queue
):
  mock_tracer.return_value = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__enter__ = MagicMock()
  mock_tracer.return_value.start_as_current_span.return_value.__exit__ = MagicMock()

  task_data = _make_task_data(task_type="test_failure")
  await _call_process_task(task_data, mock_queue, mock_manager)

  # fail_operation called with error
  mock_manager.fail_operation.assert_called_once()
  call_args = mock_manager.fail_operation.call_args
  assert call_args[0][0] == "task_01TEST"
  assert "Something went wrong" in call_args[1]["error"]
  assert call_args[1]["error_details"]["error_type"] == "ValueError"

  # complete_operation NOT called
  mock_manager.complete_operation.assert_not_called()

  # Dagster NOT called on failure
  mock_dagster.assert_not_called()

  # Cleanup STILL called
  mock_cleanup.assert_called_once()

  # Inflight entry removed even on failure
  mock_queue.lrem.assert_called_once()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.report_task_to_dagster", new_callable=AsyncMock)
@patch("robosystems.worker.consumer.cleanup_connections")
async def test_unknown_task_type(mock_cleanup, mock_dagster, mock_manager, mock_queue):
  task_data = _make_task_data(task_type="nonexistent_type")
  await _call_process_task(task_data, mock_queue, mock_manager)

  # fail_operation called for unknown type
  mock_manager.fail_operation.assert_called_once()
  assert "Unknown task type" in mock_manager.fail_operation.call_args[1]["error"]

  # Inflight entry removed
  mock_queue.lrem.assert_called_once()

  # No cleanup needed (handler never ran)
  mock_cleanup.assert_not_called()

  # No Dagster reporting
  mock_dagster.assert_not_called()


@pytest.mark.asyncio
@patch("robosystems.worker.consumer.report_task_to_dagster", new_callable=AsyncMock)
@patch("robosystems.worker.consumer.cleanup_connections")
@patch("robosystems.worker.consumer.get_tracer")
async def test_timeout_marks_failed(
  mock_tracer, mock_cleanup, mock_dagster, mock_manager, mock_queue
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
