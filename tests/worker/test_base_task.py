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
