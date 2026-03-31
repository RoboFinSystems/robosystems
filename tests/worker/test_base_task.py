"""Tests for the BaseTask class."""

from typing import Any
from unittest.mock import AsyncMock

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
