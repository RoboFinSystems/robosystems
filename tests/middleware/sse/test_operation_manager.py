"""SSE operation manager tests."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from robosystems.middleware.sse.event_storage import (
  EventType,
  OperationMetadata,
  OperationStatus,
  SSEEvent,
)
from robosystems.middleware.sse.operation_manager import (
  OperationManager,
  get_operation_manager,
)


@pytest.fixture
def mock_storage():
  storage = AsyncMock()
  storage.create_operation = AsyncMock(return_value="op-test-123")
  storage.store_event = AsyncMock(
    return_value=SSEEvent(
      event_type=EventType.OPERATION_STARTED,
      operation_id="op-test-123",
      timestamp="2024-01-01T00:00:00Z",
      data={},
      sequence_number=1,
    )
  )
  storage.get_operation_metadata = AsyncMock(
    return_value=OperationMetadata(
      operation_id="op-test-123",
      operation_type="test",
      user_id="u1",
      graph_id=None,
      status=OperationStatus.RUNNING,
      created_at="2024-01-01",
      updated_at="2024-01-01",
    )
  )
  storage.cancel_operation = AsyncMock()
  return storage


@pytest.fixture
def manager(mock_storage):
  return OperationManager(event_storage=mock_storage)


class TestStartOperation:
  @pytest.mark.asyncio
  async def test_creates_and_starts(self, manager, mock_storage):
    op_id = await manager.start_operation("graph_creation", "user1", "kg123")
    assert op_id == "op-test-123"
    mock_storage.create_operation.assert_called_once()
    mock_storage.store_event.assert_called_once()

  @pytest.mark.asyncio
  async def test_with_initial_data(self, manager, mock_storage):
    await manager.start_operation("test", "u1", initial_data={"key": "value"})
    mock_storage.store_event.assert_called_once()
    # The data dict is the third positional arg
    call_args = mock_storage.store_event.call_args
    data_arg = (
      call_args[0][2] if len(call_args[0]) > 2 else call_args[1].get("data", {})
    )
    assert "key" in data_arg


class TestEmitProgress:
  @pytest.mark.asyncio
  async def test_emits_progress_event(self, manager, mock_storage):
    await manager.emit_progress("op-123", "Processing...", progress_percent=50)
    mock_storage.store_event.assert_called_once()
    call_args = mock_storage.store_event.call_args
    assert call_args[0][1] == EventType.OPERATION_PROGRESS


class TestCompleteOperation:
  @pytest.mark.asyncio
  async def test_emits_completed_event(self, manager, mock_storage):
    await manager.complete_operation("op-123", result={"rows": 100})
    mock_storage.store_event.assert_called_once()
    call_args = mock_storage.store_event.call_args
    assert call_args[0][1] == EventType.OPERATION_COMPLETED


class TestFailOperation:
  @pytest.mark.asyncio
  async def test_emits_error_event(self, manager, mock_storage):
    await manager.fail_operation("op-123", "Connection timeout")
    mock_storage.store_event.assert_called_once()
    call_args = mock_storage.store_event.call_args
    assert call_args[0][1] == EventType.OPERATION_ERROR


class TestCancelOperation:
  @pytest.mark.asyncio
  async def test_delegates_to_storage(self, manager, mock_storage):
    await manager.cancel_operation("op-123", "User requested")
    mock_storage.cancel_operation.assert_called_once_with("op-123", "User requested")


class TestGetOperationStatus:
  @pytest.mark.asyncio
  async def test_returns_status(self, manager, mock_storage):
    status = await manager.get_operation_status("op-123")
    assert status == OperationStatus.RUNNING

  @pytest.mark.asyncio
  async def test_returns_none_for_missing(self, manager, mock_storage):
    mock_storage.get_operation_metadata.return_value = None
    status = await manager.get_operation_status("nonexistent")
    assert status is None


class TestOperationContext:
  @pytest.mark.asyncio
  async def test_success_path(self, manager, mock_storage):
    async with manager.operation_context("test", "u1") as op_id:
      assert op_id == "op-test-123"
    # Should have called store_event for start + complete
    assert mock_storage.store_event.call_count >= 2

  @pytest.mark.asyncio
  async def test_failure_path(self, manager, mock_storage):
    exc = None
    try:
      async with manager.operation_context("test", "u1") as _op_id:
        raise ValueError("boom")
    except ValueError as e:
      exc = e
    assert exc is not None
    # Should have store_event for start + error
    assert mock_storage.store_event.call_count >= 2
    last_call = mock_storage.store_event.call_args_list[-1]
    assert last_call[0][1] == EventType.OPERATION_ERROR

  @pytest.mark.asyncio
  async def test_cancellation_path(self, manager, mock_storage):
    exc = None
    try:
      async with manager.operation_context("test", "u1") as _op_id:
        raise asyncio.CancelledError()
    except asyncio.CancelledError as e:
      exc = e
    assert exc is not None
    mock_storage.cancel_operation.assert_called_once()


class TestRunOperation:
  @pytest.mark.asyncio
  async def test_runs_function(self, manager, mock_storage):
    called = False

    async def my_func(op_id):
      nonlocal called
      called = True

    op_id = await manager.run_operation("test", "u1", my_func)
    assert called
    assert op_id == "op-test-123"


class TestBatchOperation:
  @pytest.mark.asyncio
  async def test_runs_all_operations(self, manager, mock_storage):
    results = []

    async def op1(op_id, idx):
      results.append(f"op_{idx}")

    async def op2(op_id, idx):
      results.append(f"op_{idx}")

    op_id = await manager.batch_operation("batch", "u1", [op1, op2])
    assert len(results) == 2
    assert op_id == "op-test-123"


class TestGetOperationManager:
  def test_returns_instance(self):
    mgr = get_operation_manager()
    assert isinstance(mgr, OperationManager)


class TestPauseAndResume:
  @pytest.mark.asyncio
  async def test_await_input_stores_the_request_with_the_queue_payload(self):
    from robosystems.middleware.sse.event_storage import EventType
    from robosystems.middleware.sse.operation_manager import OperationManager

    storage = AsyncMock()
    manager = OperationManager(event_storage=storage)
    task = {"task_type": "operator", "graph_id": "kg1", "user_id": "u1", "params": {}}

    await manager.await_input(
      "op_1", prompt="Post it?", task=task, checkpoint={"step": 2}, details={"n": 3}
    )

    storage.store_event.assert_awaited_once()
    op_id, event_type, data = storage.store_event.call_args[0]
    assert (op_id, event_type) == ("op_1", EventType.OPERATION_AWAITING_INPUT)
    assert data["message"] == "Post it?"
    request = data["input_request"]
    assert request["prompt"] == "Post it?"
    assert request["checkpoint"] == {"step": 2}
    assert request["details"] == {"n": 3}
    assert request["task"] == task
    assert request["requested_at"]

  @pytest.mark.asyncio
  async def test_resume_operation_stores_the_answer(self):
    from robosystems.middleware.sse.event_storage import EventType
    from robosystems.middleware.sse.operation_manager import OperationManager

    storage = AsyncMock()
    manager = OperationManager(event_storage=storage)

    await manager.resume_operation("op_1", {"approved": True})

    op_id, event_type, data = storage.store_event.call_args[0]
    assert (op_id, event_type) == ("op_1", EventType.OPERATION_RESUMED)
    assert data["input"] == {"approved": True}
