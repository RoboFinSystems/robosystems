"""
Tests for the operations router.

This test suite covers:
- get_operation_status endpoint (returns operation status dict)
- cancel_operation endpoint (cancels pending/running operations)

Tests call the async endpoint functions directly with mocked dependencies
(event_storage, metrics, current_user).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.sse.event_storage import OperationStatus
from robosystems.routers.operations import cancel_operation, get_operation_status

MODULE = "robosystems.routers.operations"


def _make_mock_user(user_id="usr_test123"):
  """Helper to create a mock authenticated user."""
  mock_user = MagicMock()
  mock_user.id = user_id
  return mock_user


def _make_mock_metadata(
  operation_id="op_123",
  operation_type="graph_creation",
  status=OperationStatus.COMPLETED,
  user_id="usr_test123",
  result_data=None,
  error_message=None,
  graph_id="kg01234567890abcdef",
):
  """Helper to create a mock operation metadata object."""
  mock_metadata = MagicMock()
  mock_metadata.operation_id = operation_id
  mock_metadata.operation_type = operation_type
  mock_metadata.status = status
  mock_metadata.created_at = "2024-01-01T00:00:00Z"
  mock_metadata.updated_at = "2024-01-01T00:01:00Z"
  mock_metadata.graph_id = graph_id
  mock_metadata.user_id = user_id
  mock_metadata.result_data = result_data
  mock_metadata.error_message = error_message
  return mock_metadata


def _make_patches(mock_storage):
  """Return a combined context manager for event_storage and metrics patches."""
  mock_metrics = MagicMock()
  mock_metrics.record_business_event = MagicMock()
  return (
    patch(f"{MODULE}.get_event_storage", return_value=mock_storage),
    patch(f"{MODULE}.get_endpoint_metrics", return_value=mock_metrics),
  )


@pytest.mark.asyncio
class TestGetOperationStatus:
  """Tests for get_operation_status endpoint."""

  @pytest.mark.unit
  async def test_completed_status_with_result(self):
    """Completed operation includes result data in the response."""
    metadata = _make_mock_metadata(
      status=OperationStatus.COMPLETED,
      result_data={"key": "val"},
    )
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      result = await get_operation_status(
        operation_id="op_123",
        current_user=mock_user,
      )

    assert result["status"] == OperationStatus.COMPLETED
    assert result["result"] == {"key": "val"}
    assert result["message"] == "Operation completed successfully"

  @pytest.mark.unit
  async def test_running_status_with_cancel_link(self):
    """Running operation response includes a cancel link in _links."""
    metadata = _make_mock_metadata(status=OperationStatus.RUNNING)
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      result = await get_operation_status(
        operation_id="op_123",
        current_user=mock_user,
      )

    assert result["status"] == OperationStatus.RUNNING
    assert "cancel" in result["_links"]
    assert result["_links"]["cancel"] == "/v1/operations/op_123"
    assert result["message"] == "Operation is currently executing"

  @pytest.mark.unit
  async def test_failed_status_with_error(self):
    """Failed operation includes error message in the response."""
    metadata = _make_mock_metadata(
      status=OperationStatus.FAILED,
      error_message="timeout",
    )
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      result = await get_operation_status(
        operation_id="op_123",
        current_user=mock_user,
      )

    assert result["status"] == OperationStatus.FAILED
    assert result["error"] == "timeout"
    assert result["message"] == "Operation execution failed"

  @pytest.mark.unit
  async def test_not_found_raises_404(self):
    """Returns 404 when the operation does not exist."""
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = None
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      with pytest.raises(HTTPException) as exc_info:
        await get_operation_status(
          operation_id="op_missing",
          current_user=mock_user,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  async def test_access_denied_raises_403(self):
    """Returns 403 when the operation belongs to a different user."""
    metadata = _make_mock_metadata(user_id="usr_other_user")
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user(user_id="usr_test123")

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      with pytest.raises(HTTPException) as exc_info:
        await get_operation_status(
          operation_id="op_123",
          current_user=mock_user,
        )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  async def test_pending_status_message(self):
    """Pending operation not found in the queue keeps the generic message."""
    metadata = _make_mock_metadata(status=OperationStatus.PENDING)
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with (
      p_storage,
      p_metrics,
      patch(f"{MODULE}.get_queue_position", AsyncMock(return_value=(None, 0))),
    ):
      result = await get_operation_status(
        operation_id="op_123",
        current_user=mock_user,
      )

    assert result["status"] == OperationStatus.PENDING
    assert result["message"] == "Operation is pending execution"
    assert result["queue_position"] is None
    assert result["queue_depth"] == 0
    # Pending operations also get a cancel link
    assert "cancel" in result["_links"]

  @pytest.mark.unit
  async def test_pending_status_reports_queue_position(self):
    """A queued operation says how many tasks are ahead of it."""
    metadata = _make_mock_metadata(status=OperationStatus.PENDING)
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with (
      p_storage,
      p_metrics,
      patch(f"{MODULE}.get_queue_position", AsyncMock(return_value=(3, 4))) as pos,
    ):
      result = await get_operation_status(
        operation_id="op_123",
        current_user=mock_user,
      )

    pos.assert_awaited_once_with("op_123")
    assert result["queue_position"] == 3
    assert result["queue_depth"] == 4
    assert result["message"] == (
      "Operation is queued — 2 ahead of it in the worker queue"
    )

  @pytest.mark.unit
  async def test_pending_status_next_in_queue(self):
    metadata = _make_mock_metadata(status=OperationStatus.PENDING)
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata

    p_storage, p_metrics = _make_patches(mock_storage)
    with (
      p_storage,
      p_metrics,
      patch(f"{MODULE}.get_queue_position", AsyncMock(return_value=(1, 1))),
    ):
      result = await get_operation_status(
        operation_id="op_123",
        current_user=_make_mock_user(),
      )

    assert result["message"] == "Operation is next in the worker queue"

  @pytest.mark.unit
  async def test_running_status_does_not_read_the_queue(self):
    """Only PENDING consults the queue — a running task is not in it."""
    metadata = _make_mock_metadata(status=OperationStatus.RUNNING)
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata

    p_storage, p_metrics = _make_patches(mock_storage)
    with (
      p_storage,
      p_metrics,
      patch(f"{MODULE}.get_queue_position", AsyncMock()) as pos,
    ):
      result = await get_operation_status(
        operation_id="op_123",
        current_user=_make_mock_user(),
      )

    pos.assert_not_awaited()
    assert "queue_position" not in result


@pytest.mark.asyncio
class TestCancelOperation:
  """Tests for cancel_operation endpoint."""

  @pytest.mark.unit
  async def test_cancel_running_operation(self):
    """Cancelling a running operation calls event_storage.cancel_operation."""
    metadata = _make_mock_metadata(status=OperationStatus.RUNNING)
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      result = await cancel_operation(
        operation_id="op_123",
        current_user=mock_user,
      )

    mock_storage.cancel_operation.assert_called_once_with(
      "op_123", reason="Cancelled by user request"
    )
    assert result["status"] == "cancelled"
    assert result["operation_id"] == "op_123"

  @pytest.mark.unit
  async def test_cancel_not_found_raises_404(self):
    """Returns 404 when the operation to cancel does not exist."""
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = None
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      with pytest.raises(HTTPException) as exc_info:
        await cancel_operation(
          operation_id="op_missing",
          current_user=mock_user,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  async def test_cancel_access_denied_raises_403(self):
    """Returns 403 when trying to cancel another user's operation."""
    metadata = _make_mock_metadata(user_id="usr_other_user")
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user(user_id="usr_test123")

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      with pytest.raises(HTTPException) as exc_info:
        await cancel_operation(
          operation_id="op_123",
          current_user=mock_user,
        )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  async def test_cancel_completed_raises_409(self):
    """Returns 409 when trying to cancel an already-completed operation."""
    metadata = _make_mock_metadata(status=OperationStatus.COMPLETED)
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    mock_user = _make_mock_user()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      with pytest.raises(HTTPException) as exc_info:
        await cancel_operation(
          operation_id="op_123",
          current_user=mock_user,
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
class TestAwaitingInput:
  """The pause state on /status and the resume endpoint."""

  def _paused_metadata(self, task=None):
    metadata = _make_mock_metadata(status=OperationStatus.AWAITING_INPUT)
    metadata.input_request = {
      "prompt": "Post the close for 2026-07?",
      "details": {"drafts": 34},
      "checkpoint": {"step": "post"},
      "task": task
      if task is not None
      else {
        "task_type": "operator",
        "graph_id": "kg01234567890abcdef",
        "user_id": "usr_test123",
        "params": {"operator_type": "cypher"},
      },
      "requested_at": "2024-01-01T00:02:00Z",
    }
    return metadata

  @pytest.mark.unit
  async def test_status_exposes_the_prompt_but_not_the_checkpoint(self):
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = self._paused_metadata()

    p_storage, p_metrics = _make_patches(mock_storage)
    with p_storage, p_metrics:
      result = await get_operation_status(
        operation_id="op_123", current_user=_make_mock_user()
      )

    assert result["status"] == OperationStatus.AWAITING_INPUT
    assert result["message"] == "Operation is paused and waiting for input"
    assert result["input_request"] == {
      "prompt": "Post the close for 2026-07?",
      "details": {"drafts": 34},
      "requested_at": "2024-01-01T00:02:00Z",
    }
    assert result["_links"]["resume"] == "/v1/operations/op_123/resume"
    assert "cancel" in result["_links"]

  @pytest.mark.unit
  async def test_resume_records_the_answer_and_requeues_the_task(self):
    from robosystems.models.api.operations import OperationResumeRequest
    from robosystems.routers.operations import resume_operation

    metadata = self._paused_metadata()
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata
    manager = AsyncMock()

    with (
      patch(f"{MODULE}.get_event_storage", return_value=mock_storage),
      patch(f"{MODULE}.get_operation_manager", return_value=manager),
      patch(f"{MODULE}.requeue_task", AsyncMock()) as requeue,
    ):
      result = await resume_operation(
        body=OperationResumeRequest(input={"approved": True}),
        operation_id="op_123",
        current_user=_make_mock_user(),
      )

    manager.resume_operation.assert_awaited_once_with("op_123", {"approved": True})
    requeue.assert_awaited_once_with(
      "op_123",
      metadata.input_request["task"],
      resume={"checkpoint": {"step": "post"}, "input": {"approved": True}},
    )
    assert result["status"] == OperationStatus.RUNNING
    assert result["_links"]["stream"] == "/v1/operations/op_123/stream"

  @pytest.mark.unit
  async def test_resume_rejects_an_operation_that_is_not_paused(self):
    from robosystems.models.api.operations import OperationResumeRequest
    from robosystems.routers.operations import resume_operation

    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = _make_mock_metadata(
      status=OperationStatus.RUNNING
    )

    with (
      patch(f"{MODULE}.get_event_storage", return_value=mock_storage),
      patch(f"{MODULE}.requeue_task", AsyncMock()) as requeue,
      pytest.raises(HTTPException) as exc_info,
    ):
      await resume_operation(
        body=OperationResumeRequest(),
        operation_id="op_123",
        current_user=_make_mock_user(),
      )

    assert exc_info.value.status_code == 409
    requeue.assert_not_awaited()

  @pytest.mark.unit
  async def test_resume_rejects_a_pause_without_a_recorded_task(self):
    from robosystems.models.api.operations import OperationResumeRequest
    from robosystems.routers.operations import resume_operation

    metadata = self._paused_metadata()
    metadata.input_request = {"prompt": "?"}
    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = metadata

    with (
      patch(f"{MODULE}.get_event_storage", return_value=mock_storage),
      pytest.raises(HTTPException) as exc_info,
    ):
      await resume_operation(
        body=OperationResumeRequest(),
        operation_id="op_123",
        current_user=_make_mock_user(),
      )

    assert exc_info.value.status_code == 409

  @pytest.mark.unit
  async def test_resume_is_owner_only(self):
    from robosystems.models.api.operations import OperationResumeRequest
    from robosystems.routers.operations import resume_operation

    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = self._paused_metadata()

    with (
      patch(f"{MODULE}.get_event_storage", return_value=mock_storage),
      pytest.raises(HTTPException) as exc_info,
    ):
      await resume_operation(
        body=OperationResumeRequest(),
        operation_id="op_123",
        current_user=_make_mock_user(user_id="usr_other"),
      )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  async def test_resume_unknown_operation_is_404(self):
    from robosystems.models.api.operations import OperationResumeRequest
    from robosystems.routers.operations import resume_operation

    mock_storage = AsyncMock()
    mock_storage.get_operation_metadata.return_value = None

    with (
      patch(f"{MODULE}.get_event_storage", return_value=mock_storage),
      pytest.raises(HTTPException) as exc_info,
    ):
      await resume_operation(
        body=OperationResumeRequest(),
        operation_id="op_123",
        current_user=_make_mock_user(),
      )

    assert exc_info.value.status_code == 404
