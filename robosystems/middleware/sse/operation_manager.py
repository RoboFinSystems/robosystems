"""Operation lifecycle management on top of SSE event storage.

Wraps `SSEEventStorage` so callers emit start/progress/complete/fail
events without hand-rolling the status transitions.
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.sse.event_storage import (
  EventType,
  OperationStatus,
  SSEEventStorage,
  get_event_storage,
)


class OperationManager:
  """Context managers and helpers for creating and tracking SSE operations."""

  def __init__(self, event_storage: SSEEventStorage | None = None):
    self.event_storage = event_storage or get_event_storage()

  async def start_operation(
    self,
    operation_type: str,
    user_id: str,
    graph_id: str | None = None,
    operation_id: str | None = None,
    initial_data: dict[str, Any] | None = None,
  ) -> str:
    """Register an operation and emit its started event; returns the ID."""
    op_id = await self.event_storage.create_operation(
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=operation_id,
    )

    # Moves status to RUNNING and broadcasts over pub/sub.
    start_data = {
      "operation_type": operation_type,
      "graph_id": graph_id,
      **(initial_data or {}),
    }

    await self.event_storage.store_event(op_id, EventType.OPERATION_STARTED, start_data)

    logger.info(
      f"Started operation {op_id} of type {operation_type} for user {user_id}"
    )
    return op_id

  async def mark_running(self, operation_id: str, message: str = "Starting..."):
    """Move a queued operation to RUNNING when a worker picks it up.

    Queued work is registered PENDING by `create_operation_response` and,
    until this event, nothing distinguished "waiting in the queue" from
    "executing" — `/status` reported pending for the whole run. The started
    event carries the first progress frame so streams see it as one.
    """
    await self.event_storage.store_event(
      operation_id,
      EventType.OPERATION_STARTED,
      {"message": message, "progress_percent": 0},
    )

  async def emit_progress(
    self,
    operation_id: str,
    message: str,
    progress_percent: float | None = None,
    details: dict[str, Any] | None = None,
  ):
    """Emit a progress update. `progress_percent` is 0-100."""
    progress_data = {
      "message": message,
      "progress_percent": progress_percent,
      **(details or {}),
    }

    await self.event_storage.store_event(
      operation_id, EventType.OPERATION_PROGRESS, progress_data
    )

  async def complete_operation(
    self,
    operation_id: str,
    result: dict[str, Any] | None = None,
    message: str = "Operation completed successfully",
  ):
    """Mark an operation completed, storing `result` in its metadata."""
    completion_data = {"message": message, "result": result}

    await self.event_storage.store_event(
      operation_id, EventType.OPERATION_COMPLETED, completion_data
    )

    logger.info(f"Completed operation {operation_id}")

  async def fail_operation(
    self, operation_id: str, error: str, error_details: dict[str, Any] | None = None
  ):
    """Mark an operation failed with `error` as the recorded message."""
    error_data = {"error": error, "error_details": error_details}

    await self.event_storage.store_event(
      operation_id, EventType.OPERATION_ERROR, error_data
    )

    logger.error(f"Failed operation {operation_id}: {error}")

  async def cancel_operation(
    self, operation_id: str, reason: str = "Cancelled by user"
  ):
    """Cancel an operation."""
    await self.event_storage.cancel_operation(operation_id, reason)
    logger.info(f"Cancelled operation {operation_id}: {reason}")

  async def await_input(
    self,
    operation_id: str,
    prompt: str,
    task: dict[str, Any],
    checkpoint: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
  ):
    """Pause an operation at a checkpoint until a human answers.

    `task` is the queue payload (task_type, graph_id, user_id, params) the
    resume needs to re-enqueue the run; `checkpoint` is whatever the task
    needs to pick up where it stopped. Both travel back to the task under
    `params["resume"]` — the operation store is the only place they live
    while the run is off the queue.
    """
    input_request = {
      "prompt": prompt,
      "details": details or {},
      "checkpoint": checkpoint or {},
      "task": task,
      "requested_at": datetime.now(UTC).isoformat(),
    }
    await self.event_storage.store_event(
      operation_id,
      EventType.OPERATION_AWAITING_INPUT,
      {"message": prompt, "details": details or {}, "input_request": input_request},
    )
    logger.info(f"Operation {operation_id} is awaiting input: {prompt}")

  async def resume_operation(self, operation_id: str, user_input: dict[str, Any]):
    """Record the answer and move the operation back to RUNNING.

    Called after the task payload is back on the queue: a failed push then
    leaves the pause — and its resume link — in place for a retry, whereas
    flipping the status first would strand an unqueued operation as RUNNING.
    """
    await self.event_storage.store_event(
      operation_id,
      EventType.OPERATION_RESUMED,
      {"message": "Resumed with input", "input": user_input},
    )
    logger.info(f"Resumed operation {operation_id}")

  async def get_operation_status(self, operation_id: str) -> OperationStatus | None:
    """Get current status of an operation."""
    metadata = await self.event_storage.get_operation_metadata(operation_id)
    return metadata.status if metadata else None

  @asynccontextmanager
  async def operation_context(
    self,
    operation_type: str,
    user_id: str,
    graph_id: str | None = None,
    operation_id: str | None = None,
    initial_data: dict[str, Any] | None = None,
  ):
    """Run a block as a tracked operation, yielding the operation ID.

    Completes the operation on normal exit, fails it on exception, and
    cancels it on `asyncio.CancelledError` — always re-raising.

        async with manager.operation_context("graph_creation", user_id) as op_id:
            await manager.emit_progress(op_id, "Creating nodes...")
    """
    op_id = None
    try:
      op_id = await self.start_operation(
        operation_type=operation_type,
        user_id=user_id,
        graph_id=graph_id,
        operation_id=operation_id,
        initial_data=initial_data,
      )

      yield op_id

      await self.complete_operation(op_id)

    except asyncio.CancelledError:
      if op_id:
        await self.cancel_operation(op_id, "Operation cancelled")
      raise

    except Exception as e:
      if op_id:
        await self.fail_operation(
          op_id, error=str(e), error_details={"error_type": type(e).__name__}
        )
      raise

  async def run_operation(
    self,
    operation_type: str,
    user_id: str,
    operation_func: Callable[[str], Awaitable[Any]],
    graph_id: str | None = None,
    operation_id: str | None = None,
    initial_data: dict[str, Any] | None = None,
  ) -> str:
    """Run `operation_func` inside `operation_context`, returning its ID.

    async def create_graph(operation_id: str):
        manager = get_operation_manager()
        await manager.emit_progress(operation_id, "Creating nodes...")

    operation_id = await manager.run_operation(
        "graph_creation", user_id, create_graph
    )
    """
    async with self.operation_context(
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=operation_id,
      initial_data=initial_data,
    ) as op_id:
      await operation_func(op_id)

    return op_id

  async def batch_operation(
    self,
    operation_type: str,
    user_id: str,
    operations: list[Callable[[str, int], Awaitable[Any]]],
    graph_id: str | None = None,
    operation_id: str | None = None,
  ) -> str:
    """Run `operations` in sequence under one operation ID, emitting a
    progress event before each.
    """
    async with self.operation_context(
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=operation_id,
      initial_data={"total_operations": len(operations)},
    ) as op_id:
      for i, operation_func in enumerate(operations):
        progress_percent = (i / len(operations)) * 100
        await self.emit_progress(
          op_id,
          f"Processing operation {i + 1} of {len(operations)}",
          progress_percent=progress_percent,
        )

        await operation_func(op_id, i)

      await self.emit_progress(op_id, "All operations completed", progress_percent=100)

    return op_id


# Global instance
_operation_manager: OperationManager | None = None


def get_operation_manager() -> OperationManager:
  """Get the global operation manager instance."""
  global _operation_manager
  if _operation_manager is None:
    _operation_manager = OperationManager()
  return _operation_manager


def emit_sse_event(
  operation_id: str,
  status: OperationStatus,
  data: dict[str, Any] | None = None,
  message: str | None = None,
  progress_percentage: float | None = None,
):
  """Emit an SSE event from an `OperationStatus` rather than an `EventType`.

  Usable from sync code: it dispatches onto a running loop when there is
  one and otherwise spins up a short-lived loop.
  """
  from robosystems.middleware.sse.streaming import emit_event_to_operation

  status_to_event_type = {
    OperationStatus.PENDING: EventType.OPERATION_STARTED,
    OperationStatus.RUNNING: EventType.OPERATION_PROGRESS,
    OperationStatus.COMPLETED: EventType.OPERATION_COMPLETED,
    OperationStatus.FAILED: EventType.OPERATION_ERROR,
    OperationStatus.CANCELLED: EventType.OPERATION_CANCELLED,
    OperationStatus.AWAITING_INPUT: EventType.OPERATION_AWAITING_INPUT,
  }

  event_type = status_to_event_type.get(status, EventType.OPERATION_PROGRESS)

  event_data = data.copy() if data else {}
  if message:
    event_data["message"] = message
  if progress_percentage is not None:
    event_data["progress_percentage"] = progress_percentage

  try:
    loop = asyncio.get_event_loop()
    if loop.is_running():
      asyncio.create_task(emit_event_to_operation(operation_id, event_type, event_data))
    else:
      loop.run_until_complete(
        emit_event_to_operation(operation_id, event_type, event_data)
      )
  except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
      loop.run_until_complete(
        emit_event_to_operation(operation_id, event_type, event_data)
      )
    finally:
      loop.close()
      asyncio.set_event_loop(None)


async def create_operation_response(
  operation_type: str,
  user_id: str,
  graph_id: str | None = None,
  operation_id: str | None = None,
) -> dict[str, Any]:
  """Register an operation and return the response body endpoints should
  hand back when queueing work.

  The operation is created but not started; the worker that picks it up
  emits the started event. The returned `_links` point at the stream,
  status, and cancel endpoints in `routers/operations.py`.
  """
  manager = get_operation_manager()

  op_id = await manager.event_storage.create_operation(
    operation_type=operation_type,
    user_id=user_id,
    graph_id=graph_id,
    operation_id=operation_id,
  )

  metadata = await manager.event_storage.get_operation_metadata(op_id)

  return {
    "operation_id": op_id,
    "status": "pending",
    "operation_type": operation_type,
    "created_at": metadata.created_at if metadata else None,
    "graph_id": graph_id,
    "_links": {
      "stream": f"/v1/operations/{op_id}/stream",
      "status": f"/v1/operations/{op_id}/status",
      "cancel": f"/v1/operations/{op_id}",
    },
    "message": f"Operation {operation_type} queued. Connect to stream endpoint for real-time updates.",
  }
