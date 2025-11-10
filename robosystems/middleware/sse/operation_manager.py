"""
Unified operation manager for SSE-based operation tracking.

This module provides high-level utilities for managing operations through
their entire lifecycle, from creation to completion or failure.
"""

import asyncio
from typing import Dict, Any, Optional, Callable, Awaitable, List
from contextlib import asynccontextmanager

from robosystems.middleware.sse.event_storage import (
  get_event_storage,
  EventType,
  OperationStatus,
  SSEEventStorage,
)
from robosystems.middleware.sse.streaming import emit_event_to_operation
from robosystems.logger import logger


class OperationManager:
  """
  High-level manager for SSE operations.

  Provides context managers and utilities for operation lifecycle management,
  making it easy for endpoints to create and track operations.
  """

  def __init__(self, event_storage: Optional[SSEEventStorage] = None):
    self.event_storage = event_storage or get_event_storage()

  async def start_operation(
    self,
    operation_type: str,
    user_id: str,
    graph_id: Optional[str] = None,
    operation_id: Optional[str] = None,
    initial_data: Optional[Dict[str, Any]] = None,
  ) -> str:
    """
    Start a new operation and emit the started event.

    Args:
        operation_type: Type of operation (e.g., "graph_creation")
        user_id: User who initiated the operation
        graph_id: Graph database ID (if applicable)
        operation_id: Custom operation ID (generates one if None)
        initial_data: Additional data to include in start event

    Returns:
        str: The operation ID
    """
    # Create operation in storage
    op_id = await self.event_storage.create_operation(
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=operation_id,
    )

    # Emit started event
    start_data = {
      "operation_type": operation_type,
      "graph_id": graph_id,
      **(initial_data or {}),
    }

    await emit_event_to_operation(op_id, EventType.OPERATION_STARTED, start_data)

    logger.info(
      f"Started operation {op_id} of type {operation_type} for user {user_id}"
    )
    return op_id

  async def emit_progress(
    self,
    operation_id: str,
    message: str,
    progress_percent: Optional[float] = None,
    details: Optional[Dict[str, Any]] = None,
  ):
    """
    Emit a progress update for an operation.

    Args:
        operation_id: Operation identifier
        message: Human-readable progress message
        progress_percent: Completion percentage (0-100)
        details: Additional progress details
    """
    progress_data = {
      "message": message,
      "progress_percent": progress_percent,
      **(details or {}),
    }

    await emit_event_to_operation(
      operation_id, EventType.OPERATION_PROGRESS, progress_data
    )

  async def complete_operation(
    self,
    operation_id: str,
    result: Optional[Dict[str, Any]] = None,
    message: str = "Operation completed successfully",
  ):
    """
    Mark an operation as completed.

    Args:
        operation_id: Operation identifier
        result: Result data to store
        message: Completion message
    """
    completion_data = {"message": message, "result": result}

    await emit_event_to_operation(
      operation_id, EventType.OPERATION_COMPLETED, completion_data
    )

    logger.info(f"Completed operation {operation_id}")

  async def fail_operation(
    self, operation_id: str, error: str, error_details: Optional[Dict[str, Any]] = None
  ):
    """
    Mark an operation as failed.

    Args:
        operation_id: Operation identifier
        error: Error message
        error_details: Additional error details
    """
    error_data = {"error": error, "error_details": error_details}

    await emit_event_to_operation(operation_id, EventType.OPERATION_ERROR, error_data)

    logger.error(f"Failed operation {operation_id}: {error}")

  async def cancel_operation(
    self, operation_id: str, reason: str = "Cancelled by user"
  ):
    """
    Cancel an operation.

    Args:
        operation_id: Operation identifier
        reason: Cancellation reason
    """
    await self.event_storage.cancel_operation(operation_id, reason)
    logger.info(f"Cancelled operation {operation_id}: {reason}")

  async def get_operation_status(self, operation_id: str) -> Optional[OperationStatus]:
    """Get current status of an operation."""
    metadata = await self.event_storage.get_operation_metadata(operation_id)
    return metadata.status if metadata else None

  @asynccontextmanager
  async def operation_context(
    self,
    operation_type: str,
    user_id: str,
    graph_id: Optional[str] = None,
    operation_id: Optional[str] = None,
    initial_data: Optional[Dict[str, Any]] = None,
  ):
    """
    Context manager for operation lifecycle management.

    Automatically handles operation creation, error catching, and cleanup.

    Usage:
        async with manager.operation_context("graph_creation", user_id) as op_id:
            # Do work, emit progress events
            await manager.emit_progress(op_id, "Creating nodes...")
            # Operation is automatically completed on success
            # or failed on exception

    Args:
        operation_type: Type of operation
        user_id: User who initiated the operation
        graph_id: Graph database ID (if applicable)
        operation_id: Custom operation ID (generates one if None)
        initial_data: Additional data for start event

    Yields:
        str: The operation ID
    """
    op_id = None
    try:
      # Start operation
      op_id = await self.start_operation(
        operation_type=operation_type,
        user_id=user_id,
        graph_id=graph_id,
        operation_id=operation_id,
        initial_data=initial_data,
      )

      # Yield operation ID to calling code
      yield op_id

      # If we get here, operation succeeded
      await self.complete_operation(op_id)

    except asyncio.CancelledError:
      # Operation was cancelled
      if op_id:
        await self.cancel_operation(op_id, "Operation cancelled")
      raise

    except Exception as e:
      # Operation failed
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
    graph_id: Optional[str] = None,
    operation_id: Optional[str] = None,
    initial_data: Optional[Dict[str, Any]] = None,
  ) -> str:
    """
    Run an operation function with automatic lifecycle management.

    This is a convenience method that wraps an operation function
    with full lifecycle management.

    Args:
        operation_type: Type of operation
        user_id: User who initiated the operation
        operation_func: Async function that performs the operation
        graph_id: Graph database ID (if applicable)
        operation_id: Custom operation ID (generates one if None)
        initial_data: Additional data for start event

    Returns:
        str: The operation ID

    Example:
        async def create_graph(operation_id: str):
            manager = get_operation_manager()
            await manager.emit_progress(operation_id, "Creating nodes...")
            # ... do work

        operation_id = await manager.run_operation(
            "graph_creation",
            user_id,
            create_graph
        )
    """
    async with self.operation_context(
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=operation_id,
      initial_data=initial_data,
    ) as op_id:
      # Run the operation function
      await operation_func(op_id)

    return op_id

  async def batch_operation(
    self,
    operation_type: str,
    user_id: str,
    operations: List[Callable[[str, int], Awaitable[Any]]],
    graph_id: Optional[str] = None,
    operation_id: Optional[str] = None,
  ) -> str:
    """
    Run a batch of operations with combined progress tracking.

    Args:
        operation_type: Type of batch operation
        user_id: User who initiated the operation
        operations: List of operation functions
        graph_id: Graph database ID (if applicable)
        operation_id: Custom operation ID (generates one if None)

    Returns:
        str: The operation ID
    """
    async with self.operation_context(
      operation_type=operation_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=operation_id,
      initial_data={"total_operations": len(operations)},
    ) as op_id:
      for i, operation_func in enumerate(operations):
        # Update progress
        progress_percent = (i / len(operations)) * 100
        await self.emit_progress(
          op_id,
          f"Processing operation {i + 1} of {len(operations)}",
          progress_percent=progress_percent,
        )

        # Run operation
        await operation_func(op_id, i)

      # Final progress update
      await self.emit_progress(op_id, "All operations completed", progress_percent=100)

    return op_id


# Global instance
_operation_manager: Optional[OperationManager] = None


def get_operation_manager() -> OperationManager:
  """Get the global operation manager instance."""
  global _operation_manager
  if _operation_manager is None:
    _operation_manager = OperationManager()
  return _operation_manager


def emit_sse_event(
  operation_id: str,
  status: OperationStatus,
  data: Optional[Dict[str, Any]] = None,
  message: Optional[str] = None,
  progress_percentage: Optional[float] = None,
):
  """
  Compatibility wrapper for emitting SSE events.

  This function provides backward compatibility for code that uses the old
  emit_sse_event interface. It maps OperationStatus to EventType and
  calls emit_event_to_operation.

  Args:
      operation_id: Operation identifier
      status: Operation status (mapped to event type)
      data: Event data dictionary
      message: Optional message
      progress_percentage: Optional progress percentage
  """
  from robosystems.middleware.sse.streaming import emit_event_to_operation

  # Map OperationStatus to EventType
  status_to_event_type = {
    OperationStatus.PENDING: EventType.OPERATION_STARTED,
    OperationStatus.IN_PROGRESS: EventType.OPERATION_PROGRESS,
    OperationStatus.COMPLETED: EventType.OPERATION_COMPLETED,
    OperationStatus.FAILED: EventType.OPERATION_ERROR,
    OperationStatus.ERROR: EventType.OPERATION_ERROR,
    OperationStatus.CANCELLED: EventType.OPERATION_CANCELLED,
  }

  event_type = status_to_event_type.get(status, EventType.OPERATION_PROGRESS)

  # Merge message and progress_percentage into data
  event_data = data.copy() if data else {}
  if message:
    event_data["message"] = message
  if progress_percentage is not None:
    event_data["progress_percentage"] = progress_percentage

  # Call emit_event_to_operation - handle both async and sync contexts
  try:
    # Try to get existing event loop
    loop = asyncio.get_event_loop()
    if loop.is_running():
      # We're in an async context, schedule as task
      asyncio.create_task(emit_event_to_operation(operation_id, event_type, event_data))
    else:
      # Run in the existing loop
      loop.run_until_complete(
        emit_event_to_operation(operation_id, event_type, event_data)
      )
  except RuntimeError:
    # No event loop, create a new one (sync context)
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
  graph_id: Optional[str] = None,
  operation_id: Optional[str] = None,
) -> Dict[str, Any]:
  """
  Create a new operation and return the standardized response.

  This is what endpoints should return when starting a new operation.

  Args:
      operation_type: Type of operation
      user_id: User identifier
      graph_id: Graph database ID (if applicable)
      operation_id: Custom operation ID (generates one if None)

  Returns:
      Dict containing operation details and SSE endpoint
  """
  manager = get_operation_manager()

  # Create operation (but don't start it yet - that's for the actual worker)
  op_id = await manager.event_storage.create_operation(
    operation_type=operation_type,
    user_id=user_id,
    graph_id=graph_id,
    operation_id=operation_id,
  )

  # Get metadata for response
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
