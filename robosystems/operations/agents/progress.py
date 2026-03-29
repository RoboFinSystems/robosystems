"""Progress reporter implementations for different execution contexts.

ProgressReporter is a protocol (defined in agent_context.py). Each execution
context provides an implementation:

- CallbackProgress: API context (wraps the callback function pattern)
- OperationManagerProgress: Worker context (wraps the SSE OperationManager)
- NoOpProgress: Tests and contexts where progress isn't needed
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from collections.abc import Callable

  from robosystems.middleware.sse.operation_manager import OperationManager


class CallbackProgress:
  """For API context. Wraps the existing callback function pattern.

  The callback signature matches what the existing orchestrator/handlers
  expect: callback(stage, percent, message).
  """

  def __init__(self, callback: Callable | None = None) -> None:
    self._callback = callback

  async def report(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    if self._callback:
      self._callback(message, percent, message)

  async def is_cancelled(self) -> bool:
    return False


class OperationManagerProgress:
  """For worker context. Wraps the existing SSE OperationManager.

  Emits progress events that the frontend can stream via SSE.
  Supports cancellation checking between processing steps.
  """

  def __init__(self, task_id: str, manager: OperationManager) -> None:
    self._task_id = task_id
    self._manager = manager

  async def report(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    await self._manager.emit_progress(
      self._task_id,
      message=message,
      progress_percent=percent,
      details=details,
    )

  async def is_cancelled(self) -> bool:
    from robosystems.middleware.sse.event_storage import OperationStatus

    status = await self._manager.get_operation_status(self._task_id)
    return status == OperationStatus.CANCELLED


class NoOpProgress:
  """For tests or contexts where progress reporting isn't needed."""

  async def report(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    pass

  async def is_cancelled(self) -> bool:
    return False
