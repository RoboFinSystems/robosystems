"""Progress reporters — one per execution context.

`ProgressReporter` is a protocol defined in `operator_context.py`.

- `CallbackProgress`: API context (wraps a callback function)
- `OperationManagerProgress`: worker context (wraps the SSE OperationManager)
- `NoOpProgress`: tests and contexts with nowhere to report
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from collections.abc import Callable

  from robosystems.middleware.sse.operation_manager import OperationManager


class CallbackProgress:
  """API context. Invokes the caller's `callback(stage, percent, message)`.

  Cannot report cancellation — a sync request has no channel for it, so
  `is_cancelled` is always False.
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
  """Worker context. Emits SSE progress events via the OperationManager.

  `is_cancelled` reads the operation's live status, so a long-running operator
  that polls between steps can stop when the client cancels.
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
  """Discards progress and never reports cancellation."""

  async def report(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    pass

  async def is_cancelled(self) -> bool:
    return False
