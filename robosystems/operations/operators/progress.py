"""`ProgressReporter` implementations, one per execution context."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from robosystems.middleware.sse.operation_manager import OperationManager


class OperationManagerProgress:
  """Worker context: SSE progress via the OperationManager; `is_cancelled`
  reads the operation's live status."""

  def __init__(self, task_id: str, manager: OperationManager) -> None:
    self._task_id = task_id
    self._manager = manager

  async def report(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    # The SDK facades read `progress_percentage`; the manager writes
    # `progress_percent`. Carry both.
    await self._manager.emit_progress(
      self._task_id,
      message=message,
      progress_percent=percent,
      details={"progress_percentage": percent, **(details or {})},
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
