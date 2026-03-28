"""Base class for all worker task handlers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from robosystems.middleware.sse.event_storage import OperationStatus
from robosystems.middleware.sse.operation_manager import OperationManager


class BaseTask(ABC):
  """Base class for all worker tasks.

  Subclasses must implement execute() and return a result dict.
  Use report_progress() for SSE updates and is_cancelled() to
  check for user-initiated cancellation between steps.
  """

  def __init__(
    self,
    task_id: str,
    graph_id: str,
    user_id: str,
    params: dict[str, Any],
    manager: OperationManager,
  ) -> None:
    self.task_id = task_id
    self.graph_id = graph_id
    self.user_id = user_id
    self.params = params
    self.manager = manager

  @abstractmethod
  async def execute(self) -> dict[str, Any]:
    """Execute the task. Must return a result dict."""
    ...

  async def report_progress(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    """Emit a progress event to the SSE stream."""
    await self.manager.emit_progress(
      self.task_id,
      message=message,
      progress_percent=percent,
      details=details,
    )

  async def is_cancelled(self) -> bool:
    """Check if the user has requested cancellation."""
    status = await self.manager.get_operation_status(self.task_id)
    return status == OperationStatus.CANCELLED
