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
    graph_id: str | None,
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
    raise NotImplementedError

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

  def release_lock(self, lock_key: str | None, lock_id: str | None = None) -> None:
    """Release the distributed lock the enqueuing API call took for this task.

    Safe to call with None. ``lock_id`` (defaulting to ``params["lock_id"]``)
    makes the release a compare-and-delete: a task that finishes after the
    lock's TTL lapsed and a successor re-acquired it must not strip the
    successor's lock. Only a task enqueued without a ``lock_id`` — one queued
    before the API started passing it — falls back to the unconditional
    delete, so its lock is not stranded for the full TTL.
    """
    if not lock_key:
      return

    from robosystems.logger import get_logger

    logger = get_logger(__name__)
    lock_id = lock_id or self.params.get("lock_id")

    try:
      from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
      from robosystems.middleware.auth.distributed_lock import release_lock_by_id

      redis_client = create_redis_client(ValkeyDatabase.LOCKS)
      try:
        if lock_id:
          if release_lock_by_id(redis_client, lock_key, lock_id):
            logger.debug(f"Released lock: {lock_key}")
          else:
            logger.warning(
              f"Lock {lock_key} was not released: not held by this task "
              "(expired or re-acquired by a successor)"
            )
        else:
          redis_client.delete(f"lock:{lock_key}")
          logger.debug(f"Released lock (no lock_id, unconditional): {lock_key}")
      finally:
        redis_client.close()
    except Exception as e:
      logger.warning(f"Failed to release lock {lock_key}: {e}")
