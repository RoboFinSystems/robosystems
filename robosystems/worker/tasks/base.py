"""Base class for all worker task handlers."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar

from robosystems.logger import get_logger
from robosystems.middleware.sse.event_storage import OperationStatus
from robosystems.middleware.sse.operation_manager import OperationManager
from robosystems.worker.constants import DEFAULT_TASK_TIMEOUT, TASK_TIMEOUTS

logger = get_logger(__name__)


class TaskPaused(Exception):
  """Raised by ``BaseTask.pause_for_input`` to leave ``execute`` without a
  result: the consumer neither completes nor fails the operation, and the
  resume endpoint puts it back on the queue with the answer."""

  def __init__(self, prompt: str) -> None:
    super().__init__(prompt)
    self.prompt = prompt


class BaseTask(ABC):
  """Base class for all worker tasks.

  Subclasses must implement execute() and return a result dict.
  Use report_progress() for SSE updates and is_cancelled() to
  check for user-initiated cancellation between steps. Sync work that
  blocks — database and network calls — goes through ``run_blocking``,
  which is what keeps the consumer's budget honest for a thread.
  """

  # Stamped by ``@register_task``: the key the consumer sizes this task's
  # budget by, so a handler can derive the waits it makes from that budget
  # rather than from a constant that knows nothing about it. None on a
  # subclass that was never registered.
  task_type: ClassVar[str | None] = None

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
    self._abandoned: list[asyncio.Future[Any]] = []

  @abstractmethod
  async def execute(self) -> dict[str, Any]:
    """Execute the task. Must return a result dict."""
    raise NotImplementedError

  @property
  def budget_seconds(self) -> int:
    """The consumer's budget for this task type, from ``TASK_TIMEOUTS``."""
    return TASK_TIMEOUTS.get(self.task_type or "", DEFAULT_TASK_TIMEOUT)

  @property
  def abandoned_work(self) -> list[asyncio.Future[Any]]:
    """Blocking work still running after the budget and its grace expired."""
    return [work for work in self._abandoned if not work.done()]

  async def run_blocking(self, func: Callable[..., Any], *args: Any) -> Any:
    """Run sync work — database and network calls — in a thread.

    The consumer enforces the task budget with ``asyncio.wait_for``, which
    cancels this coroutine and cannot cancel the thread. Left alone, that
    reports the operation FAILED while the thread runs on and can still
    commit: a close that landed, described to the operator as one that did
    not. So the thread is shielded, and an expired budget becomes a bounded
    wait for it — one more budget of grace. If the thread lands inside
    that, its outcome is the task's outcome and the overrun is logged; only
    past it is the work abandoned (tracked in ``abandoned_work``, so the
    consumer can say so) and the timeout let through.

    While this waits, the consumer's ``finally`` has not run: scale-in
    protection stays on and the engines are not disposed, so a slow close is
    not exposed to scale-in mid-publish and is not handed a second tenant's
    task while its thread is still on the first.
    """
    work = asyncio.ensure_future(asyncio.to_thread(func, *args))
    try:
      return await asyncio.shield(work)
    except asyncio.CancelledError:
      if work.done() and not work.cancelled():
        return work.result()
      grace = self.budget_seconds
      logger.warning(
        f"Task {self.task_id} ({self.task_type}) ran out of budget with blocking "
        f"work still running; waiting up to {grace}s more for it to finish"
      )
      try:
        done, _ = await asyncio.wait({work}, timeout=grace)
      except asyncio.CancelledError:
        self._abandon(work)
        raise
      if work in done:
        logger.warning(
          f"Task {self.task_id} ({self.task_type}) finished its blocking work "
          "past its budget; reporting that outcome rather than a timeout"
        )
        return work.result()
      self._abandon(work)
      raise

  def _abandon(self, work: asyncio.Future[Any]) -> None:
    self._abandoned.append(work)
    work.add_done_callback(self._log_abandoned_outcome)
    logger.error(
      f"Task {self.task_id} ({self.task_type}) abandoned blocking work after its "
      f"budget and {self.budget_seconds}s grace; the thread is still running "
      "and may still commit"
    )

  def _log_abandoned_outcome(self, work: asyncio.Future[Any]) -> None:
    # Nothing awaits an abandoned future, so its outcome would otherwise be
    # dropped (or surface as "exception was never retrieved" on stderr).
    if work.cancelled():
      return
    exc = work.exception()
    if exc is None:
      logger.warning(
        f"Task {self.task_id} ({self.task_type}) abandoned blocking work "
        "finished after the operation was already reported as timed out"
      )
    else:
      logger.warning(
        f"Task {self.task_id} ({self.task_type}) abandoned blocking work "
        f"failed after the operation was reported as timed out: "
        f"{type(exc).__name__}: {exc}"
      )

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

  @property
  def resume(self) -> dict[str, Any] | None:
    """The answer a paused run was resumed with: ``{"checkpoint", "input"}``.

    None on a first run. A task that pauses reads its own checkpoint back
    from here and continues from it rather than starting over.
    """
    return self.params.get("resume")

  async def pause_for_input(
    self,
    prompt: str,
    checkpoint: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    """Stop at a checkpoint and wait for a human decision.

    Records the prompt, ``checkpoint`` and this task's queue payload on the
    operation (status ``awaiting_input``), then raises ``TaskPaused`` so
    ``execute`` unwinds without a result. ``POST /v1/operations/{id}/resume``
    re-enqueues the same operation with ``params["resume"]`` set.

    The checkpoint must be JSON-serializable and must not carry the original
    ``resume`` (a second pause records a fresh one).
    """
    params = {key: value for key, value in self.params.items() if key != "resume"}
    await self.manager.await_input(
      self.task_id,
      prompt=prompt,
      checkpoint=checkpoint,
      details=details,
      task={
        "task_type": self.task_type,
        "graph_id": self.graph_id,
        "user_id": self.user_id,
        "params": params,
      },
    )
    raise TaskPaused(prompt)

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
