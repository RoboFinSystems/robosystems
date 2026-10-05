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
  """Base class for worker tasks: implement execute() returning a result dict.

  Blocking sync work (database, network) must go through ``run_blocking``.
  """

  # Stamped by ``@register_task``; None when unregistered.
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
    # What landed before the task stopped; the consumer adds it to a failure.
    self.partial_result: dict[str, Any] = {}

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
    """Run sync work in a thread, shielded from the budget's cancellation.

    ``wait_for`` cannot cancel a thread, which could commit after the
    operation was reported FAILED. So an expired budget waits one more budget
    for the thread; if it lands, its outcome stands. Past that grace the work
    is abandoned (``abandoned_work``) and the timeout propagates. Meanwhile
    scale-in protection stays on and the engines are not disposed.
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
    # Nothing awaits an abandoned future; log its outcome here.
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

  def release_lock(self) -> None:
    """Release the graph lock the enqueuing API call took for this task."""
    release_task_lock(self.graph_id, self.params)


def release_task_lock(graph_id: str | None, params: dict[str, Any]) -> None:
  """Release the graph lock an enqueuing API call took for a task, if any.

  ``materialization_lock_token`` is the per-graph materialization lock. The
  ``lock_key``/``lock_id`` pair is the key it replaced, still carried by tasks
  queued before the switch. Both releases compare-and-delete, so a task that
  finishes after its lock lapsed cannot strip a successor's. Never raises.
  """
  token = params.get("materialization_lock_token")
  lock_key = params.get("lock_key")
  if not (token and graph_id) and not lock_key:
    return

  try:
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

    redis_client = create_redis_client(ValkeyDatabase.LOCKS)
  except Exception as e:
    logger.warning(f"Failed to release the lock for a task on {graph_id}: {e}")
    return

  try:
    if token and graph_id:
      from robosystems.graph_api.core.ladybug.materialization_lock import (
        release_token,
      )

      if not release_token(redis_client, graph_id, token):
        logger.warning(
          f"Materialization lock for {graph_id} was not released: not held by "
          "this task (expired or re-acquired by a successor)"
        )
    if lock_key:
      _release_legacy_lock(redis_client, lock_key, params.get("lock_id"))
  finally:
    redis_client.close()


def _release_legacy_lock(redis_client: Any, lock_key: str, lock_id: str | None) -> None:
  from robosystems.middleware.auth.distributed_lock import release_lock_by_id

  try:
    if lock_id:
      if not release_lock_by_id(redis_client, lock_key, lock_id):
        logger.warning(
          f"Lock {lock_key} was not released: not held by this task "
          "(expired or re-acquired by a successor)"
        )
    else:
      redis_client.delete(f"lock:{lock_key}")
  except Exception as e:
    logger.warning(f"Failed to release lock {lock_key}: {e}")
