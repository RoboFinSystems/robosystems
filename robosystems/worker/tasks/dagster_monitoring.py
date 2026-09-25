"""Worker task handler for monitoring Dagster job execution.

Submits a Dagster job and polls its status, relaying progress to SSE.
This is an orchestration task — the worker doesn't do the actual work,
it monitors a remote operation (Graph API backup/restore, Dagster
materialization) and keeps the SSE stream alive even if API containers
scale in.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import get_logger
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

logger = get_logger(__name__)


@register_task("dagster_job_monitor")
class DagsterJobMonitorTask(BaseTask):
  """Submit a Dagster job and monitor its progress, relaying to SSE.

  Params: ``job_name`` (required), plus optional ``run_config``, ``tags``,
  ``pass_operation_id`` (for jobs whose ops accept ``operation_id``) and
  ``lock_key``. Once a run is submitted, the lock is released only after
  Dagster reports the run stopped. Any other exit (a status it cannot read, a
  budget cancel, a cancel the run does not confirm) leaves the lock to its
  TTL: releasing then would let a second run write alongside this one.

  The run id is recorded against the task before polling, so a monitor
  requeued after its worker died reattaches to that run rather than
  submitting a second one. While the run lives, the lock's TTL is kept fresh.
  """

  CANCEL_SETTLE_SECONDS = 120
  # Status reads can fail for a while (the webserver restarts on every
  # deploy); keep polling through that rather than give up on the run.
  STATUS_READ_GRACE_SECONDS = 600
  LOCK_EXTEND_INTERVAL_SECONDS = 60
  RUN_RECORD_TTL_SECONDS = 2 * 86400
  SUBMITTING = "submitting"

  async def execute(self) -> dict[str, Any]:
    import asyncio

    from robosystems.middleware.sse.dagster_monitor import DagsterRunMonitor

    job_name = self.params["job_name"]
    run_config = self.params.get("run_config")
    tags = self.params.get("tags")
    if self.params.get("pass_operation_id") and run_config:
      # The job reports its result to this operation; the id only exists
      # once the task is enqueued. Opt-in: Dagster refuses unknown config keys.
      for op in run_config.get("ops", {}).values():
        op.setdefault("config", {})["operation_id"] = self.task_id
    lock_key = self.params.get("lock_key")

    monitor = DagsterRunMonitor()
    # Nothing is released until this attempt knows no earlier one left a run.
    release = False
    record = self._run_record()

    try:
      prior = await record.get()
      if prior == self.SUBMITTING:
        raise RuntimeError(
          f"An earlier attempt may have started {job_name}; not submitting it again"
        )
      if prior:
        run_id = prior
        await self.report_progress(f"Reattached to {job_name}", percent=5)
      else:
        await record.set(self.SUBMITTING)
        try:
          run_id = await asyncio.to_thread(
            monitor.submit_job, job_name, run_config, tags
          )
        except Exception:
          # A refused submit started nothing. A cancel mid-submit may have,
          # so it keeps both the lock and the marker.
          await record.delete()
          release = True
          raise
        await record.set(run_id)
        await self.report_progress(f"Submitted {job_name}", percent=5)

      unreadable_since: float | None = None
      extended_at = asyncio.get_running_loop().time()

      # Our own poll loop (not DagsterRunMonitor.monitor_run) so a user can
      # cancel a long monitor between iterations.
      while True:
        if await self.is_cancelled():
          logger.info(f"Dagster job monitor cancelled: {job_name} (run_id={run_id})")
          release = await self._stop_run(monitor, run_id)
          return {"status": "cancelled", "run_id": run_id, "job_name": job_name}

        try:
          status_info = await asyncio.to_thread(monitor.get_run_status, run_id)
        except Exception as e:
          now = asyncio.get_running_loop().time()
          unreadable_since = unreadable_since or now
          if now - unreadable_since > self.STATUS_READ_GRACE_SECONDS:
            logger.error(
              f"Dagster run {run_id} unreadable for "
              f"{self.STATUS_READ_GRACE_SECONDS}s; leaving its lock to expire"
            )
            raise
          logger.warning(f"Reading Dagster run {run_id} status failed: {e}")
          await asyncio.sleep(monitor.poll_interval)
          continue
        unreadable_since = None
        current_status = status_info["status"]
        if current_status in ("completed", "failed", "cancelled"):
          release = True

        if current_status == "completed":
          await monitor.emit_completion(self.task_id, status_info)
          logger.info(f"Dagster job {job_name} completed: run_id={run_id}")
          return status_info

        if current_status == "failed":
          await monitor.emit_error(
            self.task_id, f"Dagster job {job_name} failed", status_info
          )
          return status_info

        if current_status == "cancelled":
          await monitor.emit_error(
            self.task_id, f"Dagster job {job_name} was cancelled", status_info
          )
          return status_info

        now = asyncio.get_running_loop().time()
        if lock_key and now - extended_at >= self.LOCK_EXTEND_INTERVAL_SECONDS:
          await asyncio.to_thread(self._extend_lock, lock_key)
          extended_at = now

        # Emit progress on status change
        await self.report_progress(
          f"{job_name}: {status_info.get('dagster_status', current_status)}",
          percent=status_info.get("progress_percent"),
        )

        await asyncio.sleep(monitor.poll_interval)

    finally:
      if release:
        self.release_lock(lock_key)

  def _run_record(self) -> _RunRecord:
    return _RunRecord(f"worker:dagster_run:{self.task_id}", self.RUN_RECORD_TTL_SECONDS)

  def _extend_lock(self, lock_key: str) -> None:
    from robosystems.config.constants import INGESTION_LOCK_TTL
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
    from robosystems.middleware.auth.distributed_lock import extend_lock_by_id

    lock_id = self.params.get("lock_id")
    if not lock_id:
      return
    client = create_redis_client(ValkeyDatabase.LOCKS)
    try:
      if not extend_lock_by_id(client, lock_key, lock_id, INGESTION_LOCK_TTL):
        logger.warning(f"Lock {lock_key} is no longer held by this run's task")
    finally:
      client.close()

  async def _stop_run(self, monitor: Any, run_id: str) -> bool:
    """Terminate the run and wait for Dagster to report it stopped.

    True once it has; False if it is still running, or its state could not be
    read, after the settle window.
    """
    import asyncio

    try:
      await asyncio.to_thread(monitor.terminate_run, run_id)
    except Exception as e:
      logger.warning(f"Terminating Dagster run {run_id} failed: {e}")
    waited = 0.0
    while waited < self.CANCEL_SETTLE_SECONDS:
      try:
        status = (await asyncio.to_thread(monitor.get_run_status, run_id))["status"]
      except Exception as e:
        logger.warning(f"Reading Dagster run {run_id} status failed: {e}")
        status = None
      if status in ("completed", "failed", "cancelled"):
        return True
      await asyncio.sleep(monitor.poll_interval)
      waited += monitor.poll_interval
    logger.error(
      f"Dagster run {run_id} still running {self.CANCEL_SETTLE_SECONDS}s after "
      "cancel; leaving its lock to expire rather than release it under a writer"
    )
    return False


class _RunRecord:
  """The Dagster run a monitor task submitted, keyed by the task's id."""

  def __init__(self, key: str, ttl_seconds: int):
    self.key = key
    self.ttl_seconds = ttl_seconds

  async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
    from robosystems.config.valkey_registry import (
      ValkeyDatabase,
      create_async_redis_client,
    )

    client = create_async_redis_client(ValkeyDatabase.WORKER_QUEUE)
    try:
      return await getattr(client, method)(self.key, *args, **kwargs)
    finally:
      await client.aclose()

  async def get(self) -> str | None:
    return await self._call("get")

  async def set(self, value: str) -> None:
    await self._call("set", value, ex=self.ttl_seconds)

  async def delete(self) -> None:
    await self._call("delete")
