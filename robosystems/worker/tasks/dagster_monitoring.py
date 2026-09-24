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
  and ``lock_key``. The lock is released when the monitor exits, except when a
  cancelled run will not confirm it has stopped: releasing then would let a
  second run write alongside it, so the lock is left to its TTL.
  """

  CANCEL_SETTLE_SECONDS = 120

  async def execute(self) -> dict[str, Any]:
    import asyncio

    from robosystems.middleware.sse.dagster_monitor import DagsterRunMonitor

    job_name = self.params["job_name"]
    run_config = self.params.get("run_config")
    tags = self.params.get("tags")
    lock_key = self.params.get("lock_key")

    monitor = DagsterRunMonitor()
    release = True

    try:
      run_id = await asyncio.to_thread(monitor.submit_job, job_name, run_config, tags)
      await self.report_progress(f"Submitted {job_name}", percent=5)

      # Our own poll loop (not DagsterRunMonitor.monitor_run) so a user can
      # cancel a long monitor between iterations.
      while True:
        if await self.is_cancelled():
          logger.info(f"Dagster job monitor cancelled: {job_name} (run_id={run_id})")
          release = await self._stop_run(monitor, run_id)
          return {"status": "cancelled", "run_id": run_id, "job_name": job_name}

        status_info = await asyncio.to_thread(monitor.get_run_status, run_id)
        current_status = status_info["status"]

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

        # Emit progress on status change
        await self.report_progress(
          f"{job_name}: {status_info.get('dagster_status', current_status)}",
          percent=status_info.get("progress_percent"),
        )

        await asyncio.sleep(monitor.poll_interval)

    finally:
      if release:
        self.release_lock(lock_key)

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
