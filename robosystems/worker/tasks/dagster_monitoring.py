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
  and ``lock_key`` — the lock is released when the monitor exits, however it
  exits.
  """

  async def execute(self) -> dict[str, Any]:
    import asyncio

    from robosystems.middleware.sse.dagster_monitor import DagsterRunMonitor

    job_name = self.params["job_name"]
    run_config = self.params.get("run_config")
    tags = self.params.get("tags")
    lock_key = self.params.get("lock_key")

    monitor = DagsterRunMonitor()

    try:
      run_id = await asyncio.to_thread(monitor.submit_job, job_name, run_config, tags)
      await self.report_progress(f"Submitted {job_name}", percent=5)

      # Poll with cancellation checks between iterations.
      # DagsterRunMonitor.monitor_run handles SSE progress internally,
      # but doesn't know about worker cancellation. We wrap the poll
      # loop so the user can cancel long-running monitors (backup/restore).
      while True:
        if await self.is_cancelled():
          logger.info(f"Dagster job monitor cancelled: {job_name} (run_id={run_id})")
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
      self.release_lock(lock_key)
