"""Worker task handler for monitoring Dagster job execution.

Submits a Dagster job and polls its status, relaying progress to SSE.
This is an orchestration task — the worker doesn't do the actual work,
it monitors a remote operation (Graph API backup/restore, Dagster
materialization) and keeps the SSE stream alive even if API containers
scale in.

Replaces BackgroundTasks + run_and_monitor_dagster_job pattern.
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

  This is an orchestration task — the actual work runs in Dagster
  (backup/restore on Graph API, materialization ops). The worker
  just polls status and keeps the SSE stream alive so the frontend
  doesn't lose progress when API containers scale in.
  """

  async def execute(self) -> dict[str, Any]:
    from robosystems.middleware.sse.dagster_monitor import DagsterRunMonitor

    job_name = self.params["job_name"]
    run_config = self.params.get("run_config")
    tags = self.params.get("tags")
    lock_key = self.params.get("lock_key")

    monitor = DagsterRunMonitor()

    try:
      run_id = monitor.submit_job(job_name, run_config, tags)
      await self.report_progress(f"Submitted {job_name}", percent=5)

      result = await monitor.monitor_run(run_id, self.task_id)

      logger.info(f"Dagster job {job_name} completed: run_id={run_id}")
      return result

    finally:
      self._release_lock(lock_key)

  def _release_lock(self, lock_key: str | None) -> None:
    """Release the distributed lock if held (materialization paths)."""
    if not lock_key:
      return

    try:
      from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

      redis_client = create_redis_client(ValkeyDatabase.LOCKS)
      redis_client.delete(f"lock:{lock_key}")
      redis_client.close()
      logger.debug(f"Released lock: {lock_key}")
    except Exception as e:
      logger.warning(f"Failed to release lock {lock_key}: {e}")
