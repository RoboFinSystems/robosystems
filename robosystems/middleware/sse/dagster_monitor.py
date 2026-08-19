"""Dagster run monitor for SSE progress tracking.

Submits Dagster runs and polls their status, emitting SSE events as it
goes. Designed to run as a FastAPI background task.

Usage:
    from starlette.background import BackgroundTasks

    @router.post("/graphs")
    async def create_graph(request: CreateGraphRequest, background_tasks: BackgroundTasks):
        operation_id = await create_operation_response(...)

        background_tasks.add_task(
            run_and_monitor_dagster_job,
            job_name="create_graph_job",
            run_config=request.to_dagster_config(),
            operation_id=operation_id,
        )

        return {"operation_id": operation_id}
"""

import asyncio
import logging
from typing import Any

from robosystems.config import env
from robosystems.config.constants import DAGSTER_CLIENT_TIMEOUT_SECONDS
from robosystems.middleware.sse.event_storage import EventType, SSEEventStorage

logger = logging.getLogger(__name__)


# Dagster run status mapping
DAGSTER_STATUS_MAP = {
  "QUEUED": ("pending", 0),
  "NOT_STARTED": ("pending", 0),
  "STARTING": ("running", 5),
  "STARTED": ("running", 10),
  "SUCCESS": ("completed", 100),
  "FAILURE": ("failed", 100),
  "CANCELED": ("cancelled", 100),
}


class DagsterRunMonitor:
  """Polls a Dagster run and emits SSE progress events for it."""

  def __init__(
    self,
    dagster_host: str | None = None,
    dagster_port: int | None = None,
    poll_interval: float = 2.0,
    max_poll_time: float = 3600.0,  # 1 hour default timeout
  ):
    """Host and port default to the configured Dagster webserver."""
    self.dagster_host = dagster_host or env.DAGSTER_HOST
    self.dagster_port = dagster_port or env.DAGSTER_PORT
    self.poll_interval = poll_interval
    self.max_poll_time = max_poll_time
    self.event_storage = SSEEventStorage()
    self._client = None

  def _get_client(self):
    """Get or create DagsterGraphQLClient."""
    if self._client is None:
      try:
        from dagster_graphql import DagsterGraphQLClient

        # Explicit timeout: the library default is 300 s, and these calls run
        # on the API event loop — a hung webserver must not hold every tenant.
        self._client = DagsterGraphQLClient(
          hostname=self.dagster_host,
          port_number=self.dagster_port,
          timeout=DAGSTER_CLIENT_TIMEOUT_SECONDS,
        )
      except ImportError:
        logger.error(
          "dagster-graphql not installed. Install with: pip install dagster-graphql"
        )
        raise
    return self._client

  def submit_job(
    self,
    job_name: str,
    run_config: dict[str, Any] | None = None,
    tags: dict[str, str] | None = None,
  ) -> str:
    """Submit a Dagster job and return its run ID."""
    client = self._get_client()

    logger.info(f"Submitting Dagster job: {job_name}")

    run_id = client.submit_job_execution(
      job_name=job_name,
      run_config=run_config or {},
      tags=tags,
    )

    logger.info(f"Dagster job {job_name} submitted with run_id: {run_id}")
    return run_id

  def get_run_status(self, run_id: str) -> dict[str, Any]:
    """Return a run's status mapped onto the SSE status vocabulary."""
    client = self._get_client()

    status = client.get_run_status(run_id)

    status_str = status.name if hasattr(status, "name") else str(status)
    mapped_status, progress = DAGSTER_STATUS_MAP.get(status_str, ("running", 50))

    return {
      "dagster_status": status_str,
      "status": mapped_status,
      "progress_percent": progress,
      "run_id": run_id,
    }

  async def emit_started(
    self,
    operation_id: str,
    job_name: str,
    run_id: str,
  ):
    """Emit an operation_started event when Dagster job begins."""
    try:
      self.event_storage.store_event_sync(
        operation_id,
        EventType.OPERATION_STARTED,
        {
          "message": f"Job '{job_name}' started",
          "job_name": job_name,
          "run_id": run_id,
          "progress_percent": 0,
        },
      )
    except Exception as e:
      logger.warning(f"Failed to emit started event: {e}")

  async def emit_progress(
    self,
    operation_id: str,
    message: str,
    progress_percent: float | None = None,
    details: dict[str, Any] | None = None,
  ):
    """Emit a progress event to the SSE stream."""
    event_data = {
      "message": message,
      "progress_percent": progress_percent,
    }
    if details:
      event_data.update(details)

    try:
      self.event_storage.store_event_sync(
        operation_id,
        EventType.OPERATION_PROGRESS,
        event_data,
      )
    except Exception as e:
      logger.warning(f"Failed to emit progress event: {e}")

  async def emit_completion(
    self,
    operation_id: str,
    result: dict[str, Any],
  ):
    """Emit a completion event to the SSE stream.

    Merges the result the Dagster job recorded on the operation metadata
    (graph_id, tables_materialized, …) with the monitoring result
    (run_id, elapsed_time).
    """
    try:
      stored_result = self.event_storage.get_operation_result_sync(operation_id) or {}

      merged_result = {**stored_result, **result}

      self.event_storage.store_event_sync(
        operation_id,
        EventType.OPERATION_COMPLETED,
        {
          "message": "Operation completed successfully",
          "result": merged_result,
        },
      )
    except Exception as e:
      logger.warning(f"Failed to emit completion event: {e}")

  async def emit_error(
    self,
    operation_id: str,
    error: str,
    error_details: dict[str, Any] | None = None,
  ):
    """Emit an error event to the SSE stream."""
    try:
      self.event_storage.store_event_sync(
        operation_id,
        EventType.OPERATION_ERROR,
        {
          "message": f"Operation failed: {error}",
          "error": error,
          "error_details": error_details,
        },
      )
    except Exception as e:
      logger.warning(f"Failed to emit error event: {e}")

  async def monitor_run(
    self,
    run_id: str,
    operation_id: str,
  ) -> dict[str, Any]:
    """Poll a run to completion, emitting an event on each status change.

    Returns the final status, or a `"timeout"` status once `max_poll_time`
    elapses.
    """
    import time

    start_time = time.time()
    last_status = None

    while True:
      elapsed = time.time() - start_time
      if elapsed > self.max_poll_time:
        await self.emit_error(
          operation_id,
          f"Operation timed out after {self.max_poll_time}s",
          {"run_id": run_id, "elapsed_time": elapsed},
        )
        return {
          "status": "timeout",
          "run_id": run_id,
          "elapsed_time": elapsed,
        }

      try:
        # Sync HTTP under the hood; keep it off the event loop.
        status_info = await asyncio.to_thread(self.get_run_status, run_id)
      except Exception as e:
        logger.error(f"Failed to get run status: {e}")
        await asyncio.sleep(self.poll_interval)
        continue

      current_status = status_info["status"]

      if current_status != last_status:
        last_status = current_status

        if current_status == "completed":
          await self.emit_completion(
            operation_id,
            {
              "run_id": run_id,
              "dagster_status": status_info["dagster_status"],
              "elapsed_time": elapsed,
            },
          )
          return status_info

        elif current_status == "failed":
          await self.emit_error(
            operation_id,
            "Dagster job failed",
            {
              "run_id": run_id,
              "dagster_status": status_info["dagster_status"],
            },
          )
          return status_info

        elif current_status == "cancelled":
          await self.emit_error(
            operation_id,
            "Dagster job was cancelled",
            {"run_id": run_id},
          )
          return status_info

        else:
          await self.emit_progress(
            operation_id,
            f"Job status: {status_info['dagster_status']}",
            progress_percent=status_info["progress_percent"],
            details={"run_id": run_id},
          )

      await asyncio.sleep(self.poll_interval)


# Module-level convenience functions


async def run_and_monitor_dagster_job(
  job_name: str,
  operation_id: str,
  run_config: dict[str, Any] | None = None,
  tags: dict[str, str] | None = None,
) -> dict[str, Any]:
  """Submit a Dagster job and monitor it to completion via SSE.

  The entry point for FastAPI background tasks; returns the final run
  status and emits an error event before re-raising on failure.
  """
  monitor = DagsterRunMonitor()

  try:
    # Sync HTTP submit; runs as a background task on the API loop, so offload.
    run_id = await asyncio.to_thread(monitor.submit_job, job_name, run_config, tags)

    await monitor.emit_started(operation_id, job_name, run_id)

    result = await monitor.monitor_run(run_id, operation_id)

    return result

  except Exception as e:
    logger.error(f"Failed to run Dagster job {job_name}: {e}")
    await monitor.emit_error(
      operation_id,
      str(e),
      {"job_name": job_name, "error_type": type(e).__name__},
    )
    raise


def submit_dagster_job_sync(
  job_name: str,
  run_config: dict[str, Any] | None = None,
  tags: dict[str, str] | None = None,
) -> str:
  """Kick off a Dagster job without SSE tracking; returns the run ID."""
  monitor = DagsterRunMonitor()
  return monitor.submit_job(job_name, run_config, tags)


def build_graph_job_config(
  job_name: str,
  **kwargs,
) -> dict[str, Any]:
  """Build the Dagster `run_config` for a graph operation job.

  `kwargs` are applied as config to every op in the job that takes it —
  several of these jobs have more than one.
  """
  job_to_ops: dict[str, list[str]] = {
    "create_graph_job": ["create_graph_database", "create_graph_subscription"],
    "create_entity_graph_job": [
      "create_entity_graph_database",
      "create_entity_graph_subscription",
    ],
    "create_subgraph_job": ["create_subgraph_database", "fork_parent_to_subgraph"],
    "backup_graph_job": ["create_backup"],
    "restore_graph_job": ["restore_backup"],
    "stage_file_job": ["stage_file_in_duckdb", "materialize_file_to_graph"],
    "materialize_file_job": ["materialize_staged_file"],
    "materialize_graph_job": ["materialize_graph_tables"],
  }

  op_names = job_to_ops.get(job_name)
  if not op_names:
    raise ValueError(f"Unknown job name: {job_name}")

  ops_config = {}
  for op_name in op_names:
    ops_config[op_name] = {"config": kwargs}

  run_config: dict[str, Any] = {"ops": ops_config}

  # In-process execution avoids per-op subprocess spawn, which dominates
  # runtime locally when a job stages many small files.
  if env.ENVIRONMENT == "dev":
    run_config["execution"] = {"config": {"in_process": {}}}

  return run_config
