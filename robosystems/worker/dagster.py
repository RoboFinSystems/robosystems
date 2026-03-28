"""Dagster observability reporting for worker tasks.

Reports AssetMaterialization events to Dagster after task completion
so all worker activity appears in the Dagster asset catalog and event log.

Fire-and-forget: failures are logged as warnings, never fail the task.
Same pattern as middleware/sse/direct_monitor.py:644-710.
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

DAGSTER_REPORT_TIMEOUT = 15.0


async def report_task_to_dagster(
  task_type: str,
  task_id: str,
  graph_id: str,
  duration_ms: float,
  result: dict[str, Any] | None = None,
) -> None:
  """Report task completion to Dagster as an AssetMaterialization.

  Non-blocking: catches all exceptions and logs warnings.
  Skips reporting in test environment.

  Args:
      task_type: The task type string (e.g. "agent_mapping").
      task_id: The unique task identifier.
      graph_id: The graph this task operated on.
      duration_ms: Execution duration in milliseconds.
      result: Optional result dict from the task handler.
  """
  try:
    await asyncio.wait_for(
      asyncio.to_thread(
        _report_task_sync,
        task_type,
        task_id,
        graph_id,
        duration_ms,
        result or {},
      ),
      timeout=DAGSTER_REPORT_TIMEOUT,
    )
    logger.debug(f"Reported worker/{task_type} materialization to Dagster")
  except TimeoutError:
    logger.warning(
      f"Dagster reporting timed out after {DAGSTER_REPORT_TIMEOUT}s for {task_id}. "
      "Task succeeded but won't appear in Dagster UI."
    )
  except Exception as e:
    logger.warning(
      f"Failed to report AssetMaterialization to Dagster for {task_id}: {e}. "
      "Task succeeded but won't appear in Dagster UI."
    )


def _report_task_sync(
  task_type: str,
  task_id: str,
  graph_id: str,
  duration_ms: float,
  result: dict[str, Any],
) -> None:
  """Synchronous Dagster reporting (runs in thread via asyncio.to_thread)."""
  from robosystems.config import env

  if env.ENVIRONMENT == "test":
    logger.debug(f"Skipping Dagster reporting in test environment for {task_id}")
    return

  from dagster import AssetKey, AssetMaterialization, DagsterInstance, MetadataValue

  instance = DagsterInstance.get()

  metadata: dict[str, Any] = {
    "task_id": MetadataValue.text(task_id),
    "task_type": MetadataValue.text(task_type),
    "graph_id": MetadataValue.text(graph_id),
    "duration_ms": MetadataValue.float(duration_ms),
    "status": MetadataValue.text("completed"),
  }

  # Merge task-specific result metadata
  for key, value in result.items():
    if isinstance(value, bool):
      metadata[key] = MetadataValue.bool(value)
    elif isinstance(value, int):
      metadata[key] = MetadataValue.int(value)
    elif isinstance(value, float):
      metadata[key] = MetadataValue.float(value)
    else:
      metadata[key] = MetadataValue.text(str(value))

  materialization = AssetMaterialization(
    asset_key=AssetKey(["worker", task_type]),
    description=f"Worker task {task_type} completed for {graph_id}",
    metadata=metadata,
  )

  instance.report_runless_asset_event(materialization)
