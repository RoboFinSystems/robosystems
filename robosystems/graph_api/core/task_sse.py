"""
Generic SSE task monitoring for background operations.

This module provides a reusable SSE streaming interface for monitoring
any long-running background task (ingestion, backup, restore, etc.).
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any
from enum import Enum

from robosystems.logger import logger


class TaskType(Enum):
  """Types of background tasks that support SSE monitoring."""

  INGESTION = "ingestion"
  BACKUP = "backup"
  RESTORE = "restore"
  EXPORT = "export"
  MIGRATION = "migration"


async def generate_task_sse_events(
  task_manager,
  task_id: str,
  task_type: TaskType = TaskType.INGESTION,
  heartbeat_interval: int = 30,
) -> AsyncGenerator[Dict[str, Any], None]:
  """
  Generate SSE events for any background task with progress monitoring.

  This is a generic implementation that can be used for:
  - Data ingestion/copy operations
  - Database backups
  - Database restores
  - Any other long-running task

  Args:
      task_manager: Task manager instance with get_task method
      task_id: Unique task identifier
      task_type: Type of task being monitored
      heartbeat_interval: Seconds between heartbeat events

  Yields:
      SSE event dictionaries with event type and data
  """
  last_heartbeat = time.time()
  last_progress = -1

  # Send initial connection event
  yield {
    "event": "connected",
    "data": json.dumps(
      {
        "task_id": task_id,
        "task_type": task_type.value,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": f"Connected to {task_type.value} task monitor",
      }
    ),
  }

  while True:
    try:
      # Get current task status
      task = await task_manager.get_task(task_id)

      if not task:
        yield {
          "event": "error",
          "data": json.dumps(
            {"error": f"Task {task_id} not found", "task_type": task_type.value}
          ),
        }
        break

      # Send heartbeat every interval to prevent timeout
      current_time = time.time()
      if current_time - last_heartbeat > heartbeat_interval:
        yield {
          "event": "heartbeat",
          "data": json.dumps(
            {
              "task_id": task_id,
              "task_type": task_type.value,
              "status": task["status"],
              "timestamp": datetime.now(timezone.utc).isoformat(),
              "message": "Task is still running...",
            }
          ),
        }
        last_heartbeat = current_time
        logger.debug(f"[SSE] Sent heartbeat for {task_type.value} task {task_id}")

      # Send progress updates
      current_progress = task.get("progress_percent", 0)
      if current_progress != last_progress:
        yield {
          "event": "progress",
          "data": json.dumps(
            {
              "task_id": task_id,
              "task_type": task_type.value,
              "status": task["status"],
              "progress_percent": current_progress,
              "records_processed": task.get("records_processed", 0),
              "estimated_records": task.get("estimated_records", 0),
              "started_at": task.get("started_at"),
              "message": _get_progress_message(task_type, task),
              "metadata": task.get("metadata", {}),
            }
          ),
        }
        last_progress = current_progress

      # Check for completion
      if task["status"] == "completed":
        yield {
          "event": "completed",
          "data": json.dumps(
            {
              "task_id": task_id,
              "task_type": task_type.value,
              "status": "completed",
              "result": task.get("result"),
              "duration_seconds": _calculate_duration(task),
              "message": _get_completion_message(task_type, task),
              "metadata": task.get("metadata", {}),
            }
          ),
        }
        break

      # Check for failure
      if task["status"] == "failed":
        yield {
          "event": "failed",
          "data": json.dumps(
            {
              "task_id": task_id,
              "task_type": task_type.value,
              "status": "failed",
              "error": task.get("error"),
              "message": _get_failure_message(task_type, task),
              "metadata": task.get("metadata", {}),
            }
          ),
        }
        break

      # Wait a bit before checking again
      await asyncio.sleep(2)

    except Exception as e:
      logger.error(
        f"[SSE] Error generating events for {task_type.value} task {task_id}: {e}"
      )
      yield {
        "event": "error",
        "data": json.dumps({"error": str(e), "task_type": task_type.value}),
      }
      break


def _get_progress_message(task_type: TaskType, task: Dict[str, Any]) -> str:
  """Generate task-specific progress message."""
  if task_type == TaskType.INGESTION:
    table_name = task.get("metadata", {}).get("table_name", "table")
    return f"Processing {table_name}..."
  elif task_type == TaskType.BACKUP:
    database = task.get("metadata", {}).get("database", "database")
    return f"Backing up {database}..."
  elif task_type == TaskType.RESTORE:
    database = task.get("metadata", {}).get("database", "database")
    return f"Restoring {database}..."
  else:
    return f"Processing {task_type.value} task..."


def _get_completion_message(task_type: TaskType, task: Dict[str, Any]) -> str:
  """Generate task-specific completion message."""
  metadata = task.get("metadata", {})
  result = task.get("result", {})

  if task_type == TaskType.INGESTION:
    table_name = metadata.get("table_name", "table")
    records = result.get("records_loaded", 0)
    if records > 0:
      return f"Successfully ingested {records:,} records into {table_name}"
    else:
      return f"Successfully completed ingestion for {table_name}"
  elif task_type == TaskType.BACKUP:
    database = metadata.get("database", "database")
    size_mb = result.get("backup_size_mb", 0)
    if size_mb > 0:
      return f"Successfully backed up {database} ({size_mb:.1f} MB)"
    else:
      return f"Successfully backed up {database}"
  elif task_type == TaskType.RESTORE:
    database = metadata.get("database", "database")
    return f"Successfully restored {database}"
  else:
    return f"Successfully completed {task_type.value} task"


def _get_failure_message(task_type: TaskType, task: Dict[str, Any]) -> str:
  """Generate task-specific failure message."""
  metadata = task.get("metadata", {})

  if task_type == TaskType.INGESTION:
    table_name = metadata.get("table_name", "table")
    return f"Failed to ingest data into {table_name}"
  elif task_type == TaskType.BACKUP:
    database = metadata.get("database", "database")
    return f"Failed to backup {database}"
  elif task_type == TaskType.RESTORE:
    database = metadata.get("database", "database")
    return f"Failed to restore {database}"
  else:
    return f"Failed to complete {task_type.value} task"


def _calculate_duration(task: Dict[str, Any]) -> float:
  """Calculate task duration in seconds."""
  if task.get("completed_at") and task.get("started_at"):
    try:
      completed = datetime.fromisoformat(task["completed_at"])
      started = datetime.fromisoformat(task["started_at"])
      return (completed - started).total_seconds()
    except (ValueError, TypeError):
      pass
  return 0.0
