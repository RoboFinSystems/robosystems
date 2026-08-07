"""Task endpoints for every background operation on this node.

One SSE monitor and one status route serve all task types — ingestion, backup,
restore, staging, migration — keyed by the task ID's prefix.
"""

import json
from typing import Any

import redis.asyncio as redis_async
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi import status as http_status
from sse_starlette.sse import EventSourceResponse

from robosystems.config.valkey_registry import ValkeyDatabase
from robosystems.graph_api.core.task_manager import (
  backup_task_manager,
  migration_task_manager,
  restore_task_manager,
)
from robosystems.graph_api.core.task_sse import TaskType, generate_task_sse_events
from robosystems.graph_api.routers.databases.tables.management import (
  staging_task_manager,
)

router = APIRouter(prefix="/tasks", tags=["Tasks"])


class UnifiedTaskManager:
  """Unified task manager that can access all task types."""

  def __init__(self):
    self._redis_client = None
    self._redis_url = None
    # Task-ID prefix -> owning manager
    self.managers = {
      "backup": backup_task_manager,
      "restore": restore_task_manager,
      "staging": staging_task_manager,  # DuckDB table creation from S3
      "migration": migration_task_manager,  # Version migration export/import
    }

  async def get_redis(self) -> redis_async.Redis:
    """Async Redis client for task status storage."""
    if not self._redis_client:
      # The async factory handles SSL params correctly.
      from robosystems.config.valkey_registry import create_async_redis_client

      self._redis_client = create_async_redis_client(
        ValkeyDatabase.SSE, decode_responses=True
      )
    return self._redis_client

  async def get_task(self, task_id: str) -> dict[str, Any] | None:
    """Look up a task by ID across all task types."""
    redis_client = await self.get_redis()
    task_json = await redis_client.get(f"lbug:task:{task_id}")

    if task_json:
      return json.loads(task_json)

    # Fall back to the manager that owns the ID's prefix.
    for prefix, manager in self.managers.items():
      if task_id.startswith(prefix):
        task = await manager.get_task(task_id)
        if task:
          return task

    return None

  async def list_all_tasks(
    self, status_filter: str | None = None
  ) -> list[dict[str, Any]]:
    """List every task, newest first, optionally filtered by status."""
    redis_client = await self.get_redis()

    pattern = "lbug:task:*"
    keys = await redis_client.keys(pattern)

    tasks = []
    for key in keys:
      task_json = await redis_client.get(key)
      if task_json:
        task = json.loads(task_json)

        if status_filter and task.get("status") != status_filter:
          continue

        tasks.append(task)

    tasks.sort(key=lambda x: x.get("created_at", ""), reverse=True)

    return tasks

  def get_task_type(self, task_id: str) -> TaskType:
    """Determine task type from task ID."""
    if task_id.startswith("ingest") or task_id.startswith("copy"):
      return TaskType.INGESTION
    elif task_id.startswith("backup"):
      return TaskType.BACKUP
    elif task_id.startswith("restore"):
      return TaskType.RESTORE
    elif task_id.startswith("staging"):
      return TaskType.STAGING
    elif task_id.startswith("migration"):
      return TaskType.MIGRATION
    else:
      # Unknown prefixes are treated as ingestion.
      return TaskType.INGESTION


unified_task_manager = UnifiedTaskManager()


@router.get("")
async def list_tasks(
  status: str | None = Query(None, description="Filter by task status"),
  task_type: str | None = Query(None, description="Filter by task type prefix"),
  limit: int = Query(100, ge=1, le=1000, description="Maximum tasks to return"),
) -> list[dict[str, Any]]:
  """
  List all tasks across all types.

  Returns a list of all tasks with optional filtering by:
  - Status (pending, running, completed, failed)
  - Task type (by prefix: ingest, backup, restore, copy)

  Tasks are returned in order of creation (newest first).
  """
  tasks = await unified_task_manager.list_all_tasks(status_filter=status)

  if task_type:
    tasks = [t for t in tasks if t.get("task_id", "").startswith(task_type)]

  return tasks[:limit]


@router.get("/{task_id}/monitor")
async def monitor_task(
  task_id: str = Path(..., description="Task ID to monitor"),
) -> EventSourceResponse:
  """
  Monitor any task via Server-Sent Events.

  This is a generic endpoint that works for all task types:
  - Ingestion/Copy tasks
  - Backup tasks
  - Restore tasks
  - Any future task types

  Returns a stream of events tracking the task progress:
  - connected: Initial connection established
  - progress: Periodic progress updates
  - heartbeat: Keep-alive messages
  - completed: Operation finished successfully
  - failed: Operation failed with error
  - error: Stream error occurred
  """
  task_type = unified_task_manager.get_task_type(task_id)

  manager = unified_task_manager

  return EventSourceResponse(
    generate_task_sse_events(
      task_manager=manager,
      task_id=task_id,
      task_type=task_type,
      heartbeat_interval=30,
    )
  )


@router.get("/{task_id}/status")
async def get_task_status(
  task_id: str = Path(..., description="Task ID"),
) -> dict[str, Any]:
  """
  Get the status of any background task.

  Returns detailed status information including:
  - Current status (pending, running, completed, failed)
  - Progress information for running tasks
  - Results for completed tasks
  - Error details for failed tasks
  """
  task = await unified_task_manager.get_task(task_id)

  if not task:
    raise HTTPException(
      status_code=http_status.HTTP_404_NOT_FOUND, detail=f"Task {task_id} not found"
    )

  return task


@router.get("/stats")
async def get_task_statistics() -> dict[str, Any]:
  """
  Get statistics about all tasks.

  Returns aggregate statistics including:
  - Total tasks by status
  - Tasks by type
  - Recent completion rate
  """
  all_tasks = await unified_task_manager.list_all_tasks()

  status_counts = {}
  type_counts = {}

  for task in all_tasks:
    status = task.get("status", "unknown")
    status_counts[status] = status_counts.get(status, 0) + 1

    task_id = task.get("task_id", "")
    if task_id.startswith("ingest") or task_id.startswith("copy"):
      task_type = "ingestion"
    elif task_id.startswith("backup"):
      task_type = "backup"
    elif task_id.startswith("restore"):
      task_type = "restore"
    else:
      task_type = "other"

    type_counts[task_type] = type_counts.get(task_type, 0) + 1

  return {
    "total_tasks": len(all_tasks),
    "tasks_by_status": status_counts,
    "tasks_by_type": type_counts,
    "active_tasks": status_counts.get("running", 0),
    "pending_tasks": status_counts.get("pending", 0),
    "completed_tasks": status_counts.get("completed", 0),
    "failed_tasks": status_counts.get("failed", 0),
  }
