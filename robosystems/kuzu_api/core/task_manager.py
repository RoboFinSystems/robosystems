"""
Generic task manager for background operations.

This module provides a reusable task manager for tracking
the status of any long-running background operation.
"""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from enum import Enum

from robosystems.config.valkey_registry import ValkeyDatabase
import redis.asyncio as redis_async


class TaskStatus(str, Enum):
  """Task execution status."""

  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"


class GenericTaskManager:
  """Manages background tasks and their status in Redis."""

  def __init__(self, task_prefix: str = "task"):
    """
    Initialize task manager.

    Args:
        task_prefix: Prefix for task IDs (e.g., "backup", "restore", "ingest")
    """
    self.task_prefix = task_prefix
    self._redis_client = None
    self._redis_url = None

  async def get_redis(self) -> redis_async.Redis:
    """Get async Redis client for task storage."""
    if not self._redis_client:
      # Use dedicated database for Kuzu tasks
      # Use async factory method to handle SSL params correctly
      from robosystems.config.valkey_registry import create_async_redis_client

      self._redis_client = create_async_redis_client(
        ValkeyDatabase.KUZU_CACHE, decode_responses=True
      )
    return self._redis_client

  async def create_task(
    self,
    task_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    estimated_size: Optional[float] = None,
  ) -> str:
    """
    Create a new task.

    Args:
        task_type: Type of task (backup, restore, etc.)
        metadata: Task-specific metadata
        estimated_size: Estimated size/duration for progress tracking

    Returns:
        Task ID
    """
    task_id = f"{self.task_prefix}_{task_type}_{uuid.uuid4().hex[:8]}"

    task_data = {
      "task_id": task_id,
      "task_type": task_type,
      "status": TaskStatus.PENDING.value,
      "created_at": datetime.now(timezone.utc).isoformat(),
      "started_at": None,
      "completed_at": None,
      "progress_percent": 0,
      "estimated_size": estimated_size,
      "last_heartbeat": time.time(),
      "error": None,
      "result": None,
      "metadata": metadata or {},
    }

    # Store in Redis with 24-hour TTL
    redis_client = await self.get_redis()
    await redis_client.setex(
      f"kuzu:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

    return task_id

  async def update_task(self, task_id: str, **updates) -> None:
    """
    Update task status.

    Args:
        task_id: Task identifier
        **updates: Fields to update
    """
    redis_client = await self.get_redis()

    # Get existing task
    task_json = await redis_client.get(f"kuzu:task:{task_id}")
    if not task_json:
      raise ValueError(f"Task {task_id} not found")

    task_data = json.loads(task_json)

    # Update fields
    task_data.update(updates)
    task_data["last_heartbeat"] = time.time()

    # Store back in Redis
    await redis_client.setex(
      f"kuzu:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

  async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
    """
    Get task status.

    Args:
        task_id: Task identifier

    Returns:
        Task data or None if not found
    """
    redis_client = await self.get_redis()
    task_json = await redis_client.get(f"kuzu:task:{task_id}")

    if task_json:
      return json.loads(task_json)
    return None

  async def complete_task(
    self, task_id: str, result: Optional[Dict[str, Any]] = None
  ) -> None:
    """
    Mark task as completed.

    Args:
        task_id: Task identifier
        result: Task result data
    """
    await self.update_task(
      task_id,
      status=TaskStatus.COMPLETED.value,
      completed_at=datetime.now(timezone.utc).isoformat(),
      progress_percent=100,
      result=result or {},
    )

  async def fail_task(self, task_id: str, error: str) -> None:
    """
    Mark task as failed.

    Args:
        task_id: Task identifier
        error: Error message
    """
    await self.update_task(
      task_id,
      status=TaskStatus.FAILED.value,
      completed_at=datetime.now(timezone.utc).isoformat(),
      error=error,
    )


# Global instances for each task type
backup_task_manager = GenericTaskManager(task_prefix="backup")
restore_task_manager = GenericTaskManager(task_prefix="restore")
