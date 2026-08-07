"""
Status tracking for long-running background operations.

Task records live in Valkey with a 24-hour TTL, so a task that nothing polls
expires on its own. One manager instance per task family (backup, restore,
migration) keys its IDs by prefix.
"""

import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis_async

from robosystems.config.valkey_registry import ValkeyDatabase
from robosystems.graph_api.models.tasks import TaskStatus


class GenericTaskManager:
  """Tracks background task status in Valkey.

  ``task_prefix`` (``backup``, ``restore``, ``ingest``, ...) namespaces the
  IDs this manager mints.
  """

  def __init__(self, task_prefix: str = "task"):
    self.task_prefix = task_prefix
    self._redis_client = None
    self._redis_url = None

  async def get_redis(self) -> redis_async.Redis:
    """Get the Valkey client, creating it on first use."""
    if not self._redis_client:
      from robosystems.config.valkey_registry import create_async_redis_client

      self._redis_client = create_async_redis_client(
        ValkeyDatabase.SSE, decode_responses=True
      )
    return self._redis_client

  async def create_task(
    self,
    task_type: str,
    metadata: dict[str, Any] | None = None,
    estimated_size: float | None = None,
  ) -> str:
    """Create a task in PENDING state and return its ID.

    ``estimated_size`` is the denominator for progress reporting; without it
    progress stays at 0 until the task completes.
    """
    task_id = f"{self.task_prefix}_{task_type}_{uuid.uuid4().hex[:8]}"

    task_data = {
      "task_id": task_id,
      "task_type": task_type,
      "status": TaskStatus.PENDING.value,
      "created_at": datetime.now(UTC).isoformat(),
      "started_at": None,
      "completed_at": None,
      "progress_percent": 0,
      "estimated_size": estimated_size,
      "last_heartbeat": time.time(),
      "error": None,
      "result": None,
      "metadata": metadata or {},
    }

    redis_client = await self.get_redis()
    await redis_client.setex(
      f"lbug:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

    return task_id

  async def update_task(self, task_id: str, **updates) -> None:
    """Merge ``updates`` into a task record and refresh its heartbeat.

    Read-modify-write, not atomic: a single writer per task is assumed.
    Rewriting the key also resets the 24-hour TTL, so an active task does not
    expire mid-flight. Raises ``ValueError`` if the task is gone.
    """
    redis_client = await self.get_redis()

    task_json = await redis_client.get(f"lbug:task:{task_id}")
    if not task_json:
      raise ValueError(f"Task {task_id} not found")

    task_data = json.loads(task_json)

    task_data.update(updates)
    task_data["last_heartbeat"] = time.time()

    await redis_client.setex(
      f"lbug:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

  async def get_task(self, task_id: str) -> dict[str, Any] | None:
    """Get a task record, or None if it never existed or has expired."""
    redis_client = await self.get_redis()
    task_json = await redis_client.get(f"lbug:task:{task_id}")

    if task_json:
      return json.loads(task_json)
    return None

  async def complete_task(
    self, task_id: str, result: dict[str, Any] | None = None
  ) -> None:
    """Mark a task COMPLETED, storing ``result`` and setting progress to 100."""
    await self.update_task(
      task_id,
      status=TaskStatus.COMPLETED.value,
      completed_at=datetime.now(UTC).isoformat(),
      progress_percent=100,
      result=result or {},
    )

  async def fail_task(self, task_id: str, error: str) -> None:
    """Mark a task FAILED with ``error`` as the reason."""
    await self.update_task(
      task_id,
      status=TaskStatus.FAILED.value,
      completed_at=datetime.now(UTC).isoformat(),
      error=error,
    )


backup_task_manager = GenericTaskManager(task_prefix="backup")
restore_task_manager = GenericTaskManager(task_prefix="restore")
migration_task_manager = GenericTaskManager(task_prefix="migration")
