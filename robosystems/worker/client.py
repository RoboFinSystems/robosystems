"""Enqueue client for submitting tasks to the background worker.

Used by API routes to enqueue tasks and return 202 responses.
Creates an SSE operation (PENDING) on Valkey DB 3, then pushes
the task payload to the worker queue on Valkey DB 6.
"""

import json
from typing import Any

from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
from robosystems.middleware.sse.operation_manager import create_operation_response
from robosystems.utils.ulid import generate_prefixed_ulid

# Deduplication window in seconds. Prevents duplicate tasks from
# double-clicks or frontend retries within this window.
DEDUP_TTL = 30


async def enqueue_task(
  task_type: str,
  graph_id: str,
  user_id: str,
  params: dict[str, Any] | None = None,
) -> dict[str, Any]:
  """Enqueue a background task. Returns an operation response (202-ready).

  Creates an SSE operation in PENDING state (Valkey DB 3) with _links
  for stream/status/cancel endpoints, then RPUSH the task payload to
  the worker queue (Valkey DB 6). RPUSH + BLMOVE = FIFO ordering.

  Deduplication: A task_type + graph_id combination is rejected within
  DEDUP_TTL seconds of the previous enqueue, returning the existing
  operation response instead.

  Args:
      task_type: Registered task type (e.g. "agent_mapping").
      graph_id: The graph this task operates on.
      user_id: The user who initiated the task.
      params: Optional task-specific parameters.

  Returns:
      Operation response dict suitable for returning as a 202 response.
      Includes operation_id, status, _links (stream, status, cancel).
  """
  queue = create_async_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  try:
    # Check deduplication — if a task for this type+graph was recently enqueued,
    # return the existing operation instead of creating a duplicate.
    dedup_key = f"worker:dedup:{task_type}:{graph_id}"
    existing_task_id = await queue.get(dedup_key)
    if existing_task_id:
      return {
        "operation_id": existing_task_id,
        "status": "pending",
        "operation_type": task_type,
        "graph_id": graph_id,
        "deduplicated": True,
        "_links": {
          "stream": f"/v1/operations/{existing_task_id}/stream",
          "status": f"/v1/operations/{existing_task_id}/status",
          "cancel": f"/v1/operations/{existing_task_id}",
        },
      }

    task_id = generate_prefixed_ulid("task")

    # Create SSE operation (PENDING state, generates _links)
    response = await create_operation_response(
      operation_type=task_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=task_id,
    )

    # Set dedup key before enqueuing so concurrent requests see it
    await queue.set(dedup_key, task_id, ex=DEDUP_TTL)

    task_payload = json.dumps(
      {
        "task_id": task_id,
        "task_type": task_type,
        "graph_id": graph_id,
        "user_id": user_id,
        "attempt": 1,
        "params": params or {},
      }
    )
    await queue.rpush("worker:tasks", task_payload)

    return response
  finally:
    await queue.aclose()
