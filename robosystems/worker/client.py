"""Enqueue client for submitting tasks to the background worker.

Used by API routes to enqueue tasks and return 202 responses.
Creates an SSE operation (PENDING) on Valkey DB 3, then pushes
the task payload to the worker queue on Valkey DB 6.
"""

import hashlib
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
  graph_id: str | None,
  user_id: str,
  params: dict[str, Any] | None = None,
) -> dict[str, Any]:
  """Enqueue a background task. Returns an operation response (202-ready).

  Creates an SSE operation in PENDING state (Valkey DB 3) with _links
  for stream/status/cancel endpoints, then RPUSH the task payload to
  the worker queue (Valkey DB 6). RPUSH + BLMOVE = FIFO ordering.

  ``task_type`` must be a registered handler type (e.g. "graph_creation",
  "operator") — see worker/tasks/__init__.py.

  Deduplication: A task_type + graph_id + user_id + params combination
  is rejected within DEDUP_TTL seconds of the previous enqueue,
  returning the existing operation response instead. Dedup is skipped
  entirely when graph_id is None (e.g. new graph creation).

  Returns an operation response dict with operation_id, status, and _links
  (stream, status, cancel).
  """
  queue = create_async_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  try:
    # Check deduplication — if an identical task was recently enqueued,
    # return the existing operation instead of creating a duplicate.
    # The key includes a params hash so that different params (e.g. two
    # different subgraph names or mapping IDs) get separate dedup slots.
    # Skip dedup entirely when graph_id is None (new graph creation).
    params_hash = hashlib.md5(
      json.dumps(params or {}, sort_keys=True).encode()
    ).hexdigest()[:8]
    dedup_key = f"worker:dedup:{task_type}:{graph_id}:{user_id}:{params_hash}"
    existing_task_id = await queue.get(dedup_key) if graph_id else None
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

    task_id = generate_prefixed_ulid("op")

    # Create SSE operation (PENDING state, generates _links)
    response = await create_operation_response(
      operation_type=task_type,
      user_id=user_id,
      graph_id=graph_id,
      operation_id=task_id,
    )

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

    # Atomic: set dedup key + enqueue in a single round trip.
    # Prevents ghost dedup keys if the process crashes between the two ops.
    pipe = queue.pipeline(transaction=True)
    if graph_id:
      pipe.set(dedup_key, task_id, ex=DEDUP_TTL)
    pipe.rpush("worker:tasks", task_payload)
    await pipe.execute()

    return response
  finally:
    await queue.aclose()


async def get_queue_position(operation_id: str) -> tuple[int | None, int]:
  """Where a queued operation sits: (1-based position, queue depth).

  Position is None when the operation is not waiting in the queue — it has
  been picked up, finished, or was never enqueued. The queue is short by
  design (single digits), so a full LRANGE is cheaper than reconstructing
  the exact payload an LPOS would need.
  """
  queue = create_async_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  try:
    entries = await queue.lrange("worker:tasks", 0, -1)
  finally:
    await queue.aclose()

  position: int | None = None
  for index, raw in enumerate(entries):
    try:
      if json.loads(raw).get("task_id") == operation_id:
        position = index + 1
        break
    except (json.JSONDecodeError, AttributeError):
      continue
  return position, len(entries)
