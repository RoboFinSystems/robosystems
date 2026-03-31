"""Dagster sensor for reaping stale worker inflight tasks.

Runs in the dagster-daemon process (always-on, no cold start). Scans
all worker:inflight:* keys in Valkey DB 6 for tasks that have been
in-flight longer than their timeout — indicating the worker crashed.

Stale tasks are requeued with an incremented attempt count, or moved
to the DLQ if max retries are exceeded.
"""

import json
import time
from datetime import UTC, datetime
from typing import Any

from dagster import (
  DefaultSensorStatus,
  SensorEvaluationContext,
  SkipReason,
  sensor,
)

from robosystems.logger import get_logger
from robosystems.worker.constants import (
  DEFAULT_TASK_TIMEOUT,
  MAX_RETRIES,
  TASK_TIMEOUTS,
)

logger = get_logger(__name__)

# Grace period added to task timeout before considering it stale.
# Prevents reaping a task that's still legitimately running near its timeout.
STALE_GRACE_SECONDS = 30

# SSE metadata key prefix (matches event_storage.py)
SSE_META_PREFIX = "sse:operation:meta:"


def _get_operation_metadata(sse_client: Any, task_id: str) -> dict[str, Any] | None:
  """Fetch and parse SSE operation metadata. Returns None if not found."""
  meta_json = sse_client.get(f"{SSE_META_PREFIX}{task_id}")
  if not meta_json:
    return None

  try:
    return json.loads(meta_json)
  except json.JSONDecodeError:
    return None


def _get_age_from_metadata(meta: dict[str, Any]) -> float | None:
  """Extract operation age in seconds from parsed metadata."""
  created_at_str = meta.get("created_at")
  if not created_at_str:
    return None

  created_at = datetime.fromisoformat(created_at_str)
  if created_at.tzinfo is None:
    created_at = created_at.replace(tzinfo=UTC)

  return time.time() - created_at.timestamp()


@sensor(
  minimum_interval_seconds=60,
  default_status=DefaultSensorStatus.RUNNING,
  description="Reaps stale worker inflight tasks. Requeues crashed tasks or moves to DLQ after max retries.",
)
def worker_inflight_reaper_sensor(context: SensorEvaluationContext):
  """Scan worker inflight lists and requeue stale tasks."""
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

  queue = create_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  sse = create_redis_client(ValkeyDatabase.SSE, decode_responses=True)

  try:
    # Find all inflight lists (one per worker)
    inflight_keys = list(queue.scan_iter(match="worker:inflight:*", count=100))

    if not inflight_keys:
      return SkipReason("No inflight lists found")

    requeued = 0
    dlq_count = 0
    cleaned = 0

    for inflight_key in inflight_keys:
      tasks = queue.lrange(inflight_key, 0, -1)

      for task_json in tasks:
        try:
          task_data = json.loads(task_json)
        except json.JSONDecodeError:
          queue.lrem(inflight_key, 1, task_json)
          cleaned += 1
          continue

        task_id = task_data.get("task_id", "unknown")
        task_type = task_data.get("task_type", "unknown")
        attempt = task_data.get("attempt", 1)

        # Single fetch for both status and age
        meta = _get_operation_metadata(sse, task_id)

        if meta is None:
          # No SSE metadata — operation expired from storage. Requeue with
          # incremented attempt rather than immediately DLQ-ing, since this
          # could be caused by Valkey memory pressure or short TTL.
          queue.lrem(inflight_key, 1, task_json)
          if attempt >= MAX_RETRIES:
            task_data["dlq_reason"] = "no_sse_metadata"
            task_data["dlq_at"] = datetime.now(UTC).isoformat()
            task_data["dlq_attempts"] = attempt
            queue.rpush("worker:dlq", json.dumps(task_data))
            dlq_count += 1
            logger.warning(
              f"Task {task_id} ({task_type}) has no SSE metadata, "
              f"moved to DLQ after {attempt} attempts"
            )
          else:
            task_data["attempt"] = attempt + 1
            queue.rpush("worker:tasks", json.dumps(task_data))
            requeued += 1
            logger.warning(
              f"Task {task_id} ({task_type}) has no SSE metadata, "
              f"requeuing attempt {attempt + 1}"
            )
          continue

        # If operation already completed/failed, just clean up inflight entry
        status = meta.get("status")
        if status is not None and status not in ("pending", "running"):
          queue.lrem(inflight_key, 1, task_json)
          cleaned += 1
          continue

        # Check staleness: task age > timeout + grace period
        timeout = TASK_TIMEOUTS.get(task_type, DEFAULT_TASK_TIMEOUT)
        stale_threshold = timeout + STALE_GRACE_SECONDS

        age = _get_age_from_metadata(meta)
        if age is None or age < stale_threshold:
          # Can't determine age, or not stale yet — skip
          continue

        # Task is stale — remove from inflight
        queue.lrem(inflight_key, 1, task_json)

        if attempt >= MAX_RETRIES:
          # Exceeded retries — move to DLQ and mark failed
          task_data["dlq_reason"] = f"stale_after_{attempt}_attempts"
          task_data["dlq_at"] = datetime.now(UTC).isoformat()
          task_data["dlq_attempts"] = attempt
          queue.rpush("worker:dlq", json.dumps(task_data))
          _fail_operation_sync(sse, task_id, attempt)
          dlq_count += 1
          logger.warning(
            f"Task {task_id} ({task_type}) moved to DLQ after {attempt} attempts"
          )
        else:
          # Requeue with incremented attempt
          task_data["attempt"] = attempt + 1
          queue.rpush("worker:tasks", json.dumps(task_data))
          requeued += 1
          logger.info(
            f"Requeued stale task {task_id} ({task_type}), attempt {attempt + 1}"
          )

    # Clean up empty inflight lists
    for inflight_key in inflight_keys:
      if queue.llen(inflight_key) == 0:
        queue.delete(inflight_key)

    if requeued or dlq_count or cleaned:
      context.log.info(
        f"Reaper: requeued={requeued}, dlq={dlq_count}, cleaned={cleaned}"
      )
      return None

    return SkipReason("No stale inflight tasks found")

  finally:
    queue.close()
    sse.close()


def _fail_operation_sync(sse_client: Any, task_id: str, attempts: int) -> None:
  """Mark an SSE operation as failed by updating its metadata directly."""
  meta_key = f"{SSE_META_PREFIX}{task_id}"
  meta_json = sse_client.get(meta_key)
  if not meta_json:
    return

  try:
    meta = json.loads(meta_json)
    meta["status"] = "failed"
    meta["updated_at"] = datetime.now(UTC).isoformat()
    sse_client.set(meta_key, json.dumps(meta), keepttl=True)
  except (json.JSONDecodeError, Exception) as e:
    logger.warning(f"Failed to update SSE metadata for {task_id}: {e}")
