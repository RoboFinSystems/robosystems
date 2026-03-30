"""Background worker consumer loop.

Consumes tasks from the Valkey queue (DB 6) via BLMOVE (reliable queue
pattern), dispatches to registered task handlers, and manages the full
operation lifecycle through the existing OperationManager (DB 3).

Reliability: BLMOVE atomically moves tasks from the main queue to a
per-worker inflight list. On success, the task is removed from inflight.
On crash, the task stays in the inflight list for the reaper sensor to
find and requeue.
"""

import asyncio
import json
import logging
import os
import signal
import socket
import time
from typing import Any

from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
from robosystems.middleware.otel.setup import get_tracer
from robosystems.middleware.sse.operation_manager import (
  OperationManager,
  get_operation_manager,
)
from robosystems.worker.cleanup import cleanup_connections
from robosystems.worker.dagster import report_task_to_dagster
from robosystems.worker.metrics import QueueDepthReporter
from robosystems.worker.tasks import get_task_handler

logger = logging.getLogger(__name__)

# Per-task-type timeouts in seconds. Agent tasks can take 2+ minutes
# for complex mapping operations. Shorter tasks get tighter limits.
TASK_TIMEOUTS: dict[str, int] = {
  "agent": 300,  # 5 minutes
  "graph_creation": 60,  # 1 minute
  "subgraph_creation": 60,  # 1 minute
  "repository_provisioning": 60,  # 1 minute
  "graph_materialization": 120,  # 2 minutes
  "file_staging": 60,  # 1 minute
  "document_indexing": 120,  # 2 minutes
}
DEFAULT_TASK_TIMEOUT = 120  # 2 minutes

# Maximum retry attempts before a task is moved to the DLQ
MAX_RETRIES = 3


async def run() -> None:
  """Main worker loop. Blocks until SIGTERM/SIGINT."""
  # socket_timeout must exceed BLMOVE timeout (5s) to avoid spurious TimeoutError.
  # Default socket_timeout is 5s from get_redis_connection_params(), which races with BLMOVE.
  queue = create_async_redis_client(
    ValkeyDatabase.WORKER_QUEUE, decode_responses=True, socket_timeout=30
  )
  manager = get_operation_manager()
  shutdown = asyncio.Event()

  loop = asyncio.get_running_loop()
  for sig in (signal.SIGTERM, signal.SIGINT):
    loop.add_signal_handler(sig, shutdown.set)

  worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
  inflight_key = f"worker:inflight:{worker_id}"
  depth_reporter = QueueDepthReporter(queue, worker_id)
  logger.info(f"Worker started: {worker_id}")

  try:
    while not shutdown.is_set():
      await depth_reporter.maybe_publish()

      # BLMOVE atomically pops from main queue and pushes to inflight list.
      # If the worker crashes, the task stays in inflight for the reaper.
      task_json = await queue.blmove(
        "worker:tasks", inflight_key, timeout=5, wherefrom="RIGHT", whereto="LEFT"
      )
      if task_json is None:
        continue

      try:
        task_data = json.loads(task_json)
      except json.JSONDecodeError:
        logger.error(f"Malformed JSON in task queue: {task_json!r}")
        await queue.lrem(inflight_key, 1, task_json)
        continue

      await _process_task(task_data, task_json, queue, inflight_key, manager, worker_id)
  finally:
    logger.info(f"Worker shutting down: {worker_id}")
    await queue.aclose()


async def _process_task(
  task_data: dict[str, Any],
  task_json: str,
  queue: Any,
  inflight_key: str,
  manager: OperationManager,
  worker_id: str,
) -> None:
  """Process a single task with full lifecycle management.

  On completion (success or failure), removes the task from the inflight list.
  If the process crashes mid-task, the reaper sensor will find it in inflight.
  """
  try:
    task_id = task_data["task_id"]
    task_type = task_data["task_type"]
    graph_id = task_data["graph_id"]
    user_id = task_data["user_id"]
    params = task_data.get("params", {})
  except KeyError as e:
    logger.error(f"Malformed task payload, missing key {e}: {task_data}")
    await queue.lrem(inflight_key, 1, task_json)
    return

  handler_cls = get_task_handler(task_type)
  if handler_cls is None:
    logger.error(f"Unknown task type: {task_type}, task_id={task_id}")
    await manager.fail_operation(task_id, error=f"Unknown task type: {task_type}")
    await queue.lrem(inflight_key, 1, task_json)
    return

  tracer = get_tracer("robosystems.worker")
  start_time = time.time()

  with tracer.start_as_current_span(
    "worker.task.execute",
    attributes={
      "task_id": task_id,
      "task_type": task_type,
      "graph_id": graph_id,
      "worker_id": worker_id,
    },
  ):
    timeout = TASK_TIMEOUTS.get(task_type, DEFAULT_TASK_TIMEOUT)
    try:
      await manager.emit_progress(task_id, "Starting...", progress_percent=0)

      handler = handler_cls(task_id, graph_id, user_id, params, manager)
      result = await asyncio.wait_for(handler.execute(), timeout=timeout)
      duration_ms = (time.time() - start_time) * 1000

      await manager.complete_operation(task_id, result=result)

      logger.info(
        f"Task completed: {task_type} ({task_id}) in {duration_ms:.0f}ms",
        extra={
          "task_id": task_id,
          "task_type": task_type,
          "graph_id": graph_id,
          "duration_ms": duration_ms,
        },
      )

      await report_task_to_dagster(task_type, task_id, graph_id, duration_ms, result)

    except TimeoutError:
      duration_ms = (time.time() - start_time) * 1000
      logger.error(
        f"Task timed out: {task_type} ({task_id}) after {timeout}s",
        extra={
          "task_id": task_id,
          "task_type": task_type,
          "graph_id": graph_id,
          "timeout_seconds": timeout,
          "duration_ms": duration_ms,
        },
      )
      await manager.fail_operation(
        task_id,
        error=f"Task timed out after {timeout}s",
        error_details={"error_type": "TimeoutError", "timeout_seconds": timeout},
      )

    except Exception as e:
      logger.error(
        f"Task failed: {task_type} ({task_id}): {e}",
        exc_info=True,
        extra={
          "task_id": task_id,
          "task_type": task_type,
          "graph_id": graph_id,
        },
      )
      await manager.fail_operation(
        task_id,
        error=str(e),
        error_details={"error_type": type(e).__name__},
      )

    finally:
      # Remove from inflight — task completed (successfully or not)
      await queue.lrem(inflight_key, 1, task_json)
      cleanup_connections()
