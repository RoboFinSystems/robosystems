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

import redis.exceptions

from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
from robosystems.middleware.otel.setup import get_tracer
from robosystems.middleware.sse.operation_manager import (
  OperationManager,
  get_operation_manager,
)
from robosystems.security.error_handling import safe_error_message
from robosystems.worker.cleanup import cleanup_connections
from robosystems.worker.constants import DEFAULT_TASK_TIMEOUT, TASK_TIMEOUTS
from robosystems.worker.metrics import QueueDepthPublisher
from robosystems.worker.task_protection import (
  PROTECT_MIN_TIMEOUT_SECONDS,
  TaskProtectionManager,
)
from robosystems.worker.tasks import get_task_handler

logger = logging.getLogger(__name__)


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
  depth_publisher = QueueDepthPublisher()
  depth_publisher.start()
  protection = TaskProtectionManager()
  logger.info(f"Worker started: {worker_id}")

  shutdown_wait = asyncio.create_task(shutdown.wait())
  try:
    while not shutdown.is_set():
      # BLMOVE atomically pops from main queue and pushes to inflight list.
      # If the worker crashes, the task stays in inflight for the reaper.
      # Race against shutdown so SIGTERM exits without waiting for BLMOVE timeout.
      blmove_task = asyncio.create_task(
        queue.blmove("worker:tasks", inflight_key, timeout=5, src="LEFT", dest="LEFT")
      )
      await asyncio.wait(
        {blmove_task, shutdown_wait}, return_when=asyncio.FIRST_COMPLETED
      )
      if shutdown.is_set():
        blmove_task.cancel()
        break

      try:
        task_json = blmove_task.result()
      except redis.exceptions.ConnectionError as e:
        logger.warning(f"Valkey connection lost, retrying: {e}")
        await asyncio.sleep(1)
        continue
      if task_json is None:
        continue

      try:
        task_data = json.loads(task_json)
      except json.JSONDecodeError:
        logger.error(f"Malformed JSON in task queue: {task_json!r}")
        await queue.lrem(inflight_key, 1, task_json)
        continue

      try:
        await _process_task(
          task_data, task_json, queue, inflight_key, manager, worker_id, protection
        )
      except Exception:
        # A task must never take the worker down with it. `_process_task`
        # handles its own failure reporting; anything that escapes it is
        # logged and the loop moves on to the next task.
        logger.exception(
          f"Unhandled error processing task {task_data.get('task_id')}; "
          f"worker continues"
        )
  finally:
    logger.info(f"Worker shutting down: {worker_id}")
    depth_publisher.stop()
    shutdown_wait.cancel()
    try:
      await queue.aclose()
    except redis.exceptions.ConnectionError as exc:
      # Connection already dropped during shutdown — non-fatal; the
      # server-side close will release any remaining resources.
      logger.debug(f"Connection error during queue close (expected on shutdown): {exc}")
    logger.info(f"Worker terminated: {worker_id}")


async def _fail_quietly(manager: OperationManager, task_id: str, **kwargs: Any) -> None:
  """Record a task failure without letting the recording itself raise.

  `fail_operation` raises when the operation's metadata has already expired
  (a task outliving its SSE TTL). Raising from inside an `except` handler
  would escape `_process_task` and, with nothing above it, exit the worker —
  so one expired long task would kill the process. Log and carry on instead.
  """
  try:
    await manager.fail_operation(task_id, **kwargs)
  except Exception as exc:
    logger.error(
      f"Could not record failure for task {task_id}: {type(exc).__name__}: {exc}"
    )


async def _process_task(
  task_data: dict[str, Any],
  task_json: str,
  queue: Any,
  inflight_key: str,
  manager: OperationManager,
  worker_id: str,
  protection: TaskProtectionManager,
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
    # Only long tasks need scale-in protection; short ones drain within the
    # SIGTERM grace, so protecting them would just churn the ECS API.
    protect_task = timeout >= PROTECT_MIN_TIMEOUT_SECONDS
    try:
      if protect_task:
        await protection.protect()
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
      await _fail_quietly(
        manager,
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
      # The stored error replays to the customer via SSE and operation status;
      # infrastructure exception text (boto3/psycopg2/httpx internals) stays in
      # the server log above and the customer gets a reference id instead.
      await _fail_quietly(
        manager,
        task_id,
        error=safe_error_message(e) or f"Operation failed — reference {task_id}",
        error_details={"error_type": type(e).__name__},
      )

    finally:
      # Clear scale-in protection — worker is idle again and safe to terminate.
      if protect_task:
        await protection.unprotect()
      # Remove from inflight — task completed (successfully or not)
      await queue.lrem(inflight_key, 1, task_json)
      cleanup_connections()
