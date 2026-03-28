"""Background worker consumer loop.

Consumes tasks from the Valkey queue (DB 6) via BRPOP, dispatches
to registered task handlers, and manages the full operation lifecycle
through the existing OperationManager (DB 3).
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
from robosystems.worker.tasks import get_task_handler

logger = logging.getLogger(__name__)


async def run() -> None:
  """Main worker loop. Blocks until SIGTERM/SIGINT."""
  # socket_timeout must exceed BRPOP timeout (5s) to avoid spurious TimeoutError.
  # Default socket_timeout is 5s from get_redis_connection_params(), which races with BRPOP.
  queue = create_async_redis_client(
    ValkeyDatabase.WORKER_QUEUE, decode_responses=True, socket_timeout=30
  )
  manager = get_operation_manager()
  shutdown = asyncio.Event()

  loop = asyncio.get_running_loop()
  for sig in (signal.SIGTERM, signal.SIGINT):
    loop.add_signal_handler(sig, shutdown.set)

  worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
  logger.info(f"Worker started: {worker_id}")

  try:
    while not shutdown.is_set():
      result = await queue.brpop("worker:tasks", timeout=5)
      if result is None:
        continue

      _, task_json = result
      try:
        task_data = json.loads(task_json)
      except json.JSONDecodeError:
        logger.error(f"Malformed JSON in task queue: {task_json!r}")
        continue
      await _process_task(task_data, manager, worker_id)
  finally:
    logger.info(f"Worker shutting down: {worker_id}")
    await queue.aclose()


async def _process_task(
  task_data: dict[str, Any],
  manager: OperationManager,
  worker_id: str,
) -> None:
  """Process a single task with full lifecycle management."""
  try:
    task_id = task_data["task_id"]
    task_type = task_data["task_type"]
    graph_id = task_data["graph_id"]
    user_id = task_data["user_id"]
    params = task_data.get("params", {})
  except KeyError as e:
    logger.error(f"Malformed task payload, missing key {e}: {task_data}")
    return

  handler_cls = get_task_handler(task_type)
  if handler_cls is None:
    logger.error(f"Unknown task type: {task_type}, task_id={task_id}")
    await manager.fail_operation(task_id, error=f"Unknown task type: {task_type}")
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
    try:
      await manager.emit_progress(task_id, "Starting...", progress_percent=0)

      handler = handler_cls(task_id, graph_id, user_id, params, manager)
      result = await handler.execute()
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
      cleanup_connections()
