"""
Dead Letter Queue (DLQ) configuration for Celery tasks.

This module provides DLQ functionality for failed tasks that have exhausted
all retry attempts. Failed tasks are moved to a separate queue for manual
inspection and potential reprocessing.
"""

import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from celery import Task
from kombu import Exchange, Queue

from robosystems.celery import celery_app, QUEUE_DEFAULT
from robosystems.config import env
from robosystems.logger import logger


# DLQ Configuration - single DLQ for all queues
DLQ_NAME = "dlq"
DLQ_EXCHANGE = Exchange(DLQ_NAME, type="direct", durable=True)
DLQ_QUEUE = Queue(
  DLQ_NAME,
  exchange=DLQ_EXCHANGE,
  routing_key=DLQ_NAME,
  durable=True,
  queue_arguments={
    "x-message-ttl": 7 * 24 * 60 * 60 * 1000,  # 7 days in milliseconds
  },
)

# Add DLQ to Celery configuration
celery_app.conf.task_queues.append(DLQ_QUEUE)


class DLQTask(Task):
  """Base task class with DLQ support."""

  autoretry_for = (Exception,)
  retry_kwargs = {
    "max_retries": env.CELERY_TASK_MAX_RETRIES,
    "countdown": env.CELERY_TASK_RETRY_DELAY,
  }
  retry_backoff = True
  retry_backoff_max = 600  # Max 10 minutes between retries
  retry_jitter = True

  def on_failure(self, exc, task_id, args, kwargs, einfo):
    """Called when task fails after all retries."""
    # Check if this is the final failure (no more retries)
    if self.request.retries >= self.max_retries:
      self._send_to_dlq(exc, task_id, args, kwargs, einfo)

    # Call parent implementation
    super().on_failure(exc, task_id, args, kwargs, einfo)

  def _send_to_dlq(self, exc, task_id, args, kwargs, einfo):
    """Send failed task to DLQ."""
    try:
      # Prepare DLQ message with failure context
      dlq_message = {
        "task_id": task_id,
        "task_name": self.name,
        "args": args,
        "kwargs": kwargs,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "retries": self.request.retries,
        "exception": {
          "type": type(exc).__name__,
          "message": str(exc),
          "traceback": str(einfo) if einfo else traceback.format_exc(),
        },
        "metadata": {
          "queue": self.request.queue or QUEUE_DEFAULT,
          "routing_key": self.request.routing_key,
          "priority": self.request.priority,
          "user_id": kwargs.get("user_id"),
          "entity_id": kwargs.get("entity_id"),
          "graph_id": kwargs.get("graph_id"),
        },
      }

      # Send to DLQ
      celery_app.send_task(
        "robosystems.tasks.dlq.store_failed_task",
        args=[dlq_message],
        queue=DLQ_NAME,
        routing_key=DLQ_NAME,
        priority=1,  # Low priority for DLQ
      )

      logger.error(
        f"Task {task_id} ({self.name}) sent to DLQ after {self.request.retries} retries",
        extra={
          "task_id": task_id,
          "task_name": self.name,
          "dlq_queue": DLQ_NAME,
          "exception_type": type(exc).__name__,
        },
      )

    except Exception as dlq_error:
      # If we can't send to DLQ, log the error but don't raise
      # This prevents infinite loops
      logger.critical(
        f"Failed to send task {task_id} to DLQ: {dlq_error}",
        extra={
          "task_id": task_id,
          "task_name": self.name,
          "dlq_error": str(dlq_error),
        },
      )


# DLQ monitoring task
@celery_app.task(name="robosystems.tasks.dlq.store_failed_task", bind=True)
def store_failed_task(self, dlq_message: Dict[str, Any]) -> Dict[str, Any]:
  """
  Store failed task in DLQ for later inspection.

  In a real implementation, this would store to a database
  for easier querying and reprocessing.
  """
  task_id = dlq_message.get("task_id", "unknown")
  task_name = dlq_message.get("task_name", "unknown")

  logger.warning(
    f"DLQ: Storing failed task {task_id} ({task_name})",
    extra={
      "dlq_message": dlq_message,
      "task_id": task_id,
      "task_name": task_name,
    },
  )

  # TODO: Store in database for persistence and querying
  # For now, just log and return
  return {
    "status": "stored",
    "task_id": task_id,
    "stored_at": datetime.now(timezone.utc).isoformat(),
  }


@celery_app.task(name="robosystems.tasks.dlq.reprocess_dlq_task", bind=True)
def reprocess_dlq_task(self, dlq_message: Dict[str, Any]) -> Dict[str, Any]:
  """
  Reprocess a task from the DLQ.

  This allows manual retry of failed tasks after fixing
  the underlying issue.
  """
  original_task_name = dlq_message.get("task_name")
  original_args = dlq_message.get("args", [])
  original_kwargs = dlq_message.get("kwargs", {})

  if not original_task_name:
    raise ValueError("Cannot reprocess task without task_name")

  logger.info(
    f"Reprocessing DLQ task: {original_task_name}",
    extra={
      "original_task_id": dlq_message.get("task_id"),
      "task_name": original_task_name,
    },
  )

  # Send the original task again
  result = celery_app.send_task(
    original_task_name,
    args=original_args,
    kwargs=original_kwargs,
    queue=dlq_message.get("metadata", {}).get("queue", QUEUE_DEFAULT),
  )

  return {
    "status": "reprocessed",
    "new_task_id": result.id,
    "original_task_id": dlq_message.get("task_id"),
    "reprocessed_at": datetime.now(timezone.utc).isoformat(),
  }


@celery_app.task(name="robosystems.tasks.dlq.get_dlq_stats", bind=True)
def get_dlq_stats(self) -> Dict[str, Any]:
  """
  Get statistics about the DLQ.

  Returns counts and information about failed tasks.
  """
  try:
    # Get queue info from broker
    with celery_app.connection_or_acquire() as conn:
      channel = conn.default_channel
      queue_info = channel.queue_declare(
        queue=DLQ_NAME,
        passive=True,  # Don't create, just check
      )

      message_count = queue_info.message_count

    return {
      "queue_name": DLQ_NAME,
      "message_count": message_count,
      "status": "healthy" if message_count < 100 else "warning",
      "checked_at": datetime.now(timezone.utc).isoformat(),
    }

  except Exception as e:
    logger.error(f"Failed to get DLQ stats: {e}")
    return {
      "queue_name": DLQ_NAME,
      "message_count": -1,
      "status": "error",
      "error": str(e),
      "checked_at": datetime.now(timezone.utc).isoformat(),
    }


# Update task base class for common tasks to use DLQ
def create_dlq_task(**kwargs):
  """Factory function to create tasks with DLQ support."""

  def decorator(func):
    return celery_app.task(base=DLQTask, **kwargs)(func)

  return decorator
