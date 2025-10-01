"""
Shared utilities for SSE progress tracking in Celery tasks.

This module provides common functionality for emitting progress events
from Celery workers to SSE streams, reducing code duplication across
task implementations.
"""

import logging
from typing import Optional, Dict, Any, Callable
from celery import current_task
from robosystems.config import env
from .event_storage import SSEEventStorage, EventType

logger = logging.getLogger(__name__)


class TaskSSEProgressTracker:
  """
  Utility class for emitting SSE progress events from Celery tasks.

  Provides standardized progress tracking, error handling, and cancellation
  checking for tasks that need to report progress via SSE streams.
  """

  def __init__(self, operation_id: str, celery_app=None):
    """
    Initialize the progress tracker.

    Args:
        operation_id: SSE operation ID for progress tracking
        celery_app: Celery app instance (optional, will import if not provided)
    """
    self.operation_id = operation_id
    self.celery_app = celery_app
    self.event_storage = SSEEventStorage()
    self.sse_enabled = env.SSE_ENABLED
    self.redis_failures = 0
    self.max_redis_failures = env.SSE_MAX_REDIS_FAILURES

  def emit_progress(
    self,
    message: str,
    progress_percent: Optional[float] = None,
    details: Optional[Dict[str, Any]] = None,
  ):
    """
    Emit a progress event synchronously with Redis circuit breaker.

    Args:
        message: Progress message to display
        progress_percent: Optional progress percentage (0-100)
        details: Optional additional details dictionary
    """
    logger.info(f"emit_progress called: {message}, progress: {progress_percent}")

    # Circuit breaker: Skip SSE if disabled or too many Redis failures
    if not self.sse_enabled or self.redis_failures >= self.max_redis_failures:
      if self.redis_failures >= self.max_redis_failures:
        logger.warning(
          f"SSE circuit breaker open - Redis failures: {self.redis_failures}/{self.max_redis_failures}. "
          f"Operation {self.operation_id} continuing without SSE."
        )
      else:
        logger.debug(
          f"SSE disabled, skipping progress event for operation {self.operation_id}"
        )
      return  # Gracefully continue without SSE

    try:
      # Store the event directly in Redis using sync method
      event_data = {
        "message": message,
        "progress_percent": progress_percent,
      }
      if details:
        event_data.update(details)

      # Use the synchronous store_event method
      self.event_storage.store_event_sync(
        self.operation_id,
        EventType.OPERATION_PROGRESS,
        event_data,
      )
      logger.info(f"Progress event stored and published: {message}")

      # Reset failure counter on success
      self.redis_failures = 0

      # Emit OpenTelemetry metric for successful SSE event
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_emitted(self.operation_id, "progress")
      except Exception:
        pass  # Don't fail if metrics aren't available

    except Exception as e:
      self.redis_failures += 1
      logger.warning(
        f"Failed to emit SSE progress (failure {self.redis_failures}/{self.max_redis_failures}): {e}. "
        f"Operation {self.operation_id} continuing without this event."
      )

      # Emit OpenTelemetry metric for SSE failure
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_failed(self.operation_id, "redis_error")
      except Exception:
        pass  # Don't fail if metrics aren't available

  def emit_completion(
    self, result: Dict[str, Any], additional_context: Optional[Dict[str, Any]] = None
  ):
    """
    Emit a completion event with Redis circuit breaker.

    Args:
        result: Task result dictionary
        additional_context: Optional additional context for the completion event
    """
    # Skip if SSE is disabled or circuit breaker is open
    if not self.sse_enabled or self.redis_failures >= self.max_redis_failures:
      logger.debug(
        f"SSE unavailable, skipping completion event for operation {self.operation_id}"
      )
      return

    try:
      completion_data = {
        "message": "Operation completed successfully!",
        "result": result,
      }

      if additional_context:
        completion_data.update(additional_context)

      # Store completion event using sync method
      self.event_storage.store_event_sync(
        self.operation_id,
        EventType.OPERATION_COMPLETED,
        completion_data,
      )
      logger.info(
        f"Completion event stored and published for operation {self.operation_id}"
      )

      # Emit OpenTelemetry metric
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_emitted(self.operation_id, "completion")
      except Exception:
        pass

    except Exception as e:
      logger.warning(
        f"Failed to emit completion event: {e}. Operation completed successfully regardless."
      )

      # Emit OpenTelemetry metric for failure
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_failed(self.operation_id, "redis_error")
      except Exception:
        pass

  def emit_error(
    self, error: Exception, additional_context: Optional[Dict[str, Any]] = None
  ):
    """
    Emit an error event with Redis circuit breaker.

    Args:
        error: The exception that occurred
        additional_context: Optional additional context for the error event
    """
    error_type = type(error).__name__
    error_message = f"Operation failed: {str(error)}"

    # Skip if SSE is disabled or circuit breaker is open
    if not self.sse_enabled or self.redis_failures >= self.max_redis_failures:
      logger.debug(
        f"SSE unavailable, skipping error event for operation {self.operation_id}"
      )
      return

    try:
      error_data = {
        "message": error_message,
        "error_type": error_type,
      }

      if additional_context:
        error_data.update(additional_context)

      self.event_storage.store_event_sync(
        self.operation_id,
        EventType.OPERATION_ERROR,
        error_data,
      )
      logger.info(f"Error event stored and published for operation {self.operation_id}")

      # Emit OpenTelemetry metric
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_emitted(self.operation_id, "error")
      except Exception:
        pass

    except Exception as emit_error:
      logger.warning(
        f"Failed to emit error event: {emit_error}. Original error will still be raised."
      )

      # Emit OpenTelemetry metric for failure
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_failed(self.operation_id, "redis_error")
      except Exception:
        pass

  def emit_cancellation(self, message: str = "Operation was cancelled"):
    """
    Emit a cancellation event with Redis circuit breaker.

    Args:
        message: Cancellation message
    """
    # Skip if SSE is disabled or circuit breaker is open
    if not self.sse_enabled or self.redis_failures >= self.max_redis_failures:
      logger.debug(
        f"SSE unavailable, skipping cancellation event for operation {self.operation_id}"
      )
      return

    try:
      self.event_storage.store_event_sync(
        self.operation_id,
        EventType.OPERATION_CANCELLED,
        {"message": message},
      )
      logger.info(
        f"Cancellation event stored and published for operation {self.operation_id}"
      )

      # Emit OpenTelemetry metric
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_emitted(self.operation_id, "cancellation")
      except Exception:
        pass

    except Exception as e:
      logger.warning(
        f"Failed to emit cancellation event: {e}. Task cancellation still processed."
      )

      # Emit OpenTelemetry metric for failure
      try:
        from robosystems.middleware.otel.metrics import get_endpoint_metrics

        metrics = get_endpoint_metrics()
        metrics.record_sse_event_failed(self.operation_id, "redis_error")
      except Exception:
        pass

  def check_cancellation(self, task_request=None):
    """
    Check if the current Celery task has been cancelled.

    Args:
        task_request: Optional task request object (uses current_task if not provided)

    Raises:
        Exception: If the task was cancelled
    """
    if task_request is None:
      task_request = getattr(current_task, "request", None)

    if task_request and not getattr(task_request, "called_directly", True):
      if self.celery_app is None:
        from ...celery import celery_app

        self.celery_app = celery_app

      result = self.celery_app.AsyncResult(task_request.id)
      if result.state == "REVOKED":
        logger.info(f"Task {task_request.id} was cancelled")
        self.emit_cancellation("Celery task was cancelled")
        raise Exception("Task was cancelled")

  def create_progress_callback(self, task_request=None) -> Callable:
    """
    Create a combined progress and cancellation callback function.

    Args:
        task_request: Optional task request object

    Returns:
        Callable that emits progress and checks for cancellation
    """

    def progress_callback(message: str, progress_percent: Optional[float] = None):
      self.emit_progress(message, progress_percent)
      self.check_cancellation(task_request)

    return progress_callback
