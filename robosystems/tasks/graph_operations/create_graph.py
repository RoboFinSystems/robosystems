"""
Generic graph creation task.
"""

import logging
from typing import Dict, Any
from ...celery import celery_app
from ...operations.graph.generic_graph_service import GenericGraphServiceSync
from ...operations.graph.subscription_service import GraphSubscriptionService
from ...database import get_db_session

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="create_graph")
def create_graph_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
  """
  Create a new graph database asynchronously.

  This task creates a generic graph database that can be used for any purpose,
  not tied to a specific entity type like entity.

  Args:
      task_data: Dictionary containing:
          - graph_id: Optional requested graph ID
          - schema_extensions: List of schema extensions to install
          - metadata: Graph metadata (name, description, type, tags)
          - tier: Service tier (standard, enterprise, premium)
          - initial_data: Optional initial data to populate
          - user_id: ID of the user creating the graph
          - custom_schema: Optional custom schema definition

  Returns:
      Dictionary containing graph_id and creation details

  Raises:
      Exception: If any step of the process fails
  """
  user_id = task_data.get("user_id")
  logger.info(f"Starting graph creation task for user_id: {user_id}")

  # Initialize generic graph service
  graph_service = GenericGraphServiceSync()

  def check_cancellation():
    """Check if task has been cancelled."""
    if self.request.called_directly is False:
      result = celery_app.AsyncResult(self.request.id)
      if result.state == "REVOKED":
        logger.info(f"Task {self.request.id} was cancelled")
        raise Exception("Task was cancelled")

  try:
    # Check for initial cancellation
    check_cancellation()

    # Delegate to service
    result = graph_service.create_graph(
      graph_id=task_data.get("graph_id"),
      schema_extensions=task_data.get("schema_extensions", []),
      metadata=task_data.get("metadata", {}),
      tier=task_data.get("tier", "kuzu-standard"),
      initial_data=task_data.get("initial_data"),
      user_id=user_id,
      custom_schema=task_data.get("custom_schema"),
      cancellation_callback=check_cancellation,
    )

    # Create billing subscription for the graph
    try:
      session = next(get_db_session())
      try:
        subscription_service = GraphSubscriptionService(session)
        plan_name = task_data.get("tier", "kuzu-standard")
        graph_id = result.get("graph_id")

        subscription = subscription_service.create_graph_subscription(
          user_id=user_id,
          graph_id=graph_id,
          plan_name=plan_name,
        )

        logger.info(
          f"Created billing subscription {subscription.id} for graph {graph_id}",
          extra={
            "user_id": user_id,
            "graph_id": graph_id,
            "subscription_id": subscription.id,
            "plan_name": plan_name,
          },
        )
      finally:
        session.close()
    except Exception as billing_error:
      logger.error(
        f"Failed to create billing subscription for graph {result.get('graph_id')}: {billing_error}",
        extra={"user_id": user_id, "graph_id": result.get("graph_id")},
      )

    logger.info(f"Graph creation task completed successfully for user {user_id}")
    return result

  except Exception as e:
    # Log task-specific error details
    logger.error(
      f"Graph creation task failed for user {user_id}: {type(e).__name__}: {str(e)}"
    )
    logger.error(f"Task parameters: {task_data}")

    # Re-raise for Celery error handling
    raise


@celery_app.task(bind=True, name="create_graph_sse")
def create_graph_sse_task(
  self, task_data: Dict[str, Any], operation_id: str
) -> Dict[str, Any]:
  """
  Create a new graph database with SSE progress tracking.

  This SSE-compatible version of the graph creation task emits real-time
  progress events through the unified operation monitoring system.

  Args:
      task_data: Dictionary containing graph creation parameters
      operation_id: SSE operation ID for progress tracking

  Returns:
      Dictionary containing graph_id and creation details

  Raises:
      Exception: If any step of the process fails
  """
  user_id = task_data.get("user_id")
  logger.info(
    f"Starting SSE graph creation task for user_id: {user_id}, operation_id: {operation_id}"
  )

  # Initialize SSE progress tracker
  from robosystems.middleware.sse.task_progress import TaskSSEProgressTracker

  progress_tracker = TaskSSEProgressTracker(operation_id)

  # Initialize generic graph service
  graph_service = GenericGraphServiceSync()

  def check_cancellation():
    """Check if task has been cancelled."""
    progress_tracker.check_cancellation(self.request)

  try:
    logger.info(f"About to emit started event for operation {operation_id}")
    # Emit started event
    progress_tracker.emit_progress("Starting graph creation...", 0)

    # Check for initial cancellation
    check_cancellation()
    logger.info(f"About to emit validation event for operation {operation_id}")
    progress_tracker.emit_progress("Validating graph configuration...", 10)

    # Create enhanced progress callback that provides meaningful step updates
    def progress_callback(message: str, progress: int = None):
      if progress is not None:
        progress_tracker.emit_progress(message, progress)
      else:
        progress_tracker.emit_progress(message, 0)

    # Delegate to service with progress tracking
    result = graph_service.create_graph(
      graph_id=task_data.get("graph_id"),
      schema_extensions=task_data.get("schema_extensions", []),
      metadata=task_data.get("metadata", {}),
      tier=task_data.get("tier", "kuzu-standard"),
      initial_data=task_data.get("initial_data"),
      user_id=user_id,
      custom_schema=task_data.get("custom_schema"),
      cancellation_callback=check_cancellation,
      progress_callback=progress_callback,
    )

    # Create billing subscription for the graph
    progress_tracker.emit_progress("Creating billing subscription...", 95)
    try:
      session = next(get_db_session())
      try:
        subscription_service = GraphSubscriptionService(session)
        plan_name = task_data.get("tier", "kuzu-standard")
        graph_id = result.get("graph_id")

        subscription = subscription_service.create_graph_subscription(
          user_id=user_id,
          graph_id=graph_id,
          plan_name=plan_name,
        )

        logger.info(
          f"Created billing subscription {subscription.id} for graph {graph_id}",
          extra={
            "user_id": user_id,
            "graph_id": graph_id,
            "subscription_id": subscription.id,
            "plan_name": plan_name,
          },
        )
      finally:
        session.close()
    except Exception as billing_error:
      logger.error(
        f"Failed to create billing subscription for graph {result.get('graph_id')}: {billing_error}",
        extra={"user_id": user_id, "graph_id": result.get("graph_id")},
      )

    # Emit completion event with graph-specific context
    progress_tracker.emit_completion(
      result,
      additional_context={
        "graph_id": result.get("graph_id"),
        "graph_name": task_data.get("metadata", {}).get("graph_name"),
      },
    )

    logger.info(f"SSE graph creation task completed successfully for user {user_id}")
    return result

  except Exception as e:
    # Emit error event with graph-specific context
    progress_tracker.emit_error(
      e,
      additional_context={
        "user_id": user_id,
        "graph_name": task_data.get("metadata", {}).get("graph_name"),
      },
    )

    # Log task-specific error details
    logger.error(
      f"SSE graph creation task failed for user {user_id}: {type(e).__name__}: {str(e)}"
    )
    logger.error(
      f"Task parameters: operation_id: {operation_id}, task_data: {task_data}"
    )

    # Re-raise for Celery error handling
    raise
