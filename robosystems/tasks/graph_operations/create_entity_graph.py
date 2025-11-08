import logging
from typing import Dict, Any
from ...celery import celery_app
from ...operations.graph.entity_graph_service import EntityGraphServiceSync
from ...operations.graph.subscription_service import GraphSubscriptionService
from ...database import session as db_session, get_db_session

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="create_entity_with_new_graph")
def create_entity_with_new_graph_task(
  self, entity_data_dict: Dict[str, Any], user_id: str
) -> Dict[str, Any]:
  """
  Create a new entity with its own graph database asynchronously.

  This task serves as a thin orchestration layer that delegates the actual
  business logic to EntityService while handling Celery-specific concerns
  like cancellation checking and error reporting.

  Args:
      entity_data_dict: Entity creation data as dictionary
      user_id: ID of the user creating the entity

  Returns:
      Dictionary containing graph_id and entity information

  Raises:
      Exception: If any step of the process fails
  """
  logger.info(f"Starting entity creation task for user_id: {user_id}")

  # Initialize entity service with a fresh session
  entity_service = EntityGraphServiceSync(session=db_session)

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

    # Delegate business logic to service
    result = entity_service.create_entity_with_new_graph(
      entity_data_dict=entity_data_dict,
      user_id=user_id,
      cancellation_callback=check_cancellation,
    )

    # Create billing subscription for the graph
    try:
      session = next(get_db_session())
      try:
        subscription_service = GraphSubscriptionService(session)
        plan_name = entity_data_dict.get("graph_tier", "kuzu-standard")
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

    logger.info(f"Entity creation task completed successfully for user {user_id}")
    return result

  except Exception as e:
    # Log task-specific error details
    logger.error(
      f"Entity creation task failed for user {user_id}: {type(e).__name__}: {str(e)}"
    )
    logger.error(
      f"Task parameters - user_id: {user_id}, entity_data: {entity_data_dict}"
    )

    # Re-raise for Celery error handling
    raise


@celery_app.task(bind=True, name="create_entity_with_new_graph_sse")
def create_entity_with_new_graph_sse_task(
  self, entity_data_dict: Dict[str, Any], user_id: str, operation_id: str
) -> Dict[str, Any]:
  """
  Create a new entity with its own graph database with SSE progress tracking.

  This SSE-compatible version of the entity creation task emits real-time
  progress events through the unified operation monitoring system.

  Args:
      entity_data_dict: Entity creation data as dictionary
      user_id: ID of the user creating the entity
      operation_id: SSE operation ID for progress tracking

  Returns:
      Dictionary containing graph_id and entity information

  Raises:
      Exception: If any step of the process fails
  """
  logger.info(
    f"Starting SSE entity creation task for user_id: {user_id}, operation_id: {operation_id}"
  )

  # Initialize SSE progress tracker
  from robosystems.middleware.sse.task_progress import TaskSSEProgressTracker

  progress_tracker = TaskSSEProgressTracker(operation_id)

  # Initialize entity service with a fresh session
  entity_service = EntityGraphServiceSync(session=db_session)

  def check_cancellation():
    """Check if task has been cancelled."""
    progress_tracker.check_cancellation(self.request)

  try:
    logger.info(f"About to emit started event for operation {operation_id}")
    # Emit started event
    progress_tracker.emit_progress("Starting entity and graph creation...", 0)

    # Check for initial cancellation
    check_cancellation()
    logger.info(f"About to emit validation event for operation {operation_id}")
    progress_tracker.emit_progress("Validating entity data...", 10)

    # Create enhanced progress callback that provides meaningful step updates
    def progress_callback(message: str, progress: int = None):
      if progress is not None:
        progress_tracker.emit_progress(message, progress)
      else:
        progress_tracker.emit_progress(message, 0)

    # Delegate business logic to service with detailed progress tracking
    result = entity_service.create_entity_with_new_graph(
      entity_data_dict=entity_data_dict,
      user_id=user_id,
      cancellation_callback=check_cancellation,
      progress_callback=progress_callback,
    )

    # Create billing subscription for the graph
    progress_tracker.emit_progress("Creating billing subscription...", 95)
    try:
      session = next(get_db_session())
      try:
        subscription_service = GraphSubscriptionService(session)
        plan_name = entity_data_dict.get("graph_tier", "kuzu-standard")
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

    # Emit completion event with entity-specific context
    progress_tracker.emit_completion(
      result,
      additional_context={
        "graph_id": result.get("graph_id"),
        "entity_name": entity_data_dict.get("name"),
      },
    )

    logger.info(f"SSE entity creation task completed successfully for user {user_id}")
    return result

  except Exception as e:
    # Emit error event with entity-specific context
    progress_tracker.emit_error(
      e,
      additional_context={
        "user_id": user_id,
        "entity_name": entity_data_dict.get("name"),
      },
    )

    # Log task-specific error details
    logger.error(
      f"SSE entity creation task failed for user {user_id}: {type(e).__name__}: {str(e)}"
    )
    logger.error(
      f"Task parameters - user_id: {user_id}, operation_id: {operation_id}, entity_data: {entity_data_dict}"
    )

    # Re-raise for Celery error handling
    raise
