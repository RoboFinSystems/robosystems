"""
Graph provisioning task for payment-first flow.

This task is called by webhooks after payment confirmation to provision
a graph that was created during the checkout flow.
"""

import logging
from typing import Dict, Any
from sqlalchemy.exc import OperationalError
from ...celery import celery_app
from ...operations.graph.generic_graph_service import GenericGraphServiceSync
from ...database import get_db_session
from ...models.billing import BillingSubscription

logger = logging.getLogger(__name__)


@celery_app.task(
  bind=True,
  name="provision_graph",
  autoretry_for=(ConnectionError, TimeoutError, OperationalError),
  retry_kwargs={"max_retries": 3, "countdown": 60},
  retry_backoff=True,
  retry_backoff_max=600,
)
def provision_graph_task(
  self, user_id: str, subscription_id: str, graph_config: Dict[str, Any]
) -> Dict[str, Any]:
  """
  Provision a graph database after payment confirmation.

  This task is called by payment webhooks after a user has added a payment
  method. It creates the graph and updates the existing subscription.

  Args:
      user_id: ID of the user who owns the graph
      subscription_id: ID of the existing subscription in PENDING_PAYMENT status
      graph_config: Dictionary containing graph configuration:
          - graph_id: Optional requested graph ID
          - schema_extensions: List of schema extensions to install
          - metadata: Graph metadata (name, description, type, tags)
          - tier: Service tier (standard, enterprise, premium)
          - initial_data: Optional initial data to populate
          - custom_schema: Optional custom schema definition

  Returns:
      Dictionary containing graph_id and creation details

  Raises:
      Exception: If any step of the process fails
  """
  logger.info(
    f"Starting graph provisioning for user {user_id}, subscription {subscription_id}"
  )

  session = next(get_db_session())

  try:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise Exception(f"Subscription {subscription_id} not found")

    if subscription.status != "provisioning":
      logger.warning(
        f"Subscription {subscription_id} is in status {subscription.status}, "
        f"expected 'provisioning'"
      )

    graph_service = GenericGraphServiceSync()

    def check_cancellation():
      """Check if task has been cancelled."""
      if self.request.called_directly is False:
        result = celery_app.AsyncResult(self.request.id)
        if result.state == "REVOKED":
          logger.info(f"Task {self.request.id} was cancelled")
          raise Exception("Task was cancelled")

    check_cancellation()

    result = graph_service.create_graph(
      graph_id=graph_config.get("graph_id"),
      schema_extensions=graph_config.get("schema_extensions", []),
      metadata=graph_config.get("metadata", {}),
      tier=graph_config.get("tier", "kuzu-standard"),
      initial_data=graph_config.get("initial_data"),
      user_id=user_id,
      custom_schema=graph_config.get("custom_schema"),
      cancellation_callback=check_cancellation,
    )

    graph_id = result.get("graph_id")

    subscription.resource_id = graph_id
    subscription.activate(session)

    session.commit()

    logger.info(
      "Graph provisioning completed successfully",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "graph_id": graph_id,
      },
    )

    return result

  except Exception as e:
    logger.error(
      f"Graph provisioning failed: {type(e).__name__}: {str(e)}",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
      },
    )

    try:
      session.rollback()
    except Exception as rollback_error:
      logger.error(f"Failed to rollback transaction: {rollback_error}")

    cleanup_partial_resources = False
    if hasattr(self.request, "retries") and self.request.retries >= 3:
      cleanup_partial_resources = True
    elif not hasattr(self.request, "retries"):
      cleanup_partial_resources = True

    if (
      cleanup_partial_resources and "result" in locals() and "graph_service" in locals()
    ):
      try:
        graph_id = result["graph_id"]  # type: ignore[possibly-unbound]
        logger.warning(
          f"Attempting cleanup of partially created graph {graph_id}",
          extra={"graph_id": graph_id, "user_id": user_id},
        )

        graph_service.delete_graph(  # type: ignore[possibly-unbound]
          graph_id=graph_id, user_id=user_id
        )

        logger.info(
          f"Successfully cleaned up graph {graph_id}",
          extra={"graph_id": graph_id, "user_id": user_id},
        )
      except Exception as cleanup_error:
        logger.error(
          f"Failed to cleanup graph {graph_id}: {cleanup_error}",  # type: ignore[possibly-unbound]
          extra={"graph_id": graph_id, "cleanup_error": str(cleanup_error)},  # type: ignore[possibly-unbound]
        )

    try:
      if "subscription" not in locals():
        subscription = (
          session.query(BillingSubscription)
          .filter(BillingSubscription.id == subscription_id)
          .first()
        )
        if not subscription:
          raise Exception(f"Subscription {subscription_id} not found for status update")
      else:
        session.refresh(subscription)  # type: ignore[possibly-unbound]

      if subscription:  # type: ignore[possibly-unbound]
        subscription.status = "failed"
        if subscription.subscription_metadata:
          subscription.subscription_metadata["error"] = str(e)  # type: ignore[index]
        else:
          subscription.subscription_metadata = {"error": str(e)}
        session.commit()
    except Exception as update_error:
      logger.error(f"Failed to update subscription status: {update_error}")
      try:
        session.rollback()
      except Exception:
        pass

    raise

  finally:
    session.close()


@celery_app.task(
  bind=True,
  name="provision_graph_sse",
  autoretry_for=(ConnectionError, TimeoutError, OperationalError),
  retry_kwargs={"max_retries": 3, "countdown": 60},
  retry_backoff=True,
  retry_backoff_max=600,
)
def provision_graph_sse_task(
  self,
  user_id: str,
  subscription_id: str,
  graph_config: Dict[str, Any],
  operation_id: str,
) -> Dict[str, Any]:
  """
  Provision a graph database with SSE progress tracking.

  This SSE-compatible version emits real-time progress events through
  the unified operation monitoring system.

  Args:
      user_id: ID of the user who owns the graph
      subscription_id: ID of the existing subscription
      graph_config: Dictionary containing graph configuration
      operation_id: SSE operation ID for progress tracking

  Returns:
      Dictionary containing graph_id and creation details

  Raises:
      Exception: If any step of the process fails
  """
  logger.info(
    f"Starting SSE graph provisioning for user {user_id}, "
    f"subscription {subscription_id}, operation {operation_id}"
  )

  from robosystems.middleware.sse.task_progress import TaskSSEProgressTracker

  progress_tracker = TaskSSEProgressTracker(operation_id)
  session = next(get_db_session())

  try:
    progress_tracker.emit_progress("Starting graph provisioning...", 0)

    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise Exception(f"Subscription {subscription_id} not found")

    if subscription.status != "provisioning":
      logger.warning(
        f"Subscription {subscription_id} is in status {subscription.status}, "
        f"expected 'provisioning'"
      )

    graph_service = GenericGraphServiceSync()

    def check_cancellation():
      """Check if task has been cancelled."""
      progress_tracker.check_cancellation(self.request)

    def progress_callback(message: str, progress: int = None):
      if progress is not None:
        progress_tracker.emit_progress(message, progress)
      else:
        progress_tracker.emit_progress(message, 0)

    check_cancellation()
    progress_tracker.emit_progress("Validating graph configuration...", 10)

    result = graph_service.create_graph(
      graph_id=graph_config.get("graph_id"),
      schema_extensions=graph_config.get("schema_extensions", []),
      metadata=graph_config.get("metadata", {}),
      tier=graph_config.get("tier", "kuzu-standard"),
      initial_data=graph_config.get("initial_data"),
      user_id=user_id,
      custom_schema=graph_config.get("custom_schema"),
      cancellation_callback=check_cancellation,
      progress_callback=progress_callback,
    )

    graph_id = result.get("graph_id")

    progress_tracker.emit_progress("Activating subscription...", 95)

    subscription.resource_id = graph_id
    subscription.activate(session)

    session.commit()

    progress_tracker.emit_completion(
      result,
      additional_context={
        "graph_id": graph_id,
        "subscription_id": subscription_id,
        "graph_name": graph_config.get("metadata", {}).get("graph_name"),
      },
    )

    logger.info(
      "SSE graph provisioning completed successfully",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "graph_id": graph_id,
        "operation_id": operation_id,
      },
    )

    return result

  except Exception as e:
    progress_tracker.emit_error(
      e,
      additional_context={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "graph_name": graph_config.get("metadata", {}).get("graph_name"),
      },
    )

    logger.error(
      f"SSE graph provisioning failed: {type(e).__name__}: {str(e)}",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "operation_id": operation_id,
      },
    )

    try:
      session.rollback()
    except Exception as rollback_error:
      logger.error(f"Failed to rollback transaction: {rollback_error}")

    cleanup_partial_resources = False
    if hasattr(self.request, "retries") and self.request.retries >= 3:
      cleanup_partial_resources = True
    elif not hasattr(self.request, "retries"):
      cleanup_partial_resources = True

    if (
      cleanup_partial_resources and "result" in locals() and "graph_service" in locals()
    ):
      try:
        graph_id = result["graph_id"]  # type: ignore[possibly-unbound]
        logger.warning(
          f"Attempting cleanup of partially created graph {graph_id}",
          extra={
            "graph_id": graph_id,
            "user_id": user_id,
            "operation_id": operation_id,
          },
        )

        graph_service.delete_graph(  # type: ignore[possibly-unbound]
          graph_id=graph_id, user_id=user_id
        )

        progress_tracker.emit_progress(f"Cleaned up graph {graph_id}", 0)

        logger.info(
          f"Successfully cleaned up graph {graph_id}",
          extra={
            "graph_id": graph_id,
            "user_id": user_id,
            "operation_id": operation_id,
          },
        )
      except Exception as cleanup_error:
        logger.error(
          f"Failed to cleanup graph {graph_id}: {cleanup_error}",  # type: ignore[possibly-unbound]
          extra={
            "graph_id": graph_id,  # type: ignore[possibly-unbound]
            "cleanup_error": str(cleanup_error),
            "operation_id": operation_id,
          },
        )

    try:
      if "subscription" not in locals():
        subscription = (
          session.query(BillingSubscription)
          .filter(BillingSubscription.id == subscription_id)
          .first()
        )
        if not subscription:
          raise Exception(f"Subscription {subscription_id} not found for status update")
      else:
        session.refresh(subscription)  # type: ignore[possibly-unbound]

      if subscription:  # type: ignore[possibly-unbound]
        subscription.status = "failed"
        if subscription.subscription_metadata:
          subscription.subscription_metadata["error"] = str(e)  # type: ignore[index]
        else:
          subscription.subscription_metadata = {"error": str(e)}
        session.commit()
    except Exception as update_error:
      logger.error(f"Failed to update subscription status: {update_error}")
      try:
        session.rollback()
      except Exception:
        pass

    raise

  finally:
    session.close()
