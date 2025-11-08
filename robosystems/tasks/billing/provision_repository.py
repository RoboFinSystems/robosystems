"""
Repository access provisioning task for payment-first flow.

This task is called by webhooks after payment confirmation to grant
repository access that was requested during the checkout flow.
"""

import logging
from sqlalchemy.exc import OperationalError
from ...celery import celery_app
from ...database import get_db_session
from ...models.billing import (
  BillingSubscription,
  BillingCustomer,
  BillingAuditLog,
  BillingEventType,
)
from ...operations.graph.repository_subscription_service import (
  RepositorySubscriptionService,
)
from ...operations.graph.subscription_service import generate_subscription_invoice
from ...models.iam import RepositoryType, RepositoryPlan

logger = logging.getLogger(__name__)


@celery_app.task(
  bind=True,
  name="provision_repository_access",
  autoretry_for=(ConnectionError, TimeoutError, OperationalError),
  retry_kwargs={"max_retries": 3, "countdown": 60},
  retry_backoff=True,
  retry_backoff_max=600,
)
def provision_repository_access_task(
  self, user_id: str, subscription_id: str, repository_name: str
) -> dict:
  """
  Provision repository access after payment confirmation.

  This task is called by payment webhooks after a user has added a payment
  method. It grants access to the shared repository and activates the subscription.

  Args:
      user_id: ID of the user who will get access
      subscription_id: ID of the existing subscription in PENDING_PAYMENT status
      repository_name: Name of the repository (e.g., 'sec', 'industry', 'economic')

  Returns:
      Dictionary containing repository_name and access details

  Raises:
      Exception: If any step of the process fails
  """
  logger.info(
    f"Starting repository access provisioning for user {user_id}, "
    f"subscription {subscription_id}, repository {repository_name}"
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

    customer = BillingCustomer.get_by_user_id(user_id, session)

    if not customer:
      raise Exception(f"Customer not found for user {user_id}")

    plan_tier = (
      subscription.plan_name.split("-")[-1]
      if "-" in subscription.plan_name
      else subscription.plan_name
    )

    try:
      repository_type = RepositoryType(repository_name)
      repository_plan = RepositoryPlan(plan_tier)
    except ValueError as e:
      logger.error(f"Invalid repository type or plan: {e}")
      raise Exception(
        f"Invalid repository type '{repository_name}' or plan '{plan_tier}'"
      )

    repo_service = RepositorySubscriptionService(session)

    credits_allocated = repo_service.allocate_credits(
      repository_type=repository_type,
      repository_plan=repository_plan,
      user_id=user_id,
    )

    access_granted = repo_service.grant_access(
      repository_type=repository_type,
      user_id=user_id,
    )

    subscription.resource_id = repository_name
    subscription.activate(session)

    BillingAuditLog.log_event(
      session=session,
      event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
      org_id=customer.org_id,
      subscription_id=subscription.id,
      description=f"Activated subscription for {repository_name} repository",
      actor_type="system",
      event_data={
        "current_period_start": subscription.current_period_start.isoformat(),
        "current_period_end": subscription.current_period_end.isoformat(),
        "credits_allocated": credits_allocated,
      },
    )

    generate_subscription_invoice(
      subscription=subscription,
      customer=customer,
      description=f"{repository_name.upper()} Repository Subscription - {subscription.plan_name}",
      session=session,
    )

    session.commit()

    logger.info(
      "Repository access provisioning completed successfully",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "repository_name": repository_name,
        "credits_allocated": credits_allocated,
        "access_granted": access_granted,
      },
    )

    return {
      "repository_name": repository_name,
      "access_granted": access_granted,
      "credits_allocated": credits_allocated,
      "subscription_id": subscription_id,
    }

  except Exception as e:
    logger.error(
      f"Repository access provisioning failed: {type(e).__name__}: {str(e)}",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "repository_name": repository_name,
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
      cleanup_partial_resources
      and "access_granted" in locals()
      and "repo_service" in locals()
    ):
      try:
        logger.warning(
          f"Attempting cleanup of repository access for {repository_name}",
          extra={"user_id": user_id, "repository_name": repository_name},
        )

        repo_service.revoke_access(  # type: ignore[possibly-unbound]
          repository_type=RepositoryType(repository_name),
          user_id=user_id,
        )

        logger.info(
          f"Successfully cleaned up repository access for {repository_name}",
          extra={"user_id": user_id, "repository_name": repository_name},
        )
      except Exception as cleanup_error:
        logger.error(
          f"Failed to cleanup repository access: {cleanup_error}",
          extra={
            "user_id": user_id,
            "repository_name": repository_name,
            "cleanup_error": str(cleanup_error),
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
