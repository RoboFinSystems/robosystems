"""Repository subscription billing lifecycle.

A cancellation must reach the payment provider, the billing row, and the
member's access grant together. `graph/repository_subscription_service.py`
owns the grant layer alone; anything that stops billing belongs here.
"""

from sqlalchemy.orm import Session

from robosystems.logger import get_logger
from robosystems.models.core import UserRepository
from robosystems.models.core.billing import BillingAuditLog, BillingSubscription
from robosystems.models.core.billing.audit_log import BillingEventType

logger = get_logger(__name__)


class RepositorySubscriptionError(Exception):
  """Base class for repository subscription operation failures."""


class ProviderCancellationError(RepositorySubscriptionError):
  """The payment provider could not cancel; nothing local was changed.

  Already-canceled counts as success, so this means the customer is still
  being billed, and access must not be revoked.
  """


def cancel_repository_subscription(
  subscription: BillingSubscription,
  session: Session,
  actor_user_id: str,
  immediate: bool = False,
  reason: str = "user_request",
) -> None:
  """Cancel one repository subscription and revoke the matching access.

  Provider first, so a provider failure leaves nothing changed locally.
  `immediate=False` keeps access until the period closes.
  """
  from robosystems.operations.providers.payment_provider import get_payment_provider

  repository_id = subscription.resource_id
  subscriber_id = subscription.user_id

  if subscription.stripe_subscription_id:
    try:
      provider = get_payment_provider("stripe")
      if immediate:
        provider.cancel_subscription(subscription.stripe_subscription_id)
      else:
        provider.cancel_subscription_at_period_end(subscription.stripe_subscription_id)
    except Exception as e:
      logger.error(
        f"Failed to cancel Stripe subscription for {repository_id}: {e}",
        extra={"subscription_id": subscription.id},
        exc_info=True,
      )
      raise ProviderCancellationError(str(e)) from e

  # No period end to cancel into: stop immediately.
  subscription.cancel(
    session, immediate=immediate or subscription.current_period_end is None
  )

  if subscriber_id and repository_id:
    user_repo = UserRepository.get_by_user_and_repository(
      user_id=subscriber_id,
      repository_name=repository_id,
      session=session,
    )
    if user_repo:
      if subscription.ends_at and not immediate:
        user_repo.expires_at = subscription.ends_at
        user_repo.next_billing_at = None
        user_repo.updated_at = subscription.updated_at
        session.commit()
      else:
        user_repo.revoke_access(session, reason="Subscription canceled")

  BillingAuditLog.log_event(
    session=session,
    event_type=BillingEventType.SUBSCRIPTION_CANCELED,
    description=(
      f"Repository subscription {subscription.id} canceled for {repository_id} "
      f"({'immediate' if immediate else 'period_end'})"
    ),
    actor_type="user",
    actor_user_id=actor_user_id,
    org_id=subscription.org_id,
    subscription_id=subscription.id,
    event_data={
      "immediate": immediate,
      "reason": reason,
      "resource_type": "repository",
      "resource_id": repository_id,
      "subscriber_user_id": subscriber_id,
      "canceled_by_other": subscriber_id != actor_user_id,
    },
  )

  logger.info(
    f"Canceled repository subscription {subscription.id} for {repository_id}",
    extra={
      "subscription_id": subscription.id,
      "subscriber_user_id": subscriber_id,
      "actor_user_id": actor_user_id,
      "immediate": immediate,
      "reason": reason,
    },
  )


def cancel_user_repository_subscriptions(
  user_id: str,
  session: Session,
  actor_user_id: str,
  reason: str = "org_offboarding",
) -> list[str]:
  """Cancel every repository subscription a user still holds, immediately,
  and revoke every repository grant they still hold — whether or not a live
  subscription stands behind it.

  For org off-boarding: access the org paid for ends with the membership.
  Grants are swept separately because a period-end cancellation leaves the
  grant alive until ``expires_at``, and authorization reads the grant.

  Raises ProviderCancellationError if the provider refuses; the caller must
  abort the off-boarding.
  """
  subscriptions = BillingSubscription.get_live_subscriptions_for_user(
    user_id, session, resource_type="repository"
  )

  canceled: list[str] = []
  for subscription in subscriptions:
    cancel_repository_subscription(
      subscription,
      session=session,
      actor_user_id=actor_user_id,
      immediate=True,
      reason=reason,
    )
    canceled.append(subscription.id)

  for grant in UserRepository.get_user_repositories(user_id, session):
    grant.revoke_access(session, reason=reason)
    logger.info(
      f"Revoked repository grant {grant.repository_name} for user {user_id} "
      f"with no live subscription behind it",
      extra={
        "user_id": user_id,
        "repository": grant.repository_name,
        "actor_user_id": actor_user_id,
        "reason": reason,
      },
    )

  return canceled
