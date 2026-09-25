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


# The suspension reason `reconcile_repository_grant` writes, so it restores
# only a suspension of its own, never an admin or off-boarding revoke.
UNPAID_SUSPENSION = "subscription_unpaid"


def reconcile_repository_grant(
  subscription: BillingSubscription, session: Session
) -> None:
  """Make the member's repository grant and credit pool agree with the
  subscription's status, whichever side of Stripe changed it.

  Live (``active``, or ``past_due`` while Stripe retries the card): access
  with no expiry, lifting a suspension this function made. Canceled with a
  future ``ends_at``: access until then. Canceled otherwise: revoked.
  ``unpaid`` (retries exhausted): suspended until a payment revives it.
  """
  from datetime import UTC, datetime

  if subscription.resource_type != "repository":
    return
  if not subscription.user_id or not subscription.resource_id:
    return
  grant = UserRepository.get_by_user_and_repository(
    user_id=subscription.user_id,
    repository_name=subscription.resource_id,
    session=session,
  )
  if grant is None:
    return

  now = datetime.now(UTC)
  status = subscription.status
  credits = grant.user_credits
  ends_at = subscription.ends_at
  if ends_at is not None and ends_at.tzinfo is None:
    ends_at = ends_at.replace(tzinfo=UTC)

  if status in ("active", "past_due"):
    suspended_by_us = (
      not grant.is_active
      and credits is not None
      and credits.suspension_reason == UNPAID_SUSPENSION
    )
    if not grant.is_active and not suspended_by_us:
      return
    grant.is_active = True
    grant.expires_at = None
    grant.updated_at = now
    if credits is not None and credits.suspension_reason == UNPAID_SUSPENSION:
      credits.is_active = True
      credits.suspended_at = None
      credits.suspension_reason = None
    session.commit()
    grant.invalidate_access_cache()
  elif status == "canceled" and ends_at is not None and ends_at > now:
    if grant.is_active and grant.expires_at != ends_at:
      grant.expires_at = ends_at
      grant.next_billing_at = None
      grant.updated_at = now
      session.commit()
      grant.invalidate_access_cache()
  elif status == "canceled":
    if grant.is_active:
      grant.revoke_access(session, reason="Subscription ended")
  elif status == "unpaid":
    if grant.is_active:
      grant.revoke_access(session, reason=UNPAID_SUSPENSION)
