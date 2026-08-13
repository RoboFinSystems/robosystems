"""Repository subscription billing lifecycle.

Shared repositories are billed to the organization but granted per user, so a
cancellation has to reach three places: the payment provider, the billing row,
and the member's access record. Routers and off-boarding both go through here
so the three can never drift apart.

Distinct from `operations/graph/repository_subscription_service.py`, which owns
the access-grant layer alone (`UserRepository` rows and their credit pools) and
knows nothing about the provider. This module is the outer lifecycle and calls
into that layer; anything that stops billing belongs here.
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
  """The payment provider could not cancel — nothing local was changed.

  Both provider cancel methods treat an already-canceled or missing
  subscription as success, so a failure here means the customer is still being
  billed. Revoking access anyway would charge them for something they can no
  longer use.
  """


def cancel_repository_subscription(
  subscription: BillingSubscription,
  session: Session,
  actor_user_id: str,
  immediate: bool = False,
  reason: str = "user_request",
) -> None:
  """Cancel one repository subscription and revoke the matching access.

  Provider first: a provider failure raises before anything local changes, so
  the retry path stays open. `immediate=False` leaves access in place until the
  period closes; `immediate=True` revokes it now.
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

  # A subscription that never reached an active period has no period end to
  # cancel into — the only honest option is an immediate stop.
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
  """Cancel every repository subscription a user still holds, immediately.

  Used when a member is removed from the org: the org paid for that access
  because of the membership, so it ends with the membership rather than
  running to the period end on someone else's card.

  Raises ProviderCancellationError if the provider refuses — the caller must
  abort rather than off-board a member the org keeps paying for.
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

  return canceled
