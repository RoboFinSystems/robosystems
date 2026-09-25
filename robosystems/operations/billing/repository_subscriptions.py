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

  reconcile_repository_grant(subscription, session)

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
# only a suspension of its own, never an admin or off-boarding revoke. Kept on
# the grant as well as its credit pool, since a plan may have no pool.
UNPAID_SUSPENSION = "subscription_unpaid"


def _grant_suspension(grant: UserRepository) -> str | None:
  import json

  try:
    meta = json.loads(grant.extra_metadata) if grant.extra_metadata else {}
  except (TypeError, ValueError):
    return None
  return meta.get("suspended_by") if isinstance(meta, dict) else None


def _set_grant_suspension(grant: UserRepository, reason: str | None) -> None:
  import json

  try:
    meta = json.loads(grant.extra_metadata) if grant.extra_metadata else {}
  except (TypeError, ValueError):
    meta = {}
  if not isinstance(meta, dict):
    meta = {}
  if reason is None:
    meta.pop("suspended_by", None)
  else:
    meta["suspended_by"] = reason
  grant.extra_metadata = json.dumps(meta) if meta else None


def _as_utc(value):
  from datetime import UTC

  if value is not None and value.tzinfo is None:
    return value.replace(tzinfo=UTC)
  return value


def _current_subscription(
  session: Session, user_id: str, repository: str
) -> BillingSubscription | None:
  """The member's most recent subscription to this repository: the one the
  grant answers to. An older subscription's late events must not move it."""
  return (
    session.query(BillingSubscription)
    .filter(
      BillingSubscription.resource_type == "repository",
      BillingSubscription.user_id == user_id,
      BillingSubscription.resource_id == repository,
    )
    .order_by(BillingSubscription.created_at.desc(), BillingSubscription.id.desc())
    .first()
  )


def reconcile_repository_grant(
  subscription: BillingSubscription, session: Session
) -> None:
  """Make the member's repository grant and credit pool agree with the
  subscription's status, whichever side of Stripe changed it.

  Acts only for the member's current subscription to the repository. Active:
  access with no expiry, lifting a suspension this function made or any revoke
  from before this subscription started (a re-subscribe). Canceled with a
  future ``ends_at``: access until then. Canceled otherwise: revoked.
  ``unpaid`` (retries exhausted): suspended until a payment revives it.
  ``past_due`` leaves the grant as it is: the grace policy is still open.
  """
  from datetime import UTC, datetime

  if subscription.resource_type != "repository":
    return
  user_id, repository = subscription.user_id, subscription.resource_id
  if not user_id or not repository:
    return
  current = _current_subscription(session, user_id, repository)
  if current is None or current.id != subscription.id:
    return
  grant = UserRepository.get_by_user_and_repository(
    user_id=user_id, repository_name=repository, session=session
  )
  if grant is None:
    return

  now = datetime.now(UTC)
  status = subscription.status
  credits = grant.user_credits
  ends_at = _as_utc(subscription.ends_at)
  started_at = _as_utc(subscription.started_at)

  def _before_this_subscription(moment) -> bool:
    moment = _as_utc(moment)
    return bool(started_at and moment and moment <= started_at)

  if status == "active":
    ours = _grant_suspension(grant) == UNPAID_SUSPENSION or (
      credits is not None and credits.suspension_reason == UNPAID_SUSPENSION
    )
    if not grant.is_active and not (
      ours or _before_this_subscription(grant.expires_at)
    ):
      return
    changed = not grant.is_active or grant.expires_at is not None or ours
    grant.is_active = True
    grant.expires_at = None
    _set_grant_suspension(grant, None)
    if (
      credits is not None
      and not credits.is_active
      and (
        ours
        or credits.suspension_reason == UNPAID_SUSPENSION
        or _before_this_subscription(credits.suspended_at)
      )
    ):
      credits.is_active = True
      credits.suspended_at = None
      credits.suspension_reason = None
      changed = True
    if changed:
      grant.updated_at = now
      session.commit()
      grant.invalidate_access_cache()
  elif status == "canceled" and ends_at is not None and ends_at > now:
    if grant.is_active and _as_utc(grant.expires_at) != ends_at:
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
      _set_grant_suspension(grant, UNPAID_SUSPENSION)
      grant.revoke_access(session, reason=UNPAID_SUSPENSION)
