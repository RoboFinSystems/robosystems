"""Billing subscription model - polymorphic subscriptions for any resource type."""

from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Optional

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, String, case
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from robosystems.database import Base
from robosystems.logger import get_logger
from robosystems.utils.ulid import generate_prefixed_ulid

logger = get_logger(__name__)


class SubscriptionStatus(str, Enum):
  """Subscription status states."""

  PENDING = "pending"
  PENDING_PAYMENT = "pending_payment"
  PROVISIONING = "provisioning"
  ACTIVE = "active"
  UPGRADING = "upgrading"
  PAUSED = "paused"
  CANCELED = "canceled"
  FAILED = "failed"
  PAST_DUE = "past_due"
  UNPAID = "unpaid"


# Statuses that no longer represent a subscription in force. Rows in these
# states must not block a new subscription to the same resource — a canceled
# or payment-failed subscription is history, not a conflict.
TERMINAL_SUBSCRIPTION_STATUSES = (
  SubscriptionStatus.CANCELED.value,
  SubscriptionStatus.FAILED.value,
)

# How long a subscription may sit in ``provisioning`` before another trigger
# may re-claim it, and before the reaper writes it off as stalled. Provisioning
# is a seconds-to-minutes operation; anything still holding the claim past this
# is not running any more, it just never said so.
STALE_PROVISIONING_MINUTES = 30


class BillingInterval(str, Enum):
  """Billing interval options."""

  MONTHLY = "monthly"
  ANNUAL = "annual"
  USAGE_BASED = "usage_based"


class CancellationType(str, Enum):
  """How a subscription was canceled."""

  PERIOD_END = "period_end"
  IMMEDIATE = "immediate"


class BillingSubscription(Base):
  """A subscription to any billable resource.

  Polymorphic on ``(resource_type, resource_id)`` — graphs, repositories,
  add-ons — so billing concerns live here rather than on each resource model.
  """

  __tablename__ = "billing_subscriptions"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("bsub"))

  org_id = Column(String, ForeignKey("orgs.id"), nullable=False)

  # The member the subscription belongs to. Billing is always the org's, but
  # repository access and credits are per-user, so the subscriber has to be a
  # column rather than a value recoverable only from provider metadata.
  # Nullable because org-level rows (graphs) have no single subscriber.
  user_id = Column(String, ForeignKey("users.id"), nullable=True)

  resource_type = Column(String, nullable=False)
  resource_id = Column(String, nullable=True)

  plan_name = Column(String, nullable=False)
  billing_interval = Column(String, default="monthly", nullable=False)

  base_price_cents = Column(Integer, nullable=False)

  stripe_subscription_id = Column(String, unique=True, nullable=True)
  stripe_product_id = Column(String, nullable=True)
  stripe_price_id = Column(String, nullable=True)

  payment_provider = Column(String, default="invoice", nullable=False)
  provider_subscription_id = Column(String, unique=True, nullable=True)
  provider_customer_id = Column(String, nullable=True)

  subscription_metadata = Column(
    JSONB, default=dict, nullable=False, server_default="{}"
  )

  status = Column(String, default="pending", nullable=False)
  cancellation_type = Column(String, nullable=True)

  started_at = Column(DateTime, nullable=True)
  current_period_start = Column(DateTime, nullable=True)
  current_period_end = Column(DateTime, nullable=True)
  canceled_at = Column(DateTime, nullable=True)
  ends_at = Column(DateTime, nullable=True)

  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  __table_args__ = (
    Index("idx_billing_sub_org", "org_id"),
    Index("idx_billing_sub_user", "user_id"),
    Index("idx_billing_sub_resource", "resource_type", "resource_id"),
    Index("idx_billing_sub_status", "status"),
    Index("idx_billing_sub_stripe", "stripe_subscription_id"),
    Index("idx_billing_sub_provider", "provider_subscription_id"),
    Index("idx_billing_sub_cancellation_type", "cancellation_type"),
  )

  def __repr__(self) -> str:
    return f"<BillingSubscription {self.resource_type}:{self.resource_id} plan={self.plan_name}>"

  @classmethod
  def create_subscription(
    cls,
    org_id: str,
    resource_type: str,
    resource_id: str | None,
    plan_name: str,
    base_price_cents: int,
    session: Session,
    billing_interval: str = "monthly",
    stripe_subscription_id: str | None = None,
    user_id: str | None = None,
  ) -> "BillingSubscription":
    """Create a new subscription.

    Pass `user_id` for per-user resources (repository subscriptions) so access
    and credits can be provisioned to the right member of the paying org.
    `resource_id` may be None for pre-provisioning rows (checkout creates the
    subscription first and binds the resource after payment).
    """
    now = datetime.now(UTC)

    subscription = cls(
      org_id=org_id,
      user_id=user_id,
      resource_type=resource_type,
      resource_id=resource_id,
      plan_name=plan_name,
      base_price_cents=base_price_cents,
      billing_interval=billing_interval,
      stripe_subscription_id=stripe_subscription_id,
      status=SubscriptionStatus.PENDING.value,
      created_at=now,
    )

    session.add(subscription)
    session.commit()
    session.refresh(subscription)

    logger.info(
      f"Created billing subscription {subscription.id} for {resource_type}:{resource_id}"
    )

    return subscription

  @classmethod
  def get_by_resource(
    cls, resource_type: str, resource_id: str, session: Session
  ) -> Optional["BillingSubscription"]:
    """Get subscription for a specific resource."""
    return (
      session.query(cls)
      .filter(cls.resource_type == resource_type, cls.resource_id == resource_id)
      .first()
    )

  @classmethod
  def get_by_resource_and_org(
    cls,
    resource_type: str,
    resource_id: str,
    org_id: str,
    session: Session,
    exclude_statuses: tuple[str, ...] = (),
  ) -> Optional["BillingSubscription"]:
    """Get subscription for a specific resource and organization.

    Scoping by org matters for shared repositories, where several orgs hold
    separate subscriptions to the same resource.

    Pass `exclude_statuses=TERMINAL_SUBSCRIPTION_STATUSES` when checking for
    a *conflicting* subscription: canceled/failed rows are never deleted, and
    an unfiltered lookup would treat that history as a live duplicate.

    When several rows exist for the same resource (a canceled subscription
    followed by a resubscribe), the one still in force wins, then recency —
    so "the subscription" always resolves to the live row when there is one,
    and to the most recent terminal row when there is not.
    """
    query = session.query(cls).filter(
      cls.resource_type == resource_type,
      cls.resource_id == resource_id,
      cls.org_id == org_id,
    )
    if exclude_statuses:
      query = query.filter(cls.status.notin_(exclude_statuses))
    return query.order_by(
      case((cls.status.in_(TERMINAL_SUBSCRIPTION_STATUSES), 1), else_=0),
      cls.created_at.desc(),
    ).first()

  @classmethod
  def get_by_resource_and_user(
    cls,
    resource_type: str,
    resource_id: str,
    user_id: str,
    session: Session,
    exclude_statuses: tuple[str, ...] = (),
  ) -> Optional["BillingSubscription"]:
    """Get a specific user's subscription to a resource.

    Keyed on the subscriber, not their org: repository access and credits are
    granted per user, so one member's subscription must never resolve as
    another member's. Ordering matches `get_by_resource_and_org` — the row
    still in force wins, then recency.
    """
    query = session.query(cls).filter(
      cls.resource_type == resource_type,
      cls.resource_id == resource_id,
      cls.user_id == user_id,
    )
    if exclude_statuses:
      query = query.filter(cls.status.notin_(exclude_statuses))
    return query.order_by(
      case((cls.status.in_(TERMINAL_SUBSCRIPTION_STATUSES), 1), else_=0),
      cls.created_at.desc(),
    ).first()

  @classmethod
  def get_active_subscriptions_for_org(
    cls, org_id: str, session: Session
  ) -> list["BillingSubscription"]:
    """Get all active subscriptions for an organization."""
    return (
      session.query(cls)
      .filter(
        cls.org_id == org_id,
        cls.status == SubscriptionStatus.ACTIVE.value,
      )
      .all()
    )

  @classmethod
  def get_live_subscriptions_for_user(
    cls, user_id: str, session: Session, resource_type: str | None = None
  ) -> list["BillingSubscription"]:
    """Get a user's own subscriptions that are still in force.

    "In force" is everything outside the terminal statuses — a pending or
    past-due row still bills and still has to be cleaned up when the member
    is off-boarded or their account is deleted.
    """
    query = session.query(cls).filter(
      cls.user_id == user_id,
      cls.status.notin_(TERMINAL_SUBSCRIPTION_STATUSES),
    )
    if resource_type:
      query = query.filter(cls.resource_type == resource_type)
    return query.order_by(cls.created_at.asc()).all()

  @classmethod
  def get_by_provider_subscription_id(
    cls, provider_subscription_id: str, session: Session
  ) -> Optional["BillingSubscription"]:
    """Get subscription by payment provider subscription ID (e.g., Stripe subscription ID)."""
    return (
      session.query(cls)
      .filter(cls.provider_subscription_id == provider_subscription_id)
      .first()
    )

  @classmethod
  def get_by_stripe_subscription_id(
    cls, stripe_subscription_id: str, session: Session
  ) -> Optional["BillingSubscription"]:
    """Get subscription by the Stripe-specific column.

    ``get_by_provider_subscription_id`` is the provider-agnostic equivalent.
    """
    return (
      session.query(cls)
      .filter(cls.stripe_subscription_id == stripe_subscription_id)
      .first()
    )

  def _invalidate_access_cache(self) -> None:
    """Invalidate the subscription access cache for this resource.

    Called on any status change so require_graph_access() re-checks the DB
    instead of serving a stale cached result for up to 300s.
    """
    if self.resource_type == "graph" and self.resource_id:
      try:
        from robosystems.middleware.billing.enforcement import (
          invalidate_subscription_cache,
        )

        invalidate_subscription_cache(self.resource_id)
      except Exception as e:
        logger.warning(f"Cache invalidation failed for graph {self.resource_id}: {e}")

  def claim_for_provisioning(
    self, session: Session, stale_after_minutes: int = STALE_PROVISIONING_MINUTES
  ) -> bool:
    """Atomically claim this subscription for a provisioning run.

    Returns True to exactly one caller. Provisioning has more than one
    trigger — separate provider events, each legitimate on its own — so the
    guard has to live at the sink rather than at each caller, or adding a
    trigger later silently re-opens the hole.

    The whole decision is one conditional UPDATE, which makes it a mutex
    without an advisory lock or a new table:

    * ``resource_id IS NULL`` is the terminal condition. Once a resource
      exists the subscription is never re-provisioned, whatever its status
      and however many callers arrive.
    * ``pending``/``pending_payment`` is the ordinary first entry.
    * A ``provisioning`` row is re-claimable only once it has gone stale,
      which is what makes a genuinely dead attempt retryable (a killed
      worker, a provider redelivery after a timeout) while two concurrent
      callers still resolve to one winner: the loser re-evaluates the
      predicate after the winner's row lock clears, sees a freshly stamped
      row, and matches nothing.

    ``updated_at`` is stamped by the claim itself, so it doubles as the
    heartbeat the staleness window is measured against.
    """
    from sqlalchemy import and_, or_

    now = datetime.now(UTC)
    stale_cutoff = now - timedelta(minutes=stale_after_minutes)

    claimed = (
      session.query(BillingSubscription)
      .filter(
        BillingSubscription.id == self.id,
        BillingSubscription.resource_id.is_(None),
        or_(
          BillingSubscription.status.in_(
            (
              SubscriptionStatus.PENDING.value,
              SubscriptionStatus.PENDING_PAYMENT.value,
            )
          ),
          and_(
            BillingSubscription.status == SubscriptionStatus.PROVISIONING.value,
            BillingSubscription.updated_at < stale_cutoff,
          ),
        ),
      )
      .update(
        {
          BillingSubscription.status: SubscriptionStatus.PROVISIONING.value,
          BillingSubscription.updated_at: now,
        },
        synchronize_session=False,
      )
    )

    session.commit()
    session.refresh(self)

    if claimed:
      logger.info(f"Claimed subscription {self.id} for provisioning")
    else:
      logger.info(
        f"Provisioning claim refused for subscription {self.id} "
        f"(status={self.status}, resource_id={self.resource_id}) — "
        "another trigger already holds it or the resource exists"
      )

    return bool(claimed)

  def activate(self, session: Session) -> None:
    """Activate the subscription."""
    now = datetime.now(UTC)
    self.status = SubscriptionStatus.ACTIVE.value
    self.started_at = now
    self.current_period_start = now
    self.current_period_end = now + timedelta(days=30)
    self.updated_at = now

    session.commit()
    session.refresh(self)
    self._invalidate_access_cache()

    logger.info(f"Activated subscription {self.id}")

  def pause(self, session: Session) -> None:
    """Pause the subscription."""
    self.status = SubscriptionStatus.PAUSED.value
    self.updated_at = datetime.now(UTC)

    session.commit()
    session.refresh(self)
    self._invalidate_access_cache()

    logger.info(f"Paused subscription {self.id}")

  def cancel(self, session: Session, immediate: bool = False) -> None:
    """Cancel the subscription.

    Sets `cancellation_type` to "immediate" or "period_end" so downstream
    sensors (e.g. deprovisioning) can branch on the user's intent without
    re-deriving it from timestamps.

    Raises ValueError if `immediate=False` is requested on a subscription
    whose `current_period_end` is None — without an `ends_at` value the
    deprovision sensor's `ends_at.isnot(None)` filter would silently skip
    the sub forever, leaking infrastructure.
    """
    now = datetime.now(UTC)

    if not immediate and self.current_period_end is None:
      raise ValueError(
        f"Cannot cancel subscription {self.id} at period end: "
        "current_period_end is None (subscription was never activated). "
        "Use immediate=True instead."
      )

    self.status = SubscriptionStatus.CANCELED.value
    self.canceled_at = now

    if immediate:
      self.ends_at = now
      self.cancellation_type = CancellationType.IMMEDIATE.value
    else:
      self.ends_at = self.current_period_end
      self.cancellation_type = CancellationType.PERIOD_END.value

    self.updated_at = now

    session.commit()
    session.refresh(self)
    self._invalidate_access_cache()

    logger.info(
      f"Canceled subscription {self.id} "
      f"(type={self.cancellation_type}, ends={self.ends_at})"
    )

  def update_plan(
    self, new_plan_name: str, new_price_cents: int, session: Session
  ) -> None:
    """Update subscription plan (upgrade or downgrade)."""
    old_plan = self.plan_name
    self.plan_name = new_plan_name
    self.base_price_cents = new_price_cents
    self.updated_at = datetime.now(UTC)

    session.commit()
    session.refresh(self)
    self._invalidate_access_cache()

    logger.info(f"Updated subscription {self.id} plan: {old_plan} -> {new_plan_name}")

  def update_stripe_subscription(
    self,
    stripe_subscription_id: str,
    stripe_product_id: str,
    stripe_price_id: str,
    session: Session,
  ) -> None:
    """Update Stripe subscription IDs."""
    self.stripe_subscription_id = stripe_subscription_id
    self.stripe_product_id = stripe_product_id
    self.stripe_price_id = stripe_price_id
    self.updated_at = datetime.now(UTC)

    session.commit()
    session.refresh(self)

    logger.info(f"Updated Stripe subscription for {self.id}")

  def _get_period_delta(self) -> timedelta:
    """Get the timedelta for one billing period based on billing_interval."""
    if self.billing_interval == BillingInterval.ANNUAL.value:
      return timedelta(days=365)
    # Both "monthly" and "usage_based" bill on a monthly cadence.
    return timedelta(days=30)

  def renew_period(self, session: Session) -> None:
    """Advance to the next billing period.

    Shifts current_period_start to the old current_period_end and extends
    current_period_end by the billing interval duration. Used by the invoice
    billing renewal job.
    """
    now = datetime.now(UTC)
    delta = self._get_period_delta()
    self.current_period_start = self.current_period_end
    self.current_period_end = self.current_period_end + delta
    self.updated_at = now

    session.flush()

    logger.info(f"Renewed subscription {self.id} period to {self.current_period_end}")

  def is_active(self) -> bool:
    """Check if subscription is currently active."""
    return self.status == SubscriptionStatus.ACTIVE.value
