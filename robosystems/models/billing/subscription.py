"""Billing subscription model - polymorphic subscriptions for any resource type."""

import secrets
from datetime import datetime, timezone, timedelta
from typing import Optional
from enum import Enum
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session

from ...database import Base
from ...logger import get_logger

logger = get_logger(__name__)


class SubscriptionStatus(str, Enum):
  """Subscription status states."""

  PENDING = "pending"
  PENDING_PAYMENT = "pending_payment"
  PROVISIONING = "provisioning"
  ACTIVE = "active"
  PAUSED = "paused"
  CANCELED = "canceled"
  PAST_DUE = "past_due"
  UNPAID = "unpaid"


class BillingInterval(str, Enum):
  """Billing interval options."""

  MONTHLY = "monthly"
  ANNUAL = "annual"
  USAGE_BASED = "usage_based"


class BillingSubscription(Base):
  """Generic subscription model for any billable resource.

  Designed to be polymorphic - can bill for graphs, repositories,
  API add-ons, storage, or any future billable resource.

  Separated from resource models to isolate billing concerns.
  """

  __tablename__ = "billing_subscriptions"

  id = Column(
    String, primary_key=True, default=lambda: f"bsub_{secrets.token_urlsafe(16)}"
  )

  billing_customer_user_id = Column(String, ForeignKey("users.id"), nullable=False)

  resource_type = Column(String, nullable=False)
  resource_id = Column(String, nullable=False)

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

  started_at = Column(DateTime, nullable=True)
  current_period_start = Column(DateTime, nullable=True)
  current_period_end = Column(DateTime, nullable=True)
  canceled_at = Column(DateTime, nullable=True)
  ends_at = Column(DateTime, nullable=True)

  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  __table_args__ = (
    Index("idx_billing_sub_customer", "billing_customer_user_id"),
    Index("idx_billing_sub_resource", "resource_type", "resource_id"),
    Index("idx_billing_sub_status", "status"),
    Index("idx_billing_sub_stripe", "stripe_subscription_id"),
    Index("idx_billing_sub_provider", "provider_subscription_id"),
  )

  def __repr__(self) -> str:
    return f"<BillingSubscription {self.resource_type}:{self.resource_id} plan={self.plan_name}>"

  @classmethod
  def create_subscription(
    cls,
    user_id: str,
    resource_type: str,
    resource_id: str,
    plan_name: str,
    base_price_cents: int,
    session: Session,
    billing_interval: str = "monthly",
    stripe_subscription_id: Optional[str] = None,
  ) -> "BillingSubscription":
    """Create a new subscription."""
    now = datetime.now(timezone.utc)

    subscription = cls(
      billing_customer_user_id=user_id,
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
  def get_by_resource_and_user(
    cls,
    resource_type: str,
    resource_id: str,
    user_id: str,
    session: Session,
  ) -> Optional["BillingSubscription"]:
    """Get subscription for a specific resource and user.

    This is particularly useful for shared repositories where multiple users
    can have separate subscriptions to the same resource.
    """
    return (
      session.query(cls)
      .filter(
        cls.resource_type == resource_type,
        cls.resource_id == resource_id,
        cls.billing_customer_user_id == user_id,
      )
      .first()
    )

  @classmethod
  def get_active_subscriptions_for_user(
    cls, user_id: str, session: Session
  ) -> list["BillingSubscription"]:
    """Get all active subscriptions for a user."""
    return (
      session.query(cls)
      .filter(
        cls.billing_customer_user_id == user_id,
        cls.status == SubscriptionStatus.ACTIVE.value,
      )
      .all()
    )

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
    """Get subscription by Stripe subscription ID (legacy support)."""
    return (
      session.query(cls)
      .filter(cls.stripe_subscription_id == stripe_subscription_id)
      .first()
    )

  def activate(self, session: Session) -> None:
    """Activate the subscription."""
    now = datetime.now(timezone.utc)
    self.status = SubscriptionStatus.ACTIVE.value
    self.started_at = now
    self.current_period_start = now
    self.current_period_end = now + timedelta(days=30)
    self.updated_at = now

    session.commit()
    session.refresh(self)

    logger.info(f"Activated subscription {self.id}")

  def pause(self, session: Session) -> None:
    """Pause the subscription."""
    self.status = SubscriptionStatus.PAUSED.value
    self.updated_at = datetime.now(timezone.utc)

    session.commit()
    session.refresh(self)

    logger.info(f"Paused subscription {self.id}")

  def cancel(self, session: Session, immediate: bool = False) -> None:
    """Cancel the subscription."""
    now = datetime.now(timezone.utc)
    self.status = SubscriptionStatus.CANCELED.value
    self.canceled_at = now

    if immediate:
      self.ends_at = now
    else:
      self.ends_at = self.current_period_end

    self.updated_at = now

    session.commit()
    session.refresh(self)

    logger.info(f"Canceled subscription {self.id} (ends: {self.ends_at})")

  def update_plan(
    self, new_plan_name: str, new_price_cents: int, session: Session
  ) -> None:
    """Update subscription plan (upgrade or downgrade)."""
    old_plan = self.plan_name
    self.plan_name = new_plan_name
    self.base_price_cents = new_price_cents
    self.updated_at = datetime.now(timezone.utc)

    session.commit()
    session.refresh(self)

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
    self.updated_at = datetime.now(timezone.utc)

    session.commit()
    session.refresh(self)

    logger.info(f"Updated Stripe subscription for {self.id}")

  def is_active(self) -> bool:
    """Check if subscription is currently active."""
    return self.status == SubscriptionStatus.ACTIVE.value
