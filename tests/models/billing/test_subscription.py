"""Comprehensive tests for BillingSubscription model."""

import pytest
import uuid
from sqlalchemy.orm import Session

from robosystems.models.billing import (
  BillingSubscription,
  SubscriptionStatus,
  BillingInterval,
)
from robosystems.models.iam import User


@pytest.fixture
def test_user(db_session: Session):
  """Create a test user."""
  unique_id = str(uuid.uuid4())[:8]
  user = User(
    id=f"test_user_{unique_id}",
    email=f"test+{unique_id}@example.com",
    name="Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.commit()
  return user


class TestBillingSubscriptionCreation:
  """Tests for subscription creation."""

  def test_create_subscription_success(self, db_session: Session, test_user):
    """Test successful subscription creation."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    assert subscription.id.startswith("bsub_")
    assert subscription.billing_customer_user_id == test_user.id
    assert subscription.resource_type == "graph"
    assert subscription.resource_id == "kg123abc"
    assert subscription.plan_name == "standard"
    assert subscription.base_price_cents == 2999
    assert subscription.billing_interval == "monthly"
    assert subscription.status == SubscriptionStatus.PENDING.value
    assert subscription.created_at is not None

  def test_create_subscription_with_custom_interval(
    self, db_session: Session, test_user
  ):
    """Test subscription creation with custom billing interval."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="enterprise",
      base_price_cents=9999,
      billing_interval=BillingInterval.ANNUAL.value,
      session=db_session,
    )

    assert subscription.billing_interval == "annual"

  def test_create_subscription_with_stripe_id(self, db_session: Session, test_user):
    """Test subscription creation with Stripe subscription ID."""
    stripe_sub_id = "sub_1234567890"

    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="large",
      base_price_cents=9999,
      stripe_subscription_id=stripe_sub_id,
      session=db_session,
    )

    assert subscription.stripe_subscription_id == stripe_sub_id

  def test_create_repository_subscription(self, db_session: Session, test_user):
    """Test creating a subscription for a shared repository."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="standard",
      base_price_cents=1999,
      session=db_session,
    )

    assert subscription.resource_type == "repository"
    assert subscription.resource_id == "sec"


class TestBillingSubscriptionQueries:
  """Tests for subscription query methods."""

  def test_get_by_resource(self, db_session: Session, test_user):
    """Test getting subscription by resource."""
    resource_id = f"kg_{str(uuid.uuid4())[:8]}"
    created_sub = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id=resource_id,
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    found_sub = BillingSubscription.get_by_resource(
      resource_type="graph", resource_id=resource_id, session=db_session
    )

    assert found_sub is not None
    assert found_sub.id == created_sub.id

  def test_get_by_resource_not_found(self, db_session: Session):
    """Test getting non-existent subscription."""
    found_sub = BillingSubscription.get_by_resource(
      resource_type="graph", resource_id="nonexistent", session=db_session
    )

    assert found_sub is None

  def test_get_by_resource_and_user(self, db_session: Session, test_user):
    """Test getting subscription by resource and user."""
    created_sub = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="standard",
      base_price_cents=1999,
      session=db_session,
    )

    found_sub = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id="sec",
      user_id=test_user.id,
      session=db_session,
    )

    assert found_sub is not None
    assert found_sub.id == created_sub.id

  def test_get_by_resource_and_user_wrong_user(self, db_session: Session, test_user):
    """Test getting subscription with wrong user ID."""
    BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="standard",
      base_price_cents=1999,
      session=db_session,
    )

    found_sub = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id="sec",
      user_id="wrong_user_id",
      session=db_session,
    )

    assert found_sub is None

  def test_get_active_subscriptions_for_user(self, db_session: Session, test_user):
    """Test getting all active subscriptions for a user."""
    sub1 = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg1",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    sub1.activate(db_session)

    sub2 = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="standard",
      base_price_cents=1999,
      session=db_session,
    )
    sub2.activate(db_session)

    BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg2",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    active_subs = BillingSubscription.get_active_subscriptions_for_user(
      user_id=test_user.id, session=db_session
    )

    assert len(active_subs) == 2
    assert all(sub.status == SubscriptionStatus.ACTIVE.value for sub in active_subs)


class TestBillingSubscriptionLifecycle:
  """Tests for subscription lifecycle methods."""

  def test_activate_subscription(self, db_session: Session, test_user):
    """Test activating a subscription."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    subscription.activate(db_session)

    assert subscription.status == SubscriptionStatus.ACTIVE.value
    assert subscription.started_at is not None
    assert subscription.current_period_start is not None
    assert subscription.current_period_end is not None
    assert subscription.current_period_end > subscription.current_period_start
    assert subscription.is_active() is True

  def test_pause_subscription(self, db_session: Session, test_user):
    """Test pausing an active subscription."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    subscription.pause(db_session)

    assert subscription.status == SubscriptionStatus.PAUSED.value
    assert subscription.is_active() is False

  def test_cancel_subscription_immediate(self, db_session: Session, test_user):
    """Test immediate subscription cancellation."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    subscription.cancel(db_session, immediate=True)

    assert subscription.status == SubscriptionStatus.CANCELED.value
    assert subscription.canceled_at is not None
    assert subscription.ends_at is not None
    assert subscription.ends_at == subscription.canceled_at
    assert subscription.is_active() is False

  def test_cancel_subscription_end_of_period(self, db_session: Session, test_user):
    """Test cancellation at end of billing period."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    subscription.cancel(db_session, immediate=False)

    assert subscription.status == SubscriptionStatus.CANCELED.value
    assert subscription.canceled_at is not None
    assert subscription.ends_at == subscription.current_period_end


class TestBillingSubscriptionUpdates:
  """Tests for subscription update methods."""

  def test_update_plan(self, db_session: Session, test_user):
    """Test updating subscription plan."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    subscription.update_plan(
      new_plan_name="large", new_price_cents=9999, session=db_session
    )

    assert subscription.plan_name == "large"
    assert subscription.base_price_cents == 9999

  def test_update_plan_upgrade(self, db_session: Session, test_user):
    """Test upgrading subscription plan."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    subscription.update_plan(
      new_plan_name="xlarge", new_price_cents=19999, session=db_session
    )

    assert subscription.plan_name == "xlarge"
    assert subscription.base_price_cents == 19999

  def test_update_plan_downgrade(self, db_session: Session, test_user):
    """Test downgrading subscription plan."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="xlarge",
      base_price_cents=19999,
      session=db_session,
    )

    subscription.update_plan(
      new_plan_name="standard", new_price_cents=2999, session=db_session
    )

    assert subscription.plan_name == "standard"
    assert subscription.base_price_cents == 2999

  def test_update_stripe_subscription(self, db_session: Session, test_user):
    """Test updating Stripe subscription details."""
    unique_id = str(uuid.uuid4())[:8]
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id=f"kg_{unique_id}",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    stripe_sub_id = f"sub_{unique_id}"
    stripe_product_id = f"prod_{unique_id}"
    stripe_price_id = f"price_{unique_id}"

    subscription.update_stripe_subscription(
      stripe_subscription_id=stripe_sub_id,
      stripe_product_id=stripe_product_id,
      stripe_price_id=stripe_price_id,
      session=db_session,
    )

    assert subscription.stripe_subscription_id == stripe_sub_id
    assert subscription.stripe_product_id == stripe_product_id
    assert subscription.stripe_price_id == stripe_price_id


class TestBillingSubscriptionStatusChecks:
  """Tests for subscription status checking methods."""

  def test_is_active_for_active_subscription(self, db_session: Session, test_user):
    """Test is_active returns True for active subscription."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    assert subscription.is_active() is True

  def test_is_active_for_pending_subscription(self, db_session: Session, test_user):
    """Test is_active returns False for pending subscription."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    assert subscription.is_active() is False

  def test_is_active_for_paused_subscription(self, db_session: Session, test_user):
    """Test is_active returns False for paused subscription."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)
    subscription.pause(db_session)

    assert subscription.is_active() is False

  def test_is_active_for_canceled_subscription(self, db_session: Session, test_user):
    """Test is_active returns False for canceled subscription."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)
    subscription.cancel(db_session, immediate=True)

    assert subscription.is_active() is False


class TestBillingSubscriptionRepr:
  """Tests for subscription string representation."""

  def test_repr_format(self, db_session: Session, test_user):
    """Test subscription __repr__ format."""
    subscription = BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    repr_str = repr(subscription)

    assert "BillingSubscription" in repr_str
    assert "graph:kg123abc" in repr_str
    assert "plan=standard" in repr_str


class TestBillingSubscriptionIndexes:
  """Tests to ensure database indexes work correctly."""

  def test_query_by_customer_uses_index(self, db_session: Session, test_user):
    """Test querying by customer (should use idx_billing_sub_customer)."""
    for i in range(5):
      BillingSubscription.create_subscription(
        user_id=test_user.id,
        resource_type="graph",
        resource_id=f"kg{i}",
        plan_name="standard",
        base_price_cents=2999,
        session=db_session,
      )

    subs = (
      db_session.query(BillingSubscription)
      .filter(BillingSubscription.billing_customer_user_id == test_user.id)
      .all()
    )

    assert len(subs) == 5

  def test_query_by_resource_uses_index(self, db_session: Session, test_user):
    """Test querying by resource (should use idx_billing_sub_resource)."""
    BillingSubscription.create_subscription(
      user_id=test_user.id,
      resource_type="graph",
      resource_id="kg123",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    sub = BillingSubscription.get_by_resource(
      resource_type="graph", resource_id="kg123", session=db_session
    )

    assert sub is not None

  def test_query_by_status_uses_index(self, db_session: Session, test_user):
    """Test querying by status (should use idx_billing_sub_status)."""
    for i in range(3):
      sub = BillingSubscription.create_subscription(
        user_id=test_user.id,
        resource_type="graph",
        resource_id=f"kg{i}",
        plan_name="standard",
        base_price_cents=2999,
        session=db_session,
      )
      if i < 2:
        sub.activate(db_session)

    active_subs = (
      db_session.query(BillingSubscription)
      .filter(
        BillingSubscription.status == SubscriptionStatus.ACTIVE.value,
        BillingSubscription.billing_customer_user_id == test_user.id,
      )
      .all()
    )

    assert len(active_subs) == 2
