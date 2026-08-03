"""Comprehensive tests for BillingSubscription model."""

import uuid
from datetime import timedelta

import pytest
from sqlalchemy.orm import Session

from robosystems.models.core import User
from robosystems.models.core.billing import (
  BillingInterval,
  BillingSubscription,
  SubscriptionStatus,
)


@pytest.fixture
def test_user(db_session: Session):
  """Create a test user with org."""
  from robosystems.models.core import Org, OrgRole, OrgType, OrgUser

  unique_id = str(uuid.uuid4())[:8]

  org = Org(
    id=f"test_org_{unique_id}",
    name=f"Test Org {unique_id}",
    org_type=OrgType.PERSONAL,
  )
  db_session.add(org)
  db_session.flush()

  user = User(
    id=f"test_user_{unique_id}",
    email=f"test+{unique_id}@example.com",
    name="Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.flush()

  org_user = OrgUser(
    org_id=org.id,
    user_id=user.id,
    role=OrgRole.OWNER,
  )
  db_session.add(org_user)
  db_session.commit()
  return user


@pytest.fixture
def test_org(test_user, db_session: Session):
  """Get org for test user."""
  from robosystems.models.core import OrgUser

  org_users = OrgUser.get_user_orgs(test_user.id, db_session)
  return org_users[0].org


class TestBillingSubscriptionCreation:
  """Tests for subscription creation."""

  def test_create_subscription_success(self, db_session: Session, test_user, test_org):
    """Test successful subscription creation."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    assert subscription.id.startswith("bsub_")
    assert subscription.org_id == test_org.id
    assert subscription.resource_type == "graph"
    assert subscription.resource_id == "kg123abc"
    assert subscription.plan_name == "standard"
    assert subscription.base_price_cents == 2999
    assert subscription.billing_interval == "monthly"
    assert subscription.status == SubscriptionStatus.PENDING.value
    assert subscription.created_at is not None

  def test_create_subscription_with_custom_interval(
    self, db_session: Session, test_user, test_org
  ):
    """Test subscription creation with custom billing interval."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="enterprise",
      base_price_cents=9999,
      billing_interval=BillingInterval.ANNUAL.value,
      session=db_session,
    )

    assert subscription.billing_interval == "annual"

  def test_create_subscription_with_stripe_id(
    self, db_session: Session, test_user, test_org
  ):
    """Test subscription creation with Stripe subscription ID."""
    stripe_sub_id = "sub_1234567890"

    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="large",
      base_price_cents=9999,
      stripe_subscription_id=stripe_sub_id,
      session=db_session,
    )

    assert subscription.stripe_subscription_id == stripe_sub_id

  def test_create_repository_subscription(
    self, db_session: Session, test_user, test_org
  ):
    """Test creating a subscription for a shared repository."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_get_by_resource(self, db_session: Session, test_user, test_org):
    """Test getting subscription by resource."""
    resource_id = f"kg_{str(uuid.uuid4())[:8]}"
    created_sub = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_get_by_resource_and_user(self, db_session: Session, test_user, test_org):
    """Test getting subscription by resource and user."""
    created_sub = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="standard",
      base_price_cents=1999,
      session=db_session,
    )

    found_sub = BillingSubscription.get_by_resource_and_org(
      resource_type="repository",
      resource_id="sec",
      org_id=test_org.id,
      session=db_session,
    )

    assert found_sub is not None
    assert found_sub.id == created_sub.id

  def test_get_by_resource_and_user_wrong_user(
    self, db_session: Session, test_user, test_org
  ):
    """Test getting subscription with wrong user ID."""
    BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="standard",
      base_price_cents=1999,
      session=db_session,
    )

    found_sub = BillingSubscription.get_by_resource_and_org(
      resource_type="repository",
      resource_id="sec",
      org_id="wrong_org_id",
      session=db_session,
    )

    assert found_sub is None

  def test_get_active_subscriptions_for_org(
    self, db_session: Session, test_user, test_org
  ):
    """Test getting all active subscriptions for a user."""
    sub1 = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg1",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    sub1.activate(db_session)

    sub2 = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="standard",
      base_price_cents=1999,
      session=db_session,
    )
    sub2.activate(db_session)

    BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg2",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    active_subs = BillingSubscription.get_active_subscriptions_for_org(
      org_id=test_org.id, session=db_session
    )

    assert len(active_subs) == 2
    assert all(sub.status == SubscriptionStatus.ACTIVE.value for sub in active_subs)


class TestBillingSubscriptionLifecycle:
  """Tests for subscription lifecycle methods."""

  def test_activate_subscription(self, db_session: Session, test_user, test_org):
    """Test activating a subscription."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_pause_subscription(self, db_session: Session, test_user, test_org):
    """Test pausing an active subscription."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_cancel_subscription_immediate(
    self, db_session: Session, test_user, test_org
  ):
    """Test immediate subscription cancellation."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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
    assert subscription.cancellation_type == "immediate"
    assert subscription.is_active() is False

  def test_cancel_subscription_end_of_period(
    self, db_session: Session, test_user, test_org
  ):
    """Test cancellation at end of billing period."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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
    assert subscription.cancellation_type == "period_end"

  def test_cancel_period_end_rejected_when_never_activated(
    self, db_session: Session, test_user, test_org
  ):
    """`cancel(immediate=False)` on a sub that was never activated must
    raise — without `current_period_end` we'd silently set `ends_at=None`,
    and the deprovision sensor would skip it forever (infrastructure leak).
    """
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    # Note: no .activate() — current_period_end is None.
    assert subscription.current_period_end is None

    with pytest.raises(ValueError, match="current_period_end is None"):
      subscription.cancel(db_session, immediate=False)

    # Status untouched after rejection.
    assert subscription.status != SubscriptionStatus.CANCELED.value
    assert subscription.cancellation_type is None

  def test_cancel_immediate_works_when_never_activated(
    self, db_session: Session, test_user, test_org
  ):
    """The immediate path doesn't read `current_period_end` so it must
    still work on a sub that was never activated."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    assert subscription.current_period_end is None

    subscription.cancel(db_session, immediate=True)

    assert subscription.status == SubscriptionStatus.CANCELED.value
    assert subscription.ends_at is not None
    assert subscription.cancellation_type == "immediate"


class TestBillingSubscriptionUpdates:
  """Tests for subscription update methods."""

  def test_update_plan(self, db_session: Session, test_user, test_org):
    """Test updating subscription plan."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_update_plan_upgrade(self, db_session: Session, test_user, test_org):
    """Test upgrading subscription plan."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_update_plan_downgrade(self, db_session: Session, test_user, test_org):
    """Test downgrading subscription plan."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_update_stripe_subscription(self, db_session: Session, test_user, test_org):
    """Test updating Stripe subscription details."""
    unique_id = str(uuid.uuid4())[:8]
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_is_active_for_active_subscription(
    self, db_session: Session, test_user, test_org
  ):
    """Test is_active returns True for active subscription."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    assert subscription.is_active() is True

  def test_is_active_for_pending_subscription(
    self, db_session: Session, test_user, test_org
  ):
    """Test is_active returns False for pending subscription."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )

    assert subscription.is_active() is False

  def test_is_active_for_paused_subscription(
    self, db_session: Session, test_user, test_org
  ):
    """Test is_active returns False for paused subscription."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)
    subscription.pause(db_session)

    assert subscription.is_active() is False

  def test_is_active_for_canceled_subscription(
    self, db_session: Session, test_user, test_org
  ):
    """Test is_active returns False for canceled subscription."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)
    subscription.cancel(db_session, immediate=True)

    assert subscription.is_active() is False


class TestBillingSubscriptionRenewal:
  """Tests for subscription period renewal."""

  def test_renew_period_advances_dates(self, db_session: Session, test_user, test_org):
    """Test that renew_period shifts period start/end forward by 30 days."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    old_period_start = subscription.current_period_start
    old_period_end = subscription.current_period_end

    subscription.renew_period(db_session)

    assert subscription.current_period_start == old_period_end
    assert subscription.current_period_end == old_period_end + timedelta(days=30)
    assert subscription.current_period_start != old_period_start

  def test_renew_period_preserves_status(
    self, db_session: Session, test_user, test_org
  ):
    """Test that renew_period does not change subscription status."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    subscription.renew_period(db_session)

    assert subscription.status == SubscriptionStatus.ACTIVE.value
    assert subscription.is_active() is True

  def test_renew_period_updates_timestamp(
    self, db_session: Session, test_user, test_org
  ):
    """Test that renew_period updates the updated_at timestamp."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      session=db_session,
    )
    subscription.activate(db_session)

    subscription.renew_period(db_session)

    assert subscription.updated_at is not None

  def test_renew_period_annual_interval(self, db_session: Session, test_user, test_org):
    """Test that renew_period uses 365 days for annual subscriptions."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      billing_interval="annual",
      session=db_session,
    )
    subscription.activate(db_session)

    old_period_end = subscription.current_period_end

    subscription.renew_period(db_session)

    assert subscription.current_period_start == old_period_end
    assert subscription.current_period_end == old_period_end + timedelta(days=365)

  def test_renew_period_monthly_interval(
    self, db_session: Session, test_user, test_org
  ):
    """Test that renew_period uses 30 days for monthly subscriptions."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="graph",
      resource_id="kg123abc",
      plan_name="standard",
      base_price_cents=2999,
      billing_interval="monthly",
      session=db_session,
    )
    subscription.activate(db_session)

    old_period_end = subscription.current_period_end

    subscription.renew_period(db_session)

    assert subscription.current_period_start == old_period_end
    assert subscription.current_period_end == old_period_end + timedelta(days=30)


class TestBillingSubscriptionRepr:
  """Tests for subscription string representation."""

  def test_repr_format(self, db_session: Session, test_user, test_org):
    """Test subscription __repr__ format."""
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_query_by_customer_uses_index(self, db_session: Session, test_user, test_org):
    """Test querying by customer (should use idx_billing_sub_customer)."""
    for i in range(5):
      BillingSubscription.create_subscription(
        org_id=test_org.id,
        resource_type="graph",
        resource_id=f"kg{i}",
        plan_name="standard",
        base_price_cents=2999,
        session=db_session,
      )

    subs = (
      db_session.query(BillingSubscription)
      .filter(BillingSubscription.org_id == test_org.id)
      .all()
    )

    assert len(subs) == 5

  def test_query_by_resource_uses_index(self, db_session: Session, test_user, test_org):
    """Test querying by resource (should use idx_billing_sub_resource)."""
    BillingSubscription.create_subscription(
      org_id=test_org.id,
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

  def test_query_by_status_uses_index(self, db_session: Session, test_user, test_org):
    """Test querying by status (should use idx_billing_sub_status)."""
    for i in range(3):
      sub = BillingSubscription.create_subscription(
        org_id=test_org.id,
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
        BillingSubscription.org_id == test_org.id,
      )
      .all()
    )

    assert len(active_subs) == 2


class TestResubscribeLookups:
  """Canceled/failed rows are kept as history and must neither block a new
  subscription to the same resource nor shadow the live one in lookups.

  An unfiltered `.first()` here is what turned one canceled repository
  subscription (or one declined card) into a permanent 409 on resubscribe.
  """

  def _repo_sub(self, db_session, org_id, status: str | None = None):
    sub = BillingSubscription.create_subscription(
      org_id=org_id,
      resource_type="repository",
      resource_id="sec",
      plan_name="starter",
      base_price_cents=2900,
      session=db_session,
      billing_interval="monthly",
    )
    if status is not None:
      sub.status = status
      db_session.commit()
    return sub

  def test_terminal_rows_do_not_count_as_conflicts(
    self, db_session: Session, test_user, test_org
  ):
    from robosystems.models.core.billing.subscription import (
      TERMINAL_SUBSCRIPTION_STATUSES,
    )

    for status in TERMINAL_SUBSCRIPTION_STATUSES:
      self._repo_sub(db_session, test_org.id, status=status)

    conflict = BillingSubscription.get_by_resource_and_org(
      resource_type="repository",
      resource_id="sec",
      org_id=test_org.id,
      session=db_session,
      exclude_statuses=TERMINAL_SUBSCRIPTION_STATUSES,
    )

    assert conflict is None

  def test_live_row_still_counts_as_conflict(
    self, db_session: Session, test_user, test_org
  ):
    from robosystems.models.core.billing.subscription import (
      TERMINAL_SUBSCRIPTION_STATUSES,
    )

    self._repo_sub(db_session, test_org.id, status=SubscriptionStatus.ACTIVE.value)

    conflict = BillingSubscription.get_by_resource_and_org(
      resource_type="repository",
      resource_id="sec",
      org_id=test_org.id,
      session=db_session,
      exclude_statuses=TERMINAL_SUBSCRIPTION_STATUSES,
    )

    assert conflict is not None

  def test_live_row_shadows_terminal_history(
    self, db_session: Session, test_user, test_org
  ):
    """After a cancel-then-resubscribe, unfiltered lookups (status GET,
    plan change, cancel) must resolve to the live subscription, not
    whichever row the database happens to return first."""
    self._repo_sub(db_session, test_org.id, status=SubscriptionStatus.CANCELED.value)
    live = self._repo_sub(
      db_session, test_org.id, status=SubscriptionStatus.ACTIVE.value
    )

    found = BillingSubscription.get_by_resource_and_org(
      resource_type="repository",
      resource_id="sec",
      org_id=test_org.id,
      session=db_session,
    )

    assert found is not None
    assert found.id == live.id

  def test_most_recent_terminal_row_when_none_live(
    self, db_session: Session, test_user, test_org
  ):
    """With only history left, lookups return the most recent chapter of it —
    so a status GET after a full cancel still reads 'canceled'."""
    self._repo_sub(db_session, test_org.id, status=SubscriptionStatus.FAILED.value)
    latest = self._repo_sub(
      db_session, test_org.id, status=SubscriptionStatus.CANCELED.value
    )
    latest.created_at = latest.created_at + timedelta(seconds=5)
    db_session.commit()

    found = BillingSubscription.get_by_resource_and_org(
      resource_type="repository",
      resource_id="sec",
      org_id=test_org.id,
      session=db_session,
    )

    assert found is not None
    assert found.id == latest.id


class TestPerSubscriberLookups:
  """Repository subscriptions are billed to the org but held per user.

  These pin the fix for the multi-user case: one member's subscription must
  never resolve as another member's, or member B is refused a subscription
  (409) while getting neither access nor credits from member A's.
  """

  def _repo_sub(self, db_session, org_id, user_id, status: str | None = None):
    sub = BillingSubscription.create_subscription(
      org_id=org_id,
      resource_type="repository",
      resource_id="sec",
      plan_name="starter",
      base_price_cents=2900,
      session=db_session,
      user_id=user_id,
    )
    if status is not None:
      sub.status = status
      db_session.commit()
    return sub

  def _second_member(self, db_session, test_org):
    from robosystems.models.core import OrgRole, OrgUser

    unique_id = str(uuid.uuid4())[:8]
    user = User(
      id=f"test_user_{unique_id}",
      email=f"second+{unique_id}@example.com",
      name="Second Member",
      password_hash="test_hash",
    )
    db_session.add(user)
    db_session.flush()
    OrgUser.create(
      org_id=test_org.id, user_id=user.id, role=OrgRole.MEMBER, session=db_session
    )
    return user

  def test_lookup_is_scoped_to_the_subscriber(
    self, db_session: Session, test_user, test_org
  ):
    other = self._second_member(db_session, test_org)
    mine = self._repo_sub(db_session, test_org.id, test_user.id)

    found = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id="sec",
      user_id=test_user.id,
      session=db_session,
    )
    assert found is not None
    assert found.id == mine.id

    assert (
      BillingSubscription.get_by_resource_and_user(
        resource_type="repository",
        resource_id="sec",
        user_id=other.id,
        session=db_session,
      )
      is None
    )

  def test_colleagues_subscription_is_not_a_conflict(
    self, db_session: Session, test_user, test_org
  ):
    """Member B subscribing after member A is the upsell, not a duplicate."""
    from robosystems.models.core.billing.subscription import (
      TERMINAL_SUBSCRIPTION_STATUSES,
    )

    other = self._second_member(db_session, test_org)
    self._repo_sub(
      db_session, test_org.id, test_user.id, status=SubscriptionStatus.ACTIVE.value
    )

    conflict = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id="sec",
      user_id=other.id,
      session=db_session,
      exclude_statuses=TERMINAL_SUBSCRIPTION_STATUSES,
    )

    assert conflict is None

  def test_live_subscriptions_for_user_excludes_terminal_rows(
    self, db_session: Session, test_user, test_org
  ):
    self._repo_sub(
      db_session, test_org.id, test_user.id, status=SubscriptionStatus.CANCELED.value
    )
    live = self._repo_sub(
      db_session, test_org.id, test_user.id, status=SubscriptionStatus.ACTIVE.value
    )

    found = BillingSubscription.get_live_subscriptions_for_user(
      test_user.id, db_session, resource_type="repository"
    )

    assert [s.id for s in found] == [live.id]
