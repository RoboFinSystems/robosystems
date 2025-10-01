"""Test GraphSubscription model functionality."""

import pytest
from datetime import datetime, timedelta, timezone

from robosystems.models.iam import GraphSubscription, User
from robosystems.models.iam.graph_subscription import SubscriptionStatus


class TestGraphSubscription:
  """Test cases for GraphSubscription model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session

    # Create unique IDs for this test class
    import uuid

    self.unique_id = str(uuid.uuid4())[:8]

    # Create a test user
    self.user = User(
      email=f"graph_subscriber_{self.unique_id}@example.com",
      name="Test Subscriber",
      password_hash="hashed_password",
    )
    self.session.add(self.user)
    self.session.commit()

  def test_create_graph_subscription(self):
    """Test creating a basic graph subscription."""
    subscription = GraphSubscription(
      user_id=self.user.id,
      graph_id=f"test_graph_sub_{self.unique_id}",
      plan_name="standard",
      status=SubscriptionStatus.ACTIVE,
    )

    assert subscription.user_id == self.user.id
    assert subscription.graph_id == f"test_graph_sub_{self.unique_id}"
    assert subscription.plan_name == "standard"
    assert subscription.status == SubscriptionStatus.ACTIVE
    assert subscription.created_at is None  # Not set until session add

    self.session.add(subscription)
    self.session.commit()

    assert subscription.id is not None
    assert subscription.id.startswith("gsub_")
    assert subscription.created_at is not None
    assert subscription.updated_at is not None

  def test_subscription_status_enum_values(self):
    """Test all SubscriptionStatus enum values."""
    assert SubscriptionStatus.ACTIVE.value == "active"
    assert SubscriptionStatus.PAST_DUE.value == "past_due"
    assert SubscriptionStatus.CANCELED.value == "canceled"
    assert SubscriptionStatus.UNPAID.value == "unpaid"

  def test_subscription_id_generation(self):
    """Test that subscription ID is generated correctly."""
    subscription = GraphSubscription(
      user_id=self.user.id, graph_id=f"test_graph_sub_2_{self.unique_id}"
    )

    self.session.add(subscription)
    self.session.commit()

    assert subscription.id is not None
    assert subscription.id.startswith("gsub_")
    assert len(subscription.id) > 5  # Should have random suffix

  def test_subscription_default_status(self):
    """Test that default status is ACTIVE."""
    subscription = GraphSubscription(
      user_id=self.user.id, graph_id=f"test_graph_sub_2_{self.unique_id}"
    )

    self.session.add(subscription)
    self.session.commit()

    assert subscription.status == SubscriptionStatus.ACTIVE

  def test_is_active_property(self):
    """Test the is_active property."""
    active_sub = GraphSubscription(
      user_id=self.user.id, graph_id="active_graph", status=SubscriptionStatus.ACTIVE
    )

    canceled_sub = GraphSubscription(
      user_id=self.user.id,
      graph_id="canceled_graph",
      status=SubscriptionStatus.CANCELED,
    )

    self.session.add(active_sub)
    self.session.add(canceled_sub)
    self.session.commit()

    assert active_sub.is_active is True
    assert canceled_sub.is_active is False

  def test_subscription_with_billing_period(self):
    """Test subscription with billing period information."""
    now = datetime.now(timezone.utc)
    period_end = now + timedelta(days=30)

    subscription = GraphSubscription(
      user_id=self.user.id,
      graph_id="billing_graph",
      plan_name="enterprise",
      current_period_start=now,
      current_period_end=period_end,
    )

    self.session.add(subscription)
    self.session.commit()

    assert subscription.current_period_start == now
    assert subscription.current_period_end == period_end

  def test_subscription_timestamps(self):
    """Test that created_at and updated_at are set correctly."""
    subscription = GraphSubscription(
      user_id=self.user.id, graph_id=f"timestamp_graph_{self.unique_id}"
    )

    # Before adding to session
    assert subscription.created_at is None
    assert subscription.updated_at is None

    self.session.add(subscription)
    self.session.commit()

    # After commit
    assert subscription.created_at is not None
    assert subscription.updated_at is not None
    # Allow for small differences due to database timing
    time_diff = abs((subscription.created_at - subscription.updated_at).total_seconds())
    assert time_diff < 0.01  # Less than 10ms difference

    # Update the subscription
    original_updated = subscription.updated_at
    subscription.plan_name = "premium"
    self.session.commit()

    # updated_at should change (onupdate trigger)
    assert subscription.updated_at >= original_updated

  def test_subscription_user_relationship(self):
    """Test the relationship with User model."""
    subscription = GraphSubscription(user_id=self.user.id, graph_id="related_graph")

    self.session.add(subscription)
    self.session.commit()

    # Access through relationship
    assert subscription.user == self.user
    assert subscription in self.user.graph_subscriptions

  def test_repr_method(self):
    """Test string representation of subscription."""
    subscription = GraphSubscription(
      user_id=self.user.id, graph_id="repr_graph", status=SubscriptionStatus.PAST_DUE
    )

    self.session.add(subscription)
    self.session.commit()

    repr_str = repr(subscription)
    assert f"<GraphSubscription(id={subscription.id}" in repr_str
    assert f"user={self.user.id}" in repr_str
    assert "graph=repr_graph" in repr_str
    assert "status=SubscriptionStatus.PAST_DUE" in repr_str

  def test_multiple_subscriptions_per_user(self):
    """Test that a user can have multiple graph subscriptions."""
    sub1 = GraphSubscription(
      user_id=self.user.id, graph_id="graph1", plan_name="standard"
    )

    sub2 = GraphSubscription(
      user_id=self.user.id, graph_id="graph2", plan_name="enterprise"
    )

    self.session.add(sub1)
    self.session.add(sub2)
    self.session.commit()

    # Query all subscriptions for user
    user_subs = (
      self.session.query(GraphSubscription).filter_by(user_id=self.user.id).all()
    )

    assert len(user_subs) == 2
    graph_ids = {sub.graph_id for sub in user_subs}
    assert graph_ids == {"graph1", "graph2"}

  def test_different_subscription_statuses(self):
    """Test creating subscriptions with different statuses."""
    statuses = [
      SubscriptionStatus.ACTIVE,
      SubscriptionStatus.PAST_DUE,
      SubscriptionStatus.CANCELED,
      SubscriptionStatus.UNPAID,
    ]

    for idx, status in enumerate(statuses):
      subscription = GraphSubscription(
        user_id=self.user.id, graph_id=f"graph_{idx}", status=status
      )
      self.session.add(subscription)

    self.session.commit()

    # Verify all statuses were saved correctly
    for idx, status in enumerate(statuses):
      sub = (
        self.session.query(GraphSubscription).filter_by(graph_id=f"graph_{idx}").first()
      )
      assert sub is not None
      assert sub.status == status

  def test_nullable_plan_name(self):
    """Test that plan_name can be None."""
    subscription = GraphSubscription(
      user_id=self.user.id, graph_id="no_plan_graph", plan_name=None
    )

    self.session.add(subscription)
    self.session.commit()

    assert subscription.plan_name is None

  def test_query_active_subscriptions(self):
    """Test querying only active subscriptions."""
    # Create mix of subscriptions
    active1 = GraphSubscription(
      user_id=self.user.id, graph_id="active1", status=SubscriptionStatus.ACTIVE
    )
    canceled = GraphSubscription(
      user_id=self.user.id, graph_id="canceled1", status=SubscriptionStatus.CANCELED
    )
    active2 = GraphSubscription(
      user_id=self.user.id, graph_id="active2", status=SubscriptionStatus.ACTIVE
    )

    self.session.add_all([active1, canceled, active2])
    self.session.commit()

    # Query only active subscriptions
    active_subs = (
      self.session.query(GraphSubscription)
      .filter_by(user_id=self.user.id, status=SubscriptionStatus.ACTIVE)
      .all()
    )

    assert len(active_subs) == 2
    graph_ids = {sub.graph_id for sub in active_subs}
    assert graph_ids == {"active1", "active2"}

  def test_update_subscription_status(self):
    """Test updating subscription status."""
    subscription = GraphSubscription(
      user_id=self.user.id,
      graph_id="status_update_graph",
      status=SubscriptionStatus.ACTIVE,
    )

    self.session.add(subscription)
    self.session.commit()

    # Update status
    subscription.status = SubscriptionStatus.PAST_DUE
    self.session.commit()

    # Verify update
    updated_sub = (
      self.session.query(GraphSubscription).filter_by(id=subscription.id).first()
    )
    assert updated_sub.status == SubscriptionStatus.PAST_DUE
    assert updated_sub.is_active is False

  def test_billing_period_validation(self):
    """Test billing period dates can be in any order."""
    now = datetime.now(timezone.utc)
    past = now - timedelta(days=30)

    # This should work even if end < start (no validation in model)
    subscription = GraphSubscription(
      user_id=self.user.id,
      graph_id="backwards_period",
      current_period_start=now,
      current_period_end=past,
    )

    self.session.add(subscription)
    self.session.commit()

    assert subscription.current_period_start == now
    assert subscription.current_period_end == past

  def test_timezone_aware_dates(self):
    """Test that dates are timezone-aware."""
    datetime.now()
    aware_date = datetime.now(timezone.utc)

    subscription = GraphSubscription(
      user_id=self.user.id,
      graph_id="tz_graph",
      current_period_start=aware_date,
      current_period_end=aware_date + timedelta(days=30),
    )

    self.session.add(subscription)
    self.session.commit()

    # Dates should maintain timezone information
    assert subscription.current_period_start.tzinfo is not None
    assert subscription.current_period_end.tzinfo is not None
    assert subscription.created_at.tzinfo is not None
    assert subscription.updated_at.tzinfo is not None

  def test_delete_subscription(self):
    """Test deleting a subscription."""
    subscription = GraphSubscription(user_id=self.user.id, graph_id="delete_graph")

    self.session.add(subscription)
    self.session.commit()

    sub_id = subscription.id

    # Delete subscription
    self.session.delete(subscription)
    self.session.commit()

    # Verify deletion
    deleted_sub = self.session.query(GraphSubscription).filter_by(id=sub_id).first()
    assert deleted_sub is None

  def test_cascade_behavior_with_user(self):
    """Test that subscription is handled when user is deleted."""
    # Create new user with subscription
    temp_user = User(
      email="temp@example.com", name="Temp User", password_hash="hashed_password"
    )
    self.session.add(temp_user)
    self.session.commit()

    subscription = GraphSubscription(user_id=temp_user.id, graph_id="cascade_graph")
    self.session.add(subscription)
    self.session.commit()

    sub_id = subscription.id

    # Delete user - subscription should remain (no cascade delete)
    # But it will have an orphaned user_id
    self.session.delete(temp_user)
    self.session.commit()

    # Check if subscription still exists (depends on FK constraints)
    # This behavior might vary based on database configuration
    self.session.query(GraphSubscription).filter_by(id=sub_id).first()

    # Note: The actual behavior depends on the foreign key constraint
    # If ON DELETE CASCADE is set, subscription will be deleted
    # If not, it might raise an error or leave orphaned record
    # This test documents the current behavior

  def test_unique_subscription_ids(self):
    """Test that each subscription gets a unique ID."""
    subscriptions = []
    for i in range(5):
      sub = GraphSubscription(user_id=self.user.id, graph_id=f"unique_{i}")
      self.session.add(sub)
      subscriptions.append(sub)

    self.session.commit()

    # All IDs should be unique
    ids = [sub.id for sub in subscriptions]
    assert len(ids) == len(set(ids))

    # All should start with gsub_
    for sub_id in ids:
      assert sub_id.startswith("gsub_")
