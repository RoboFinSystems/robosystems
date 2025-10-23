"""Tests for the subscription service."""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError

from robosystems.operations.graph.subscription_service import (
  GraphSubscriptionService,
)
from robosystems.models.iam import (
  UserLimits,
  GraphSubscription,
  SubscriptionStatus,
)


class TestGraphSubscriptionService:
  """Tests for GraphSubscriptionService class."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    session = MagicMock()
    return session

  @pytest.fixture
  def subscription_service(self, mock_session):
    """Create a GraphSubscriptionService instance."""
    return GraphSubscriptionService(mock_session)

  @pytest.fixture
  def sample_user_limits(self):
    """Create sample user limits for testing."""
    user_limits = Mock(spec=UserLimits)
    user_limits.user_id = "user123"
    user_limits.is_active = True
    user_limits.max_user_graphs = 3
    user_limits.subscription_tier = "free"
    user_limits.max_api_calls_per_hour = 1000
    return user_limits

  @pytest.fixture
  def sample_billing_plans(self):
    """Create sample billing plans for testing."""
    plans = [
      Mock(
        id="plan_free",
        name="free",
        base_price_cents=0,
        max_graphs=1,
        max_storage_gb=1,
        is_active=True,
      ),
      Mock(
        id="plan_starter",
        name="starter",
        base_price_cents=1999,
        max_graphs=5,
        max_storage_gb=10,
        is_active=True,
      ),
      Mock(
        id="plan_pro",
        name="pro",
        base_price_cents=9999,
        max_graphs=20,
        max_storage_gb=100,
        is_active=True,
      ),
      Mock(
        id="plan_enterprise",
        name="enterprise",
        base_price_cents=49999,
        max_graphs=100,
        max_storage_gb=1000,
        is_active=True,
      ),
    ]
    return plans

  def test_create_graph_subscription_standard(self, subscription_service, mock_session):
    """Test creating a standard subscription."""
    user_id = "user123"
    graph_id = "graph456"

    # Mock BillingConfig.get_subscription_plan
    plan_config = {"name": "kuzu-standard", "price": 4999}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock no existing subscription
      mock_session.query.return_value.filter.return_value.first.return_value = None

      subscription_service.create_graph_subscription(
        user_id, graph_id, plan_name="kuzu-standard"
      )

      # Verify subscription creation
      assert mock_session.add.called
      assert mock_session.commit.called
      added_subscription = mock_session.add.call_args[0][0]
      assert added_subscription.user_id == user_id
      assert added_subscription.graph_id == graph_id
      assert added_subscription.plan_name == "kuzu-standard"
      assert added_subscription.status == SubscriptionStatus.ACTIVE.value

  def test_create_graph_subscription_new(
    self, subscription_service, sample_billing_plans, mock_session
  ):
    """Test creating a new graph subscription."""
    user_id = "user123"
    graph_id = "graph456"
    plan_name = "kuzu-standard"  # Use a plan that's in the available plans

    # Mock BillingConfig.get_subscription_plan
    plan_config = {"name": "kuzu-standard", "price": 1999}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock existing subscription check (none exists)
      mock_session.query.return_value.filter.return_value.first.return_value = None

      # Call the method
      subscription_service.create_graph_subscription(user_id, graph_id, plan_name)

      # Verify subscription creation
      assert mock_session.add.called
      assert mock_session.commit.called
      added_subscription = mock_session.add.call_args[0][0]
      assert added_subscription.user_id == user_id
      assert added_subscription.graph_id == graph_id
      assert added_subscription.plan_name == "kuzu-standard"
      assert added_subscription.status == SubscriptionStatus.ACTIVE.value

  def test_create_graph_subscription_existing(self, subscription_service, mock_session):
    """Test creating subscription when one already exists."""
    user_id = "user123"
    graph_id = "graph456"

    # Mock BillingConfig.get_subscription_plan
    plan_config = {"name": "kuzu-standard", "price": 0}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock existing subscription
      existing_sub = Mock(spec=GraphSubscription)
      mock_session.query.return_value.filter.return_value.first.return_value = (
        existing_sub
      )

      result = subscription_service.create_graph_subscription(user_id, graph_id)

      assert result == existing_sub
      assert not mock_session.add.called

  def test_create_graph_subscription_invalid_plan(
    self, subscription_service, mock_session
  ):
    """Test creating subscription with invalid plan."""
    # Mock BillingConfig.get_subscription_plan to return None
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=None,
    ):
      # Since "invalid" will be downgraded to "kuzu-xlarge" (max tier), the error will mention kuzu-xlarge
      with pytest.raises(ValueError, match="Billing plan 'kuzu-xlarge' not found"):
        subscription_service.create_graph_subscription("user123", "graph456", "invalid")

  def test_create_graph_subscription_commit_failure(
    self, subscription_service, sample_billing_plans, mock_session
  ):
    """Test handling database commit failure."""
    # Mock BillingConfig.get_subscription_plan
    plan_config = {"name": "kuzu-standard", "price": 0}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock no existing subscription
      mock_session.query.return_value.filter.return_value.first.return_value = None
      mock_session.commit.side_effect = SQLAlchemyError("Database error")

      with pytest.raises(SQLAlchemyError):
        subscription_service.create_graph_subscription("user123", "graph456")

      assert mock_session.rollback.called

  def test_get_next_billing_date(self, subscription_service):
    """Test calculating next billing date."""
    # Test regular month
    date1 = datetime(2024, 5, 15, tzinfo=timezone.utc)
    next_date1 = subscription_service._get_next_billing_date(date1)
    assert next_date1.month == 6
    assert next_date1.day == 15

    # Test December (year rollover)
    date2 = datetime(2024, 12, 20, tzinfo=timezone.utc)
    next_date2 = subscription_service._get_next_billing_date(date2)
    assert next_date2.year == 2025
    assert next_date2.month == 1
    assert next_date2.day == 20


class TestSubscriptionHelperFunctions:
  """Test module-level helper functions."""

  def test_get_available_plans_dev_restricted(self):
    """Test available plans in dev with premium disabled."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_PREMIUM_PLANS_ENABLED", False),
      patch.object(sub_service, "ENVIRONMENT", "dev"),
    ):
      plans = sub_service.get_available_plans()
      assert plans == ["kuzu-standard"]  # Updated to match actual return values

  def test_get_available_plans_dev_unrestricted(self):
    """Test available plans in dev with premium enabled."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_PREMIUM_PLANS_ENABLED", True),
      patch.object(sub_service, "ENVIRONMENT", "dev"),
    ):
      plans = sub_service.get_available_plans()
      assert plans == ["kuzu-standard", "kuzu-large", "kuzu-xlarge"]

  def test_get_available_plans_prod(self):
    """Test available plans in prod (restrictions don't apply)."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_PREMIUM_PLANS_ENABLED", False),
      patch.object(sub_service, "ENVIRONMENT", "prod"),
    ):
      plans = sub_service.get_available_plans()
      # In prod, restrictions don't apply
      assert plans == ["kuzu-standard", "kuzu-large", "kuzu-xlarge"]

  def test_is_payment_required_dev_bypass(self):
    """Test payment requirement in dev with bypass."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_ENABLED", True),
      patch.object(sub_service, "ENVIRONMENT", "dev"),
    ):
      assert sub_service.is_payment_required() is False

  def test_is_payment_required_dev_no_bypass(self):
    """Test payment requirement in dev with billing disabled."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_ENABLED", False),
      patch.object(sub_service, "ENVIRONMENT", "dev"),
    ):
      assert sub_service.is_payment_required() is False

  def test_is_payment_required_prod(self):
    """Test payment requirement in prod (bypass doesn't apply)."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_ENABLED", True),
      patch.object(sub_service, "ENVIRONMENT", "prod"),
    ):
      assert sub_service.is_payment_required() is True

  def test_get_max_plan_tier_dev_restricted(self):
    """Test max plan tier in dev with premium disabled."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_PREMIUM_PLANS_ENABLED", False),
      patch.object(sub_service, "ENVIRONMENT", "dev"),
    ):
      assert sub_service.get_max_plan_tier() == "kuzu-standard"

  def test_get_max_plan_tier_dev_unrestricted(self):
    """Test max plan tier in dev with premium enabled."""
    import robosystems.operations.graph.subscription_service as sub_service

    # Patch the module-level variables directly
    with (
      patch.object(sub_service, "BILLING_PREMIUM_PLANS_ENABLED", True),
      patch.object(sub_service, "ENVIRONMENT", "dev"),
    ):
      assert sub_service.get_max_plan_tier() == "kuzu-xlarge"
