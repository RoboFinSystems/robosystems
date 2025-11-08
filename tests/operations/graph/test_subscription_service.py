"""Tests for the subscription service."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy.exc import SQLAlchemyError

from robosystems.operations.graph.subscription_service import (
  GraphSubscriptionService,
)
from robosystems.models.iam import OrgLimits
from robosystems.models.billing import BillingSubscription, SubscriptionStatus


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
    user_limits = Mock(spec=OrgLimits)
    user_limits.user_id = "user123"
    user_limits.is_active = True
    user_limits.max_graph_users = 3
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
    plan_config = {"name": "kuzu-standard", "base_price_cents": 4999}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock no existing subscription
      mock_session.query.return_value.filter.return_value.first.return_value = None
      mock_session.query.return_value.filter.return_value.count.return_value = 0

      result = subscription_service.create_graph_subscription(
        user_id, graph_id, plan_name="kuzu-standard"
      )

      # Verify subscription creation
      assert mock_session.add.called
      assert mock_session.commit.called
      assert result.org_id is not None
      assert result.resource_type == "graph"
      assert result.resource_id == graph_id
      assert result.plan_name == "kuzu-standard"
      assert result.status == SubscriptionStatus.ACTIVE.value

  def test_create_graph_subscription_new(
    self, subscription_service, sample_billing_plans, mock_session
  ):
    """Test creating a new graph subscription."""
    user_id = "user123"
    graph_id = "graph456"
    plan_name = "kuzu-standard"  # Use a plan that's in the available plans

    # Mock BillingConfig.get_subscription_plan
    plan_config = {"name": "kuzu-standard", "base_price_cents": 1999}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock existing subscription check (none exists)
      mock_session.query.return_value.filter.return_value.first.return_value = None
      mock_session.query.return_value.filter.return_value.count.return_value = 0

      # Call the method
      result = subscription_service.create_graph_subscription(
        user_id, graph_id, plan_name
      )

      # Verify subscription creation
      assert mock_session.add.called
      assert mock_session.commit.called
      assert result.org_id is not None
      assert result.resource_type == "graph"
      assert result.resource_id == graph_id
      assert result.plan_name == "kuzu-standard"
      assert result.status == SubscriptionStatus.ACTIVE.value

  def test_create_graph_subscription_existing(self, subscription_service, mock_session):
    """Test creating subscription when one already exists."""
    user_id = "user123"
    graph_id = "graph456"

    # Mock BillingConfig.get_subscription_plan
    plan_config = {"name": "kuzu-standard", "base_price_cents": 0}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock existing subscription
      existing_sub = Mock(spec=BillingSubscription)
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
    plan_config = {"name": "kuzu-standard", "base_price_cents": 0}
    with patch(
      "robosystems.operations.graph.subscription_service.BillingConfig.get_subscription_plan",
      return_value=plan_config,
    ):
      # Mock no existing subscription
      mock_session.query.return_value.filter.return_value.first.return_value = None
      mock_session.commit.side_effect = SQLAlchemyError("Database error")

      with pytest.raises(SQLAlchemyError):
        subscription_service.create_graph_subscription("user123", "graph456")

      # Note: Service doesn't handle rollback - exception propagates to caller


class TestSubscriptionHelperFunctions:
  """Test module-level helper functions."""

  def test_get_available_plans(self):
    """Test available plans returns all plans from centralized config."""
    import robosystems.operations.graph.subscription_service as sub_service

    plans = sub_service.get_available_plans()
    assert plans == ["kuzu-standard", "kuzu-large", "kuzu-xlarge"]

  def test_get_max_plan_tier(self):
    """Test max plan tier returns last plan from centralized config."""
    import robosystems.operations.graph.subscription_service as sub_service

    assert sub_service.get_max_plan_tier() == "kuzu-xlarge"
