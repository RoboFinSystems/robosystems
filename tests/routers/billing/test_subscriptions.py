"""Tests for billing subscriptions endpoints."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from fastapi import HTTPException
from robosystems.routers.billing.subscriptions import (
  list_subscriptions,
  get_subscription,
  cancel_subscription,
)
from robosystems.models.billing import BillingSubscription
from robosystems.models.iam import User


class TestListSubscriptions:
  """Tests for list_subscriptions endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    return user

  @pytest.fixture
  def mock_db(self):
    db = Mock()
    return db

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_list_subscriptions_empty(self, mock_get_org_user, mock_user, mock_db):
    """Test listing subscriptions when org has none."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.order_by.return_value.all.return_value = []

    result = await list_subscriptions("org_123", mock_user, mock_db, None)

    assert result == []

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_list_subscriptions_success(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test successful subscription listing."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_sub1 = Mock(spec=BillingSubscription)
    mock_sub1.id = "sub_1"
    mock_sub1.resource_type = "graph"
    mock_sub1.resource_id = "kg_123"
    mock_sub1.plan_name = "standard"
    mock_sub1.billing_interval = "monthly"
    mock_sub1.status = "active"
    mock_sub1.base_price_cents = 2999
    mock_sub1.current_period_start = datetime(2025, 1, 1)
    mock_sub1.current_period_end = datetime(2025, 2, 1)
    mock_sub1.started_at = datetime(2025, 1, 1)
    mock_sub1.canceled_at = None
    mock_sub1.created_at = datetime(2025, 1, 1)

    mock_sub2 = Mock(spec=BillingSubscription)
    mock_sub2.id = "sub_2"
    mock_sub2.resource_type = "repository"
    mock_sub2.resource_id = "sec"
    mock_sub2.plan_name = "starter"
    mock_sub2.billing_interval = "monthly"
    mock_sub2.status = "canceled"
    mock_sub2.base_price_cents = 999
    mock_sub2.current_period_start = None
    mock_sub2.current_period_end = None
    mock_sub2.started_at = datetime(2024, 12, 1)
    mock_sub2.canceled_at = datetime(2025, 1, 15)
    mock_sub2.created_at = datetime(2024, 12, 1)

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.order_by.return_value.all.return_value = [
      mock_sub1,
      mock_sub2,
    ]

    result = await list_subscriptions("org_123", mock_user, mock_db, None)

    assert len(result) == 2
    assert result[0].id == "sub_1"
    assert result[0].resource_type == "graph"
    assert result[0].resource_id == "kg_123"
    assert result[0].plan_name == "standard"
    assert result[0].status == "active"
    assert result[0].base_price_cents == 2999

    assert result[1].id == "sub_2"
    assert result[1].resource_type == "repository"
    assert result[1].status == "canceled"
    assert result[1].canceled_at is not None

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_list_subscriptions_error_handling(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test error handling in subscription listing."""
    mock_get_org_user.side_effect = Exception("Database error")

    with pytest.raises(HTTPException) as exc:
      await list_subscriptions("org_123", mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to retrieve subscriptions" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_list_subscriptions_requires_membership(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Non-members should receive 403."""
    mock_get_org_user.return_value = None

    with pytest.raises(HTTPException) as exc:
      await list_subscriptions("org_123", mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "You are not a member of this organization"


class TestGetSubscription:
  """Tests for get_subscription endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    return user

  @pytest.fixture
  def mock_db(self):
    return Mock()

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_get_subscription_success(self, mock_get_org_user, mock_user, mock_db):
    """Test successful subscription retrieval."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_123"
    mock_subscription.resource_type = "graph"
    mock_subscription.resource_id = "kg_456"
    mock_subscription.plan_name = "enterprise"
    mock_subscription.billing_interval = "monthly"
    mock_subscription.status = "active"
    mock_subscription.base_price_cents = 9999
    mock_subscription.current_period_start = datetime(2025, 1, 1)
    mock_subscription.current_period_end = datetime(2025, 2, 1)
    mock_subscription.started_at = datetime(2025, 1, 1)
    mock_subscription.canceled_at = None
    mock_subscription.created_at = datetime(2025, 1, 1)

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.first.return_value = mock_subscription

    result = await get_subscription("org_123", "sub_123", mock_user, mock_db, None)

    assert result.id == "sub_123"
    assert result.resource_type == "graph"
    assert result.resource_id == "kg_456"
    assert result.plan_name == "enterprise"
    assert result.status == "active"

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_get_subscription_not_found(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test getting subscription that doesn't exist."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.first.return_value = None

    with pytest.raises(HTTPException) as exc:
      await get_subscription("org_123", "sub_999", mock_user, mock_db, None)

    assert exc.value.status_code == 404
    assert "Subscription not found" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_get_subscription_null_resource_id(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test getting subscription with null resource_id."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_123"
    mock_subscription.resource_type = "graph"
    mock_subscription.resource_id = None
    mock_subscription.plan_name = "standard"
    mock_subscription.billing_interval = "monthly"
    mock_subscription.status = "pending_provisioning"
    mock_subscription.base_price_cents = 2999
    mock_subscription.current_period_start = None
    mock_subscription.current_period_end = None
    mock_subscription.started_at = None
    mock_subscription.canceled_at = None
    mock_subscription.created_at = datetime(2025, 1, 1)

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.first.return_value = mock_subscription

    result = await get_subscription("org_123", "sub_123", mock_user, mock_db, None)

    assert result.resource_id == ""
    assert result.status == "pending_provisioning"

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_get_subscription_error_handling(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test error handling in subscription retrieval."""
    mock_get_org_user.side_effect = Exception("Database error")

    with pytest.raises(HTTPException) as exc:
      await get_subscription("org_123", "sub_123", mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to retrieve subscription" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_get_subscription_requires_membership(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Users outside the org should be blocked."""
    mock_get_org_user.return_value = None

    with pytest.raises(HTTPException) as exc:
      await get_subscription("org_123", "sub_1", mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "You are not a member of this organization"


class TestCancelSubscription:
  """Tests for cancel_subscription endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    return user

  @pytest.fixture
  def mock_db(self):
    return Mock()

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_cancel_subscription_success(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test successful subscription cancellation."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.id = "sub_123"
    mock_subscription.resource_type = "graph"
    mock_subscription.resource_id = "kg_456"
    mock_subscription.plan_name = "standard"
    mock_subscription.billing_interval = "monthly"
    mock_subscription.status = "active"
    mock_subscription.base_price_cents = 2999
    mock_subscription.current_period_start = datetime(2025, 1, 1)
    mock_subscription.current_period_end = datetime(2025, 2, 1)
    mock_subscription.started_at = datetime(2025, 1, 1)
    mock_subscription.canceled_at = None
    mock_subscription.created_at = datetime(2025, 1, 1)

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.first.return_value = mock_subscription

    result = await cancel_subscription("org_123", "sub_123", mock_user, mock_db, None)

    mock_subscription.cancel.assert_called_once_with(mock_db, immediate=False)
    assert result.id == "sub_123"

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_cancel_subscription_not_found(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test canceling subscription that doesn't exist."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.first.return_value = None

    with pytest.raises(HTTPException) as exc:
      await cancel_subscription("org_123", "sub_999", mock_user, mock_db, None)

    assert exc.value.status_code == 404
    assert "Subscription not found" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_cancel_subscription_already_canceled(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test canceling already canceled subscription."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.status = "canceled"

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.first.return_value = mock_subscription

    with pytest.raises(HTTPException) as exc:
      await cancel_subscription("org_123", "sub_123", mock_user, mock_db, None)

    assert exc.value.status_code == 400
    assert "already canceled" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_cancel_subscription_canceling_status(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test canceling subscription in canceling status."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_subscription = Mock(spec=BillingSubscription)
    mock_subscription.status = "canceling"

    mock_query = mock_db.query.return_value
    mock_query.filter.return_value.first.return_value = mock_subscription

    with pytest.raises(HTTPException) as exc:
      await cancel_subscription("org_123", "sub_123", mock_user, mock_db, None)

    assert exc.value.status_code == 400
    assert "already canceled" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_cancel_subscription_error_handling(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test error handling in subscription cancellation."""
    mock_get_org_user.side_effect = Exception("Database error")

    with pytest.raises(HTTPException) as exc:
      await cancel_subscription("org_123", "sub_123", mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to cancel subscription" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_cancel_subscription_requires_owner_role(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Admins should not be able to cancel subscriptions."""
    from robosystems.models.iam import OrgRole

    mock_membership = Mock()
    mock_membership.role = OrgRole.ADMIN
    mock_get_org_user.return_value = mock_membership

    with pytest.raises(HTTPException) as exc:
      await cancel_subscription("org_123", "sub_123", mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "Only organization owners can cancel subscriptions"
