"""Tests for graph provisioning Celery task."""

import pytest
from unittest.mock import Mock, patch
from robosystems.tasks.graph_operations.provision_graph import provision_graph_task
from robosystems.models.billing import BillingSubscription
from robosystems.models.iam import User


class TestProvisionGraphTask:
  """Tests for provision_graph_task."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def mock_subscription(self):
    sub = Mock(spec=BillingSubscription)
    sub.id = "sub_456"
    sub.billing_customer_user_id = "user_123"
    sub.resource_type = "graph"
    sub.plan_name = "standard"
    sub.status = "provisioning"
    sub.subscription_metadata = {}
    return sub

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.provision_graph.GenericGraphServiceSync")
  def test_provision_graph_success(
    self, mock_service_class, mock_get_db, mock_user, mock_subscription
  ):
    """Test successful graph provisioning."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_service = Mock()
    mock_service_class.return_value = mock_service
    mock_service.create_graph.return_value = {"graph_id": "kg_789"}

    provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config={"tier": "standard"},
    )

    mock_service.create_graph.assert_called_once()
    assert mock_subscription.resource_id == "kg_789"
    mock_subscription.activate.assert_called_once_with(mock_session)

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  def test_provision_graph_subscription_not_found(self, mock_get_db):
    """Test handling when subscription is not found."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = None

    with pytest.raises(Exception, match="Subscription sub_123 not found"):
      provision_graph_task(  # type: ignore[call-arg]
        user_id="user_999", subscription_id="sub_123", graph_config={}
      )

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.provision_graph.GenericGraphServiceSync")
  def test_provision_graph_failure_updates_subscription(
    self, mock_service_class, mock_get_db, mock_user, mock_subscription
  ):
    """Test that provisioning failure updates subscription."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_service = Mock()
    mock_service_class.return_value = mock_service
    mock_service.create_graph.side_effect = Exception("Allocation failed")

    with pytest.raises(Exception):
      provision_graph_task(  # type: ignore[call-arg]
        user_id="user_123", subscription_id="sub_456", graph_config={}
      )

    assert mock_subscription.status == "failed"
    assert "error" in mock_subscription.subscription_metadata
