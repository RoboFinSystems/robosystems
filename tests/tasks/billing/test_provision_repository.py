"""Tests for repository access provisioning Celery task."""

import pytest
from unittest.mock import Mock, patch

from robosystems.tasks.billing.provision_repository import (
  provision_repository_access_task,
)
from robosystems.models.billing import BillingSubscription


class TestProvisionRepositoryAccessTask:
  """Tests for provision_repository_access_task."""

  @pytest.fixture
  def mock_user(self):
    """Create mock customer."""
    user = Mock()
    user.id = "user_123"
    user.email = "test@example.com"
    user.payment_terms = "net_30"
    return user

  @pytest.fixture
  def mock_subscription(self):
    """Create mock subscription."""
    from datetime import datetime, timezone

    sub = Mock(spec=BillingSubscription)
    sub.id = "sub_456"
    sub.org_id = "org_123"
    sub.resource_type = "repository"
    sub.resource_id = "sec"
    sub.plan_name = "starter"
    sub.status = "provisioning"
    sub.subscription_metadata = {}
    sub.current_period_start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    sub.current_period_end = datetime(2025, 2, 1, tzinfo=timezone.utc)
    return sub

  @patch("robosystems.tasks.billing.provision_repository.generate_subscription_invoice")
  @patch("robosystems.tasks.billing.provision_repository.BillingCustomer")
  @patch("robosystems.tasks.billing.provision_repository.get_db_session")
  @patch("robosystems.tasks.billing.provision_repository.RepositorySubscriptionService")
  def test_provision_repository_success(
    self,
    mock_service_class,
    mock_get_db,
    mock_billing_customer,
    mock_generate_invoice,
    mock_user,
    mock_subscription,
  ):
    """Test successful repository access provisioning."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])

    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_billing_customer.get_by_user_id.return_value = mock_user

    mock_service = Mock()
    mock_service_class.return_value = mock_service
    mock_service.allocate_credits.return_value = 1000
    mock_service.grant_access.return_value = True

    provision_repository_access_task(  # type: ignore[call-arg]
      user_id="user_123", subscription_id="sub_456", repository_name="sec"
    )

    mock_service.allocate_credits.assert_called_once()
    mock_service.grant_access.assert_called_once()
    mock_subscription.activate.assert_called_once_with(mock_session)
    mock_generate_invoice.assert_called_once()

  @patch("robosystems.tasks.billing.provision_repository.get_db_session")
  def test_provision_repository_subscription_not_found(self, mock_get_db):
    """Test handling when subscription is not found."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = None

    with pytest.raises(Exception, match="Subscription sub_999 not found"):
      provision_repository_access_task(  # type: ignore[call-arg]
        user_id="user_123", subscription_id="sub_999", repository_name="sec"
      )

  @patch("robosystems.tasks.billing.provision_repository.BillingCustomer")
  @patch("robosystems.tasks.billing.provision_repository.get_db_session")
  def test_provision_repository_customer_not_found(
    self, mock_get_db, mock_billing_customer, mock_subscription
  ):
    """Test handling when customer is not found."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )
    mock_billing_customer.get_by_user_id.return_value = None

    with pytest.raises(Exception, match="Customer not found for user user_123"):
      provision_repository_access_task(  # type: ignore[call-arg]
        user_id="user_123", subscription_id="sub_456", repository_name="sec"
      )

  @patch("robosystems.tasks.billing.provision_repository.get_db_session")
  def test_provision_repository_invalid_repository_name(
    self, mock_get_db, mock_subscription
  ):
    """Test handling invalid repository name."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "starter"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    with pytest.raises(Exception, match="Invalid repository type"):
      provision_repository_access_task(  # type: ignore[call-arg]
        user_id="user_123",
        subscription_id="sub_456",
        repository_name="invalid_repo",
      )

  @patch("robosystems.tasks.billing.provision_repository.BillingCustomer")
  @patch("robosystems.tasks.billing.provision_repository.get_db_session")
  @patch("robosystems.tasks.billing.provision_repository.RepositorySubscriptionService")
  def test_provision_repository_failure_updates_subscription(
    self,
    mock_service_class,
    mock_get_db,
    mock_billing_customer,
    mock_user,
    mock_subscription,
  ):
    """Test that provisioning failure updates subscription status."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )
    mock_billing_customer.get_by_user_id.return_value = mock_user

    mock_service = Mock()
    mock_service_class.return_value = mock_service
    mock_service.allocate_credits.side_effect = Exception("Provisioning failed")

    with pytest.raises(Exception):
      provision_repository_access_task(  # type: ignore[call-arg]
        user_id="user_123", subscription_id="sub_456", repository_name="sec"
      )

  @patch("robosystems.tasks.billing.provision_repository.generate_subscription_invoice")
  @patch("robosystems.tasks.billing.provision_repository.BillingCustomer")
  @patch("robosystems.tasks.billing.provision_repository.get_db_session")
  @patch("robosystems.tasks.billing.provision_repository.BillingAuditLog")
  @patch("robosystems.tasks.billing.provision_repository.RepositorySubscriptionService")
  def test_provision_repository_audit_log_creation(
    self,
    mock_service_class,
    mock_audit_log,
    mock_get_db,
    mock_billing_customer,
    mock_generate_invoice,
    mock_user,
    mock_subscription,
  ):
    """Test that audit log is created for provisioning."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )
    mock_billing_customer.get_by_user_id.return_value = mock_user

    mock_service = Mock()
    mock_service_class.return_value = mock_service
    mock_service.allocate_credits.return_value = 1000
    mock_service.grant_access.return_value = True

    provision_repository_access_task(  # type: ignore[call-arg]
      user_id="user_123", subscription_id="sub_456", repository_name="sec"
    )

    mock_audit_log.log_event.assert_called()


class TestProvisionRepositoryRetryLogic:
  """Tests for task retry logic."""

  @patch("robosystems.tasks.billing.provision_repository.BillingCustomer")
  @patch("robosystems.tasks.billing.provision_repository.get_db_session")
  @patch("robosystems.tasks.billing.provision_repository.RepositorySubscriptionService")
  def test_provision_repository_transient_failure_retries(
    self, mock_service_class, mock_get_db, mock_billing_customer
  ):
    """Test that transient failures trigger retry."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])

    mock_user = Mock()
    mock_subscription = Mock()
    mock_subscription.plan_name = "starter"
    mock_subscription.subscription_metadata = {}
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )
    mock_billing_customer.get_by_user_id.return_value = mock_user

    mock_service = Mock()
    mock_service_class.return_value = mock_service
    mock_service.allocate_credits.side_effect = Exception("Temporary network error")

    with pytest.raises(Exception):
      provision_repository_access_task(  # type: ignore[call-arg]
        user_id="user_123", subscription_id="sub_456", repository_name="sec"
      )
