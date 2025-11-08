"""Comprehensive tests for Stripe webhook handlers."""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from main import app
from robosystems.models.billing import BillingCustomer, BillingSubscription


class TestStripeWebhookEndpoint:
  """Tests for Stripe webhook endpoint."""

  @pytest.fixture
  def client(self):
    """Create test client."""
    return TestClient(app)

  @pytest.fixture
  def mock_db_session(self):
    """Create mock database session."""
    with patch("robosystems.routers.admin.webhooks.get_db_session") as mock:
      session = Mock(spec=Session)
      mock.return_value.__next__ = Mock(return_value=session)
      yield session

  def test_webhook_missing_signature_header(self, client):
    """Test webhook request missing signature header."""
    response = client.post(
      "/admin/v1/webhooks/stripe",
      json={"type": "test.event"},
    )

    assert response.status_code == 400
    assert "Missing stripe-signature header" in response.json()["detail"]

  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_invalid_signature(self, mock_get_provider, client, mock_db_session):
    """Test webhook with invalid signature."""
    mock_provider = Mock()
    mock_provider.verify_webhook.side_effect = ValueError("Invalid signature")
    mock_get_provider.return_value = mock_provider

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json={"type": "test.event"},
      headers={"stripe-signature": "invalid_signature"},
    )

    assert response.status_code == 400
    assert "Invalid webhook signature" in response.json()["detail"]

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  @patch("robosystems.routers.admin.webhooks.handle_checkout_completed")
  def test_webhook_checkout_completed_event(
    self, mock_handle, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test handling checkout.session.completed event."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_test123",
      "type": "checkout.session.completed",
      "data": {"object": {"id": "cs_test", "customer": "cus_123"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_handle.return_value = AsyncMock()
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "success"}
    mock_handle.assert_called_once()
    mock_audit_log.is_webhook_processed.assert_called_once()
    mock_audit_log.mark_webhook_processed.assert_called_once()

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  @patch("robosystems.routers.admin.webhooks.handle_payment_succeeded")
  def test_webhook_payment_succeeded_event(
    self, mock_handle, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test handling invoice.payment_succeeded event."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_test456",
      "type": "invoice.payment_succeeded",
      "data": {"object": {"id": "in_123", "subscription": "sub_456"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_handle.return_value = AsyncMock()
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    mock_handle.assert_called_once()

  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  @patch("robosystems.routers.admin.webhooks.handle_payment_failed")
  def test_webhook_payment_failed_event(
    self, mock_handle, mock_get_provider, client, mock_db_session
  ):
    """Test handling invoice.payment_failed event."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_test789",
      "type": "invoice.payment_failed",
      "data": {"object": {"id": "in_789", "subscription": "sub_123"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_handle.return_value = AsyncMock()

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    mock_handle.assert_called_once()

  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  @patch("robosystems.routers.admin.webhooks.handle_subscription_updated")
  def test_webhook_subscription_updated_event(
    self, mock_handle, mock_get_provider, client, mock_db_session
  ):
    """Test handling customer.subscription.updated event."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_sub_update",
      "type": "customer.subscription.updated",
      "data": {"object": {"id": "sub_123", "status": "active"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_handle.return_value = AsyncMock()

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    mock_handle.assert_called_once()

  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  @patch("robosystems.routers.admin.webhooks.handle_subscription_deleted")
  def test_webhook_subscription_deleted_event(
    self, mock_handle, mock_get_provider, client, mock_db_session
  ):
    """Test handling customer.subscription.deleted event."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_sub_delete",
      "type": "customer.subscription.deleted",
      "data": {"object": {"id": "sub_123", "status": "canceled"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_handle.return_value = AsyncMock()

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    mock_handle.assert_called_once()

  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_unhandled_event_type(
    self, mock_get_provider, client, mock_db_session
  ):
    """Test handling unknown event type."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_unknown",
      "type": "some.unknown.event",
      "data": {"object": {}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert response.json() == {"status": "success"}


class TestCheckoutCompletedHandler:
  """Tests for checkout.session.completed handler."""

  @pytest.fixture
  def mock_db(self):
    """Create mock database session."""
    return Mock(spec=Session)

  @pytest.fixture
  def session_data(self):
    """Create test checkout session data."""
    return {
      "id": "cs_test123",
      "customer": "cus_test456",
      "subscription": "sub_test789",
      "payment_status": "paid",
      "metadata": {
        "user_id": "user_123",
        "resource_type": "graph",
        "resource_id": "kg_456",
        "plan_name": "standard",
      },
    }

  @pytest.mark.asyncio
  @patch("robosystems.tasks.graph_operations.provision_graph.provision_graph_task")
  async def test_checkout_completed_triggers_graph_provisioning(
    self, mock_task, mock_db, session_data
  ):
    """Test that checkout completion triggers graph provisioning."""
    from robosystems.routers.admin.webhooks import handle_checkout_completed

    mock_customer = Mock()
    mock_customer.user_id = "user_123"
    mock_customer.has_payment_method = False
    mock_customer.stripe_customer_id = None

    mock_subscription = Mock()
    mock_subscription.id = "bsub_123"
    mock_subscription.billing_customer_user_id = "user_123"
    mock_subscription.resource_type = "graph"
    mock_subscription.subscription_metadata = {"resource_config": {}}
    mock_subscription.status = "pending_payment"

    def query_side_effect(model):
      if model == BillingSubscription:
        result = Mock()
        result.filter.return_value.first.return_value = mock_subscription
        return result
      elif model == BillingCustomer:
        result = Mock()
        result.filter.return_value.first.return_value = mock_customer
        return result
      return Mock()

    mock_db.query.side_effect = query_side_effect

    await handle_checkout_completed(session_data, mock_db)

    mock_task.delay.assert_called_once()
    call_args = mock_task.delay.call_args
    assert call_args[1]["user_id"] == "user_123"
    assert call_args[1]["subscription_id"] == "bsub_123"
    assert mock_customer.has_payment_method is True
    assert mock_subscription.status == "provisioning"

  @pytest.mark.asyncio
  @patch(
    "robosystems.tasks.billing.provision_repository.provision_repository_access_task"
  )
  async def test_checkout_completed_triggers_repository_provisioning(
    self, mock_task, mock_db
  ):
    """Test that checkout completion triggers repository provisioning."""
    from robosystems.routers.admin.webhooks import handle_checkout_completed

    session_data = {
      "id": "cs_test123",
      "customer": "cus_test456",
      "subscription": "sub_test789",
      "payment_status": "paid",
      "metadata": {
        "user_id": "user_123",
        "resource_type": "repository",
        "resource_id": "sec",
        "plan_name": "starter",
      },
    }

    mock_customer = Mock()
    mock_customer.user_id = "user_123"
    mock_customer.has_payment_method = False
    mock_customer.stripe_customer_id = None

    mock_subscription = Mock()
    mock_subscription.id = "bsub_456"
    mock_subscription.billing_customer_user_id = "user_123"
    mock_subscription.resource_type = "repository"
    mock_subscription.subscription_metadata = {
      "resource_config": {"repository_name": "sec"}
    }
    mock_subscription.status = "pending_payment"

    def query_side_effect(model):
      if model == BillingSubscription:
        result = Mock()
        result.filter.return_value.first.return_value = mock_subscription
        return result
      elif model == BillingCustomer:
        result = Mock()
        result.filter.return_value.first.return_value = mock_customer
        return result
      return Mock()

    mock_db.query.side_effect = query_side_effect

    await handle_checkout_completed(session_data, mock_db)

    mock_task.delay.assert_called_once()
    call_args = mock_task.delay.call_args
    assert call_args[1]["user_id"] == "user_123"
    assert call_args[1]["repository_name"] == "sec"
    assert mock_subscription.status == "active"

  @pytest.mark.asyncio
  async def test_checkout_completed_subscription_not_found(self, mock_db, session_data):
    """Test handling when subscription is not found."""
    from robosystems.routers.admin.webhooks import handle_checkout_completed

    mock_db.query.return_value.filter.return_value.first.return_value = None

    await handle_checkout_completed(session_data, mock_db)


class TestPaymentSucceededHandler:
  """Tests for invoice.payment_succeeded handler."""

  @pytest.fixture
  def mock_db(self):
    """Create mock database session."""
    return Mock(spec=Session)

  @pytest.fixture
  def invoice_data(self):
    """Create test invoice data."""
    return {
      "id": "in_test123",
      "subscription": "sub_test456",
      "amount_paid": 2999,
      "status": "paid",
    }

  @pytest.mark.asyncio
  @patch("robosystems.tasks.graph_operations.provision_graph.provision_graph_task")
  async def test_payment_succeeded_activates_subscription(
    self, mock_task, mock_db, invoice_data
  ):
    """Test that payment success activates subscription."""
    from robosystems.routers.admin.webhooks import handle_payment_succeeded

    invoice_data["customer"] = "cus_test123"

    mock_customer = Mock()
    mock_customer.has_payment_method = False

    mock_subscription = Mock()
    mock_subscription.id = "bsub_123"
    mock_subscription.billing_customer_user_id = "user_123"
    mock_subscription.status = "pending_payment"
    mock_subscription.resource_type = "graph"
    mock_subscription.subscription_metadata = {"resource_config": {}}
    mock_subscription.stripe_subscription_id = "sub_test456"

    def query_side_effect(model):
      if model == BillingSubscription:
        result = Mock()
        result.filter.return_value.first.return_value = mock_subscription
        return result
      elif model == BillingCustomer:
        result = Mock()
        result.filter.return_value.first.return_value = mock_customer
        return result
      return Mock()

    mock_db.query.side_effect = query_side_effect

    await handle_payment_succeeded(invoice_data, mock_db)

    mock_task.delay.assert_called_once()
    assert mock_customer.has_payment_method is True
    mock_db.commit.assert_called()

  @pytest.mark.asyncio
  async def test_payment_succeeded_subscription_not_found(self, mock_db, invoice_data):
    """Test handling when subscription is not found."""
    from robosystems.routers.admin.webhooks import handle_payment_succeeded

    mock_db.query.return_value.filter.return_value.first.return_value = None

    await handle_payment_succeeded(invoice_data, mock_db)


class TestPaymentFailedHandler:
  """Tests for invoice.payment_failed handler."""

  @pytest.fixture
  def mock_db(self):
    """Create mock database session."""
    return Mock(spec=Session)

  @pytest.fixture
  def invoice_data(self):
    """Create test invoice data."""
    return {
      "id": "in_test789",
      "subscription": "sub_test012",
      "amount_due": 4999,
      "status": "uncollectible",
    }

  @pytest.mark.asyncio
  async def test_payment_failed_updates_subscription_status(
    self, mock_db, invoice_data
  ):
    """Test that payment failure updates subscription status."""
    from robosystems.routers.admin.webhooks import handle_payment_failed

    mock_subscription = Mock()
    mock_subscription.status = "pending_payment"
    mock_subscription.stripe_subscription_id = "sub_test012"
    mock_subscription.subscription_metadata = {}
    mock_db.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    await handle_payment_failed(invoice_data, mock_db)

    assert mock_subscription.status == "unpaid"
    mock_db.commit.assert_called()

  @pytest.mark.asyncio
  async def test_payment_failed_adds_error_to_metadata(self, mock_db, invoice_data):
    """Test that payment failure adds error to subscription metadata."""
    from robosystems.routers.admin.webhooks import handle_payment_failed

    mock_subscription = Mock()
    mock_subscription.status = "pending_payment"
    mock_subscription.stripe_subscription_id = "sub_test012"
    mock_subscription.subscription_metadata = {}
    mock_db.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    await handle_payment_failed(invoice_data, mock_db)

    assert "error" in mock_subscription.subscription_metadata

  @pytest.mark.asyncio
  async def test_payment_failed_subscription_not_found(self, mock_db, invoice_data):
    """Test handling when subscription is not found."""
    from robosystems.routers.admin.webhooks import handle_payment_failed

    mock_db.query.return_value.filter.return_value.first.return_value = None

    await handle_payment_failed(invoice_data, mock_db)


class TestSubscriptionUpdatedHandler:
  """Tests for customer.subscription.updated handler."""

  @pytest.fixture
  def mock_db(self):
    """Create mock database session."""
    return Mock(spec=Session)

  @pytest.fixture
  def subscription_data(self):
    """Create test subscription data."""
    return {
      "id": "sub_test123",
      "status": "active",
      "items": {"data": [{"price": {"id": "price_456"}}]},
    }

  @pytest.mark.asyncio
  async def test_subscription_updated_syncs_status(self, mock_db, subscription_data):
    """Test that subscription update syncs status."""
    from robosystems.routers.admin.webhooks import handle_subscription_updated

    mock_subscription = Mock()
    mock_subscription.status = "trialing"
    mock_subscription.stripe_subscription_id = "sub_test123"
    mock_db.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    await handle_subscription_updated(subscription_data, mock_db)

    assert mock_subscription.status == "active"
    mock_db.commit.assert_called()

  @pytest.mark.asyncio
  async def test_subscription_updated_handles_all_statuses(self, mock_db):
    """Test that subscription update handles all Stripe statuses."""
    from robosystems.routers.admin.webhooks import handle_subscription_updated

    status_mapping = {
      "active": "active",
      "trialing": "active",
      "past_due": "past_due",
      "canceled": "canceled",
      "unpaid": "unpaid",
    }

    for stripe_status, expected_status in status_mapping.items():
      mock_subscription = Mock()
      mock_subscription.status = "old_status"
      mock_subscription.stripe_subscription_id = "sub_test"
      mock_db.query.return_value.filter.return_value.first.return_value = (
        mock_subscription
      )

      subscription_data = {
        "id": "sub_test",
        "status": stripe_status,
        "items": {"data": [{"price": {"id": "price_123"}}]},
      }

      await handle_subscription_updated(subscription_data, mock_db)

      assert mock_subscription.status == expected_status

  @pytest.mark.asyncio
  async def test_subscription_updated_not_found(self, mock_db, subscription_data):
    """Test handling when subscription is not found."""
    from robosystems.routers.admin.webhooks import handle_subscription_updated

    mock_db.query.return_value.filter.return_value.first.return_value = None

    await handle_subscription_updated(subscription_data, mock_db)


class TestSubscriptionDeletedHandler:
  """Tests for customer.subscription.deleted handler."""

  @pytest.fixture
  def mock_db(self):
    """Create mock database session."""
    return Mock(spec=Session)

  @pytest.fixture
  def subscription_data(self):
    """Create test subscription data."""
    return {
      "id": "sub_test789",
      "status": "canceled",
      "canceled_at": 1234567890,
    }

  @pytest.mark.asyncio
  async def test_subscription_deleted_marks_canceled(self, mock_db, subscription_data):
    """Test that subscription deletion marks as canceled."""
    from robosystems.routers.admin.webhooks import handle_subscription_deleted

    mock_subscription = Mock()
    mock_subscription.status = "active"
    mock_subscription.stripe_subscription_id = "sub_test789"

    def cancel_subscription(db, immediate=False):
      mock_subscription.status = "canceled"

    mock_subscription.cancel = cancel_subscription

    mock_db.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    await handle_subscription_deleted(subscription_data, mock_db)

    assert mock_subscription.status == "canceled"

  @pytest.mark.asyncio
  async def test_subscription_deleted_not_found(self, mock_db, subscription_data):
    """Test handling when subscription is not found."""
    from robosystems.routers.admin.webhooks import handle_subscription_deleted

    mock_db.query.return_value.filter.return_value.first.return_value = None

    await handle_subscription_deleted(subscription_data, mock_db)
