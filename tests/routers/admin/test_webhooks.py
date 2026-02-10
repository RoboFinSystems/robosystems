"""Comprehensive tests for Stripe webhook handlers.

The webhook endpoint validates events and processes them directly via background tasks.
Handler logic is tested in tests/dagster/jobs/test_billing.py.
"""

from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from main import app


class TestStripeWebhookEndpoint:
  """Tests for Stripe webhook endpoint.

  The endpoint validates webhooks and processes them directly as background tasks.
  """

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
  def test_webhook_accepted_for_processing(
    self, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test that valid webhook events are accepted for direct processing."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_test123",
      "type": "checkout.session.completed",
      "data": {"object": {"id": "cs_test", "customer": "cus_123"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert response.json() == {
      "status": "success",
      "message": "Webhook accepted for processing",
    }
    # Verify idempotency check was called with correct event_id and source
    mock_audit_log.is_webhook_processed.assert_called_once()
    call_args = mock_audit_log.is_webhook_processed.call_args[0]
    assert call_args[0] == "evt_test123"
    assert call_args[1] == "stripe"

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_idempotency_check(
    self, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test that already-processed webhooks are skipped."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_already_processed",
      "type": "checkout.session.completed",
      "data": {"object": {"id": "cs_test", "customer": "cus_123"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = True  # Already processed

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert response.json() == {
      "status": "success",
      "message": "Event already processed",
    }

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_payment_succeeded_accepted(
    self, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test that invoice.payment_succeeded events are accepted."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_test456",
      "type": "invoice.payment_succeeded",
      "data": {"object": {"id": "in_123", "subscription": "sub_456"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert "accepted" in response.json()["message"].lower()

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_payment_failed_accepted(
    self, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test that invoice.payment_failed events are accepted."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_test789",
      "type": "invoice.payment_failed",
      "data": {"object": {"id": "in_789", "subscription": "sub_123"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert "accepted" in response.json()["message"].lower()

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_subscription_updated_accepted(
    self, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test that customer.subscription.updated events are accepted."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_sub_update",
      "type": "customer.subscription.updated",
      "data": {"object": {"id": "sub_123", "status": "active"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert "accepted" in response.json()["message"].lower()

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_subscription_deleted_accepted(
    self, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test that customer.subscription.deleted events are accepted."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_sub_delete",
      "type": "customer.subscription.deleted",
      "data": {"object": {"id": "sub_123", "status": "canceled"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert "accepted" in response.json()["message"].lower()

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_unhandled_event_type_still_accepted(
    self, mock_get_provider, mock_audit_log, client, mock_db_session
  ):
    """Test that unknown event types are still accepted (handler logs and marks processed)."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_unknown",
      "type": "some.unknown.event",
      "data": {"object": {}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 200
    assert "accepted" in response.json()["message"].lower()
