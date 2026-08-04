"""Comprehensive tests for Stripe webhook handlers.

The webhook endpoint validates events and processes them before responding, so
the HTTP status reflects the processing outcome — Stripe only redelivers on a
non-2xx response. Handler logic is tested in tests/dagster/jobs/test_billing.py.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from main import app
from robosystems.dagster.jobs.billing import SubscriptionNotFoundError


class TestStripeWebhookEndpoint:
  """Tests for Stripe webhook endpoint.

  The endpoint validates webhooks, processes them synchronously, and answers
  with a status Stripe can act on (2xx = done, non-2xx = redeliver).
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

  @pytest.fixture
  def mock_processing_session(self):
    """Mock the session `_process_webhook_event` opens for handler work."""
    with patch("robosystems.routers.admin.webhooks.SessionFactory") as factory:
      session = Mock(spec=Session)
      factory.return_value = session
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

  @patch(
    "robosystems.dagster.jobs.billing._handle_checkout_completed",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_processed_and_marked(
    self,
    mock_get_provider,
    mock_audit_log,
    mock_handler,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """A valid event is dispatched to its handler and marked processed."""
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
      "message": "Webhook processed",
    }
    mock_handler.assert_awaited_once()
    # Verify idempotency check was called with correct event_id and source
    mock_audit_log.is_webhook_processed.assert_called_once()
    call_args = mock_audit_log.is_webhook_processed.call_args[0]
    assert call_args[0] == "evt_test123"
    assert call_args[1] == "stripe"
    # Marked processed only after the handler succeeded
    mock_audit_log.mark_webhook_processed.assert_called_once()
    assert mock_audit_log.mark_webhook_processed.call_args[0][0] == "evt_test123"

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

  @patch(
    "robosystems.dagster.jobs.billing._handle_payment_succeeded",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_payment_succeeded_dispatched(
    self,
    mock_get_provider,
    mock_audit_log,
    mock_handler,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """Test that invoice.payment_succeeded events reach their handler."""
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
    mock_handler.assert_awaited_once()
    mock_audit_log.mark_webhook_processed.assert_called_once()

  @patch(
    "robosystems.dagster.jobs.billing._handle_payment_failed",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_payment_failed_dispatched(
    self,
    mock_get_provider,
    mock_audit_log,
    mock_handler,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """Test that invoice.payment_failed events reach their handler."""
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
    mock_handler.assert_awaited_once()

  @patch(
    "robosystems.dagster.jobs.billing._handle_subscription_updated",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_subscription_updated_dispatched(
    self,
    mock_get_provider,
    mock_audit_log,
    mock_handler,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """Test that customer.subscription.updated events reach their handler."""
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
    mock_handler.assert_awaited_once()

  @patch(
    "robosystems.dagster.jobs.billing._handle_subscription_deleted",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_subscription_deleted_dispatched(
    self,
    mock_get_provider,
    mock_audit_log,
    mock_handler,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """Test that customer.subscription.deleted events reach their handler."""
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
    mock_handler.assert_awaited_once()

  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_unhandled_event_type_still_accepted(
    self,
    mock_get_provider,
    mock_audit_log,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """Unknown event types are acknowledged and marked so Stripe stops sending."""
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
    mock_audit_log.mark_webhook_processed.assert_called_once()

  @patch(
    "robosystems.dagster.jobs.billing._handle_payment_succeeded",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_unresolvable_subscription_returns_409_and_leaves_event_unmarked(
    self,
    mock_get_provider,
    mock_audit_log,
    mock_handler,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """Stripe redelivers only on a non-2xx, so a subscription we cannot
    resolve must surface as one — a 200 here means the event is dropped
    forever, since there is no reconciliation job behind the webhook."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_race",
      "type": "invoice.payment_succeeded",
      "data": {"object": {"id": "in_123", "subscription": "sub_unknown"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False
    mock_handler.side_effect = SubscriptionNotFoundError("sub_unknown not found")

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 409
    mock_audit_log.mark_webhook_processed.assert_not_called()

  @patch(
    "robosystems.dagster.jobs.billing._handle_payment_succeeded",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_handler_failure_returns_500_and_leaves_event_unmarked(
    self,
    mock_get_provider,
    mock_audit_log,
    mock_handler,
    client,
    mock_db_session,
    mock_processing_session,
  ):
    """A handler crash must not acknowledge the event; Stripe redelivers."""
    mock_provider = Mock()
    mock_event = {
      "id": "evt_crash",
      "type": "invoice.payment_succeeded",
      "data": {"object": {"id": "in_123", "subscription": "sub_456"}},
    }
    mock_provider.verify_webhook.return_value = mock_event
    mock_get_provider.return_value = mock_provider
    mock_audit_log.is_webhook_processed.return_value = False
    mock_handler.side_effect = RuntimeError("boom")

    response = client.post(
      "/admin/v1/webhooks/stripe",
      json=mock_event,
      headers={"stripe-signature": "valid_signature"},
    )

    assert response.status_code == 500
    mock_audit_log.mark_webhook_processed.assert_not_called()
