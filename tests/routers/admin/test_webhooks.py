"""Comprehensive tests for Stripe webhook handlers.

The webhook endpoint queues events for Dagster processing.
Handler logic is tested in tests/dagster/jobs/test_billing.py.
"""

from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from main import app


class TestStripeWebhookEndpoint:
  """Tests for Stripe webhook endpoint.

  The endpoint validates webhooks and queues them for Dagster processing.
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

  @patch("robosystems.routers.admin.webhooks.run_and_monitor_dagster_job")
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_queues_dagster_job(
    self, mock_get_provider, mock_audit_log, mock_dagster_job, client, mock_db_session
  ):
    """Test that valid webhook events are queued for Dagster processing."""
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
      "message": "Webhook queued for processing",
    }
    # Verify idempotency check was called with correct event_id and source
    mock_audit_log.is_webhook_processed.assert_called_once()
    call_args = mock_audit_log.is_webhook_processed.call_args[0]
    assert call_args[0] == "evt_test123"
    assert call_args[1] == "stripe"

  @patch("robosystems.routers.admin.webhooks.run_and_monitor_dagster_job")
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_idempotency_check(
    self, mock_get_provider, mock_audit_log, mock_dagster_job, client, mock_db_session
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

  @patch("robosystems.routers.admin.webhooks.run_and_monitor_dagster_job")
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_payment_succeeded_queued(
    self, mock_get_provider, mock_audit_log, mock_dagster_job, client, mock_db_session
  ):
    """Test that invoice.payment_succeeded events are queued."""
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
    assert "queued" in response.json()["message"]

  @patch("robosystems.routers.admin.webhooks.run_and_monitor_dagster_job")
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_payment_failed_queued(
    self, mock_get_provider, mock_audit_log, mock_dagster_job, client, mock_db_session
  ):
    """Test that invoice.payment_failed events are queued."""
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
    assert "queued" in response.json()["message"]

  @patch("robosystems.routers.admin.webhooks.run_and_monitor_dagster_job")
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_subscription_updated_queued(
    self, mock_get_provider, mock_audit_log, mock_dagster_job, client, mock_db_session
  ):
    """Test that customer.subscription.updated events are queued."""
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
    assert "queued" in response.json()["message"]

  @patch("robosystems.routers.admin.webhooks.run_and_monitor_dagster_job")
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_subscription_deleted_queued(
    self, mock_get_provider, mock_audit_log, mock_dagster_job, client, mock_db_session
  ):
    """Test that customer.subscription.deleted events are queued."""
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
    assert "queued" in response.json()["message"]

  @patch("robosystems.routers.admin.webhooks.run_and_monitor_dagster_job")
  @patch("robosystems.routers.admin.webhooks.BillingAuditLog")
  @patch("robosystems.routers.admin.webhooks.get_payment_provider")
  def test_webhook_unhandled_event_type_still_queued(
    self, mock_get_provider, mock_audit_log, mock_dagster_job, client, mock_db_session
  ):
    """Test that unknown event types are still queued (Dagster handles filtering)."""
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
    assert "queued" in response.json()["message"]


class TestBuildStripeWebhookJobConfig:
  """Tests for build_stripe_webhook_job_config function."""

  def test_build_config_basic(self):
    """Test basic job config building."""
    from robosystems.dagster.jobs.billing import build_stripe_webhook_job_config

    config = build_stripe_webhook_job_config(
      event_id="evt_123",
      event_type="checkout.session.completed",
      event_data={"id": "cs_test"},
    )

    assert "ops" in config
    assert "process_stripe_webhook_event" in config["ops"]
    op_config = config["ops"]["process_stripe_webhook_event"]["config"]
    assert op_config["event_id"] == "evt_123"
    assert op_config["event_type"] == "checkout.session.completed"
    assert op_config["event_data"] == {"id": "cs_test"}

  def test_build_config_with_operation_id(self):
    """Test job config with operation_id for SSE tracking."""
    from robosystems.dagster.jobs.billing import build_stripe_webhook_job_config

    config = build_stripe_webhook_job_config(
      event_id="evt_456",
      event_type="invoice.payment_succeeded",
      event_data={"id": "in_test"},
      operation_id="op_tracking_123",
    )

    op_config = config["ops"]["process_stripe_webhook_event"]["config"]
    assert op_config["operation_id"] == "op_tracking_123"

  def test_build_config_no_operation_id(self):
    """Test job config without operation_id."""
    from robosystems.dagster.jobs.billing import build_stripe_webhook_job_config

    config = build_stripe_webhook_job_config(
      event_id="evt_789",
      event_type="invoice.payment_failed",
      event_data={"id": "in_fail"},
    )

    op_config = config["ops"]["process_stripe_webhook_event"]["config"]
    assert "operation_id" not in op_config


# NOTE: Stripe webhook event handler tests have been moved to tests/dagster/jobs/test_billing.py
# The handlers are now part of the Dagster job and tested there with proper Dagster test utilities.
