"""Tests for invoice billing renewal Dagster job and sensor."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from dagster import build_sensor_context

from robosystems.dagster.jobs.invoice_billing import _renew_subscriptions
from robosystems.dagster.sensors.invoice_billing import (
  invoice_subscription_renewal_sensor,
)


class TestInvoiceSubscriptionRenewalSensor:
  """Tests for the invoice_subscription_renewal_sensor."""

  def test_finds_due_subscriptions(self):
    """Detects invoice-billed subscriptions past their period end."""
    mock_sub = MagicMock()
    mock_sub.id = "bsub_due1"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub
      ]

      context = build_sensor_context()
      runs = list(invoice_subscription_renewal_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["renew_invoice_subscriptions"]["config"]
      assert config["subscription_ids"] == ["bsub_due1"]

  def test_skips_when_no_matches(self):
    """Returns nothing when no subscriptions are due."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context()
      runs = list(invoice_subscription_renewal_sensor(context))

      assert len(runs) == 0

  def test_session_cleanup(self):
    """Database session is always cleaned up."""
    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []

      context = build_sensor_context()
      list(invoice_subscription_renewal_sensor(context))

      mock_db.remove.assert_called_once()

  def test_multiple_due_subscriptions(self):
    """Handles multiple subscriptions due for renewal."""
    mock_sub1 = MagicMock()
    mock_sub1.id = "bsub_due1"
    mock_sub2 = MagicMock()
    mock_sub2.id = "bsub_due2"

    with patch("robosystems.database.session") as mock_db:
      mock_session = MagicMock()
      mock_db.return_value = mock_session
      mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [
        mock_sub1,
        mock_sub2,
      ]

      context = build_sensor_context()
      runs = list(invoice_subscription_renewal_sensor(context))

      assert len(runs) == 1
      config = runs[0].run_config["ops"]["renew_invoice_subscriptions"]["config"]
      assert config["subscription_ids"] == ["bsub_due1", "bsub_due2"]


class TestRenewSubscriptions:
  """Tests for the _renew_subscriptions core logic."""

  def _make_mock_subscription(
    self,
    sub_id="bsub_test1",
    org_id="org_test1",
    active=True,
    stripe_sub_id=None,
    plan_name="ladybug-large",
    base_price_cents=9999,
  ):
    """Create a mock subscription."""
    sub = MagicMock()
    sub.id = sub_id
    sub.org_id = org_id
    sub.status = "active" if active else "canceled"
    sub.stripe_subscription_id = stripe_sub_id
    sub.plan_name = plan_name
    sub.base_price_cents = base_price_cents
    sub.resource_type = "graph"
    sub.resource_id = "kg_test123"
    sub.current_period_start = datetime(2025, 1, 1, tzinfo=UTC)
    sub.current_period_end = datetime(2025, 1, 31, tzinfo=UTC)
    sub.is_active.return_value = active
    return sub

  def _make_mock_customer(self, org_id="org_test1", invoice_billing=True):
    """Create a mock billing customer."""
    customer = MagicMock()
    customer.org_id = org_id
    customer.invoice_billing_enabled = invoice_billing
    customer.payment_terms = "net_30"
    return customer

  def test_renews_eligible_subscription(self):
    """Renews an active invoice-billed subscription."""
    mock_session = MagicMock()
    mock_sub = self._make_mock_subscription()
    mock_customer = self._make_mock_customer()
    mock_invoice = MagicMock()
    mock_invoice.id = "binv_test1"

    mock_session.query.return_value.filter.return_value.first.return_value = mock_sub
    log = logging.getLogger("test")

    with (
      patch(
        "robosystems.models.billing.customer.BillingCustomer.get_by_org_id",
        return_value=mock_customer,
      ),
      patch(
        "robosystems.operations.graph.subscription_service.generate_subscription_invoice",
        return_value=mock_invoice,
      ),
    ):
      result = _renew_subscriptions(["bsub_test1"], mock_session, log)

    assert result["renewed_count"] == 1
    assert result["skipped_count"] == 0
    assert result["errors"] == []
    mock_sub.renew_period.assert_called_once_with(mock_session)

  def test_skips_not_found_subscription(self):
    """Skips subscription that no longer exists."""
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = None
    log = logging.getLogger("test")

    result = _renew_subscriptions(["bsub_gone"], mock_session, log)

    assert result["renewed_count"] == 0
    assert result["skipped_count"] == 1

  def test_skips_inactive_subscription(self):
    """Skips subscription that is no longer active."""
    mock_session = MagicMock()
    mock_sub = self._make_mock_subscription(active=False)
    mock_customer = self._make_mock_customer()

    mock_session.query.return_value.filter.return_value.first.return_value = mock_sub
    log = logging.getLogger("test")

    with patch(
      "robosystems.models.billing.customer.BillingCustomer.get_by_org_id",
      return_value=mock_customer,
    ):
      result = _renew_subscriptions(["bsub_test1"], mock_session, log)

    assert result["renewed_count"] == 0
    assert result["skipped_count"] == 1

  def test_skips_stripe_managed_subscription(self):
    """Skips subscription that has a Stripe subscription ID."""
    mock_session = MagicMock()
    mock_sub = self._make_mock_subscription(stripe_sub_id="sub_stripe123")
    mock_customer = self._make_mock_customer()

    mock_session.query.return_value.filter.return_value.first.return_value = mock_sub
    log = logging.getLogger("test")

    with patch(
      "robosystems.models.billing.customer.BillingCustomer.get_by_org_id",
      return_value=mock_customer,
    ):
      result = _renew_subscriptions(["bsub_test1"], mock_session, log)

    assert result["renewed_count"] == 0
    assert result["skipped_count"] == 1

  def test_skips_non_invoice_billing_customer(self):
    """Skips subscription whose customer does not have invoice billing enabled."""
    mock_session = MagicMock()
    mock_sub = self._make_mock_subscription()
    mock_customer = self._make_mock_customer(invoice_billing=False)

    mock_session.query.return_value.filter.return_value.first.return_value = mock_sub
    log = logging.getLogger("test")

    with patch(
      "robosystems.models.billing.customer.BillingCustomer.get_by_org_id",
      return_value=mock_customer,
    ):
      result = _renew_subscriptions(["bsub_test1"], mock_session, log)

    assert result["renewed_count"] == 0
    assert result["skipped_count"] == 1

  def test_skips_missing_customer(self):
    """Skips subscription with no billing customer record."""
    mock_session = MagicMock()
    mock_sub = self._make_mock_subscription()

    mock_session.query.return_value.filter.return_value.first.return_value = mock_sub
    log = logging.getLogger("test")

    with patch(
      "robosystems.models.billing.customer.BillingCustomer.get_by_org_id",
      return_value=None,
    ):
      result = _renew_subscriptions(["bsub_test1"], mock_session, log)

    assert result["renewed_count"] == 0
    assert result["skipped_count"] == 1

  def test_handles_exception_gracefully(self):
    """Records errors instead of crashing on individual subscription failure."""
    mock_session = MagicMock()
    mock_sub = self._make_mock_subscription()
    mock_customer = self._make_mock_customer()

    mock_session.query.return_value.filter.return_value.first.return_value = mock_sub
    mock_sub.renew_period.side_effect = RuntimeError("DB connection lost")
    log = logging.getLogger("test")

    with patch(
      "robosystems.models.billing.customer.BillingCustomer.get_by_org_id",
      return_value=mock_customer,
    ):
      result = _renew_subscriptions(["bsub_test1"], mock_session, log)

    assert result["renewed_count"] == 0
    assert len(result["errors"]) == 1
    assert "bsub_test1" in result["errors"][0]
