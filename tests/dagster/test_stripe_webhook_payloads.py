"""Tests using real Stripe webhook payloads from production audit logs.

These payloads are copied verbatim from billing_audit_logs.event_data
to verify that _extract_stripe_subscription_id and _resolve_subscription
work against actual Stripe API responses for both new subscriptions and
renewal cycles.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.dagster.jobs.billing import (
  SubscriptionNotFoundError,
  _extract_stripe_subscription_id,
  _resolve_subscription,
)

# ============================================================================
# Real payloads from production (trimmed to relevant fields)
# ============================================================================

# --- New subscription: invoice.created (subscription_create) ---
# Customer: cus_U7N0bqyU2634ii, Sub: sub_1T98GkReD8VoQizP4kyPVtC1
NEW_SUB_INVOICE_CREATED = {
  "id": "in_1T98GhReD8VoQizPQeSrAGIx",
  "object": "invoice",
  "customer": "cus_U7N0bqyU2634ii",
  "billing_reason": "subscription_create",
  "status": "paid",
  "total": 1450,
  "parent": {
    "type": "subscription_details",
    "subscription_details": {
      "metadata": {},
      "subscription": "sub_1T98GkReD8VoQizP4kyPVtC1",
    },
  },
  "lines": {
    "data": [
      {
        "id": "il_1T98GhReD8VoQizPYNXprsh6",
        "amount": 2900,
        "parent": {
          "type": "subscription_item_details",
          "subscription_item_details": {
            "subscription": "sub_1T98GkReD8VoQizP4kyPVtC1",
            "subscription_item": "si_U7N01fsv6Nunzn",
            "proration": False,
          },
        },
        "period": {"end": 1775758263, "start": 1773079863},
      }
    ],
    "object": "list",
  },
  "period_start": 1773079863,
  "period_end": 1773079863,
}

# --- New subscription: checkout.session.completed ---
NEW_SUB_CHECKOUT_COMPLETED = {
  "id": "cs_live_b1MtDRyr3sHdO417FldaLTudh94zSy0OkvEUAmIqksgoMrvV0pKXdtOmeg",
  "object": "checkout.session",
  "customer": "cus_U7N0bqyU2634ii",
  "subscription": "sub_1T98GkReD8VoQizP4kyPVtC1",
  "payment_status": "paid",
  "metadata": {
    "user_id": "user_01KK9VZZEKPNP7YKF0V2MZND9E",
    "resource_type": "repository",
    "subscription_id": "bsub_01KK9WP4KDFMFR7G579ABNME6N",
  },
}

# --- Renewal: invoice.created (subscription_cycle) ---
# Customer: cus_Txe6Gs8nXx6gMj, Sub: sub_1SzioUReD8VoQizPuBWqzco7
RENEWAL_INVOICE_CREATED = {
  "id": "in_1T9sBzReD8VoQizPj0uBNNqp",
  "object": "invoice",
  "customer": "cus_Txe6Gs8nXx6gMj",
  "billing_reason": "subscription_cycle",
  "status": "draft",
  "total": 9900,
  "parent": {
    "type": "subscription_details",
    "subscription_details": {
      "metadata": {},
      "subscription": "sub_1SzioUReD8VoQizPuBWqzco7",
    },
  },
  "lines": {
    "data": [
      {
        "id": "il_1T9s9nReD8VoQizPpCNtVidJ",
        "amount": 9900,
        "parent": {
          "type": "subscription_item_details",
          "subscription_item_details": {
            "subscription": "sub_1SzioUReD8VoQizPuBWqzco7",
            "subscription_item": "si_U75e0hmtrsttCv",
            "proration": False,
          },
        },
        "period": {"end": 1775934659, "start": 1773256259},
      }
    ],
    "object": "list",
  },
  "period_start": 1770837059,
  "period_end": 1773256259,
}

# --- Renewal: invoice.paid (subscription_cycle) ---
RENEWAL_INVOICE_PAID = {
  "id": "in_1T9sBzReD8VoQizPj0uBNNqp",
  "object": "invoice",
  "customer": "cus_Txe6Gs8nXx6gMj",
  "billing_reason": "subscription_cycle",
  "status": "paid",
  "total": 9900,
  "amount_paid": 9900,
  "parent": {
    "type": "subscription_details",
    "subscription_details": {
      "metadata": {},
      "subscription": "sub_1SzioUReD8VoQizPuBWqzco7",
    },
  },
  "lines": {
    "data": [
      {
        "id": "il_1T9s9nReD8VoQizPpCNtVidJ",
        "amount": 9900,
        "parent": {
          "type": "subscription_item_details",
          "subscription_item_details": {
            "subscription": "sub_1SzioUReD8VoQizPuBWqzco7",
            "subscription_item": "si_U75e0hmtrsttCv",
            "proration": False,
          },
        },
        "period": {"end": 1775934659, "start": 1773256259},
      }
    ],
    "object": "list",
  },
  "period_start": 1770837059,
  "period_end": 1773256259,
  "invoice_pdf": "https://pay.stripe.com/invoice/example/pdf",
  "hosted_invoice_url": "https://invoice.stripe.com/i/example",
}

# --- Renewal: customer.subscription.updated ---
# NOTE: No top-level current_period_start/end — only in items.data[]
RENEWAL_SUBSCRIPTION_UPDATED = {
  "id": "sub_1SzioUReD8VoQizPuBWqzco7",
  "object": "subscription",
  "status": "active",
  "customer": "cus_Txe6Gs8nXx6gMj",
  "cancel_at_period_end": False,
  "items": {
    "data": [
      {
        "id": "si_U75e0hmtrsttCv",
        "object": "subscription_item",
        "subscription": "sub_1SzioUReD8VoQizPuBWqzco7",
        "current_period_start": 1773256259,
        "current_period_end": 1775934659,
        "quantity": 1,
      }
    ],
    "object": "list",
  },
  "created": 1770837059,
  "billing_cycle_anchor": 1770837059,
}

# --- Legacy format: invoice with top-level subscription field ---
LEGACY_INVOICE = {
  "id": "in_legacy_example",
  "object": "invoice",
  "customer": "cus_legacy",
  "subscription": "sub_legacy_123",
  "billing_reason": "subscription_create",
  "status": "paid",
  "total": 2900,
  "lines": {
    "data": [
      {
        "id": "il_legacy",
        "amount": 2900,
        "subscription": "sub_legacy_123",
        "period": {"end": 1775758263, "start": 1773079863},
      }
    ],
  },
  "period_start": 1773079863,
  "period_end": 1773079863,
}


# ============================================================================
# Tests: _extract_stripe_subscription_id
# ============================================================================


class TestExtractStripeSubscriptionId:
  """Test subscription ID extraction from various Stripe payload formats."""

  def test_new_sub_invoice_created(self):
    """New subscription invoice — sub ID in parent.subscription_details."""
    result = _extract_stripe_subscription_id(NEW_SUB_INVOICE_CREATED)
    assert result == "sub_1T98GkReD8VoQizP4kyPVtC1"

  def test_checkout_session_completed(self):
    """Checkout session — sub ID at top level."""
    result = _extract_stripe_subscription_id(NEW_SUB_CHECKOUT_COMPLETED)
    assert result == "sub_1T98GkReD8VoQizP4kyPVtC1"

  def test_renewal_invoice_created(self):
    """Renewal invoice — sub ID in parent.subscription_details."""
    result = _extract_stripe_subscription_id(RENEWAL_INVOICE_CREATED)
    assert result == "sub_1SzioUReD8VoQizPuBWqzco7"

  def test_renewal_invoice_paid(self):
    """Renewal payment — sub ID in parent.subscription_details."""
    result = _extract_stripe_subscription_id(RENEWAL_INVOICE_PAID)
    assert result == "sub_1SzioUReD8VoQizPuBWqzco7"

  def test_subscription_updated_event(self):
    """Subscription updated — sub ID extracted from object's own id field."""
    result = _extract_stripe_subscription_id(RENEWAL_SUBSCRIPTION_UPDATED)
    assert result == "sub_1SzioUReD8VoQizPuBWqzco7"

  def test_subscription_object_with_empty_items(self):
    """Subscription object with no items still resolves via id field."""
    payload = {
      "id": "sub_empty_items",
      "object": "subscription",
      "customer": "cus_test",
      "status": "active",
      "items": {"data": [], "object": "list"},
    }
    result = _extract_stripe_subscription_id(payload)
    assert result == "sub_empty_items"

  def test_legacy_invoice_top_level_subscription(self):
    """Legacy format — sub ID at top level."""
    result = _extract_stripe_subscription_id(LEGACY_INVOICE)
    assert result == "sub_legacy_123"

  def test_legacy_invoice_line_item_subscription(self):
    """Legacy line item format — sub ID in line.subscription."""
    payload = {
      "id": "in_test",
      "customer": "cus_test",
      "lines": {
        "data": [{"subscription": "sub_from_line_item"}],
      },
    }
    result = _extract_stripe_subscription_id(payload)
    assert result == "sub_from_line_item"

  def test_empty_payload_returns_none(self):
    result = _extract_stripe_subscription_id({})
    assert result is None

  def test_no_subscription_anywhere(self):
    """Payload with no subscription reference anywhere."""
    payload = {
      "id": "in_orphan",
      "customer": "cus_test",
      "lines": {"data": [{"id": "il_1", "amount": 100}]},
    }
    result = _extract_stripe_subscription_id(payload)
    assert result is None

  def test_subscription_field_is_dict_not_string(self):
    """Some Stripe objects expand subscription to a full object."""
    payload = {"subscription": {"id": "sub_expanded", "status": "active"}}
    result = _extract_stripe_subscription_id(payload)
    # Should NOT return the dict — only strings
    assert result is None


# ============================================================================
# Tests: _resolve_subscription
# ============================================================================


class TestResolveSubscription:
  """Test subscription resolution with mocked DB lookups."""

  def _make_mock_subscription(self, sub_id="bsub_test", status="active"):
    sub = MagicMock()
    sub.id = sub_id
    sub.status = status
    sub.org_id = "org_test"
    return sub

  def _make_mock_customer(self, org_id="org_test"):
    customer = MagicMock()
    customer.org_id = org_id
    return customer

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_new_sub_invoice_resolves_by_provider_id(self, MockSub, MockCustomer):
    """New subscription invoice resolves via provider_subscription_id."""
    mock_sub = self._make_mock_subscription()
    MockSub.get_by_provider_subscription_id.return_value = mock_sub

    ctx = MagicMock()
    db = MagicMock()

    result = _resolve_subscription(NEW_SUB_INVOICE_CREATED, db, ctx)
    assert result == mock_sub
    MockSub.get_by_provider_subscription_id.assert_called_with(
      "sub_1T98GkReD8VoQizP4kyPVtC1", db
    )

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_renewal_invoice_resolves_by_provider_id(self, MockSub, MockCustomer):
    """Renewal invoice resolves via parent.subscription_details.subscription."""
    mock_sub = self._make_mock_subscription()
    MockSub.get_by_provider_subscription_id.return_value = mock_sub

    ctx = MagicMock()
    db = MagicMock()

    result = _resolve_subscription(RENEWAL_INVOICE_CREATED, db, ctx)
    assert result == mock_sub
    MockSub.get_by_provider_subscription_id.assert_called_with(
      "sub_1SzioUReD8VoQizPuBWqzco7", db
    )

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_renewal_payment_resolves_by_provider_id(self, MockSub, MockCustomer):
    """Renewal payment_succeeded resolves via parent.subscription_details."""
    mock_sub = self._make_mock_subscription()
    MockSub.get_by_provider_subscription_id.return_value = mock_sub

    ctx = MagicMock()
    db = MagicMock()

    result = _resolve_subscription(RENEWAL_INVOICE_PAID, db, ctx)
    assert result == mock_sub

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_subscription_updated_resolves(self, MockSub, MockCustomer):
    """Subscription updated event resolves (items.data[].subscription)."""
    mock_sub = self._make_mock_subscription()
    # First call (provider_subscription_id) might find it
    MockSub.get_by_provider_subscription_id.return_value = mock_sub

    ctx = MagicMock()
    db = MagicMock()

    result = _resolve_subscription(RENEWAL_SUBSCRIPTION_UPDATED, db, ctx)
    assert result == mock_sub

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_falls_back_to_stripe_subscription_id(self, MockSub, MockCustomer):
    """Falls back to stripe_subscription_id column when provider_id miss."""
    mock_sub = self._make_mock_subscription()
    MockSub.get_by_provider_subscription_id.return_value = None
    MockSub.get_by_stripe_subscription_id.return_value = mock_sub

    ctx = MagicMock()
    db = MagicMock()

    result = _resolve_subscription(RENEWAL_INVOICE_CREATED, db, ctx)
    assert result == mock_sub
    MockSub.get_by_stripe_subscription_id.assert_called_with(
      "sub_1SzioUReD8VoQizPuBWqzco7", db
    )

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_falls_back_to_customer_lookup(self, MockSub, MockCustomer):
    """Falls back to customer → org → subscription when sub ID lookup fails."""
    mock_sub = self._make_mock_subscription()
    mock_customer = self._make_mock_customer()

    MockSub.get_by_provider_subscription_id.return_value = None
    MockSub.get_by_stripe_subscription_id.return_value = None
    MockCustomer.get_by_stripe_customer_id.return_value = mock_customer

    ctx = MagicMock()
    db = MagicMock()

    # Mock the query chain
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.first.return_value = mock_sub
    db.query.return_value = mock_query

    result = _resolve_subscription(RENEWAL_INVOICE_CREATED, db, ctx)
    assert result == mock_sub
    MockCustomer.get_by_stripe_customer_id.assert_called_with("cus_Txe6Gs8nXx6gMj", db)

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_customer_fallback_no_recency_guard(self, MockSub, MockCustomer):
    """Customer fallback works for renewal (no 5-minute recency guard).

    This was the original bug — the old code had a 5-minute recency filter
    that excluded subscriptions created more than 5 minutes ago, which
    meant renewal invoices could never find their subscription.
    """
    mock_sub = self._make_mock_subscription()
    mock_customer = self._make_mock_customer()

    MockSub.get_by_provider_subscription_id.return_value = None
    MockSub.get_by_stripe_subscription_id.return_value = None
    MockCustomer.get_by_stripe_customer_id.return_value = mock_customer

    ctx = MagicMock()
    db = MagicMock()

    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.first.return_value = mock_sub
    db.query.return_value = mock_query

    result = _resolve_subscription(RENEWAL_INVOICE_PAID, db, ctx)
    assert result == mock_sub

    # Verify NO created_at filter was applied (no recency guard)
    filter_calls = mock_query.filter.call_args_list
    for call in filter_calls:
      for arg in call[0]:
        # Ensure no created_at comparison in the filter
        assert "created_at" not in str(arg)

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_raises_value_error_when_not_found(self, MockSub, MockCustomer):
    """Raises SubscriptionNotFoundError when subscription cannot be found."""
    MockSub.get_by_provider_subscription_id.return_value = None
    MockSub.get_by_stripe_subscription_id.return_value = None
    MockCustomer.get_by_stripe_customer_id.return_value = None

    ctx = MagicMock()
    db = MagicMock()

    with pytest.raises(SubscriptionNotFoundError, match="Subscription not found"):
      _resolve_subscription(RENEWAL_INVOICE_CREATED, db, ctx)

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_raises_when_customer_has_no_subscriptions(self, MockSub, MockCustomer):
    """Raises SubscriptionNotFoundError when customer exists but has no subs."""
    mock_customer = self._make_mock_customer()

    MockSub.get_by_provider_subscription_id.return_value = None
    MockSub.get_by_stripe_subscription_id.return_value = None
    MockCustomer.get_by_stripe_customer_id.return_value = mock_customer

    ctx = MagicMock()
    db = MagicMock()

    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.first.return_value = None
    db.query.return_value = mock_query

    with pytest.raises(SubscriptionNotFoundError, match="Subscription not found"):
      _resolve_subscription(RENEWAL_INVOICE_CREATED, db, ctx)

  @patch("robosystems.models.billing.BillingCustomer")
  @patch("robosystems.models.billing.BillingSubscription")
  def test_checkout_session_resolves_by_top_level_subscription(
    self, MockSub, MockCustomer
  ):
    """Checkout session has subscription at top level."""
    mock_sub = self._make_mock_subscription()
    MockSub.get_by_provider_subscription_id.return_value = mock_sub

    ctx = MagicMock()
    db = MagicMock()

    result = _resolve_subscription(NEW_SUB_CHECKOUT_COMPLETED, db, ctx)
    assert result == mock_sub
    MockSub.get_by_provider_subscription_id.assert_called_with(
      "sub_1T98GkReD8VoQizP4kyPVtC1", db
    )
