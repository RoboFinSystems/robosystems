# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportOptionalMemberAccess=false
"""Unit tests for Stripe webhook handler functions in billing.py.

Tests the logic of each handler function in isolation using mocks,
without requiring a real database or Stripe connection.

NOTE on patching strategy:
  All billing models are imported *inside* each handler function body via:
    from robosystems.models.billing import BillingSubscription, ...
  This means the names are NOT attributes of the billing module, so we must
  patch at the source location (e.g., "robosystems.models.billing.BillingSubscription")
  rather than "robosystems.dagster.jobs.billing.BillingSubscription".
  Similarly, the SSE provisioning helpers are imported inside
  _trigger_resource_provisioning and must be patched at their source path.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Patch target constants (avoids repeating long strings in every test)
# ---------------------------------------------------------------------------

_BILLING = "robosystems.models.billing"
_IAM = "robosystems.models.iam"
_SSE = "robosystems.middleware.sse.direct_monitor"
_BILLING_JOBS = "robosystems.dagster.jobs.billing"

PATCH_BILLING_SUB = f"{_BILLING}.BillingSubscription"
PATCH_BILLING_CUST = f"{_BILLING}.BillingCustomer"
PATCH_BILLING_INV = f"{_BILLING}.BillingInvoice"
PATCH_BILLING_LINE_ITEM = f"{_BILLING}.BillingInvoiceLineItem"
PATCH_BILLING_AUDIT = f"{_BILLING}.BillingAuditLog"
PATCH_BILLING_EVENT_TYPE = f"{_BILLING}.BillingEventType"
PATCH_IAM_ORG_USER = f"{_IAM}.OrgUser"
PATCH_IAM_ORG_ROLE = f"{_IAM}.OrgRole"
PATCH_RUN_GRAPH = f"{_SSE}.run_graph_provisioning"
PATCH_RUN_REPO = f"{_SSE}.run_user_repository_provisioning"
PATCH_TRIGGER = f"{_BILLING_JOBS}._trigger_resource_provisioning"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_context():
  ctx = MagicMock()
  ctx.log = MagicMock()
  ctx.log.info = MagicMock()
  ctx.log.warning = MagicMock()
  ctx.log.error = MagicMock()
  return ctx


@pytest.fixture
def mock_db_session():
  session = MagicMock()
  session.query.return_value.filter.return_value.first.return_value = None
  session.commit = MagicMock()
  session.add = MagicMock()
  session.flush = MagicMock()
  return session


@pytest.fixture
def mock_subscription():
  sub = MagicMock()
  sub.id = "bsub_01ABC123"
  sub.org_id = "org_01ABC123"
  sub.status = "pending_payment"
  sub.resource_type = "graph"
  sub.resource_id = "kg_01ABC123"
  sub.plan_name = "ladybug-standard"
  sub.provider_subscription_id = "sub_stripe_abc"
  sub.stripe_subscription_id = None
  sub.subscription_metadata = {}
  sub.ends_at = None
  sub.canceled_at = None
  sub.current_period_start = None
  sub.current_period_end = None
  sub.cancel = MagicMock()
  return sub


@pytest.fixture
def mock_customer():
  customer = MagicMock()
  customer.org_id = "org_01ABC123"
  customer.stripe_customer_id = "cus_stripe_abc"
  customer.has_payment_method = False
  return customer


@pytest.fixture
def mock_invoice():
  inv = MagicMock()
  inv.id = "binv_01ABC123"
  inv.org_id = "org_01ABC123"
  inv.invoice_number = "INV-2026-02-0001"
  inv.stripe_invoice_id = "in_stripe_abc"
  inv.status = "open"
  inv.paid_at = None
  inv.payment_method = None
  inv.payment_reference = None
  inv.invoice_pdf = None
  inv.hosted_invoice_url = None
  inv.period_start = datetime(2026, 1, 1, tzinfo=UTC)
  inv.period_end = datetime(2026, 2, 1, tzinfo=UTC)
  return inv


# ---------------------------------------------------------------------------
# Stripe event data factories
# ---------------------------------------------------------------------------


def make_checkout_session(
  session_id="cs_session_abc",
  customer_id="cus_stripe_abc",
  payment_status="paid",
  subscription_id="sub_stripe_abc",
  metadata=None,
):
  return {
    "id": session_id,
    "customer": customer_id,
    "payment_status": payment_status,
    "subscription": subscription_id,
    "metadata": metadata or {},
  }


def make_invoice_data(
  invoice_id="in_stripe_abc",
  subscription_id="sub_stripe_abc",
  customer_id="cus_stripe_abc",
  status="open",
  number="INV-2026-02-0001",
  period_start=1700000000,
  period_end=1702592000,
  due_date=None,
  lines=None,
):
  return {
    "id": invoice_id,
    "subscription": subscription_id,
    "customer": customer_id,
    "status": status,
    "number": number,
    "period_start": period_start,
    "period_end": period_end,
    "due_date": due_date,
    "subtotal": 2999,
    "tax": 0,
    "total": 2999,
    "currency": "usd",
    "invoice_pdf": "https://invoice.stripe.com/inv.pdf",
    "hosted_invoice_url": "https://invoice.stripe.com/inv",
    "lines": {"data": lines or []},
  }


def make_charge_data(
  charge_id="ch_stripe_abc",
  invoice_id="in_stripe_abc",
  amount_refunded=1000,
):
  return {
    "id": charge_id,
    "invoice": invoice_id,
    "amount_refunded": amount_refunded,
  }


def make_subscription_data(
  subscription_id="sub_stripe_abc",
  status="active",
  cancel_at_period_end=False,
  period_start=1700000000,
  period_end=1702592000,
):
  return {
    "id": subscription_id,
    "status": status,
    "cancel_at_period_end": cancel_at_period_end,
    "current_period_start": period_start,
    "current_period_end": period_end,
  }


def make_setup_intent_data(customer_id="cus_stripe_abc"):
  return {"customer": customer_id}


# ---------------------------------------------------------------------------
# _handle_checkout_completed
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleCheckoutCompleted:
  async def test_happy_path_paid_checkout(
    self, mock_db_session, mock_context, mock_subscription, mock_customer
  ):
    """Payment collected: updates customer, sets provisioning, triggers provisioning."""
    session_data = make_checkout_session(
      session_id="cs_session_abc",
      subscription_id="sub_stripe_new",
    )

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
      patch(PATCH_TRIGGER, new_callable=AsyncMock) as mock_trigger,
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      # customer lookup
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_customer
      )

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    assert mock_customer.has_payment_method is True
    assert mock_subscription.stripe_subscription_id == "sub_stripe_new"
    assert mock_subscription.provider_subscription_id == "sub_stripe_new"
    assert mock_subscription.status == "provisioning"
    assert mock_subscription.provider_customer_id == "cus_stripe_abc"
    mock_db_session.commit.assert_called()
    mock_trigger.assert_awaited_once_with(
      mock_subscription, mock_db_session, mock_context
    )

  async def test_sets_stripe_customer_id_when_missing(
    self, mock_db_session, mock_context, mock_subscription, mock_customer
  ):
    """Sets stripe_customer_id on customer if it was not previously set."""
    mock_customer.stripe_customer_id = None
    session_data = make_checkout_session(customer_id="cus_new_123")

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
      patch(PATCH_TRIGGER, new_callable=AsyncMock),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_customer
      )

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    assert mock_customer.stripe_customer_id == "cus_new_123"

  async def test_preserves_checkout_session_id_in_metadata(
    self, mock_db_session, mock_context, mock_subscription, mock_customer
  ):
    """Preserves original checkout session ID in metadata before overwriting."""
    mock_subscription.provider_subscription_id = "cs_session_abc"
    mock_subscription.subscription_metadata = {"user_id": "user_123"}
    session_data = make_checkout_session(
      session_id="cs_session_abc",
      subscription_id="sub_stripe_new",
    )

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
      patch(PATCH_TRIGGER, new_callable=AsyncMock),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_customer
      )

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    assert mock_subscription.subscription_metadata.get("checkout_session_id") == (
      "cs_session_abc"
    )

  async def test_subscription_lookup_falls_back_to_metadata(
    self, mock_db_session, mock_context, mock_subscription, mock_customer
  ):
    """Falls back to metadata subscription_id lookup when session ID not found."""
    session_data = make_checkout_session(metadata={"subscription_id": "bsub_from_meta"})

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
      patch(PATCH_TRIGGER, new_callable=AsyncMock),
    ):
      MockSub.get_by_provider_subscription_id.return_value = None
      # First call is metadata lookup (BillingSubscription.id == ...)
      # Second call is customer lookup (BillingCustomer.org_id == ...)
      mock_db_session.query.return_value.filter.return_value.first.side_effect = [
        mock_subscription,
        mock_customer,
      ]

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    assert mock_subscription.status == "provisioning"

  async def test_subscription_not_found_logs_warning_and_returns(
    self, mock_db_session, mock_context
  ):
    """Returns early and logs warning when subscription cannot be found."""
    session_data = make_checkout_session(metadata={})

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
    ):
      MockSub.get_by_provider_subscription_id.return_value = None
      mock_db_session.query.return_value.filter.return_value.first.return_value = None

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    mock_context.log.warning.assert_called_once()
    mock_db_session.commit.assert_not_called()

  async def test_customer_not_found_logs_error_and_returns(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Returns early and logs error when customer cannot be found."""
    session_data = make_checkout_session()

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = None

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    mock_context.log.error.assert_called_once()
    mock_db_session.commit.assert_not_called()

  async def test_unpaid_checkout_logs_warning_no_provisioning(
    self, mock_db_session, mock_context, mock_subscription, mock_customer
  ):
    """Logs warning and skips provisioning when payment_status is not 'paid'."""
    session_data = make_checkout_session(payment_status="unpaid")

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
      patch(PATCH_TRIGGER, new_callable=AsyncMock) as mock_trigger,
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_customer
      )

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    mock_context.log.warning.assert_called_once()
    mock_trigger.assert_not_awaited()
    mock_db_session.commit.assert_not_called()

  async def test_no_stripe_subscription_id_skips_id_update(
    self, mock_db_session, mock_context, mock_subscription, mock_customer
  ):
    """Does not update stripe_subscription_id when checkout has no subscription."""
    session_data = make_checkout_session(subscription_id=None)
    original_stripe_sub_id = mock_subscription.stripe_subscription_id

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST),
      patch(PATCH_TRIGGER, new_callable=AsyncMock),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_customer
      )

      from robosystems.dagster.jobs.billing import _handle_checkout_completed

      await _handle_checkout_completed(session_data, mock_db_session, mock_context)

    assert mock_subscription.stripe_subscription_id == original_stripe_sub_id


# ---------------------------------------------------------------------------
# _handle_invoice_created
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleInvoiceCreated:
  async def test_happy_path_creates_invoice_with_line_items(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Creates BillingInvoice and associated line items from Stripe data."""
    lines = [
      {
        "description": "Ladybug Standard",
        "quantity": 1,
        "unit_amount_excluding_tax": 2999,
        "amount": 2999,
        "period": {"start": 1700000000, "end": 1702592000},
      }
    ]
    invoice_data = make_invoice_data(lines=lines)

    mock_invoice_instance = MagicMock()
    mock_invoice_instance.id = "binv_new"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_INV) as MockInvoice,
      patch(PATCH_BILLING_LINE_ITEM),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      # existing invoice check returns None (no duplicate)
      mock_db_session.query.return_value.filter.return_value.first.return_value = None
      MockInvoice.return_value = mock_invoice_instance

      from robosystems.dagster.jobs.billing import _handle_invoice_created

      await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    # invoice + 1 line item = 2 adds
    assert mock_db_session.add.call_count == 2
    mock_db_session.flush.assert_called_once()
    mock_db_session.commit.assert_called_once()
    mock_context.log.info.assert_called()

  async def test_no_subscription_id_raises_not_found(
    self, mock_db_session, mock_context
  ):
    """Raises SubscriptionNotFoundError when invoice has no subscription ID."""
    invoice_data = make_invoice_data(subscription_id=None)

    from robosystems.dagster.jobs.billing import (
      SubscriptionNotFoundError,
      _handle_invoice_created,
    )

    with pytest.raises(SubscriptionNotFoundError):
      await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    mock_db_session.add.assert_not_called()

  async def test_subscription_not_found_raises_error(
    self, mock_db_session, mock_context
  ):
    """Raises SubscriptionNotFoundError when subscription cannot be resolved."""
    invoice_data = make_invoice_data()

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(f"{_BILLING}.BillingCustomer") as MockCustomer,
    ):
      MockSub.get_by_provider_subscription_id.return_value = None
      MockSub.get_by_stripe_subscription_id.return_value = None
      MockCustomer.get_by_stripe_customer_id.return_value = None

      from robosystems.dagster.jobs.billing import (
        SubscriptionNotFoundError,
        _handle_invoice_created,
      )

      with pytest.raises(SubscriptionNotFoundError):
        await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    mock_db_session.add.assert_not_called()

  async def test_already_synced_invoice_skips_creation(
    self, mock_db_session, mock_context, mock_subscription, mock_invoice
  ):
    """Returns early when invoice already exists in database (idempotency)."""
    invoice_data = make_invoice_data()

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_INV),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_created

      await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    mock_db_session.add.assert_not_called()
    logged_msgs = [str(c) for c in mock_context.log.info.call_args_list]
    assert any("already synced" in msg for msg in logged_msgs)

  async def test_draft_invoice_without_number_uses_fallback(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Uses fallback invoice number format for drafts with no Stripe number yet."""
    invoice_data = make_invoice_data(number=None, status="draft")
    invoice_data["id"] = "in_abcdefgh"

    mock_invoice_instance = MagicMock()
    mock_invoice_instance.id = "binv_new"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_INV) as MockInvoice,
      patch(PATCH_BILLING_LINE_ITEM),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = None
      MockInvoice.return_value = mock_invoice_instance

      from robosystems.dagster.jobs.billing import _handle_invoice_created

      await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    call_kwargs = MockInvoice.call_args[1]
    assert call_kwargs["invoice_number"] == "STRIPE-abcdefgh"

  async def test_multiple_line_items_all_added(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """All Stripe line items are added to the database."""
    lines = [
      {
        "description": "Plan",
        "quantity": 1,
        "unit_amount_excluding_tax": 2999,
        "amount": 2999,
        "period": {"start": 1700000000, "end": 1702592000},
      },
      {
        "description": "Storage",
        "quantity": 10,
        "unit_amount_excluding_tax": 50,
        "amount": 500,
        "period": {"start": 1700000000, "end": 1702592000},
      },
    ]
    invoice_data = make_invoice_data(lines=lines)

    mock_invoice_instance = MagicMock()
    mock_invoice_instance.id = "binv_new"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_INV) as MockInvoice,
      patch(PATCH_BILLING_LINE_ITEM),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = None
      MockInvoice.return_value = mock_invoice_instance

      from robosystems.dagster.jobs.billing import _handle_invoice_created

      await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    # 1 invoice + 2 line items = 3 adds
    assert mock_db_session.add.call_count == 3

  async def test_line_item_falls_back_to_subscription_plan_name(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Uses subscription plan_name as description when line has no description."""
    mock_subscription.plan_name = "ladybug-standard"
    lines = [
      {
        "description": None,
        "quantity": 1,
        "unit_amount_excluding_tax": 2999,
        "amount": 2999,
        "period": {"start": 1700000000, "end": 1702592000},
      }
    ]
    invoice_data = make_invoice_data(lines=lines)

    mock_invoice_instance = MagicMock()
    mock_invoice_instance.id = "binv_new"
    captured_line_items: list[dict] = []

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_INV) as MockInvoice,
      patch(PATCH_BILLING_LINE_ITEM) as MockLineItem,
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = None
      MockInvoice.return_value = mock_invoice_instance

      def capture(**kwargs):
        captured_line_items.append(kwargs)
        return MagicMock()

      MockLineItem.side_effect = capture

      from robosystems.dagster.jobs.billing import _handle_invoice_created

      await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    assert len(captured_line_items) == 1
    assert captured_line_items[0]["description"] == "ladybug-standard"

  async def test_null_tax_defaults_to_zero(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Handles null tax field from Stripe gracefully."""
    invoice_data = make_invoice_data()
    invoice_data["tax"] = None

    mock_invoice_instance = MagicMock()
    mock_invoice_instance.id = "binv_new"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_INV) as MockInvoice,
      patch(PATCH_BILLING_LINE_ITEM),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      mock_db_session.query.return_value.filter.return_value.first.return_value = None
      MockInvoice.return_value = mock_invoice_instance

      from robosystems.dagster.jobs.billing import _handle_invoice_created

      await _handle_invoice_created(invoice_data, mock_db_session, mock_context)

    call_kwargs = MockInvoice.call_args[1]
    assert call_kwargs["tax_cents"] == 0


# ---------------------------------------------------------------------------
# _handle_payment_succeeded
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandlePaymentSucceeded:
  async def test_happy_path_marks_invoice_paid_and_triggers_provisioning(
    self, mock_db_session, mock_context, mock_subscription, mock_customer, mock_invoice
  ):
    """Marks invoice paid, updates customer, triggers provisioning."""
    invoice_data = make_invoice_data()
    mock_subscription.status = "pending_payment"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST) as MockCust,
      patch(PATCH_BILLING_INV),
      patch(PATCH_TRIGGER, new_callable=AsyncMock) as mock_trigger,
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      MockCust.get_by_stripe_customer_id.return_value = mock_customer
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_payment_succeeded

      await _handle_payment_succeeded(invoice_data, mock_db_session, mock_context)

    assert mock_customer.has_payment_method is True
    assert mock_invoice.status == "paid"
    assert mock_invoice.payment_method == "stripe"
    assert mock_invoice.payment_reference == "in_stripe_abc"
    assert mock_invoice.paid_at is not None
    mock_trigger.assert_awaited_once()

  async def test_provisioning_status_triggers_provisioning(
    self, mock_db_session, mock_context, mock_subscription, mock_customer, mock_invoice
  ):
    """Triggers provisioning when subscription is in 'provisioning' status."""
    invoice_data = make_invoice_data()
    mock_subscription.status = "provisioning"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST) as MockCust,
      patch(PATCH_BILLING_INV),
      patch(PATCH_TRIGGER, new_callable=AsyncMock) as mock_trigger,
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      MockCust.get_by_stripe_customer_id.return_value = mock_customer
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_payment_succeeded

      await _handle_payment_succeeded(invoice_data, mock_db_session, mock_context)

    mock_trigger.assert_awaited_once()

  async def test_active_subscription_does_not_trigger_provisioning(
    self, mock_db_session, mock_context, mock_subscription, mock_customer, mock_invoice
  ):
    """Does not trigger provisioning when subscription is already active."""
    invoice_data = make_invoice_data()
    mock_subscription.status = "active"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST) as MockCust,
      patch(PATCH_BILLING_INV),
      patch(PATCH_TRIGGER, new_callable=AsyncMock) as mock_trigger,
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      MockCust.get_by_stripe_customer_id.return_value = mock_customer
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_payment_succeeded

      await _handle_payment_succeeded(invoice_data, mock_db_session, mock_context)

    mock_trigger.assert_not_awaited()

  async def test_no_subscription_id_raises_not_found(
    self, mock_db_session, mock_context
  ):
    """Raises SubscriptionNotFoundError when invoice has no subscription ID."""
    invoice_data = make_invoice_data(subscription_id=None)

    from robosystems.dagster.jobs.billing import (
      SubscriptionNotFoundError,
      _handle_payment_succeeded,
    )

    with pytest.raises(SubscriptionNotFoundError):
      await _handle_payment_succeeded(invoice_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()

  async def test_missing_invoice_creates_from_payment_data(
    self, mock_db_session, mock_context, mock_subscription, mock_customer
  ):
    """Creates invoice from payment data when invoice not found (race condition)."""
    invoice_data = make_invoice_data()
    mock_subscription.status = "active"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST) as MockCust,
      patch(PATCH_BILLING_INV),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      MockCust.get_by_stripe_customer_id.return_value = mock_customer
      mock_db_session.query.return_value.filter.return_value.first.return_value = None

      from robosystems.dagster.jobs.billing import _handle_payment_succeeded

      await _handle_payment_succeeded(invoice_data, mock_db_session, mock_context)

    mock_context.log.info.assert_called()

  async def test_missing_customer_skips_payment_method_update(
    self, mock_db_session, mock_context, mock_subscription, mock_invoice
  ):
    """Skips customer payment method update when customer not found."""
    invoice_data = make_invoice_data()
    mock_subscription.status = "active"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST) as MockCust,
      patch(PATCH_BILLING_INV),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      MockCust.get_by_stripe_customer_id.return_value = None
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_payment_succeeded

      await _handle_payment_succeeded(invoice_data, mock_db_session, mock_context)

    # No error raised; invoice still updated
    assert mock_invoice.status == "paid"

  async def test_preserves_existing_pdf_url_if_not_provided(
    self, mock_db_session, mock_context, mock_subscription, mock_customer, mock_invoice
  ):
    """Keeps existing PDF URL when Stripe event does not provide one."""
    mock_invoice.invoice_pdf = "https://existing.pdf"
    invoice_data = make_invoice_data()
    invoice_data["invoice_pdf"] = None
    mock_subscription.status = "active"

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(PATCH_BILLING_CUST) as MockCust,
      patch(PATCH_BILLING_INV),
    ):
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription
      MockCust.get_by_stripe_customer_id.return_value = mock_customer
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_payment_succeeded

      await _handle_payment_succeeded(invoice_data, mock_db_session, mock_context)

    assert mock_invoice.invoice_pdf == "https://existing.pdf"


# ---------------------------------------------------------------------------
# _handle_payment_failed
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandlePaymentFailed:
  async def test_pending_payment_subscription_set_to_unpaid(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Sets subscription to 'unpaid' and records error in metadata."""
    mock_subscription.status = "pending_payment"
    mock_subscription.subscription_metadata = {}
    invoice_data = make_invoice_data()

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_payment_failed

      await _handle_payment_failed(invoice_data, mock_db_session, mock_context)

    assert mock_subscription.status == "unpaid"
    assert mock_subscription.subscription_metadata.get("error") == "Payment failed"
    mock_db_session.commit.assert_called_once()
    mock_context.log.warning.assert_called()

  async def test_non_pending_subscription_not_changed(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Does not change status when subscription is already active or unpaid."""
    mock_subscription.status = "active"
    invoice_data = make_invoice_data()

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_payment_failed

      await _handle_payment_failed(invoice_data, mock_db_session, mock_context)

    assert mock_subscription.status == "active"
    mock_db_session.commit.assert_not_called()

  async def test_no_subscription_id_raises_not_found(
    self, mock_db_session, mock_context
  ):
    """Raises SubscriptionNotFoundError when invoice has no subscription ID."""
    invoice_data = make_invoice_data(subscription_id=None)

    from robosystems.dagster.jobs.billing import (
      SubscriptionNotFoundError,
      _handle_payment_failed,
    )

    with pytest.raises(SubscriptionNotFoundError):
      await _handle_payment_failed(invoice_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()

  async def test_preserves_existing_metadata_keys(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Preserves existing metadata when adding error key."""
    mock_subscription.status = "pending_payment"
    mock_subscription.subscription_metadata = {"user_id": "user_abc", "tier": "large"}
    invoice_data = make_invoice_data()

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_payment_failed

      await _handle_payment_failed(invoice_data, mock_db_session, mock_context)

    assert mock_subscription.subscription_metadata.get("user_id") == "user_abc"
    assert mock_subscription.subscription_metadata.get("tier") == "large"
    assert mock_subscription.subscription_metadata.get("error") == "Payment failed"

  async def test_none_metadata_handled_gracefully(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Handles None subscription_metadata without raising TypeError."""
    mock_subscription.status = "pending_payment"
    mock_subscription.subscription_metadata = None
    invoice_data = make_invoice_data()

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_payment_failed

      await _handle_payment_failed(invoice_data, mock_db_session, mock_context)

    # dict(None or {}) is {} — metadata should be set with error key
    assert isinstance(mock_subscription.subscription_metadata, dict)
    assert mock_subscription.subscription_metadata.get("error") == "Payment failed"


# ---------------------------------------------------------------------------
# _handle_invoice_updated
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleInvoiceUpdated:
  async def test_updates_mutable_fields(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Updates status, PDF URL, and hosted URL on existing invoice."""
    invoice_data = {
      "id": "in_stripe_abc",
      "status": "paid",
      "invoice_pdf": "https://new.pdf",
      "hosted_invoice_url": "https://new.url",
      "number": None,
    }
    mock_invoice.status = "open"
    mock_invoice.invoice_number = "INV-2026-02-0001"

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_updated

      await _handle_invoice_updated(invoice_data, mock_db_session, mock_context)

    assert mock_invoice.status == "paid"
    assert mock_invoice.invoice_pdf == "https://new.pdf"
    assert mock_invoice.hosted_invoice_url == "https://new.url"
    mock_db_session.commit.assert_called_once()

  async def test_sets_paid_at_on_transition_to_paid(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Sets paid_at and payment_method when transitioning to 'paid' status."""
    invoice_data = {
      "id": "in_stripe_abc",
      "status": "paid",
      "invoice_pdf": None,
      "hosted_invoice_url": None,
      "number": None,
    }
    mock_invoice.status = "open"
    mock_invoice.invoice_number = "INV-2026-02-0001"
    mock_invoice.paid_at = None

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_updated

      await _handle_invoice_updated(invoice_data, mock_db_session, mock_context)

    assert mock_invoice.paid_at is not None
    assert mock_invoice.payment_method == "stripe"

  async def test_does_not_reset_paid_at_if_already_paid(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Does not overwrite paid_at when invoice is already paid."""
    original_paid_at = datetime(2026, 1, 15, tzinfo=UTC)
    mock_invoice.status = "paid"
    mock_invoice.paid_at = original_paid_at
    mock_invoice.invoice_number = "INV-2026-02-0001"

    invoice_data = {
      "id": "in_stripe_abc",
      "status": "paid",
      "invoice_pdf": None,
      "hosted_invoice_url": None,
      "number": None,
    }

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_updated

      await _handle_invoice_updated(invoice_data, mock_db_session, mock_context)

    # paid_at not reassigned since old_status == new_status == "paid"
    assert mock_invoice.paid_at == original_paid_at

  async def test_updates_invoice_number_on_draft_to_open_transition(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Replaces fallback invoice number when Stripe assigns a real one."""
    mock_invoice.invoice_number = "STRIPE-abcdefgh"
    mock_invoice.status = "draft"
    invoice_data = {
      "id": "in_abcdefgh",
      "status": "open",
      "number": "INV-2026-02-0001",
      "invoice_pdf": None,
      "hosted_invoice_url": None,
    }

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_updated

      await _handle_invoice_updated(invoice_data, mock_db_session, mock_context)

    assert mock_invoice.invoice_number == "INV-2026-02-0001"

  async def test_does_not_overwrite_real_invoice_number(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Does not replace a real invoice number even when Stripe provides a number."""
    mock_invoice.invoice_number = "INV-2026-02-0001"
    mock_invoice.status = "open"
    invoice_data = {
      "id": "in_stripe_abc",
      "status": "open",
      "number": "STRIPE-99999999",
      "invoice_pdf": None,
      "hosted_invoice_url": None,
    }

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_updated

      await _handle_invoice_updated(invoice_data, mock_db_session, mock_context)

    assert mock_invoice.invoice_number == "INV-2026-02-0001"

  async def test_invoice_not_found_logs_info_and_returns(
    self, mock_db_session, mock_context
  ):
    """Logs info and returns when invoice not found for update."""
    invoice_data = {
      "id": "in_unknown",
      "status": "paid",
      "invoice_pdf": None,
      "hosted_invoice_url": None,
      "number": None,
    }

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = None

      from robosystems.dagster.jobs.billing import _handle_invoice_updated

      await _handle_invoice_updated(invoice_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()
    mock_context.log.info.assert_called_once()

  async def test_preserves_existing_pdf_url_when_update_has_none(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Keeps existing PDF URL when update provides None."""
    mock_invoice.invoice_pdf = "https://existing.pdf"
    mock_invoice.invoice_number = "INV-2026-02-0001"
    mock_invoice.status = "open"
    invoice_data = {
      "id": "in_stripe_abc",
      "status": "open",
      "invoice_pdf": None,
      "hosted_invoice_url": None,
      "number": None,
    }

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_updated

      await _handle_invoice_updated(invoice_data, mock_db_session, mock_context)

    assert mock_invoice.invoice_pdf == "https://existing.pdf"


# ---------------------------------------------------------------------------
# _handle_invoice_voided
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleInvoiceVoided:
  async def test_sets_invoice_status_to_void(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Sets invoice status to 'void' and commits."""
    invoice_data = {"id": "in_stripe_abc"}

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_invoice_voided

      await _handle_invoice_voided(invoice_data, mock_db_session, mock_context)

    assert mock_invoice.status == "void"
    mock_db_session.commit.assert_called_once()
    mock_context.log.info.assert_called_once()

  async def test_invoice_not_found_logs_info_and_returns(
    self, mock_db_session, mock_context
  ):
    """Logs info when invoice not found and returns without committing."""
    invoice_data = {"id": "in_unknown"}

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = None

      from robosystems.dagster.jobs.billing import _handle_invoice_voided

      await _handle_invoice_voided(invoice_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()
    mock_context.log.info.assert_called_once()


# ---------------------------------------------------------------------------
# _handle_charge_refunded
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleChargeRefunded:
  async def test_adds_negative_line_item_and_recalculates(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Adds negative line item for refund and calls _recalculate_totals."""
    charge_data = make_charge_data(amount_refunded=1500)

    mock_line_item_instance = MagicMock()

    with (
      patch(PATCH_BILLING_INV),
      patch(PATCH_BILLING_LINE_ITEM) as MockLineItem,
      patch(PATCH_BILLING_AUDIT) as MockAudit,
      patch(PATCH_BILLING_EVENT_TYPE),
    ):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )
      MockLineItem.return_value = mock_line_item_instance

      from robosystems.dagster.jobs.billing import _handle_charge_refunded

      await _handle_charge_refunded(charge_data, mock_db_session, mock_context)

    mock_db_session.add.assert_called_once_with(mock_line_item_instance)
    mock_invoice._recalculate_totals.assert_called_once_with(mock_db_session)
    MockAudit.log_event.assert_called_once()
    mock_context.log.info.assert_called_once()

  async def test_negative_line_item_amount_equals_refund(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Line item unit_price_cents and amount_cents are negated refund amount."""
    charge_data = make_charge_data(amount_refunded=2500)
    captured_kwargs: dict = {}

    with (
      patch(PATCH_BILLING_INV),
      patch(PATCH_BILLING_LINE_ITEM) as MockLineItem,
      patch(PATCH_BILLING_AUDIT),
      patch(PATCH_BILLING_EVENT_TYPE),
    ):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      def capture(**kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

      MockLineItem.side_effect = capture

      from robosystems.dagster.jobs.billing import _handle_charge_refunded

      await _handle_charge_refunded(charge_data, mock_db_session, mock_context)

    assert captured_kwargs["unit_price_cents"] == -2500
    assert captured_kwargs["amount_cents"] == -2500

  async def test_no_invoice_id_on_charge_logs_info_and_returns(
    self, mock_db_session, mock_context
  ):
    """Returns early when charge has no associated invoice."""
    charge_data = {"id": "ch_no_invoice", "invoice": None, "amount_refunded": 1000}

    from robosystems.dagster.jobs.billing import _handle_charge_refunded

    await _handle_charge_refunded(charge_data, mock_db_session, mock_context)

    mock_db_session.add.assert_not_called()
    mock_context.log.info.assert_called_once()

  async def test_invoice_not_found_logs_warning_and_returns(
    self, mock_db_session, mock_context
  ):
    """Logs warning and returns when invoice not found for refunded charge."""
    charge_data = make_charge_data()

    with patch(PATCH_BILLING_INV):
      mock_db_session.query.return_value.filter.return_value.first.return_value = None

      from robosystems.dagster.jobs.billing import _handle_charge_refunded

      await _handle_charge_refunded(charge_data, mock_db_session, mock_context)

    mock_db_session.add.assert_not_called()
    mock_context.log.warning.assert_called_once()

  async def test_audit_log_contains_correct_event_data(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Audit log event_data includes charge ID, invoice ID, and refund amount."""
    charge_data = make_charge_data(
      charge_id="ch_test_123",
      invoice_id="in_stripe_abc",
      amount_refunded=3000,
    )

    with (
      patch(PATCH_BILLING_INV),
      patch(PATCH_BILLING_LINE_ITEM),
      patch(PATCH_BILLING_AUDIT) as MockAudit,
      patch(PATCH_BILLING_EVENT_TYPE),
    ):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      from robosystems.dagster.jobs.billing import _handle_charge_refunded

      await _handle_charge_refunded(charge_data, mock_db_session, mock_context)

    call_kwargs = MockAudit.log_event.call_args[1]
    assert call_kwargs["event_data"]["stripe_charge_id"] == "ch_test_123"
    assert call_kwargs["event_data"]["stripe_invoice_id"] == "in_stripe_abc"
    assert call_kwargs["event_data"]["amount_refunded_cents"] == 3000

  async def test_refund_line_item_uses_invoice_period(
    self, mock_db_session, mock_context, mock_invoice
  ):
    """Refund line item period_start/period_end inherit from the invoice."""
    charge_data = make_charge_data()
    captured_kwargs: dict = {}

    with (
      patch(PATCH_BILLING_INV),
      patch(PATCH_BILLING_LINE_ITEM) as MockLineItem,
      patch(PATCH_BILLING_AUDIT),
      patch(PATCH_BILLING_EVENT_TYPE),
    ):
      mock_db_session.query.return_value.filter.return_value.first.return_value = (
        mock_invoice
      )

      def capture(**kwargs):
        captured_kwargs.update(kwargs)
        return MagicMock()

      MockLineItem.side_effect = capture

      from robosystems.dagster.jobs.billing import _handle_charge_refunded

      await _handle_charge_refunded(charge_data, mock_db_session, mock_context)

    assert captured_kwargs["period_start"] == mock_invoice.period_start
    assert captured_kwargs["period_end"] == mock_invoice.period_end


# ---------------------------------------------------------------------------
# _handle_subscription_updated
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleSubscriptionUpdated:
  async def test_portal_cancellation_cancel_at_period_end(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Cancels subscription gracefully when cancel_at_period_end=True."""
    mock_subscription.status = "active"
    sub_data = make_subscription_data(status="active", cancel_at_period_end=True)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    mock_subscription.cancel.assert_called_once_with(mock_db_session, immediate=False)
    mock_context.log.info.assert_called()

  async def test_already_canceled_subscription_skips_cancel(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Does not call cancel() when subscription is already canceled."""
    mock_subscription.status = "canceled"
    sub_data = make_subscription_data(status="active", cancel_at_period_end=True)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    mock_subscription.cancel.assert_not_called()

  async def test_portal_reactivation_restores_active_status(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Reactivates subscription when cancel_at_period_end cleared on canceled sub."""
    future_ends_at = datetime.now(UTC) + timedelta(days=15)
    mock_subscription.status = "canceled"
    mock_subscription.ends_at = future_ends_at
    mock_subscription.resource_type = "other"  # Not a graph — simpler path
    sub_data = make_subscription_data(status="active", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    assert mock_subscription.status == "active"
    assert mock_subscription.canceled_at is None
    assert mock_subscription.ends_at is None
    mock_db_session.commit.assert_called()
    mock_context.log.info.assert_called()

  async def test_portal_reactivation_restores_suspended_graph(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Restores graph from SUSPENDED to ACTIVE on subscription reactivation."""
    future_ends_at = datetime.now(UTC) + timedelta(days=15)
    mock_subscription.status = "canceled"
    mock_subscription.ends_at = future_ends_at
    mock_subscription.resource_type = "graph"
    mock_subscription.resource_id = "kg_01ABC123"
    sub_data = make_subscription_data(status="active", cancel_at_period_end=False)

    mock_graph = MagicMock()

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch("robosystems.models.iam.graph.Graph.get_by_id", return_value=mock_graph),
      patch("robosystems.models.iam.graph.GraphStatus") as MockGraphStatus,
    ):
      from robosystems.models.iam.graph import GraphStatus

      mock_graph.status = GraphStatus.SUSPENDED.value
      MockGraphStatus.SUSPENDED = GraphStatus.SUSPENDED
      MockGraphStatus.ACTIVE = GraphStatus.ACTIVE
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    # Subscription should have been reactivated
    assert mock_subscription.status == "active"
    mock_graph.transition_status.assert_called_once()

  async def test_status_transition_past_due(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Maps Stripe 'past_due' to local 'past_due' and commits."""
    mock_subscription.status = "active"
    sub_data = make_subscription_data(status="past_due", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    assert mock_subscription.status == "past_due"
    mock_db_session.commit.assert_called()

  async def test_status_transition_incomplete_becomes_pending_payment(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Maps Stripe 'incomplete' to local 'pending_payment'."""
    mock_subscription.status = "active"
    sub_data = make_subscription_data(status="incomplete", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    assert mock_subscription.status == "pending_payment"

  async def test_status_transition_trialing_becomes_active(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Maps Stripe 'trialing' to local 'active'."""
    mock_subscription.status = "pending_payment"
    sub_data = make_subscription_data(status="trialing", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    assert mock_subscription.status == "active"

  async def test_status_transition_incomplete_expired_becomes_canceled(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Maps Stripe 'incomplete_expired' to local 'canceled' via cancel()."""
    mock_subscription.status = "pending_payment"
    sub_data = make_subscription_data(
      status="incomplete_expired", cancel_at_period_end=False
    )

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    mock_subscription.cancel.assert_called_once_with(mock_db_session, immediate=True)

  async def test_canceled_stripe_status_calls_immediate_cancel(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Calls cancel(immediate=True) when Stripe sends 'canceled' status."""
    mock_subscription.status = "active"
    sub_data = make_subscription_data(status="canceled", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    mock_subscription.cancel.assert_called_once_with(mock_db_session, immediate=True)

  async def test_same_status_still_commits_period_dates(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Commits period date sync even when subscription status is unchanged."""
    mock_subscription.status = "active"
    sub_data = make_subscription_data(status="active", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_called()

  async def test_syncs_period_dates_from_stripe(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Updates current_period_start and current_period_end from Stripe data."""
    mock_subscription.status = "active"
    sub_data = make_subscription_data(
      status="active",
      cancel_at_period_end=False,
      period_start=1700000000,
      period_end=1702592000,
    )

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    assert mock_subscription.current_period_start is not None
    assert mock_subscription.current_period_end is not None

  async def test_subscription_not_found_raises_error(
    self, mock_db_session, mock_context
  ):
    """Raises SubscriptionNotFoundError when subscription not found."""
    sub_data = make_subscription_data()

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(f"{_BILLING}.BillingCustomer") as MockCustomer,
    ):
      MockSub.get_by_provider_subscription_id.return_value = None
      MockSub.get_by_stripe_subscription_id.return_value = None
      MockCustomer.get_by_stripe_customer_id.return_value = None

      from robosystems.dagster.jobs.billing import (
        SubscriptionNotFoundError,
        _handle_subscription_updated,
      )

      with pytest.raises(SubscriptionNotFoundError):
        await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()

  async def test_reactivation_requires_future_ends_at(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Does not reactivate when ends_at is in the past (access already expired)."""
    past_ends_at = datetime.now(UTC) - timedelta(days=5)
    mock_subscription.status = "canceled"
    mock_subscription.ends_at = past_ends_at
    sub_data = make_subscription_data(status="active", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    # Should NOT have been reactivated (falls through to status mapping)
    # The status mapping "active" → "active" means no change, so cancel() never called
    mock_subscription.cancel.assert_not_called()

  async def test_reactivation_requires_ends_at_to_be_set(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Does not reactivate via portal path when ends_at is None."""
    mock_subscription.status = "canceled"
    mock_subscription.ends_at = None
    sub_data = make_subscription_data(status="active", cancel_at_period_end=False)

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_updated

      await _handle_subscription_updated(sub_data, mock_db_session, mock_context)

    # Falls through to status mapping; "canceled"→"canceled" so no change
    mock_subscription.cancel.assert_not_called()


# ---------------------------------------------------------------------------
# _handle_subscription_deleted
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleSubscriptionDeleted:
  async def test_already_canceled_terminates_access_immediately(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Sets ends_at to now for already-canceled subscriptions."""
    past_canceled_at = datetime.now(UTC) - timedelta(days=30)
    mock_subscription.status = "canceled"
    mock_subscription.canceled_at = past_canceled_at
    sub_data = {"id": "sub_stripe_abc"}

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_deleted

      await _handle_subscription_deleted(sub_data, mock_db_session, mock_context)

    # ends_at should now be approximately now (not None)
    assert mock_subscription.ends_at is not None
    mock_db_session.commit.assert_called_once()
    mock_context.log.info.assert_called_once()

  async def test_unexpected_deletion_calls_immediate_cancel(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Calls cancel(immediate=True) for non-canceled subscriptions."""
    mock_subscription.status = "active"
    sub_data = {"id": "sub_stripe_abc"}

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_deleted

      await _handle_subscription_deleted(sub_data, mock_db_session, mock_context)

    mock_subscription.cancel.assert_called_once_with(mock_db_session, immediate=True)
    mock_context.log.info.assert_called_once()

  async def test_pending_payment_subscription_immediate_cancel(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Handles deletion of pending_payment subscription via immediate cancel."""
    mock_subscription.status = "pending_payment"
    sub_data = {"id": "sub_stripe_abc"}

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_deleted

      await _handle_subscription_deleted(sub_data, mock_db_session, mock_context)

    mock_subscription.cancel.assert_called_once_with(mock_db_session, immediate=True)

  async def test_subscription_not_found_raises_error(
    self, mock_db_session, mock_context
  ):
    """Raises SubscriptionNotFoundError when subscription not found for deletion."""
    sub_data = {"id": "sub_unknown"}

    with (
      patch(PATCH_BILLING_SUB) as MockSub,
      patch(f"{_BILLING}.BillingCustomer") as MockCustomer,
    ):
      MockSub.get_by_provider_subscription_id.return_value = None
      MockSub.get_by_stripe_subscription_id.return_value = None
      MockCustomer.get_by_stripe_customer_id.return_value = None

      from robosystems.dagster.jobs.billing import (
        SubscriptionNotFoundError,
        _handle_subscription_deleted,
      )

      with pytest.raises(SubscriptionNotFoundError):
        await _handle_subscription_deleted(sub_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()

  async def test_provisioning_subscription_immediate_cancel(
    self, mock_db_session, mock_context, mock_subscription
  ):
    """Handles deletion of provisioning subscription via immediate cancel."""
    mock_subscription.status = "provisioning"
    sub_data = {"id": "sub_stripe_abc"}

    with patch(PATCH_BILLING_SUB) as MockSub:
      MockSub.get_by_provider_subscription_id.return_value = mock_subscription

      from robosystems.dagster.jobs.billing import _handle_subscription_deleted

      await _handle_subscription_deleted(sub_data, mock_db_session, mock_context)

    mock_subscription.cancel.assert_called_once_with(mock_db_session, immediate=True)


# ---------------------------------------------------------------------------
# _handle_setup_intent_succeeded
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestHandleSetupIntentSucceeded:
  async def test_marks_customer_as_having_payment_method(
    self, mock_db_session, mock_context, mock_customer
  ):
    """Sets has_payment_method=True and commits when customer found."""
    mock_customer.has_payment_method = False
    mock_customer.org_id = "org_01ABC123"
    intent_data = make_setup_intent_data()

    with patch(PATCH_BILLING_CUST) as MockCust:
      MockCust.get_by_stripe_customer_id.return_value = mock_customer

      from robosystems.dagster.jobs.billing import _handle_setup_intent_succeeded

      await _handle_setup_intent_succeeded(intent_data, mock_db_session, mock_context)

    assert mock_customer.has_payment_method is True
    mock_db_session.commit.assert_called_once()
    mock_context.log.info.assert_called()
    logged = [str(c) for c in mock_context.log.info.call_args_list]
    assert any("Marked" in msg for msg in logged)

  async def test_customer_already_has_payment_method_skips_commit(
    self, mock_db_session, mock_context, mock_customer
  ):
    """Does not commit when customer already has a payment method."""
    mock_customer.has_payment_method = True
    mock_customer.org_id = "org_01ABC123"
    intent_data = make_setup_intent_data()

    with patch(PATCH_BILLING_CUST) as MockCust:
      MockCust.get_by_stripe_customer_id.return_value = mock_customer

      from robosystems.dagster.jobs.billing import _handle_setup_intent_succeeded

      await _handle_setup_intent_succeeded(intent_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()
    logged = [str(c) for c in mock_context.log.info.call_args_list]
    assert any("already has" in msg for msg in logged)

  async def test_no_customer_id_logs_info_and_returns(
    self, mock_db_session, mock_context
  ):
    """Returns early when setup intent has no customer ID."""
    intent_data = {"customer": None}

    from robosystems.dagster.jobs.billing import _handle_setup_intent_succeeded

    await _handle_setup_intent_succeeded(intent_data, mock_db_session, mock_context)

    mock_db_session.commit.assert_not_called()
    mock_context.log.info.assert_called_once()

  async def test_customer_not_found_logs_warning(self, mock_db_session, mock_context):
    """Logs warning when customer not found for setup intent."""
    intent_data = make_setup_intent_data(customer_id="cus_unknown")

    with patch(PATCH_BILLING_CUST) as MockCust:
      MockCust.get_by_stripe_customer_id.return_value = None

      from robosystems.dagster.jobs.billing import _handle_setup_intent_succeeded

      await _handle_setup_intent_succeeded(intent_data, mock_db_session, mock_context)

    mock_context.log.warning.assert_called_once()
    mock_db_session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# _trigger_resource_provisioning
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
class TestTriggerResourceProvisioning:
  @pytest.fixture
  def graph_subscription(self):
    sub = MagicMock()
    sub.id = "bsub_graph_01"
    sub.org_id = "org_01ABC123"
    sub.resource_type = "graph"
    sub.resource_id = "kg_01ABC123"
    sub.plan_name = "ladybug-standard"
    sub.subscription_metadata = {"user_id": "user_01ABC123", "resource_config": {}}
    return sub

  @pytest.fixture
  def repository_subscription(self):
    sub = MagicMock()
    sub.id = "bsub_repo_01"
    sub.org_id = "org_01ABC123"
    sub.resource_type = "repository"
    sub.resource_id = None
    sub.plan_name = "starter"
    sub.subscription_metadata = {
      "user_id": "user_01ABC123",
      "resource_config": {"repository_name": "sec"},
    }
    return sub

  async def test_graph_provisioning_happy_path(
    self, mock_db_session, mock_context, graph_subscription
  ):
    """Calls run_graph_provisioning with correct args and logs success."""
    mock_result = {"graph_id": "kg_new_123"}

    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(
        PATCH_RUN_GRAPH, new_callable=AsyncMock, return_value=mock_result
      ) as mock_provision,
      patch(PATCH_RUN_REPO, new_callable=AsyncMock),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      await _trigger_resource_provisioning(
        graph_subscription, mock_db_session, mock_context
      )

    mock_provision.assert_awaited_once_with(
      operation_id=None,
      subscription_id=str(graph_subscription.id),
      user_id="user_01ABC123",
      tier="ladybug-standard",
    )
    mock_db_session.commit.assert_called()
    mock_context.log.info.assert_called()

  async def test_repository_provisioning_happy_path(
    self, mock_db_session, mock_context, repository_subscription
  ):
    """Calls run_user_repository_provisioning with correct args."""
    mock_result = {"repository_name": "sec"}

    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(PATCH_RUN_GRAPH, new_callable=AsyncMock),
      patch(
        PATCH_RUN_REPO, new_callable=AsyncMock, return_value=mock_result
      ) as mock_provision,
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      await _trigger_resource_provisioning(
        repository_subscription, mock_db_session, mock_context
      )

    mock_provision.assert_awaited_once_with(
      operation_id=None,
      subscription_id=str(repository_subscription.id),
      user_id="user_01ABC123",
      repository_name="sec",
    )
    mock_db_session.commit.assert_called()

  async def test_falls_back_to_org_owner_when_no_user_id(
    self, mock_db_session, mock_context, graph_subscription
  ):
    """Looks up org owner when user_id not in subscription_metadata."""
    graph_subscription.subscription_metadata = {"resource_config": {}}
    mock_owner = MagicMock()
    mock_owner.user_id = "owner_user_456"
    mock_db_session.query.return_value.filter.return_value.first.return_value = (
      mock_owner
    )

    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(
        PATCH_RUN_GRAPH, new_callable=AsyncMock, return_value={"graph_id": "kg_new"}
      ) as mock_provision,
      patch(PATCH_RUN_REPO, new_callable=AsyncMock),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      await _trigger_resource_provisioning(
        graph_subscription, mock_db_session, mock_context
      )

    mock_provision.assert_awaited_once_with(
      operation_id=None,
      subscription_id=str(graph_subscription.id),
      user_id="owner_user_456",
      tier="ladybug-standard",
    )

  async def test_no_org_owner_sets_failed_status(
    self, mock_db_session, mock_context, graph_subscription
  ):
    """Sets subscription status to 'failed' when no org owner found."""
    graph_subscription.subscription_metadata = {"resource_config": {}}
    mock_db_session.query.return_value.filter.return_value.first.return_value = None

    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      await _trigger_resource_provisioning(
        graph_subscription, mock_db_session, mock_context
      )

    assert graph_subscription.status == "failed"
    mock_context.log.error.assert_called_once()
    mock_db_session.commit.assert_called()

  async def test_unknown_resource_type_sets_failed_status(
    self, mock_db_session, mock_context
  ):
    """Sets subscription status to 'failed' for unknown resource types."""
    sub = MagicMock()
    sub.id = "bsub_unknown"
    sub.org_id = "org_01"
    sub.resource_type = "unknown_type"
    sub.subscription_metadata = {"user_id": "user_01", "resource_config": {}}

    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(PATCH_RUN_GRAPH, new_callable=AsyncMock),
      patch(PATCH_RUN_REPO, new_callable=AsyncMock),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      await _trigger_resource_provisioning(sub, mock_db_session, mock_context)

    assert sub.status == "failed"
    mock_context.log.error.assert_called()
    mock_db_session.commit.assert_called()

  async def test_graph_provisioning_failure_propagates_exception(
    self, mock_db_session, mock_context, graph_subscription
  ):
    """Re-raises exception when graph provisioning fails."""
    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(
        PATCH_RUN_GRAPH,
        new_callable=AsyncMock,
        side_effect=RuntimeError("Provisioning infrastructure down"),
      ),
      patch(PATCH_RUN_REPO, new_callable=AsyncMock),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      with pytest.raises(RuntimeError, match="Provisioning infrastructure down"):
        await _trigger_resource_provisioning(
          graph_subscription, mock_db_session, mock_context
        )

    mock_context.log.error.assert_called_once()

  async def test_repository_provisioning_failure_propagates_exception(
    self, mock_db_session, mock_context, repository_subscription
  ):
    """Re-raises exception when repository provisioning fails."""
    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(PATCH_RUN_GRAPH, new_callable=AsyncMock),
      patch(
        PATCH_RUN_REPO,
        new_callable=AsyncMock,
        side_effect=RuntimeError("Repository service unavailable"),
      ),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      with pytest.raises(RuntimeError, match="Repository service unavailable"):
        await _trigger_resource_provisioning(
          repository_subscription, mock_db_session, mock_context
        )

    mock_context.log.error.assert_called_once()

  async def test_graph_provisioning_sets_provisioning_status_before_calling(
    self, mock_db_session, mock_context, graph_subscription
  ):
    """Sets subscription status to 'provisioning' before calling provisioner."""
    graph_subscription.subscription_metadata = {
      "user_id": "user_01",
      "resource_config": {"tier": "large"},
    }
    status_at_provision_call = []

    async def capture_status(**kwargs):
      status_at_provision_call.append(graph_subscription.status)
      return {"graph_id": "kg_new"}

    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(PATCH_RUN_GRAPH, side_effect=capture_status),
      patch(PATCH_RUN_REPO, new_callable=AsyncMock),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      await _trigger_resource_provisioning(
        graph_subscription, mock_db_session, mock_context
      )

    assert len(status_at_provision_call) == 1
    assert status_at_provision_call[0] == "provisioning"

  async def test_repository_provisioning_sets_provisioning_status_before_calling(
    self, mock_db_session, mock_context, repository_subscription
  ):
    """Sets subscription status to 'provisioning' before calling provisioner."""
    status_at_provision_call = []

    async def capture_status(**kwargs):
      status_at_provision_call.append(repository_subscription.status)
      return {"repository_name": "sec"}

    with (
      patch(PATCH_IAM_ORG_USER),
      patch(PATCH_IAM_ORG_ROLE),
      patch(PATCH_RUN_GRAPH, new_callable=AsyncMock),
      patch(PATCH_RUN_REPO, side_effect=capture_status),
    ):
      from robosystems.dagster.jobs.billing import _trigger_resource_provisioning

      await _trigger_resource_provisioning(
        repository_subscription, mock_db_session, mock_context
      )

    assert len(status_at_provision_call) == 1
    assert status_at_provision_call[0] == "provisioning"
