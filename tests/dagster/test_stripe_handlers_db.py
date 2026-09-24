"""Stripe webhook handlers against a real Postgres session and current payload
shapes (API version 2026-01-28.clover). Identifiers are fictional; the field
layout is copied from live objects."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from robosystems.dagster.jobs.billing import (
  _handle_charge_refunded,
  _handle_invoice_updated,
  _handle_subscription_updated,
)
from robosystems.models.core import Org, OrgType
from robosystems.models.core.billing import (
  BillingInvoice,
  BillingInvoiceLineItem,
  BillingSubscription,
)


def refunded_charge(charge_id: str, payment_intent: str, amount_refunded: int):
  """A `charge.refunded` object as current API versions send it: no
  `invoice` field, only `payment_intent`."""
  return {
    "id": charge_id,
    "object": "charge",
    "amount": 9900,
    "amount_captured": 9900,
    "amount_refunded": amount_refunded,
    "captured": True,
    "currency": "usd",
    "customer": "cus_test_refund",
    "description": "Subscription creation",
    "paid": True,
    "payment_intent": payment_intent,
    "refunded": amount_refunded >= 9900,
    "status": "succeeded",
  }


def invoice_payments_list(invoice_id: str, payment_intent: str):
  """`GET /v1/invoice_payments?payment[payment_intent]=…` as returned live."""
  return {
    "object": "list",
    "data": [
      {
        "id": "inpay_test_1",
        "object": "invoice_payment",
        "amount_paid": 9900,
        "amount_requested": 9900,
        "currency": "usd",
        "invoice": invoice_id,
        "is_default": True,
        "payment": {"payment_intent": payment_intent, "type": "payment_intent"},
        "status": "paid",
      }
    ],
    "has_more": False,
    "url": "/v1/invoice_payments",
  }


def _org(session):
  uid = uuid.uuid4().hex[:8]
  org = Org(id=f"stripe_org_{uid}", name=f"Stripe {uid}", org_type=OrgType.PERSONAL)
  session.add(org)
  session.commit()
  return org


def _paid_invoice(session, org_id: str, stripe_invoice_id: str) -> BillingInvoice:
  now = datetime.now(UTC)
  invoice = BillingInvoice.create_invoice(
    org_id=org_id,
    period_start=now - timedelta(days=30),
    period_end=now,
    session=session,
  )
  invoice.stripe_invoice_id = stripe_invoice_id
  invoice.add_line_item(
    subscription_id=None,
    resource_type="graph",
    resource_id="kgtest",
    description="Standard",
    amount_cents=9900,
    session=session,
  )
  invoice.status = "paid"
  session.commit()
  return invoice


@pytest.mark.unit
class TestChargeRefunded:
  @pytest.mark.asyncio
  async def test_a_refund_on_a_current_charge_reaches_its_invoice(self, test_db):
    org = _org(test_db)
    stripe_invoice = f"in_test_{uuid.uuid4().hex[:10]}"
    invoice = _paid_invoice(test_db, org.id, stripe_invoice)
    payment_intent = f"pi_test_{uuid.uuid4().hex[:10]}"

    with patch(
      "stripe.StripeClient.raw_request",
      return_value=invoice_payments_list(stripe_invoice, payment_intent),
    ):
      await _handle_charge_refunded(
        refunded_charge("ch_test_full", payment_intent, 9900), test_db, MagicMock()
      )

    test_db.expire_all()
    assert test_db.get(BillingInvoice, invoice.id).total_cents == 0

  @pytest.mark.asyncio
  async def test_partial_refunds_record_the_running_total_once(self, test_db):
    org = _org(test_db)
    stripe_invoice = f"in_test_{uuid.uuid4().hex[:10]}"
    invoice = _paid_invoice(test_db, org.id, stripe_invoice)
    payment_intent = f"pi_test_{uuid.uuid4().hex[:10]}"

    with patch(
      "stripe.StripeClient.raw_request",
      return_value=invoice_payments_list(stripe_invoice, payment_intent),
    ):
      for cumulative in (3000, 5000, 5000):
        await _handle_charge_refunded(
          refunded_charge("ch_test_partial", payment_intent, cumulative),
          test_db,
          MagicMock(),
        )

    test_db.expire_all()
    refunds = (
      test_db.query(BillingInvoiceLineItem)
      .filter(
        BillingInvoiceLineItem.invoice_id == invoice.id,
        BillingInvoiceLineItem.resource_type == "refund",
      )
      .all()
    )
    assert [r.amount_cents for r in refunds] == [-5000]
    assert test_db.get(BillingInvoice, invoice.id).total_cents == 4900


def _subscription(session, status: str, **fields) -> BillingSubscription:
  org = _org(session)
  sub = BillingSubscription(
    org_id=org.id,
    resource_type="graph",
    resource_id=f"kg{uuid.uuid4().hex[:16]}",
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status=status,
    stripe_subscription_id=f"sub_test_{uuid.uuid4().hex[:10]}",
    subscription_metadata={},
    **fields,
  )
  session.add(sub)
  session.commit()
  return sub


def subscription_updated(sub: BillingSubscription, status: str):
  now = int(datetime.now(UTC).timestamp())
  return {
    "id": sub.stripe_subscription_id,
    "object": "subscription",
    "status": status,
    "cancel_at_period_end": False,
    "metadata": {},
    "items": {
      "object": "list",
      "data": [
        {
          "id": "si_test_1",
          "current_period_start": now,
          "current_period_end": now + 30 * 86400,
        }
      ],
    },
  }


@pytest.mark.unit
class TestTerminalStatesStayTerminal:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("stripe_status", ["active", "past_due", "trialing"])
  async def test_a_late_event_does_not_revive_a_failed_subscription(
    self, test_db, stripe_status
  ):
    sub = _subscription(test_db, "failed", ends_at=datetime.now(UTC))

    await _handle_subscription_updated(
      subscription_updated(sub, stripe_status), test_db, MagicMock()
    )

    test_db.expire_all()
    assert test_db.get(BillingSubscription, sub.id).status == "failed"

  @pytest.mark.asyncio
  async def test_a_late_event_does_not_revive_an_ended_cancellation(self, test_db):
    sub = _subscription(
      test_db,
      "canceled",
      canceled_at=datetime.now(UTC) - timedelta(days=2),
      ends_at=datetime.now(UTC) - timedelta(days=1),
    )

    await _handle_subscription_updated(
      subscription_updated(sub, "active"), test_db, MagicMock()
    )

    test_db.expire_all()
    assert test_db.get(BillingSubscription, sub.id).status == "canceled"

  @pytest.mark.asyncio
  async def test_a_paid_invoice_does_not_reopen(self, test_db):
    org = _org(test_db)
    stripe_invoice = f"in_test_{uuid.uuid4().hex[:10]}"
    invoice = _paid_invoice(test_db, org.id, stripe_invoice)

    await _handle_invoice_updated(
      {"id": stripe_invoice, "object": "invoice", "status": "open"},
      test_db,
      MagicMock(),
    )

    test_db.expire_all()
    assert test_db.get(BillingInvoice, invoice.id).status == "paid"


@pytest.mark.unit
class TestPortalReactivation:
  @pytest.mark.asyncio
  async def test_removing_a_pending_cancellation_reactivates(self, test_db):
    sub = _subscription(
      test_db,
      "canceled",
      canceled_at=datetime.now(UTC),
      ends_at=datetime.now(UTC) + timedelta(days=10),
    )

    await _handle_subscription_updated(
      subscription_updated(sub, "active"), test_db, MagicMock()
    )

    test_db.expire_all()
    fresh = test_db.get(BillingSubscription, sub.id)
    assert fresh.status == "active"
    assert fresh.ends_at is None
