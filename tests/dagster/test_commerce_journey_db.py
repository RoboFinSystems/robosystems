"""A repository subscription driven through whole Stripe lifecycles.

Against a real Postgres session, with the webhook handlers called in the
order Stripe delivers. Each step asserts what the member can reach, since
access reads the grant and every pass-4 commerce high was a later step
undoing an earlier one (SS2, SS3).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from robosystems.dagster.jobs.billing import (
  _as_utc,
  _handle_payment_failed,
  _handle_payment_succeeded,
  _handle_subscription_deleted,
  _handle_subscription_updated,
)
from robosystems.models.core.billing import BillingInvoice
from tests.dagster.test_billing_repository_lifecycle_db import (
  _event,
  _has_access,
  _resubscribe,
  _subscribed,
)

pytestmark = pytest.mark.unit


def _invoice(subscription, *, invoice_id: str, period_end: datetime) -> dict:
  start = period_end - timedelta(days=30)
  return {
    "id": invoice_id,
    "object": "invoice",
    "subscription": subscription.stripe_subscription_id,
    "status": "open",
    "billing_reason": "subscription_cycle",
    "amount_due": 4900,
    "amount_paid": 4900,
    "subtotal": 4900,
    "total": 4900,
    "period_start": int(start.timestamp()),
    "period_end": int(period_end.timestamp()),
    "lines": {
      "data": [
        {
          "amount": 4900,
          "quantity": 1,
          "period": {
            "start": int(start.timestamp()),
            "end": int(period_end.timestamp()),
          },
        }
      ]
    },
  }


def _invoices(session, invoice_id: str) -> list[BillingInvoice]:
  session.expire_all()
  return (
    session.query(BillingInvoice)
    .filter(BillingInvoice.stripe_invoice_id == invoice_id)
    .all()
  )


async def test_cancel_resubscribe_late_end_then_renewal(test_db, test_user, test_org):
  log = MagicMock()
  member, older, grant = _subscribed(test_db, test_user, test_org)

  await _handle_subscription_updated(
    _event(older, "active", cancel_at_period_end=True), test_db, log
  )
  test_db.refresh(older)
  test_db.refresh(grant)
  assert older.status == "canceled"
  assert grant.expires_at is not None
  assert _has_access(test_db, member)

  newer = _resubscribe(test_db, member, test_org)
  test_db.refresh(grant)
  assert grant.expires_at is None
  assert _has_access(test_db, member)

  past = datetime.now(UTC) - timedelta(minutes=1)
  await _handle_subscription_deleted(
    _event(older, "canceled", period_end=past), test_db, log
  )
  assert _has_access(test_db, member)

  next_end = datetime.now(UTC) + timedelta(days=50)
  renewal = f"in_test_{uuid4().hex[:10]}"
  await _handle_payment_succeeded(
    _invoice(newer, invoice_id=renewal, period_end=next_end), test_db, log
  )
  await _handle_subscription_updated(
    _event(newer, "active", period_end=next_end), test_db, log
  )

  test_db.refresh(newer)
  test_db.refresh(older)
  test_db.refresh(grant)
  assert newer.status == "active"
  assert newer.current_period_end is not None
  assert abs((_as_utc(newer.current_period_end) - next_end).total_seconds()) < 1
  assert older.status == "canceled"
  assert [invoice.status for invoice in _invoices(test_db, renewal)] == ["paid"]
  assert grant.expires_at is None
  assert grant.user_credits.is_active is True
  assert _has_access(test_db, member)


async def test_past_due_then_unpaid_then_paid(test_db, test_user, test_org):
  log = MagicMock()
  member, subscription, grant = _subscribed(test_db, test_user, test_org)
  renewal = _invoice(
    subscription,
    invoice_id=f"in_test_{uuid4().hex[:10]}",
    period_end=datetime.now(UTC) + timedelta(days=30),
  )

  await _handle_payment_failed(renewal, test_db, log)
  test_db.refresh(subscription)
  assert subscription.status == "past_due"
  assert _has_access(test_db, member)

  await _handle_subscription_updated(_event(subscription, "past_due"), test_db, log)
  assert _has_access(test_db, member)

  # Retries exhausted: a second failure notice changes nothing further, and
  # Stripe's unpaid suspends.
  await _handle_payment_failed(renewal, test_db, log)
  await _handle_subscription_updated(_event(subscription, "unpaid"), test_db, log)
  test_db.refresh(subscription)
  test_db.refresh(grant)
  assert subscription.status == "unpaid"
  assert grant.user_credits.is_active is False
  assert not _has_access(test_db, member)

  await _handle_payment_succeeded(renewal, test_db, log)
  await _handle_subscription_updated(_event(subscription, "active"), test_db, log)
  test_db.refresh(subscription)
  test_db.refresh(grant)
  assert subscription.status == "active"
  assert [invoice.status for invoice in _invoices(test_db, renewal["id"])] == ["paid"]
  assert grant.user_credits.is_active is True
  assert _has_access(test_db, member)
