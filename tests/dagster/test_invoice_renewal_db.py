"""Invoice-billed renewal against a real Postgres session."""

import logging
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from robosystems.dagster.jobs.invoice_billing import _renew_subscriptions
from robosystems.models.core import Org, OrgType
from robosystems.models.core.billing import (
  BillingCustomer,
  BillingInvoice,
  BillingSubscription,
)

LOG = logging.getLogger("test")


def _due_subscription(session, period_end: datetime, started_at=None):
  uid = uuid.uuid4().hex[:8]
  org = Org(id=f"inv_org_{uid}", name=f"Inv {uid}", org_type=OrgType.PERSONAL)
  session.add(org)
  session.flush()
  session.add(BillingCustomer(org_id=org.id, invoice_billing_enabled=True))
  sub = BillingSubscription(
    org_id=org.id,
    resource_type="graph",
    resource_id=f"kg{uid}00000000fake",
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status="active",
    started_at=started_at,
    current_period_start=period_end - timedelta(days=30),
    current_period_end=period_end,
    subscription_metadata={},
  )
  session.add(sub)
  session.commit()
  return org, sub


def _invoices(session, org_id):
  return session.query(BillingInvoice).filter(BillingInvoice.org_id == org_id).count()


@pytest.mark.unit
class TestRenewalIsOneTransaction:
  def test_a_renewal_is_reported_as_renewed(self, test_db):
    org, sub = _due_subscription(test_db, datetime.now(UTC) - timedelta(minutes=5))
    old_end = sub.current_period_end

    result = _renew_subscriptions([sub.id], test_db, LOG)

    assert result["renewed_count"] == 1, result
    assert result["errors"] == []
    test_db.expire_all()
    assert test_db.get(BillingSubscription, sub.id).current_period_start == old_end
    assert _invoices(test_db, org.id) == 1

  def test_a_failure_midway_leaves_nothing_behind(self, test_db):
    org, sub = _due_subscription(test_db, datetime.now(UTC) - timedelta(minutes=5))
    old_end = sub.current_period_end

    with patch(
      "robosystems.models.core.billing.BillingAuditLog.log_event",
      side_effect=RuntimeError("audit store down"),
    ):
      result = _renew_subscriptions([sub.id], test_db, LOG)

    assert result["renewed_count"] == 0
    assert len(result["errors"]) == 1
    test_db.expire_all()
    assert test_db.get(BillingSubscription, sub.id).current_period_end == old_end
    assert _invoices(test_db, org.id) == 0


@pytest.mark.unit
class TestCalendarMonthPeriods:
  def test_twelve_monthly_renewals_span_one_year(self, test_db):
    start = datetime(2026, 1, 15, tzinfo=UTC)
    _, sub = _due_subscription(test_db, start, started_at=start)
    for _ in range(12):
      sub.renew_period(test_db)
    assert sub.current_period_end.date().isoformat() == "2027-01-15"

  def test_month_end_anchor_is_clamped_and_kept(self, test_db):
    start = datetime(2026, 1, 31, tzinfo=UTC)
    _, sub = _due_subscription(test_db, start, started_at=start)
    ends = []
    for _ in range(3):
      sub.renew_period(test_db)
      ends.append(sub.current_period_end.date().isoformat())
    assert ends == ["2026-02-28", "2026-03-31", "2026-04-30"]
