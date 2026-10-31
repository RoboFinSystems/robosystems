"""Unused-time credits on the invoice-billed lane.

Real Postgres: the credit is a write across an invoice, its lines and the
audit log, and whether a second pass finds the first is a query result.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from robosystems.models.core.billing import (
  BillingAuditLog,
  BillingInvoice,
  BillingSubscription,
  InvoiceStatus,
  SubscriptionStatus,
)
from robosystems.operations.billing.invoice_credits import (
  UNUSED_TIME_CREDIT,
  apply_unused_time_credit,
  plan_unused_time_credit,
)

PERIOD_START = datetime(2026, 10, 1, tzinfo=UTC)
PERIOD_END = datetime(2026, 10, 31, tzinfo=UTC)
HALFWAY = PERIOD_START + timedelta(days=15)


@pytest.fixture
def org_id(db_session: Session) -> str:
  from robosystems.models.core import Org, OrgType

  org = Org(
    id=f"credit_org_{uuid.uuid4().hex[:8]}",
    name="Credit Org",
    org_type=OrgType.PERSONAL,
  )
  db_session.add(org)
  db_session.commit()
  return org.id


def _billed_subscription(
  session: Session, org_id: str, *, amount_cents: int = 9900
) -> tuple[BillingSubscription, BillingInvoice]:
  """An invoice-billed graph subscription with its period invoice, as
  `generate_subscription_invoice` writes it."""
  subscription = BillingSubscription(
    org_id=org_id,
    resource_type="graph",
    resource_id=f"kg{uuid.uuid4().hex[:18]}",
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status=SubscriptionStatus.ACTIVE.value,
    current_period_start=PERIOD_START,
    current_period_end=PERIOD_END,
  )
  session.add(subscription)
  session.commit()

  invoice = BillingInvoice.create_invoice(
    org_id=org_id,
    period_start=PERIOD_START,
    period_end=PERIOD_END,
    session=session,
  )
  invoice.add_line_item(
    subscription_id=subscription.id,
    resource_type="graph",
    resource_id=subscription.resource_id,
    description="Graph subscription - Standard",
    amount_cents=amount_cents,
    session=session,
  )
  invoice.finalize(session)
  return subscription, invoice


def _end_access(session: Session, subscription: BillingSubscription, at: datetime):
  subscription.status = SubscriptionStatus.CANCELED.value
  subscription.canceled_at = at
  subscription.ends_at = at
  session.commit()


def _credit_lines(session: Session, subscription_id: str):
  from robosystems.models.core.billing import BillingInvoiceLineItem

  return [
    line
    for line in session.query(BillingInvoiceLineItem)
    .filter(BillingInvoiceLineItem.subscription_id == subscription_id)
    .all()
    if (line.line_metadata or {}).get("kind") == UNUSED_TIME_CREDIT
  ]


class TestUnusedTimeCredit:
  def test_open_invoice_is_billed_only_for_the_time_used(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, invoice = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, HALFWAY)

    credit = apply_unused_time_credit(subscription, db_session, actor_type="user")

    assert credit is not None and credit.on_billed_invoice
    assert credit.credit_cents == 4950
    db_session.refresh(invoice)
    assert invoice.total_cents == 9900 - 4950
    assert invoice.status == InvoiceStatus.OPEN.value
    [line] = _credit_lines(db_session, subscription.id)
    assert line.invoice_id == invoice.id
    assert line.amount_cents == -4950
    assert line.period_start.replace(tzinfo=UTC) == HALFWAY

  def test_paid_invoice_is_left_alone_and_a_credit_invoice_issued(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, invoice = _billed_subscription(db_session, org_id)
    invoice.mark_paid(db_session, payment_method="bank_transfer")
    _end_access(db_session, subscription, HALFWAY)

    credit = apply_unused_time_credit(subscription, db_session, actor_type="user")

    assert credit is not None and not credit.on_billed_invoice
    db_session.refresh(invoice)
    assert invoice.total_cents == 9900
    [line] = _credit_lines(db_session, subscription.id)
    credit_invoice = (
      db_session.query(BillingInvoice)
      .filter(BillingInvoice.id == line.invoice_id)
      .one()
    )
    assert credit_invoice.id != invoice.id
    assert credit_invoice.org_id == org_id
    assert credit_invoice.total_cents == -4950
    assert credit_invoice.status == InvoiceStatus.OPEN.value

  def test_a_second_pass_finds_the_credit_already_recorded(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, _ = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, HALFWAY)

    assert apply_unused_time_credit(subscription, db_session, actor_type="user")
    assert (
      apply_unused_time_credit(subscription, db_session, actor_type="admin") is None
    )
    assert len(_credit_lines(db_session, subscription.id)) == 1

  def test_writes_an_audit_event(self, db_session: Session, org_id: str) -> None:
    subscription, invoice = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, HALFWAY)

    apply_unused_time_credit(subscription, db_session, actor_type="admin")

    event = (
      db_session.query(BillingAuditLog)
      .filter(
        BillingAuditLog.subscription_id == subscription.id,
        BillingAuditLog.event_type == "invoice_credited",
      )
      .one()
    )
    assert event.invoice_id == invoice.id
    assert event.event_data["credit_cents"] == 4950

  def test_period_end_cancel_has_nothing_unused(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, _ = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, PERIOD_END)

    assert plan_unused_time_credit(subscription, db_session) is None

  def test_stripe_billed_subscription_is_stripes_to_credit(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, _ = _billed_subscription(db_session, org_id)
    subscription.stripe_subscription_id = "sub_stripe_xyz"
    _end_access(db_session, subscription, HALFWAY)

    assert plan_unused_time_credit(subscription, db_session) is None

  def test_void_invoice_is_not_credited(self, db_session: Session, org_id: str) -> None:
    subscription, invoice = _billed_subscription(db_session, org_id)
    invoice.status = InvoiceStatus.VOID.value
    db_session.commit()
    _end_access(db_session, subscription, HALFWAY)

    assert plan_unused_time_credit(subscription, db_session) is None

  def test_billing_off_zero_invoice_has_nothing_to_credit(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, _ = _billed_subscription(db_session, org_id, amount_cents=0)
    _end_access(db_session, subscription, HALFWAY)

    assert plan_unused_time_credit(subscription, db_session) is None

  def test_access_ending_before_the_period_credits_it_whole(
    self, db_session: Session, org_id: str
  ) -> None:
    """A graph deleted minutes after the invoice was issued: nearly all of it."""
    subscription, _ = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, PERIOD_START + timedelta(hours=2))

    credit = plan_unused_time_credit(subscription, db_session)

    assert credit is not None
    assert credit.credit_cents == round(9900 * (30 * 24 - 2) / (30 * 24))


class TestCreditConsumptionReducesTheCredit:
  """The invoice lane refunds the smaller unused share of time and credits,
  reading the snapshot the delete left once the graph's pool is gone."""

  def _snapshot(self, subscription: BillingSubscription, session: Session, **fields):
    from robosystems.operations.billing.invoice_credits import (
      CREDIT_CONSUMPTION_SNAPSHOT,
    )

    subscription.subscription_metadata = {
      **(subscription.subscription_metadata or {}),
      CREDIT_CONSUMPTION_SNAPSHOT: {
        "allocation": 8000,
        "consumed": 8000,
        "consumed_fraction": 1.0,
        "elapsed_fraction": 0.5,
        "clawback_cents": 4950,
        **fields,
      },
    }
    session.commit()

  def test_a_burned_allocation_leaves_nothing_to_credit(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, _ = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, HALFWAY)
    self._snapshot(subscription, db_session)

    assert plan_unused_time_credit(subscription, db_session) is None
    assert apply_unused_time_credit(subscription, db_session, actor_type="user") is None
    assert _credit_lines(db_session, subscription.id) == []

  def test_credits_used_ahead_of_time_reduce_the_credit(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, invoice = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, HALFWAY)
    # 75 % of the credits used with half the period gone: refund 25 %, not 50 %.
    self._snapshot(
      subscription,
      db_session,
      consumed=6000,
      consumed_fraction=0.75,
      clawback_cents=2475,
    )

    credit = plan_unused_time_credit(subscription, db_session)

    assert credit is not None
    assert credit.time_credit_cents == 4950
    assert credit.credit_cents == 2475
    assert credit.credits_consumed_fraction == 0.75

    applied = apply_unused_time_credit(subscription, db_session, actor_type="user")
    assert applied is not None
    (line,) = _credit_lines(db_session, subscription.id)
    assert line.amount_cents == -2475
    assert "less AI credits used ahead of time" in line.description
    assert line.line_metadata["time_credit_cents"] == 4950
    assert line.line_metadata["credits_consumed_fraction"] == 0.75

  def test_credits_used_in_step_with_time_leave_the_credit_whole(
    self, db_session: Session, org_id: str
  ) -> None:
    subscription, _ = _billed_subscription(db_session, org_id)
    _end_access(db_session, subscription, HALFWAY)
    self._snapshot(
      subscription,
      db_session,
      consumed=4000,
      consumed_fraction=0.5,
      clawback_cents=0,
    )

    credit = plan_unused_time_credit(subscription, db_session)

    assert credit is not None
    assert credit.credit_cents == credit.time_credit_cents == 4950
    assert not credit.reduced_by_consumption
