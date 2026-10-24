"""Unused-time credits for invoice-billed subscriptions.

An invoice-billed subscription is invoiced for its whole period when the
period starts. When access ends early (an immediate delete), the unused part
of that billed line is credited back: as a negative line on the same invoice
while it is still open, so it is billed only for the time used; as a separate
credit invoice once it is paid, since a paid invoice is a record of what was
paid. Stripe-billed subscriptions get the equivalent from Stripe itself.

Idempotent per billed line: a second pass finds the credit already recorded.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from robosystems.config.billing import BillingConfig
from robosystems.logger import get_logger
from robosystems.models.core.billing import (
  BillingAuditLog,
  BillingCustomer,
  BillingInvoice,
  BillingInvoiceLineItem,
  BillingSubscription,
  InvoiceStatus,
)
from robosystems.models.core.billing.audit_log import BillingEventType

logger = get_logger(__name__)

UNUSED_TIME_CREDIT = "unused_time_credit"

_CREDITABLE_ON_INVOICE = (InvoiceStatus.DRAFT.value, InvoiceStatus.OPEN.value)


def _as_utc(value: datetime) -> datetime:
  return value.replace(tzinfo=UTC) if value.tzinfo is None else value


@dataclass(frozen=True)
class UnusedTimeCredit:
  """A credit owed for a subscription's unused billed time."""

  subscription_id: str
  org_id: str
  billed_line_id: str
  billed_invoice_id: str
  billed_invoice_number: str
  billed_invoice_status: str
  billed_cents: int
  credit_cents: int
  unused_from: datetime
  period_end: datetime

  @property
  def on_billed_invoice(self) -> bool:
    """True when the credit lands on the billed invoice itself."""
    return self.billed_invoice_status in _CREDITABLE_ON_INVOICE


def plan_unused_time_credit(
  subscription: BillingSubscription, session: Session
) -> UnusedTimeCredit | None:
  """The credit owed for a canceled subscription's unused billed time, or None.

  Reads only. None when Stripe bills the subscription, access has not ended,
  no billed line covers the moment it ended, the line was already credited,
  its invoice is void or uncollectible, or nothing of the period is unused.
  """
  if subscription.stripe_subscription_id or subscription.ends_at is None:
    return None

  ended = _as_utc(subscription.ends_at)
  lines = (
    session.query(BillingInvoiceLineItem)
    .filter(BillingInvoiceLineItem.subscription_id == subscription.id)
    .all()
  )
  credited = {
    (line.line_metadata or {}).get("credited_line_id")
    for line in lines
    if (line.line_metadata or {}).get("kind") == UNUSED_TIME_CREDIT
  }
  covering = [
    line
    for line in lines
    if line.amount_cents > 0
    and (line.line_metadata or {}).get("kind") != UNUSED_TIME_CREDIT
    and _as_utc(line.period_start) <= ended < _as_utc(line.period_end)
  ]
  if not covering:
    return None
  billed = max(covering, key=lambda line: _as_utc(line.period_start))
  if billed.id in credited:
    return None

  invoice = (
    session.query(BillingInvoice).filter(BillingInvoice.id == billed.invoice_id).first()
  )
  if invoice is None or invoice.status in (
    InvoiceStatus.VOID.value,
    InvoiceStatus.UNCOLLECTIBLE.value,
  ):
    return None

  period_start = _as_utc(billed.period_start)
  period_end = _as_utc(billed.period_end)
  unused_from = max(ended, period_start)
  period_seconds = (period_end - period_start).total_seconds()
  if period_seconds <= 0:
    return None
  credit_cents = round(
    billed.amount_cents * (period_end - unused_from).total_seconds() / period_seconds
  )
  if credit_cents <= 0:
    return None

  return UnusedTimeCredit(
    subscription_id=subscription.id,
    org_id=subscription.org_id,
    billed_line_id=billed.id,
    billed_invoice_id=invoice.id,
    billed_invoice_number=invoice.invoice_number,
    billed_invoice_status=invoice.status,
    billed_cents=billed.amount_cents,
    credit_cents=credit_cents,
    unused_from=unused_from,
    period_end=period_end,
  )


def apply_unused_time_credit(
  subscription: BillingSubscription,
  session: Session,
  *,
  actor_type: str,
  actor_user_id: str | None = None,
) -> UnusedTimeCredit | None:
  """Record the credit `plan_unused_time_credit` finds, and commit it."""
  credit = plan_unused_time_credit(subscription, session)
  if credit is None:
    return None

  plan_config = BillingConfig.get_subscription_plan(subscription.plan_name)
  plan_label = plan_config["display_name"] if plan_config else subscription.plan_name
  description = (
    f"Unused time on {plan_label} after {credit.unused_from:%d %b %Y} "
    f"(credit against {credit.billed_invoice_number})"
  )
  line_metadata = {
    "kind": UNUSED_TIME_CREDIT,
    "credited_line_id": credit.billed_line_id,
    "credited_invoice_id": credit.billed_invoice_id,
    "unused_from": credit.unused_from.isoformat(),
  }

  if credit.on_billed_invoice:
    target = (
      session.query(BillingInvoice)
      .filter(BillingInvoice.id == credit.billed_invoice_id)
      .one()
    )
  else:
    customer = BillingCustomer.get_by_org_id(subscription.org_id, session)
    target = BillingInvoice.create_invoice(
      org_id=subscription.org_id,
      period_start=credit.unused_from,
      period_end=credit.period_end,
      payment_terms=(customer.payment_terms if customer else None) or "net_30",
      session=session,
      commit=False,
    )

  line = target.add_line_item(
    subscription_id=subscription.id,
    resource_type=subscription.resource_type or "unknown",
    resource_id=subscription.resource_id or "",
    description=description,
    amount_cents=-credit.credit_cents,
    session=session,
    line_metadata=line_metadata,
    commit=False,
  )
  line.period_start = credit.unused_from
  line.period_end = credit.period_end
  if not credit.on_billed_invoice:
    target.finalize(session, commit=False)

  BillingAuditLog.log_event(
    session=session,
    event_type=BillingEventType.INVOICE_CREDITED,
    description=(
      f"Credited {credit.credit_cents} cents of unused time on subscription "
      f"{subscription.id} to invoice {target.invoice_number}"
    ),
    actor_type=actor_type,
    actor_user_id=actor_user_id,
    org_id=subscription.org_id,
    subscription_id=subscription.id,
    invoice_id=target.id,
    event_data={
      "credit_cents": credit.credit_cents,
      "billed_cents": credit.billed_cents,
      "billed_invoice_id": credit.billed_invoice_id,
      "on_billed_invoice": credit.on_billed_invoice,
      "unused_from": credit.unused_from.isoformat(),
    },
    commit=False,
  )
  session.commit()

  logger.info(
    f"Credited {credit.credit_cents} cents of unused time on subscription "
    f"{subscription.id} to invoice {target.invoice_number}",
    extra={
      "subscription_id": subscription.id,
      "invoice_id": target.id,
      "on_billed_invoice": credit.on_billed_invoice,
    },
  )
  return credit
