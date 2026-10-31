"""Unused-time credits for invoice-billed subscriptions.

An invoice-billed subscription is invoiced for its whole period when the
period starts. When access ends early (an immediate delete), the unused part
of that billed line is credited back: as a negative line on the same invoice
while it is still open, so it is billed only for the time used; as a separate
credit invoice once it is paid, since a paid invoice is a record of what was
paid. Stripe-billed subscriptions get the equivalent from Stripe itself.

Idempotent per billed line: a second pass finds the credit already recorded.

A period buys time and an AI-credit allocation, so the credit refunds the
smaller unused share of the two: a graph that burned its allocation on day
one gets nothing back for the days it did not use, or it could be deleted
and recreated for a fresh allocation at a fraction of the price. The same
rule feeds the Stripe lane as a positive line against Stripe's own
proration (`plan_credit_clawback`).
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

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
from robosystems.models.core.graph.graph_credits import GraphCredits

logger = get_logger(__name__)

UNUSED_TIME_CREDIT = "unused_time_credit"
CREDIT_CONSUMPTION_SNAPSHOT = "unused_time_credit_consumption"

_CREDITABLE_ON_INVOICE = (InvoiceStatus.DRAFT.value, InvoiceStatus.OPEN.value)


def _as_utc(value: datetime) -> datetime:
  return value.replace(tzinfo=UTC) if value.tzinfo is None else value


@dataclass(frozen=True)
class CreditConsumption:
  """How far a graph's AI-credit use ran ahead of its billing period.

  ``clawback_cents`` is the part of a time-only credit that consumed credits
  have already used up: price * max(0, consumed share - elapsed share).
  """

  allocation: int
  consumed: int
  consumed_fraction: float
  elapsed_fraction: float
  clawback_cents: int

  @property
  def description(self) -> str:
    return (
      f"AI credits used ahead of the period elapsed: {self.consumed:,} of "
      f"{self.allocation:,} credits with {self.elapsed_fraction:.0%} of the "
      "period elapsed"
    )


def _credit_pool(graph_id: str, session: Session) -> tuple[int, int] | None:
  """The graph's (allocation, consumed since the last refill), or None."""
  try:
    credits = GraphCredits.get_by_graph_id(graph_id, session)
    if credits is None:
      return None
    allocation = credits.monthly_allocation
    consumed = max(Decimal(0), allocation - credits.current_balance)
    return int(allocation), int(consumed)
  except Exception as e:
    logger.warning(
      f"Could not read the credit pool of {graph_id}: {e}",
      extra={"graph_id": graph_id},
    )
    return None


def plan_credit_clawback(
  subscription: BillingSubscription,
  session: Session,
  *,
  now: datetime | None = None,
) -> CreditConsumption | None:
  """How much of a time-based unused-time credit the graph's AI-credit use
  has already consumed, or None when it cannot be known.

  Reads the graph's live credit pool, or the snapshot a delete left in
  ``subscription_metadata`` once teardown has removed the pool, so a later
  pass applies the rule the delete saw. Never raises.
  """
  if subscription.resource_type != "graph" or not subscription.resource_id:
    return None
  start, end = subscription.current_period_start, subscription.current_period_end
  if start is None or end is None:
    return None
  start, end = _as_utc(start), _as_utc(end)
  period_seconds = (end - start).total_seconds()
  if period_seconds <= 0:
    return None
  at = _as_utc(now) if now is not None else datetime.now(UTC)
  elapsed_fraction = min(1.0, max(0.0, (at - start).total_seconds() / period_seconds))

  pool = _credit_pool(subscription.resource_id, session)
  if pool is not None:
    allocation, consumed = pool
  else:
    snapshot = (subscription.subscription_metadata or {}).get(
      CREDIT_CONSUMPTION_SNAPSHOT
    )
    if not snapshot:
      return None
    allocation = int(snapshot["allocation"])
    consumed = int(snapshot["consumed"])
    elapsed_fraction = float(snapshot.get("elapsed_fraction", elapsed_fraction))
  if allocation <= 0:
    return None

  consumed_fraction = min(1.0, consumed / allocation)
  clawback_fraction = max(0.0, consumed_fraction - elapsed_fraction)
  return CreditConsumption(
    allocation=allocation,
    consumed=consumed,
    consumed_fraction=consumed_fraction,
    elapsed_fraction=elapsed_fraction,
    clawback_cents=round(subscription.base_price_cents * clawback_fraction),
  )


def snapshot_credit_consumption(
  subscription: BillingSubscription,
  consumption: CreditConsumption,
  *,
  now: datetime | None = None,
) -> None:
  """Keep what the delete saw of the credit pool on the subscription row.

  Teardown deletes the pool minutes later; a later pass (the admin backfill)
  reads this instead and reaches the same credit. Reassigning the dict marks
  the JSONB column dirty; the caller's commit carries it.
  """
  at = now if now is not None else datetime.now(UTC)
  subscription.subscription_metadata = {
    **(subscription.subscription_metadata or {}),
    CREDIT_CONSUMPTION_SNAPSHOT: {
      "allocation": consumption.allocation,
      "consumed": consumption.consumed,
      "consumed_fraction": consumption.consumed_fraction,
      "elapsed_fraction": consumption.elapsed_fraction,
      "clawback_cents": consumption.clawback_cents,
      "snapshot_at": at.isoformat(),
    },
  }


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
  # The time-only figure, before AI credits used ahead of time reduced it.
  time_credit_cents: int = 0
  credits_consumed_fraction: float | None = None

  @property
  def on_billed_invoice(self) -> bool:
    """True when the credit lands on the billed invoice itself."""
    return self.billed_invoice_status in _CREDITABLE_ON_INVOICE

  @property
  def reduced_by_consumption(self) -> bool:
    return self.credit_cents < self.time_credit_cents


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
  unused_fraction = (period_end - unused_from).total_seconds() / period_seconds
  time_credit_cents = round(billed.amount_cents * unused_fraction)
  if time_credit_cents <= 0:
    return None

  # The smaller unused share of time and credits (see the module docstring).
  consumption = plan_credit_clawback(subscription, session, now=ended)
  credit_fraction = unused_fraction
  consumed_fraction = None
  if consumption is not None:
    consumed_fraction = consumption.consumed_fraction
    credit_fraction = min(unused_fraction, 1.0 - consumed_fraction)
  credit_cents = round(billed.amount_cents * credit_fraction)
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
    time_credit_cents=time_credit_cents,
    credits_consumed_fraction=consumed_fraction,
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
  reduction = (
    ", less AI credits used ahead of time" if credit.reduced_by_consumption else ""
  )
  description = (
    f"Unused time on {plan_label} after {credit.unused_from:%d %b %Y}{reduction} "
    f"(credit against {credit.billed_invoice_number})"
  )
  line_metadata = {
    "kind": UNUSED_TIME_CREDIT,
    "credited_line_id": credit.billed_line_id,
    "credited_invoice_id": credit.billed_invoice_id,
    "unused_from": credit.unused_from.isoformat(),
    "time_credit_cents": credit.time_credit_cents,
    "credits_consumed_fraction": credit.credits_consumed_fraction,
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
