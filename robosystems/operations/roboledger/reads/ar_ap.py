"""Open AR / AP reads driven by the event duality chain.

Open balance = sum of originating-event amounts minus sum of discharging-event
amounts that point at them. The math runs on ``events.amount`` directly,
so the answer is one SQL query and doesn't depend on GL roll-up. See
``event-driven-ledger.md`` §5.1 for the design rationale.

The originating event-type set is config — AR uses ``invoice_issued`` (+
``sales_receipt_recorded`` for QB-imported deposit applications); AP
uses ``bill_received``. The discharge side is whichever event whose
``discharges_event_id`` happens to point at an originating row, so we
don't have to enumerate payment event types — the FK is the contract.

Status filter: only ``committed`` and ``fulfilled`` events contribute.
Captured/classified events represent inbox-pending data the operator
hasn't approved yet; counting them would inflate AR/AP with unconfirmed
amounts. Voided/superseded events are excluded for the obvious reason.
"""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.ar_ap import (
  OpenBalanceAggregate,
  OpenBalanceByAgent,
)
from robosystems.models.extensions.roboledger.event import Event

# Event types that originate an AR obligation (something the entity is
# owed). QB-imported invoices and sales receipts both produce AR rows
# whose ``discharges_event_id`` can be pointed at by a later payment.
_AR_ORIGINATING_TYPES: tuple[str, ...] = ("invoice_issued", "sales_receipt_recorded")

# Event types that originate an AP obligation (something the entity owes).
_AP_ORIGINATING_TYPES: tuple[str, ...] = ("bill_received",)

# Status values that contribute to open-balance math. ``captured`` and
# ``classified`` are pre-handler / inbox-pending; ``voided`` /
# ``superseded`` are terminal off-ramps that shouldn't count.
_OPEN_BALANCE_STATUSES: tuple[str, ...] = ("committed", "fulfilled")


def _build_open_balance_subquery(
  *,
  originating_types: Iterable[str],
):
  """Per-event open-balance subquery: amount minus sum of discharges.

  Returns a Core Selectable that yields ``(originating_id, agent_id,
  open_amount, currency)`` for each originating event. ``open_amount``
  is signed (positive for normal AR/AP; negative when overpaid). Rows
  with no discharges still appear — the LEFT JOIN coalesces NULL to 0.

  Implementation: aliases the events table at the Core level so the
  self-join (originating events LEFT JOIN their discharging events)
  compiles with a distinct table alias. ORM-level ``aliased(Event)``
  doesn't propagate through ``Event.__table__.outerjoin(...)`` and
  emits ``"events" specified more than once`` against Postgres.
  """
  originating = Event.__table__
  discharges = Event.__table__.alias("discharges_e")
  return (
    select(
      originating.c.id.label("originating_id"),
      originating.c.agent_id.label("agent_id"),
      (
        func.coalesce(originating.c.amount, 0)
        - func.coalesce(func.sum(discharges.c.amount), 0)
      ).label("open_amount"),
      originating.c.currency.label("currency"),
    )
    .select_from(
      originating.outerjoin(
        discharges,
        (discharges.c.discharges_event_id == originating.c.id)
        & (discharges.c.status.in_(_OPEN_BALANCE_STATUSES)),
      )
    )
    .where(
      originating.c.event_type.in_(tuple(originating_types)),
      originating.c.status.in_(_OPEN_BALANCE_STATUSES),
      originating.c.amount.is_not(None),
    )
    .group_by(
      originating.c.id,
      originating.c.agent_id,
      originating.c.amount,
      originating.c.currency,
    )
  )


def _aggregate(
  session: Session, *, originating_types: Iterable[str]
) -> OpenBalanceAggregate:
  """Roll a per-event subquery into the graph-wide totals."""
  per_event = _build_open_balance_subquery(
    originating_types=originating_types
  ).subquery("per_event")
  nonzero = per_event.c.open_amount != 0
  row = session.execute(
    select(
      func.coalesce(func.sum(per_event.c.open_amount), 0).label("total_open"),
      func.count(func.distinct(case((nonzero, per_event.c.agent_id)))).label(
        "counterparty_count"
      ),
      func.count(case((nonzero, per_event.c.originating_id))).label("open_event_count"),
      func.coalesce(func.max(per_event.c.currency), "USD").label("currency"),
    )
  ).one()
  return OpenBalanceAggregate(
    total_open_cents=int(row.total_open or 0),
    counterparty_count=int(row.counterparty_count or 0),
    open_event_count=int(row.open_event_count or 0),
    currency=str(row.currency or "USD"),
  )


def _by_agent(
  session: Session,
  *,
  originating_types: Iterable[str],
  agent_id: str | None,
) -> list[OpenBalanceByAgent]:
  """Per-agent open-balance rows.

  When ``agent_id`` is None, returns one row per agent with any
  nonzero open balance, ordered by absolute balance descending.
  When ``agent_id`` is set, returns at most one row (the matching
  agent, or [] when zero).

  Agents with NULL on the originating events are aggregated under a
  single NULL key — the caller can decide whether to surface them.
  """
  per_event = _build_open_balance_subquery(
    originating_types=originating_types
  ).subquery("per_event")
  stmt = (
    select(
      per_event.c.agent_id,
      func.coalesce(func.sum(per_event.c.open_amount), 0).label("open_balance"),
      func.count(
        case((per_event.c.open_amount != 0, per_event.c.originating_id))
      ).label("open_event_count"),
      func.coalesce(func.max(per_event.c.currency), "USD").label("currency"),
    )
    .group_by(per_event.c.agent_id)
    .having(func.coalesce(func.sum(per_event.c.open_amount), 0) != 0)
  )
  if agent_id is not None:
    stmt = stmt.where(per_event.c.agent_id == agent_id)
  else:
    stmt = stmt.order_by(func.abs(func.sum(per_event.c.open_amount)).desc())
  rows = session.execute(stmt).all()
  return [
    OpenBalanceByAgent(
      agent_id=str(r.agent_id) if r.agent_id else "",
      open_balance_cents=int(r.open_balance),
      open_event_count=int(r.open_event_count),
      currency=str(r.currency or "USD"),
    )
    for r in rows
  ]


# ---- Public read surface ----------------------------------------------------


def compute_open_receivables(session: Session) -> OpenBalanceAggregate:
  """Graph-wide open AR — total + counterparty count."""
  return _aggregate(session, originating_types=_AR_ORIGINATING_TYPES)


def compute_open_payables(session: Session) -> OpenBalanceAggregate:
  """Graph-wide open AP — total + counterparty count."""
  return _aggregate(session, originating_types=_AP_ORIGINATING_TYPES)


def list_open_receivables_by_agent(
  session: Session,
) -> list[OpenBalanceByAgent]:
  """Open AR per counterparty, descending by absolute balance."""
  return _by_agent(session, originating_types=_AR_ORIGINATING_TYPES, agent_id=None)


def list_open_payables_by_agent(session: Session) -> list[OpenBalanceByAgent]:
  """Open AP per counterparty, descending by absolute balance."""
  return _by_agent(session, originating_types=_AP_ORIGINATING_TYPES, agent_id=None)


def get_open_receivable_for_agent(
  session: Session, agent_id: str
) -> OpenBalanceByAgent | None:
  """One agent's open AR — None when the agent has no open balance."""
  rows = _by_agent(session, originating_types=_AR_ORIGINATING_TYPES, agent_id=agent_id)
  return rows[0] if rows else None


def get_open_payable_for_agent(
  session: Session, agent_id: str
) -> OpenBalanceByAgent | None:
  """One agent's open AP — None when the agent has no open balance."""
  rows = _by_agent(session, originating_types=_AP_ORIGINATING_TYPES, agent_id=agent_id)
  return rows[0] if rows else None
