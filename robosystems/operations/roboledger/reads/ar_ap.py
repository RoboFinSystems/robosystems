"""Open AR / AP from the event duality chain, not the GL.

Open balance = originating event amounts minus the amounts of events whose
``discharges_event_id`` points at them (the FK is the contract, so payment
event types need not be enumerated). Only ``committed`` and ``fulfilled``
events count; inbox-pending and voided/superseded ones do not. Both sides are
taken in magnitude: a bank line paying a bill carries a negative amount (money
out), and its sign says which way the cash went, not how much it settles.
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta
from typing import Literal

from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.ar_ap import (
  GroupOpenItems,
  IntercompanyPair,
  OpenBalanceAggregate,
  OpenBalanceByAgent,
  OpenItem,
)
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.entity_scope import (
  find_entity_id,
  group_members,
  resolve_entity_id,
)

# A sales receipt is paid at the sale and never posts to receivables, so it is
# not an open item, whatever links to it.
_AR_ORIGINATING_TYPES: tuple[str, ...] = ("invoice_issued",)

_AP_ORIGINATING_TYPES: tuple[str, ...] = ("bill_received",)

_OPEN_BALANCE_STATUSES: tuple[str, ...] = ("committed", "fulfilled")


def _build_open_balance_subquery(
  *,
  originating_types: Iterable[str],
  entity_id: str | None = None,
):
  """``(originating_id, agent_id, open_amount, currency)`` per originating
  event; ``open_amount`` is negative when overpaid.

  The self-join aliases at the Core level: ORM ``aliased(Event)`` does not
  propagate through ``Event.__table__.outerjoin`` and Postgres rejects the
  duplicate table name.
  """
  originating = Event.__table__
  discharges = Event.__table__.alias("discharges_e")
  scope = (originating.c.entity_id == entity_id,) if entity_id is not None else ()
  return (
    select(
      originating.c.id.label("originating_id"),
      originating.c.agent_id.label("agent_id"),
      (
        func.abs(func.coalesce(originating.c.amount, 0))
        - func.coalesce(func.sum(func.abs(discharges.c.amount)), 0)
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
      *scope,
    )
    .group_by(
      originating.c.id,
      originating.c.agent_id,
      originating.c.amount,
      originating.c.currency,
    )
  )


def _aggregate(
  session: Session, *, originating_types: Iterable[str], entity_id: str | None
) -> OpenBalanceAggregate:
  per_event = _build_open_balance_subquery(
    originating_types=originating_types,
    entity_id=find_entity_id(session, entity_id),
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
  entity_id: str | None,
) -> list[OpenBalanceByAgent]:
  """Nonzero open balances per agent, by absolute balance descending (at most
  one row when ``agent_id`` is set). Events without an agent group under a
  NULL key."""
  per_event = _build_open_balance_subquery(
    originating_types=originating_types,
    entity_id=find_entity_id(session, entity_id),
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


def compute_open_receivables(
  session: Session, entity_id: str | None = None
) -> OpenBalanceAggregate:
  """One entity's open AR, default the group parent's — total + counterparty
  count."""
  return _aggregate(
    session, originating_types=_AR_ORIGINATING_TYPES, entity_id=entity_id
  )


def compute_open_payables(
  session: Session, entity_id: str | None = None
) -> OpenBalanceAggregate:
  """One entity's open AP, default the group parent's — total + counterparty
  count."""
  return _aggregate(
    session, originating_types=_AP_ORIGINATING_TYPES, entity_id=entity_id
  )


def list_open_receivables_by_agent(
  session: Session, entity_id: str | None = None
) -> list[OpenBalanceByAgent]:
  """Open AR per counterparty, descending by absolute balance."""
  return _by_agent(
    session,
    originating_types=_AR_ORIGINATING_TYPES,
    agent_id=None,
    entity_id=entity_id,
  )


def list_open_payables_by_agent(
  session: Session, entity_id: str | None = None
) -> list[OpenBalanceByAgent]:
  """Open AP per counterparty, descending by absolute balance."""
  return _by_agent(
    session,
    originating_types=_AP_ORIGINATING_TYPES,
    agent_id=None,
    entity_id=entity_id,
  )


def get_open_receivable_for_agent(
  session: Session, agent_id: str, entity_id: str | None = None
) -> OpenBalanceByAgent | None:
  """One agent's open AR — None when the agent has no open balance."""
  rows = _by_agent(
    session,
    originating_types=_AR_ORIGINATING_TYPES,
    agent_id=agent_id,
    entity_id=entity_id,
  )
  return rows[0] if rows else None


def get_open_payable_for_agent(
  session: Session, agent_id: str, entity_id: str | None = None
) -> OpenBalanceByAgent | None:
  """One agent's open AP — None when the agent has no open balance."""
  rows = _by_agent(
    session,
    originating_types=_AP_ORIGINATING_TYPES,
    agent_id=agent_id,
    entity_id=entity_id,
  )
  return rows[0] if rows else None


# ---- Open items as of a date ------------------------------------------------

OpenItemKind = Literal["receivables", "payables"]

_ORIGINATING_TYPES: dict[str, tuple[str, ...]] = {
  "receivables": _AR_ORIGINATING_TYPES,
  "payables": _AP_ORIGINATING_TYPES,
}


def _end_of(day: date) -> datetime:
  """The first instant after ``day``, the bound an as-of read takes."""
  return datetime.combine(day + timedelta(days=1), time.min)


def _open_items(
  session: Session,
  kind: OpenItemKind,
  *,
  as_of: date,
  entity_ids: list[str],
  agent_id: str | None = None,
) -> list[OpenItem]:
  """Every obligation of ``kind`` in ``entity_ids`` issued on or before
  ``as_of`` with something left open at ``as_of``: a settlement dated after it
  has not happened yet. Oldest first."""
  if not entity_ids:
    return []
  originating = Event.__table__
  discharges = Event.__table__.alias("discharges_e")
  bound = _end_of(as_of)
  original = func.abs(originating.c.amount)
  settled = func.coalesce(func.sum(func.abs(discharges.c.amount)), 0)
  stmt = (
    select(
      originating.c.id,
      originating.c.entity_id,
      originating.c.agent_id,
      originating.c.occurred_at,
      originating.c.metadata["qb_doc_number"].astext.label("document_number"),
      originating.c.description,
      original.label("original"),
      settled.label("settled"),
      originating.c.currency,
    )
    .select_from(
      originating.outerjoin(
        discharges,
        (discharges.c.discharges_event_id == originating.c.id)
        & discharges.c.status.in_(_OPEN_BALANCE_STATUSES)
        & (discharges.c.occurred_at < bound),
      )
    )
    .where(
      originating.c.event_type.in_(_ORIGINATING_TYPES[kind]),
      originating.c.status.in_(_OPEN_BALANCE_STATUSES),
      originating.c.amount.is_not(None),
      originating.c.entity_id.in_(entity_ids),
      originating.c.occurred_at < bound,
    )
    .group_by(originating.c.id)
    .having(original != settled)
    .order_by(originating.c.occurred_at.asc(), originating.c.id.asc())
  )
  if agent_id is not None:
    stmt = stmt.where(originating.c.agent_id == agent_id)
  return [
    OpenItem(
      event_id=str(row.id),
      entity_id=str(row.entity_id),
      agent_id=str(row.agent_id) if row.agent_id else None,
      occurred_on=row.occurred_at.date(),
      document_number=row.document_number or None,
      description=row.description,
      original_cents=int(row.original),
      settled_cents=int(row.settled),
      open_cents=int(row.original) - int(row.settled),
      currency=str(row.currency or "USD"),
    )
    for row in session.execute(stmt)
  ]


def list_open_items(
  session: Session,
  kind: OpenItemKind,
  *,
  as_of: date,
  entity_id: str | None = None,
  agent_id: str | None = None,
) -> list[OpenItem]:
  """One entity's open receivables or payables at ``as_of``, default the
  group parent's; one counterparty's when ``agent_id`` is set."""
  resolved = find_entity_id(session, entity_id)
  return _open_items(
    session,
    kind,
    as_of=as_of,
    entity_ids=[resolved] if resolved else [],
    agent_id=agent_id,
  )


def _member_ids(session: Session, parent_id: str, as_of: date) -> list[str]:
  """The parent and every member consolidated in full in ``as_of``'s month:
  the entities whose receivables the group's are."""
  start = as_of.replace(day=1)
  end = as_of.replace(day=calendar.monthrange(as_of.year, as_of.month)[1])
  return [
    member.id
    for member in group_members(session, parent_id, consolidated=False)
    if member.is_member_for(start, end)
  ]


def _self_agents(session: Session, entity_ids: list[str]) -> dict[str, str]:
  """``{agent_id: entity_id}`` for the `self` agents standing for
  ``entity_ids``, the counterparty an intercompany event names."""
  if not entity_ids:
    return {}
  rows = session.execute(
    select(Agent.id, Agent.external_id).where(
      Agent.agent_type == "self", Agent.external_id.in_(entity_ids)
    )
  )
  return {str(agent_id): str(entity) for agent_id, entity in rows}


def list_group_open_items(
  session: Session,
  kind: OpenItemKind,
  *,
  as_of: date,
  parent_id: str | None = None,
) -> GroupOpenItems:
  """A group's open receivables or payables at ``as_of``. Items with another
  summed member as counterparty are intercompany: consolidation eliminates
  them, so they are set apart from the external items and shown against the
  other side's books, pair by pair. A member on the equity method is not
  summed, and an item owed by one stays external."""
  parent = resolve_entity_id(session, parent_id)
  members = _member_ids(session, parent, as_of)
  self_agents = _self_agents(session, members)

  def split(items: list[OpenItem]) -> tuple[list[OpenItem], list[OpenItem]]:
    external = [i for i in items if i.agent_id not in self_agents]
    intercompany = [i for i in items if i.agent_id in self_agents]
    return external, intercompany

  external, intercompany = split(
    _open_items(session, kind, as_of=as_of, entity_ids=members)
  )
  pairs: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
  receivables = (
    intercompany
    if kind == "receivables"
    else split(_open_items(session, "receivables", as_of=as_of, entity_ids=members))[1]
  )
  payables = (
    intercompany
    if kind == "payables"
    else split(_open_items(session, "payables", as_of=as_of, entity_ids=members))[1]
  )
  for item in receivables:
    pairs[(item.entity_id, self_agents[str(item.agent_id)])][0] += item.open_cents
  for item in payables:
    pairs[(self_agents[str(item.agent_id)], item.entity_id)][1] += item.open_cents
  return GroupOpenItems(
    kind=kind,
    as_of=as_of,
    member_entity_ids=members,
    external=external,
    intercompany=intercompany,
    pairs=[
      IntercompanyPair(
        creditor_entity_id=creditor,
        debtor_entity_id=debtor,
        receivable_cents=receivable,
        payable_cents=payable,
        difference_cents=receivable - payable,
      )
      for (creditor, debtor), (receivable, payable) in sorted(pairs.items())
    ],
  )
