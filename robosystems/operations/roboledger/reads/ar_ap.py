"""Open AR / AP from the event duality chain, not the GL.

Open balance = originating event amounts minus what payments applied to them:
a payment's ``event_discharges`` rows where it has them, else its full amount
against the obligation its ``discharges_event_id`` names (the FK is the
contract, so payment event types need not be enumerated). Only ``committed``
and ``fulfilled`` events count; inbox-pending and voided/superseded ones do
not. Both sides are taken in magnitude: a bank line paying a bill carries a
negative amount (money out), and its sign says which way the cash went, not
how much it settles.
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from collections.abc import Iterable
from datetime import date, datetime, time, timedelta
from typing import Literal

from sqlalchemy import case, exists, func, or_, select, union_all
from sqlalchemy.orm import Session, aliased

from robosystems.models.api.extensions.ar_ap import (
  AgedAgentRow,
  AgedBalances,
  AgingBuckets,
  ControlAccount,
  ControlTieOut,
  DischargeCandidates,
  GroupOpenItems,
  IntercompanyPair,
  OpenBalanceAggregate,
  OpenBalanceByAgent,
  OpenItem,
)
from robosystems.models.extensions import Association, Element
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_discharge import EventDischarge
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.discharges import settles_type
from robosystems.operations.roboledger.entity_scope import (
  find_entity_id,
  group_members,
  resolve_entity_id,
)
from robosystems.operations.roboledger.entry_status import LANDED_ENTRY_STATUSES
from robosystems.operations.roboledger.reads.accounts import (
  account_scope,
  entity_accounts_clause,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)

# A sales receipt is paid at the sale and never posts to receivables, so it is
# not an open item, whatever links to it.
_AR_ORIGINATING_TYPES: tuple[str, ...] = ("invoice_issued",)

_AP_ORIGINATING_TYPES: tuple[str, ...] = ("bill_received",)

_OPEN_BALANCE_STATUSES: tuple[str, ...] = ("committed", "fulfilled")


def _settled(bound: datetime | None = None, *, excluding: str | None = None):
  """``(obligation_id, settled)``: what committed payments, dated before
  ``bound`` when one is given, have applied to each obligation. A payment with
  allocation rows settles through them; one without settles the obligation its
  ``discharges_event_id`` names, at its full magnitude. ``excluding`` leaves
  one payment out, so its own settlement does not hide what it may settle."""
  payments = Event.__table__.alias("payments_e")
  allocations = EventDischarge.__table__
  dated = (payments.c.occurred_at < bound,) if bound is not None else ()
  if excluding is not None:
    dated = (*dated, payments.c.id != excluding)
  allocated = (
    select(
      allocations.c.obligation_event_id.label("obligation_id"),
      allocations.c.amount.label("amount"),
    )
    .select_from(
      allocations.join(payments, payments.c.id == allocations.c.payment_event_id)
    )
    .where(payments.c.status.in_(_OPEN_BALANCE_STATUSES), *dated)
  )
  linked = select(
    payments.c.discharges_event_id.label("obligation_id"),
    func.abs(payments.c.amount).label("amount"),
  ).where(
    payments.c.discharges_event_id.is_not(None),
    payments.c.status.in_(_OPEN_BALANCE_STATUSES),
    ~exists().where(allocations.c.payment_event_id == payments.c.id),
    *dated,
  )
  settlements = union_all(allocated, linked).subquery("settlements")
  return (
    select(
      settlements.c.obligation_id,
      func.sum(settlements.c.amount).label("settled"),
    )
    .group_by(settlements.c.obligation_id)
    .subquery("settled")
  )


def _build_open_balance_subquery(
  *,
  originating_types: Iterable[str],
  entity_id: str | None = None,
):
  """``(originating_id, agent_id, open_amount, currency)`` per originating
  event; ``open_amount`` is negative when overpaid."""
  originating = Event.__table__
  settled = _settled()
  scope = (originating.c.entity_id == entity_id,) if entity_id is not None else ()
  return (
    select(
      originating.c.id.label("originating_id"),
      originating.c.agent_id.label("agent_id"),
      (
        func.abs(func.coalesce(originating.c.amount, 0))
        - func.coalesce(settled.c.settled, 0)
      ).label("open_amount"),
      originating.c.currency.label("currency"),
    )
    .select_from(
      originating.outerjoin(settled, settled.c.obligation_id == originating.c.id)
    )
    .where(
      originating.c.event_type.in_(tuple(originating_types)),
      originating.c.status.in_(_OPEN_BALANCE_STATUSES),
      originating.c.amount.is_not(None),
      *scope,
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


_BUCKET_BOUNDS: tuple[tuple[int, str], ...] = (
  (0, "current"),
  (30, "one_to_thirty"),
  (60, "thirty_one_to_sixty"),
  (90, "sixty_one_to_ninety"),
)


def _bucket(open_cents: int, days_past_due: int) -> str:
  """An item's aging bucket. An overpayment is an unapplied credit, whatever
  its age."""
  if open_cents < 0:
    return "unapplied_credit"
  for bound, name in _BUCKET_BOUNDS:
    if days_past_due <= bound:
      return name
  return "over_ninety"


def _open_items(
  session: Session,
  kind: OpenItemKind,
  *,
  as_of: date,
  entity_ids: list[str],
  agent_id: str | None = None,
  excluding_payment: str | None = None,
) -> list[OpenItem]:
  """Every obligation of ``kind`` in ``entity_ids`` issued on or before
  ``as_of`` with something left open at ``as_of``: a settlement dated after it
  has not happened yet. Oldest first."""
  if not entity_ids:
    return []
  originating = Event.__table__
  bound = _end_of(as_of)
  by_obligation = _settled(bound, excluding=excluding_payment)
  original = func.abs(originating.c.amount)
  settled = func.coalesce(by_obligation.c.settled, 0)
  stmt = (
    select(
      originating.c.id,
      originating.c.entity_id,
      originating.c.agent_id,
      originating.c.occurred_at,
      originating.c.due_date,
      originating.c.metadata["qb_doc_number"].astext.label("document_number"),
      originating.c.description,
      original.label("original"),
      settled.label("settled"),
      originating.c.currency,
    )
    .select_from(
      originating.outerjoin(
        by_obligation, by_obligation.c.obligation_id == originating.c.id
      )
    )
    .where(
      originating.c.event_type.in_(_ORIGINATING_TYPES[kind]),
      originating.c.status.in_(_OPEN_BALANCE_STATUSES),
      originating.c.amount.is_not(None),
      originating.c.entity_id.in_(entity_ids),
      originating.c.occurred_at < bound,
      original != settled,
    )
    .order_by(originating.c.occurred_at.asc(), originating.c.id.asc())
  )
  if agent_id is not None:
    stmt = stmt.where(originating.c.agent_id == agent_id)
  items = []
  for row in session.execute(stmt):
    issued = row.occurred_at.date()
    due = row.due_date or issued
    open_cents = int(row.original) - int(row.settled)
    days_past_due = (as_of - due).days
    items.append(
      OpenItem(
        event_id=str(row.id),
        entity_id=str(row.entity_id),
        agent_id=str(row.agent_id) if row.agent_id else None,
        occurred_on=issued,
        document_number=row.document_number or None,
        description=row.description,
        original_cents=int(row.original),
        settled_cents=int(row.settled),
        open_cents=open_cents,
        currency=str(row.currency or "USD"),
        due_date=due,
        due_date_basis="stated" if row.due_date else "defaulted",
        days_past_due=days_past_due,
        bucket=_bucket(open_cents, days_past_due),
      )
    )
  return items


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


# ---- Aged balances ----------------------------------------------------------

# The concept each kind's control accounts map to in the book framework, the
# one a tenant chart can map to, and QuickBooks' own account type for it.
_CONTROL: dict[str, tuple[str, str]] = {
  "receivables": ("rs-gaap:ReceivablesNetCurrent", "Accounts Receivable"),
  "payables": ("rs-gaap:AccountsPayableCurrent", "Accounts Payable"),
}

AgingScope = Literal["entity", "group"]


def _buckets(items: list[OpenItem]) -> AgingBuckets:
  totals: dict[str, int] = defaultdict(int)
  for item in items:
    totals[item.bucket] += item.open_cents
  return AgingBuckets(
    current_cents=totals["current"],
    one_to_thirty_cents=totals["one_to_thirty"],
    thirty_one_to_sixty_cents=totals["thirty_one_to_sixty"],
    sixty_one_to_ninety_cents=totals["sixty_one_to_ninety"],
    over_ninety_cents=totals["over_ninety"],
    unapplied_credit_cents=totals["unapplied_credit"],
    total_cents=sum(totals.values()),
  )


def _agent_rows(session: Session, items: list[OpenItem]) -> list[AgedAgentRow]:
  """A row per counterparty, largest open balance first."""
  by_agent: dict[str | None, list[OpenItem]] = defaultdict(list)
  for item in items:
    by_agent[item.agent_id].append(item)
  ids = [agent_id for agent_id in by_agent if agent_id]
  names = (
    dict(session.execute(select(Agent.id, Agent.name).where(Agent.id.in_(ids))).all())
    if ids
    else {}
  )
  rows = [
    AgedAgentRow(
      agent_id=agent_id,
      agent_name=names.get(agent_id) if agent_id else None,
      buckets=_buckets(agent_items),
      open_item_count=len(agent_items),
      oldest_due_date=min(i.due_date for i in agent_items if i.open_cents > 0)
      if any(i.open_cents > 0 for i in agent_items)
      else None,
    )
    for agent_id, agent_items in by_agent.items()
  ]
  return sorted(rows, key=lambda r: (-abs(r.buckets.total_cents), r.agent_name or ""))


def _control_accounts(
  session: Session, kind: OpenItemKind, entity_id: str
) -> list[Element]:
  """The entity's accounts for ``kind``: mapped to its concept in the entity's
  book mapping, or QuickBooks' own A/R or A/P account."""
  concept, source_type = _CONTROL[kind]
  matches = [Element.metadata_["account_type"].astext == source_type]
  mapping = find_entity_mapping(session, entity_id, BOOK_FRAMEWORK)
  if mapping is not None:
    target = aliased(Element)
    matches.append(
      Element.id.in_(
        select(Association.from_element_id)
        .join(target, target.id == Association.to_element_id)
        .where(
          Association.structure_id == mapping.id,
          Association.association_type == "mapping",
          target.qname == concept,
        )
      )
    )
  return list(
    session.execute(
      select(Element)
      .where(entity_accounts_clause(account_scope(session, entity_id)), or_(*matches))
      .order_by(Element.code)
    ).scalars()
  )


def _tie_out(
  session: Session,
  kind: OpenItemKind,
  *,
  as_of: date,
  entity_id: str,
  open_items_cents: int,
) -> ControlTieOut:
  accounts = _control_accounts(session, kind, entity_id)
  balance: int | None = None
  if accounts:
    net = session.execute(
      select(func.coalesce(func.sum(LineItem.debit_amount - LineItem.credit_amount), 0))
      .join(Entry, Entry.id == LineItem.entry_id)
      .where(
        LineItem.element_id.in_([a.id for a in accounts]),
        Entry.entity_id == entity_id,
        Entry.status.in_(LANDED_ENTRY_STATUSES),
        Entry.posting_date <= as_of,
      )
    ).scalar_one()
    balance = int(net) if kind == "receivables" else -int(net)
  return ControlTieOut(
    entity_id=entity_id,
    accounts=[
      ControlAccount(element_id=str(a.id), code=a.code, name=str(a.name))
      for a in accounts
    ],
    control_balance_cents=balance,
    open_items_cents=open_items_cents,
    difference_cents=None if balance is None else open_items_cents - balance,
  )


def get_aged_balances(
  session: Session,
  kind: OpenItemKind,
  *,
  as_of: date,
  entity_id: str | None = None,
  scope: AgingScope = "entity",
) -> AgedBalances:
  """Receivables or payables aged at ``as_of``: one entity's, default the
  group parent's, or with ``scope='group'`` the group's, intercompany items
  set apart. Each summed entity's open items are tied out against its control
  accounts."""
  if scope == "group":
    group = list_group_open_items(session, kind, as_of=as_of, parent_id=entity_id)
    resolved: str | None = resolve_entity_id(session, entity_id)
    members = group.member_entity_ids
    external, intercompany, pairs = group.external, group.intercompany, group.pairs
  else:
    resolved = find_entity_id(session, entity_id)
    members = [resolved] if resolved else []
    external = _open_items(session, kind, as_of=as_of, entity_ids=members)
    intercompany, pairs = [], []
  per_entity: dict[str, int] = defaultdict(int)
  for item in (*external, *intercompany):
    per_entity[item.entity_id] += item.open_cents
  return AgedBalances(
    kind=kind,
    as_of=as_of,
    scope=scope,
    entity_id=resolved,
    member_entity_ids=members,
    buckets=_buckets(external),
    agents=_agent_rows(session, external),
    defaulted_due_date_count=sum(
      1 for item in external if item.due_date_basis == "defaulted"
    ),
    intercompany=intercompany,
    pairs=pairs,
    tie_outs=[
      _tie_out(
        session,
        kind,
        as_of=as_of,
        entity_id=member,
        open_items_cents=per_entity[member],
      )
      for member in members
    ],
  )


# ---- Discharge candidates ---------------------------------------------------


def list_discharge_candidates(
  session: Session, event_id: str
) -> DischargeCandidates | None:
  """The open items a payment or bank line may settle: its own entity's, its
  counterparty's when it names one, open at its date. An exact amount match
  first, then the oldest due date. None when there is no such event."""
  payment = session.get(Event, event_id)
  if payment is None:
    return None
  magnitude = abs(payment.amount or 0)
  obligation_type = settles_type(payment)
  if obligation_type is None:
    return DischargeCandidates(
      event_id=event_id,
      amount_cents=magnitude,
      classified_to_control=False,
      candidates=[],
    )
  kind: OpenItemKind = (
    "payables" if obligation_type == "bill_received" else "receivables"
  )
  items = [
    item
    for item in _open_items(
      session,
      kind,
      as_of=payment.occurred_at.date(),
      entity_ids=[str(payment.entity_id)],
      agent_id=payment.agent_id,
      excluding_payment=event_id,
    )
    if item.open_cents > 0
  ]
  metadata = payment.metadata_ or {}
  chosen = {metadata.get("classified_element_id")} | {
    part.get("element_id")
    for part in metadata.get("classified_allocations") or []
    if isinstance(part, dict)
  }
  control = {
    str(a.id) for a in _control_accounts(session, kind, str(payment.entity_id))
  }
  return DischargeCandidates(
    event_id=event_id,
    kind=kind,
    amount_cents=magnitude,
    classified_to_control=bool(chosen & control),
    candidates=sorted(
      items,
      key=lambda i: (i.open_cents != magnitude, i.due_date, i.occurred_on),
    ),
  )
