"""Balance observations: what something outside the ledger said an account
held on a date.

An observation is a ``support`` event, so it writes no books and the inbox
never sees it. Its ``amount`` is a balance, a state at ``effective_at``; every
other event's amount is a flow. It is kept debit-positive, like the ledger
side it is compared with.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Event

BALANCE_OBSERVED_EVENT_TYPE = "balance_observed"
STATEMENT_ENDING = "statement_ending"
# A bank feed's own figures: what the bank says the account holds, and what
# of it can be spent. Current is the reconciliation's number; available is
# recorded for cash accounts only, where it means funds (on a card it is the
# unused limit, which is not a balance).
BANK_CURRENT = "bank_current"
BANK_AVAILABLE = "bank_available"
FEED_BALANCE_KINDS = frozenset({BANK_CURRENT, BANK_AVAILABLE})


@dataclass(frozen=True)
class BalanceObservation:
  event_id: str
  element_id: str
  as_of: date
  amount_cents: int
  document_id: str | None
  note: str | None
  recorded_by: str


def _observation(event: Event) -> BalanceObservation:
  metadata = event.metadata_ or {}
  return BalanceObservation(
    event_id=str(event.id),
    element_id=str(event.resource_element_id),
    as_of=date.fromisoformat(metadata["as_of"]),
    amount_cents=int(event.amount or 0),
    document_id=event.document_id,
    note=metadata.get("note"),
    recorded_by=str(event.created_by),
  )


def _live_statements(element_ids: set[str], start: date, end: date):
  return (
    select(Event)
    .where(
      Event.event_type == BALANCE_OBSERVED_EVENT_TYPE,
      Event.status == "committed",
      Event.metadata_["kind"].astext == STATEMENT_ENDING,
      Event.resource_element_id.in_(element_ids),
      Event.effective_at >= datetime.combine(start, time.min),
      Event.effective_at < datetime.combine(end + timedelta(days=1), time.min),
    )
    .order_by(Event.effective_at.desc(), Event.id.desc())
  )


def statement_observations(
  session: Session, element_ids: frozenset[str], start: date, end: date
) -> dict[str, BalanceObservation]:
  """Each account's latest recorded statement ending within the window."""
  if not element_ids:
    return {}
  latest: dict[str, BalanceObservation] = {}
  for event in session.execute(
    _live_statements(set(element_ids), start, end)
  ).scalars():
    latest.setdefault(str(event.resource_element_id), _observation(event))
  return latest


def record_statement_observation(
  session: Session,
  *,
  element: Element,
  entity_id: str,
  as_of: date,
  stated_cents: int,
  document_id: str | None,
  note: str | None,
  created_by: str,
) -> BalanceObservation:
  """Record a statement's ending balance for an account, on the books of the
  entity whose chart it is in. Flushes.

  ``stated_cents`` is the balance as the statement shows it, positive in the
  account's normal direction. Recording the same account and date again
  supersedes the earlier observation; recording it unchanged returns it.
  """
  amount = stated_cents if element.balance_type == "debit" else -stated_cents
  live = session.execute(
    _live_statements({str(element.id)}, as_of, as_of).limit(1)
  ).scalar_one_or_none()
  if live is not None:
    current = _observation(live)
    if (current.amount_cents, current.document_id, current.note) == (
      amount,
      document_id,
      note,
    ):
      return current
    live.status = "superseded"

  event = Event(
    entity_id=entity_id,
    event_type=BALANCE_OBSERVED_EVENT_TYPE,
    event_category="reconciliation",
    event_class="support",
    resource_type="money",
    resource_element_id=element.id,
    occurred_at=datetime.now(UTC),
    effective_at=datetime.combine(as_of, time.min),
    status="committed",
    source="manual",
    amount=amount,
    description=f"Statement balance: {element.name}, {as_of.isoformat()}",
    replaces_event_id=live.id if live is not None else None,
    document_id=document_id,
    metadata_={
      "kind": STATEMENT_ENDING,
      "as_of": as_of.isoformat(),
      "stated_balance_cents": stated_cents,
      "note": note,
    },
    created_by=created_by,
  )
  session.add(event)
  session.flush()
  if live is not None:
    live.replaced_by_event_id = event.id
    session.flush()
  return _observation(event)


@dataclass(frozen=True)
class FeedBalance:
  """One figure a bank feed reported for an account: ``stated_cents`` as the
  bank states it, positive in the account's normal direction; ``observed_at``
  when the figure was current at the bank; ``as_of`` the day it is recorded
  against."""

  account_id: str
  kind: str
  as_of: date
  stated_cents: int
  observed_at: datetime
  currency: str | None = None


def _live_feed_reading(source: str, kind: str, element_id: str, as_of: date):
  return (
    select(Event)
    .where(
      Event.event_type == BALANCE_OBSERVED_EVENT_TYPE,
      Event.status == "committed",
      Event.source == source,
      Event.metadata_["kind"].astext == kind,
      Event.resource_element_id == element_id,
      Event.effective_at >= datetime.combine(as_of, time.min),
      Event.effective_at < datetime.combine(as_of + timedelta(days=1), time.min),
    )
    .order_by(Event.occurred_at.desc(), Event.id.desc())
    .limit(1)
  )


def _as_utc(stamp: datetime) -> datetime:
  """A stored timestamp (naive, UTC wall time) made comparable."""
  return stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC)


def record_feed_balance(
  session: Session,
  *,
  element: Element,
  entity_id: str,
  source: str,
  connection_id: str,
  reading: FeedBalance,
  created_by: str,
) -> tuple[BalanceObservation, Literal["recorded", "unchanged"]]:
  """Record what a feed said an account held on a day. Flushes.

  One observation per account, kind and day: a later reading of the same
  day with a different figure supersedes the earlier one, so the day keeps
  the reading closest to its close; an unchanged figure, or one no newer
  than the reading already kept (a bank's stamp delivered out of order, a
  replay of a superseded reading), is returned as it stands. The event
  belongs to the entity whose books the account keeps.
  """
  if reading.kind not in FEED_BALANCE_KINDS:
    raise ValueError(f"not a feed balance kind: {reading.kind!r}")
  amount = (
    reading.stated_cents if element.balance_type == "debit" else -reading.stated_cents
  )
  observed_at = reading.observed_at.astimezone(UTC)
  live = session.execute(
    _live_feed_reading(source, reading.kind, str(element.id), reading.as_of)
  ).scalar_one_or_none()
  if live is not None and (
    int(live.amount or 0) == amount or observed_at <= _as_utc(live.occurred_at)
  ):
    return _observation(live), "unchanged"
  if live is not None:
    live.status = "superseded"

  label = "available" if reading.kind == BANK_AVAILABLE else "current"
  event = Event(
    entity_id=entity_id,
    event_type=BALANCE_OBSERVED_EVENT_TYPE,
    event_category="reconciliation",
    event_class="support",
    resource_type="money",
    resource_element_id=element.id,
    occurred_at=observed_at,
    effective_at=datetime.combine(reading.as_of, time.min),
    status="committed",
    source=source,
    external_id=(
      f"{source}_balance_{reading.kind}_{reading.account_id}_"
      f"{observed_at.strftime('%Y%m%dT%H%M%SZ')}"
    ),
    amount=amount,
    description=(
      f"Bank balance ({label}): {element.name}, {reading.as_of.isoformat()}"
    ),
    replaces_event_id=live.id if live is not None else None,
    metadata_={
      "kind": reading.kind,
      "as_of": reading.as_of.isoformat(),
      "observed_at": observed_at.isoformat(),
      "stated_balance_cents": reading.stated_cents,
      "currency": reading.currency,
      "account_id": reading.account_id,
      "connection_id": connection_id,
    },
    created_by=created_by,
  )
  session.add(event)
  session.flush()
  if live is not None:
    live.replaced_by_event_id = event.id
    session.flush()
  return _observation(event), "recorded"
