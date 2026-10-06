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

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Event
from robosystems.operations.roboledger.entity_scope import ensure_entity_id

BALANCE_OBSERVED_EVENT_TYPE = "balance_observed"
STATEMENT_ENDING = "statement_ending"


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
    document_id=metadata.get("document_id"),
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
  as_of: date,
  stated_cents: int,
  document_id: str | None,
  note: str | None,
  created_by: str,
) -> BalanceObservation:
  """Record a statement's ending balance for an account. Flushes.

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
    entity_id=ensure_entity_id(session),
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
    metadata_={
      "kind": STATEMENT_ENDING,
      "as_of": as_of.isoformat(),
      "stated_balance_cents": stated_cents,
      "document_id": document_id,
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
