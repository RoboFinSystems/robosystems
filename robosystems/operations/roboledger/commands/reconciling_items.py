"""Reconciling items: planning and disposing of upstream changes to posted events.

A sync that finds a posted event's source payload changed does not touch the
books; it flags the event and stashes the payload at ``metadata.drift_payload``.
``plan_reconciling_item`` reports the difference (read-only) and
``resolve_reconciling_item`` carries out a disposition.

Every disposition, including the one that posts nothing, replaces the live
payload with the accepted one: the sync detects drift by comparing live
metadata to the incoming payload, so clearing the flag alone would be re-raised
on the next sync. The trail goes to ``metadata.reconciliation_history``, which
that comparison excludes.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import Select, or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.reconciling_items import (
  PreviewReconcilingItemRequest,
  ReconcilingItemCatchUp,
  ReconcilingItemDeltaLine,
  ReconcilingItemDisposition,
  ReconcilingItemEntrySummary,
  ReconcilingItemPlan,
  ReconcilingItemRegenerated,
  ResolveReconcilingItemRequest,
  ResolveReconcilingItemResponse,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.dimension_junctions import (
  entry_dimensions,
  line_item_dimensions,
  transaction_dimensions,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  fire_handler_on_commit,
)
from robosystems.operations.event_block.qb_writeback import ROUND_TRIP_BASELINE_KEY
from robosystems.operations.locking import (
  RowLockedError,
  bounded_lock_wait,
  ordered_lock_column,
)
from robosystems.operations.roboledger.commands._guards import (
  assert_period_not_closed,
  closed_periods,
)
from robosystems.operations.roboledger.commands.connections import SEVERABLE_SOURCES
from robosystems.operations.roboledger.entity_scope import owner_entity_id
from robosystems.operations.roboledger.fiscal_calendar.periods import period_date_range
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  PUBLISH_TO_SOURCE_KEY,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  FiscalCalendarService,
)

RECONCILIATION_HISTORY_KEY = "reconciliation_history"
# The trail record an item gets when the entry that settled it is retracted.
REOPENED = "reopened"
_RETRACTED_EVENT_STATUSES = ("voided", "superseded")


class ReconcilingItemNotFoundError(LookupError):
  """No event with this id on this graph."""

  def __init__(self, event_id: str) -> None:
    super().__init__(f"Event {event_id} not found")
    self.event_id = event_id


class NotAReconcilingItemError(ValueError):
  """The event is not flagged (including on a repeat call after resolving)."""

  def __init__(self, event_id: str, *, last_disposition: dict | None = None) -> None:
    if last_disposition:
      detail = (
        f" It was already resolved as {last_disposition.get('disposition')!r} "
        f"at {last_disposition.get('resolved_at')} "
        f"by {last_disposition.get('resolved_by')}."
      )
    else:
      detail = ""
    super().__init__(
      f"Event {event_id} is not a reconciling item — its payload matches the "
      f"source system.{detail}"
    )
    self.event_id = event_id
    self.last_disposition = last_disposition


class SettlementNotReopenableError(ValueError):
  """Retracting this entry would leave an item it settled unbooked, and the
  item cannot be put back as it was."""

  def __init__(self, event_id: str, item_id: str) -> None:
    super().__init__(
      f"Event {event_id} settled reconciling item {item_id}, a QuickBooks round "
      "trip that has moved on since, so retracting it would leave that "
      "difference unbooked with no item to show it. Post the correction as a "
      "new entry instead."
    )
    self.event_id = event_id
    self.item_id = item_id


class InvalidSettlementReferenceError(ValueError):
  """The event named as the booking of an item's difference cannot be one."""

  def __init__(self, item_id: str, reference_id: str, reason: str) -> None:
    super().__init__(
      f"Event {reference_id} cannot settle reconciling item {item_id}: {reason}."
    )
    self.item_id = item_id
    self.reference_id = reference_id
    self.reason = reason


class SettlementNotMovableError(ValueError):
  """The event superseding an entry cannot carry the items that entry settled."""

  def __init__(
    self, event_id: str, successor_id: str, item_id: str, reason: str
  ) -> None:
    super().__init__(
      f"Event {event_id} settled reconciling item {item_id}, and {successor_id} "
      f"cannot take that over: {reason}. Supersede it with an entry that books "
      "the same amounts on the item's accounts, or void it, which flags the "
      "item again, and settle the item against the correction."
    )
    self.event_id = event_id
    self.successor_id = successor_id
    self.item_id = item_id
    self.reason = reason


class RestateBlockedError(ValueError):
  """Restate cannot run against these entries; the reasons say why."""

  def __init__(self, event_id: str, blockers: list[str]) -> None:
    super().__init__(
      f"Cannot restate event {event_id}: {'; '.join(blockers)}. "
      f"Use disposition='catch_up' to post the difference in an open period "
      f"instead, or clear the blocker and retry."
    )
    self.event_id = event_id
    self.blockers = blockers


def _entry_specs(payload: dict) -> list[dict]:
  """The entry specs in an accepted payload, whichever shape it uses."""
  entries = payload.get("entries")
  if isinstance(entries, list):
    return [e for e in entries if isinstance(e, dict)]
  # Flat shape: the payload itself is the single entry.
  if payload.get("line_items"):
    return [payload]
  return []


def _net_accepted_lines(payload: dict) -> dict[tuple[str | None, str | None], int]:
  """Net the accepted payload's lines per account, debit-positive."""
  nets: dict[tuple[str | None, str | None], int] = defaultdict(int)
  for spec in _entry_specs(payload):
    for line in spec.get("line_items") or []:
      if not isinstance(line, dict):
        continue
      key = (line.get("element_id"), line.get("element_external_id"))
      nets[key] += int(line.get("debit_amount") or 0) - int(
        line.get("credit_amount") or 0
      )
  return nets


def _net_posted_lines(
  session: Session, entry_ids: list[str]
) -> dict[tuple[str | None, str | None], int]:
  """Net the event's existing line items per account, debit-positive."""
  nets: dict[tuple[str | None, str | None], int] = defaultdict(int)
  if not entry_ids:
    return nets
  rows = session.execute(
    select(LineItem.element_id, LineItem.debit_amount, LineItem.credit_amount).where(
      LineItem.entry_id.in_(entry_ids)
    )
  ).all()
  for element_id, debit, credit in rows:
    nets[(element_id, None)] += int(debit or 0) - int(credit or 0)
  return nets


def _round_trip(accepted: dict) -> dict | None:
  """The round-trip record of a flagged written-back entry, else ``None``."""
  round_trip = accepted.get("round_trip")
  return round_trip if isinstance(round_trip, dict) else None


def _net_shape_lines(shape: dict | None) -> dict[tuple[str | None, str | None], int]:
  """Net a round-trip shape's ``[account, debit, credit]`` lines per QB account."""
  nets: dict[tuple[str | None, str | None], int] = defaultdict(int)
  for external_id, debit, credit in (shape or {}).get("lines") or []:
    nets[(None, external_id)] += int(debit or 0) - int(credit or 0)
  return nets


def _catch_up_entry_ids(session: Session, event_id: str) -> list[str]:
  """Entries already posted to level this event: earlier catch-ups, and any
  reversal of the event's or those catch-ups' entries."""
  catch_up_events = select(Event.id).where(
    Event.metadata_["reconciles_event_id"].astext == event_id,
    Event.status.notin_(("voided", "superseded")),
  )
  own = select(Entry.id).where(Entry.triggered_by_event_id == event_id)
  ids = set(
    session.execute(
      select(Entry.id).where(Entry.triggered_by_event_id.in_(catch_up_events))
    ).scalars()
  )
  ids |= set(
    session.execute(
      select(Entry.id).where(
        Entry.reversal_of.in_(own.union(select(Entry.id).where(Entry.id.in_(ids))))
      )
    ).scalars()
  )
  return sorted(str(i) for i in ids)


def _acknowledgements(metadata: dict) -> list[dict]:
  """The trail's acknowledgements that name the entry which booked them."""
  history = metadata.get(RECONCILIATION_HISTORY_KEY)
  return [
    record
    for record in (history if isinstance(history, list) else [])
    if isinstance(record, dict)
    and record.get("disposition") == "acknowledge"
    and record.get("reference_event_id")
  ]


def _standing_events(session: Session, event_ids: set[str]) -> set[str]:
  """The events still on the books: not voided or superseded, and none of
  their entries reversed."""
  if not event_ids:
    return set()
  live = {
    str(event_id)
    for event_id in session.execute(
      select(Event.id).where(
        Event.id.in_(event_ids), Event.status.notin_(_RETRACTED_EVENT_STATUSES)
      )
    ).scalars()
  }
  if not live:
    return live
  reversed_ = {
    str(event_id)
    for event_id in session.execute(
      select(Entry.triggered_by_event_id).where(
        Entry.triggered_by_event_id.in_(live), Entry.status == "reversed"
      )
    ).scalars()
  }
  return live - reversed_


def _standing_acknowledgements(session: Session, metadata: dict) -> list[dict]:
  """Acknowledgements whose entry still stands: what they booked is on the
  books, the way a catch-up's entry is."""
  records = _acknowledgements(metadata)
  standing = _standing_events(
    session, {str(record["reference_event_id"]) for record in records}
  )
  return [r for r in records if str(r["reference_event_id"]) in standing]


def _acknowledged_nets(
  records: list[dict],
) -> dict[tuple[str | None, str | None], int]:
  """Net the delta each acknowledgement recorded, per account."""
  nets: dict[tuple[str | None, str | None], int] = defaultdict(int)
  for record in records:
    for line in record.get("delta") or []:
      if not isinstance(line, dict):
        continue
      element_id = line.get("element_id")
      key = (
        (element_id, None) if element_id else (None, line.get("element_external_id"))
      )
      nets[key] += int(line.get("delta") or 0)
  return nets


def _settling_record(history: list, event_id: str) -> tuple[int, dict] | None:
  """The latest trail record that ``event_id`` settled the item by: an
  acknowledgement naming it, or the catch-up it is."""
  for position in range(len(history) - 1, -1, -1):
    record = history[position]
    if not isinstance(record, dict):
      continue
    disposition = record.get("disposition")
    if (
      disposition == "acknowledge" and record.get("reference_event_id") == event_id
    ) or (disposition == "catch_up" and record.get("catch_up_event_id") == event_id):
      return position, record
  return None


def _handed_on(history: list, event_id: str) -> bool:
  """Whether the settlement by ``event_id`` already ended: re-opened, or moved
  to the event that superseded it."""
  return any(
    isinstance(r, dict)
    and (
      (r.get("disposition") == REOPENED and r.get("reference_event_id") == event_id)
      or r.get("superseded_event_id") == event_id
    )
    for r in history
  )


def _items_settled_by_query(
  settling: Event | None, event_id: str
) -> Select[tuple[Event]]:
  """Events that ``event_id`` may have settled: named by an acknowledgement,
  or levelled by it as a catch-up."""
  needle = {
    RECONCILIATION_HISTORY_KEY: [
      {"disposition": "acknowledge", "reference_event_id": event_id}
    ]
  }
  levelled = (settling.metadata_ or {}).get("reconciles_event_id") if settling else None
  match = Event.metadata_.contains(needle)
  if levelled:
    match = or_(match, Event.id == str(levelled))
  return select(Event).where(match)


def items_settled_by(session: Session, settling: Event) -> list[str]:
  """The reconciling items ``settling`` settles and still counts toward."""
  event_id = str(settling.id)
  settled: list[str] = []
  for item in session.execute(_items_settled_by_query(settling, event_id)).scalars():
    history = list((item.metadata_ or {}).get(RECONCILIATION_HISTORY_KEY) or [])
    if _settling_record(history, event_id) and not _handed_on(history, event_id):
      settled.append(str(item.id))
  return settled


def _lock_items_settled_by(session: Session, event_id: str) -> list[Event]:
  with bounded_lock_wait(
    session,
    f"A reconciling item settled by {event_id} is being written by another "
    "process. Retry in a moment.",
  ):
    return list(
      session.execute(
        _items_settled_by_query(session.get(Event, event_id), event_id)
        .order_by(ordered_lock_column())
        .with_for_update()
        .execution_options(populate_existing=True)
      )
      .scalars()
      .all()
    )


def _held_upstream(item: Event) -> bool:
  """Whether QuickBooks already holds the change the item reports: a synced
  transaction, or an entry written back and edited there."""
  return str(item.source).lower() in SEVERABLE_SOURCES or bool(
    (item.metadata_ or {}).get("qb_external_id")
  )


def _reference_refusal(
  session: Session, item: Event, reference: Event | None, reference_id: str
) -> str | None:
  """Why ``reference`` cannot be the booking of the item's difference, if it
  cannot. What it books is counted toward the item while it stands."""
  if reference_id == str(item.id):
    return "it is the item itself"
  if reference is None:
    raise ReconcilingItemNotFoundError(reference_id)
  if (reference.metadata_ or {}).get("reconciles_event_id") == str(item.id):
    return "it is the item's own catch-up, which is already counted"
  if owner_entity_id(session, reference) != owner_entity_id(session, item):
    return "it is on another entity's books"
  session.flush()
  if not _standing_events(session, {reference_id}):
    return "it is voided, superseded or reversed"
  if not session.execute(
    select(Entry.id).where(Entry.triggered_by_event_id == reference_id).limit(1)
  ).first():
    return "it books no journal entry"
  return None


def _keep_local(item: Event, reference: Event) -> str | None:
  """An entry that books a change QuickBooks already holds stays local, like a
  catch-up: published, it would apply the change there a second time. Returns
  why it cannot, if it asks to publish."""
  if not _held_upstream(item):
    return None
  metadata = dict(reference.metadata_ or {})
  flag = metadata.get(PUBLISH_TO_SOURCE_KEY)
  if flag is True:
    return (
      "it is set to publish to QuickBooks, which already holds the change "
      "(set publish_to_source to false)"
    )
  if flag is not False:
    reference.metadata_ = {**metadata, PUBLISH_TO_SOURCE_KEY: False}
  return None


def _resolve_element_labels(
  session: Session,
  *,
  element_ids: set[str],
  external_ids: set[str],
  source: str,
  connection_id: str | None,
) -> tuple[dict[str, tuple[str | None, str | None]], dict[str, str]]:
  """Returns ``(labels_by_element_id, element_id_by_external_id)``. External
  ids resolve as the handler resolves them at dispatch, so the preview names
  the accounts the write would hit."""
  by_external: dict[str, str] = {}
  if external_ids:
    stmt = select(Element.id, Element.external_id).where(
      Element.external_source == source,
      Element.external_id.in_(external_ids),
    )
    if connection_id is not None:
      stmt = stmt.where(Element.connection_id == connection_id)
    for element_id, external_id in session.execute(stmt).all():
      by_external[external_id] = element_id

  all_ids = set(element_ids) | set(by_external.values())
  labels: dict[str, tuple[str | None, str | None]] = {}
  if all_ids:
    for element_id, code, name in session.execute(
      select(Element.id, Element.code, Element.name).where(Element.id.in_(all_ids))
    ).all():
      labels[element_id] = (code, name)
  return labels, by_external


def _entry_summaries(
  session: Session, entries: list[Entry]
) -> list[ReconcilingItemEntrySummary]:
  totals: dict[str, tuple[int, int]] = {}
  entry_ids = [str(e.id) for e in entries]
  if entry_ids:
    rows = session.execute(
      select(LineItem.entry_id, LineItem.debit_amount, LineItem.credit_amount).where(
        LineItem.entry_id.in_(entry_ids)
      )
    ).all()
    for entry_id, debit, credit in rows:
      prior_debit, prior_credit = totals.get(entry_id, (0, 0))
      totals[entry_id] = (
        prior_debit + int(debit or 0),
        prior_credit + int(credit or 0),
      )
  summaries = []
  for entry in entries:
    debit, credit = totals.get(str(entry.id), (0, 0))
    summaries.append(
      ReconcilingItemEntrySummary(
        entry_id=str(entry.id),
        external_id=entry.number,
        posting_date=entry.posting_date,
        memo=entry.memo,
        status=str(entry.status),
        total_debit=debit,
        total_credit=credit,
      )
    )
  return summaries


def _accepted_entry_summaries(payload: dict) -> list[ReconcilingItemEntrySummary]:
  summaries = []
  for spec in _entry_specs(payload):
    debit = sum(
      int(line.get("debit_amount") or 0) for line in spec.get("line_items") or []
    )
    credit = sum(
      int(line.get("credit_amount") or 0) for line in spec.get("line_items") or []
    )
    posting_date = spec.get("posting_date")
    if isinstance(posting_date, str):
      try:
        posting_date = date.fromisoformat(posting_date)
      except ValueError:
        posting_date = None
    summaries.append(
      ReconcilingItemEntrySummary(
        entry_id=None,
        external_id=spec.get("external_id"),
        posting_date=posting_date if isinstance(posting_date, date) else None,
        memo=spec.get("memo"),
        status=None,
        total_debit=debit,
        total_credit=credit,
      )
    )
  return summaries


def _load_event(session: Session, event_id: str, *, for_update: bool = False) -> Event:
  query = session.query(Event).filter(Event.id == event_id)
  if for_update:
    query = query.populate_existing().with_for_update()
  event = query.first()
  if event is None:
    raise ReconcilingItemNotFoundError(event_id)
  return event


def _require_flagged(event: Event) -> dict:
  metadata = dict(event.metadata_ or {})
  history = metadata.get(RECONCILIATION_HISTORY_KEY) or []
  if not event.payload_drift:
    last = history[-1] if isinstance(history, list) and history else None
    raise NotAReconcilingItemError(str(event.id), last_disposition=last)
  accepted = metadata.get("drift_payload")
  if not isinstance(accepted, dict):
    raise NotAReconcilingItemError(str(event.id))
  return accepted


def _event_entries(session: Session, event_id: str) -> list[Entry]:
  return (
    session.query(Entry)
    .filter(Entry.triggered_by_event_id == event_id)
    .order_by(Entry.id.asc())
    .all()
  )


def _restate_blockers(
  session: Session, event: Event, entries: list[Entry], accepted: dict
) -> list[str]:
  """Reasons regenerating these entries would be unsafe: a delete that fails
  on a foreign key, takes something else with it, or leaves the event
  disagreeing with its rows."""
  blockers: list[str] = []
  entry_ids = [str(e.id) for e in entries]

  if _round_trip(accepted) is not None:
    blockers.append(
      "this entry was written to QuickBooks and edited there, so QuickBooks "
      "already holds the change — catch up, or acknowledge it as handled"
    )
  elif _catch_up_entry_ids(session, str(event.id)):
    blockers.append(
      "an earlier catch-up entry levels this event, and restating would leave "
      "it counted twice — catch up instead"
    )
  elif any(
    record.get("delta")
    for record in _standing_acknowledgements(session, dict(event.metadata_ or {}))
  ):
    blockers.append(
      "an alignment entry already books part of this change, and restating "
      "would count it twice — catch up instead"
    )

  # The handler never downgrades `fulfilled`, so drafting its entries would
  # leave a fulfilled event over draft rows. Unreachable via QuickBooks (it
  # always posts) but reachable for a source that does not auto-commit.
  if str(event.status) == "fulfilled" and accepted.get("status") == "draft":
    blockers.append(
      "the source now reports this as a draft while the event here is "
      "fulfilled and its entries posted — catch up instead"
    )

  # With no entry in the accepted payload the handler would rebuild the old
  # rows from the event's own columns.
  if not _entry_specs(accepted):
    if accepted.get("source_removed"):
      blockers.append(
        "the source retracted this line — restate would recreate it; catch up "
        "reverses it"
      )
    else:
      blockers.append(
        "the source no longer carries an entry to restate to — catch up posts "
        "the difference"
      )

  closed = _closed_period_names(
    session, [e.posting_date for e in entries], owner_entity_id(session, event)
  )
  if closed:
    blockers.append(
      f"posted in closed period(s) {', '.join(closed)} — reopen first, or catch up"
    )

  not_posted = [str(e.id) for e in entries if str(e.status) != "posted"]
  if not_posted:
    blockers.append(f"some of its entries are not posted ({', '.join(not_posted)})")

  if entry_ids:
    reversed_ids = (
      session.execute(select(Entry.reversal_of).where(Entry.reversal_of.in_(entry_ids)))
      .scalars()
      .all()
    )
    if reversed_ids:
      blockers.append(
        "some of its entries were already reversed "
        f"({', '.join(str(r) for r in reversed_ids)})"
      )

  transaction_ids = {str(e.transaction_id) for e in entries if e.transaction_id}
  if transaction_ids:
    foreign = (
      session.execute(
        select(Entry.id)
        .where(Entry.transaction_id.in_(transaction_ids))
        .where(
          (Entry.triggered_by_event_id != str(event.id))
          | (Entry.triggered_by_event_id.is_(None))
        )
      )
      .scalars()
      .all()
    )
    if foreign:
      blockers.append(
        "other entries share this event's transaction "
        f"({', '.join(str(f) for f in foreign)})"
      )

  if entry_ids and _has_dimension_links(session, entry_ids, transaction_ids):
    blockers.append(
      "entries or their transaction carry dimension assignments, which a "
      "rebuild would drop"
    )

  return blockers


def _has_dimension_links(
  session: Session, entry_ids: list[str], transaction_ids: set[str]
) -> bool:
  line_item_ids = (
    session.execute(select(LineItem.id).where(LineItem.entry_id.in_(entry_ids)))
    .scalars()
    .all()
  )
  checks = [
    select(entry_dimensions.c.entry_id).where(
      entry_dimensions.c.entry_id.in_(entry_ids)
    ),
  ]
  if transaction_ids:
    checks.append(
      select(transaction_dimensions.c.transaction_id).where(
        transaction_dimensions.c.transaction_id.in_(transaction_ids)
      )
    )
  if line_item_ids:
    checks.append(
      select(line_item_dimensions.c.line_item_id).where(
        line_item_dimensions.c.line_item_id.in_([str(i) for i in line_item_ids])
      )
    )
  return any(session.execute(check.limit(1)).first() is not None for check in checks)


def _closed_period_names(
  session: Session, posting_dates: list[date], entity_id: str
) -> list[str]:
  return [
    month for month, _ in closed_periods(session, posting_dates, entity_id=entity_id)
  ]


def _default_catch_up_date(
  session: Session, graph_id: str, entity_id: str
) -> date | None:
  period = FiscalCalendarService()._earliest_open_period(session, graph_id, entity_id)
  if period is None:
    return None
  return period_date_range(period)[1]


def find_unresolved_reconciling_items(
  session: Session, *, as_of: date, entity_id: str | None = None
) -> list[tuple[str, str | None]]:
  """``(event_id, external_id)`` of flagged events with entries posting on or
  before ``as_of``, across the graph or for one entity; read by the close
  gate."""
  query = (
    session.query(Event.id, Event.external_id)
    .join(Entry, Entry.triggered_by_event_id == Event.id)
    .filter(Event.payload_drift.is_(True), Entry.posting_date <= as_of)
  )
  if entity_id is not None:
    query = query.filter(Event.entity_id == entity_id)
  return [
    (str(event_id), external_id)
    for event_id, external_id in query.distinct().order_by(Event.id.asc()).all()
  ]


def plan_reconciling_item(
  session: Session, event_id: str, *, graph_id: str
) -> ReconcilingItemPlan:
  """Describe the difference and what each disposition would do about it.

  Read-only. The netting is per account and debit-positive, so the ``delta``
  column is exactly the entry a catch-up would post.
  """
  plan, _stamp = _plan_with_stamp(session, event_id, graph_id=graph_id)
  return plan


def _plan_with_stamp(
  session: Session, event_id: str, *, graph_id: str
) -> tuple[ReconcilingItemPlan, str | None]:
  """The plan, plus the raw stored ``drift_detected_at`` string.

  The resolver compares the raw string to detect a re-flag; a parsed and
  re-serialized timestamp (``Z`` vs ``+00:00``) would report false re-flags,
  and an unparseable one would block resolution forever.
  """
  event = _load_event(session, event_id)
  accepted = _require_flagged(event)
  metadata = dict(event.metadata_ or {})

  entries = _event_entries(session, str(event.id))
  entry_ids = [str(e.id) for e in entries]
  round_trip = _round_trip(accepted)

  if round_trip is not None:
    # Measured from the version the ledger last accepted, so a second edit in
    # QuickBooks posts only its own change.
    prior_nets = _net_shape_lines(round_trip.get("baseline"))
    accepted_nets = _net_shape_lines(round_trip.get("accepted"))
  else:
    # Everything already booked against this event: its own entries, earlier
    # catch-ups, and what each standing acknowledgement's entry booked.
    prior_nets = _net_posted_lines(
      session, entry_ids + _catch_up_entry_ids(session, str(event.id))
    )
    for key, net in _acknowledged_nets(
      _standing_acknowledgements(session, metadata)
    ).items():
      prior_nets[key] += net
    accepted_nets = _net_accepted_lines(accepted)

  labels, by_external = _resolve_element_labels(
    session,
    element_ids={
      element_id
      for element_id, _ in list(prior_nets) + list(accepted_nets)
      if element_id
    },
    external_ids={
      external_id
      for _, external_id in list(prior_nets) + list(accepted_nets)
      if external_id
    },
    # A written-back entry's accounts are named by their QuickBooks ids.
    source="quickbooks" if round_trip is not None else str(event.source),
    connection_id=accepted.get("connection_id") or metadata.get("connection_id"),
  )

  # Fold both sides onto resolved element ids so the same account nets
  # against itself whichever way each side named it.
  def _fold(
    nets: dict[tuple[str | None, str | None], int],
  ) -> dict[str | None, tuple[int, str | None]]:
    folded: dict[str | None, tuple[int, str | None]] = {}
    for (element_id, external_id), net in nets.items():
      resolved = element_id or (by_external.get(external_id) if external_id else None)
      key = resolved or (f"external:{external_id}" if external_id else None)
      prior_net, prior_external = folded.get(key, (0, None))
      folded[key] = (prior_net + net, prior_external or external_id)
    return folded

  prior_folded = _fold(prior_nets)
  accepted_folded = _fold(accepted_nets)

  delta_lines: list[ReconcilingItemDeltaLine] = []
  for key in sorted(set(prior_folded) | set(accepted_folded), key=lambda k: str(k)):
    prior_net, prior_external = prior_folded.get(key, (0, None))
    accepted_net, accepted_external = accepted_folded.get(key, (0, None))
    if prior_net == accepted_net:
      continue
    element_id = key if key and not str(key).startswith("external:") else None
    code, name = labels.get(element_id or "", (None, None))
    delta_lines.append(
      ReconcilingItemDeltaLine(
        element_id=element_id,
        element_external_id=accepted_external or prior_external,
        element_code=code,
        element_name=name,
        prior_net=prior_net,
        accepted_net=accepted_net,
        delta=accepted_net - prior_net,
      )
    )

  unmapped = sorted(
    {
      external_id
      for _, external_id in accepted_nets
      if external_id and external_id not in by_external
    }
  )

  posting_dates = sorted({e.posting_date for e in entries if e.posting_date})
  entity_id = owner_entity_id(session, event)
  closed_periods = _closed_period_names(session, posting_dates, entity_id)
  blockers = _restate_blockers(session, event, entries, accepted)

  drift_detected_at = metadata.get("drift_detected_at")
  if isinstance(drift_detected_at, str):
    try:
      drift_detected_at = datetime.fromisoformat(drift_detected_at)
    except ValueError:
      drift_detected_at = None

  raw_stamp = metadata.get("drift_detected_at")
  plan = ReconcilingItemPlan(
    event_id=str(event.id),
    external_id=event.external_id,
    source=str(event.source),
    event_type=str(event.event_type),
    event_status=str(event.status),
    drift_detected_at=drift_detected_at
    if isinstance(drift_detected_at, datetime)
    else None,
    # Never default to a disposition the plan already knows is blocked.
    default_disposition="catch_up" if (closed_periods or blockers) else "restate",
    default_posting_date=_default_catch_up_date(session, graph_id, entity_id),
    affected_posting_dates=posting_dates,
    closed_periods=closed_periods,
    prior_entries=_entry_summaries(session, entries),
    accepted_entries=_accepted_entry_summaries(accepted),
    delta=delta_lines,
    no_gl_effect=not delta_lines,
    restate_blockers=blockers,
    unmapped_element_external_ids=unmapped,
  )
  return plan, raw_stamp if isinstance(raw_stamp, str) else None


def preview_reconciling_item(
  session: Session, body: PreviewReconcilingItemRequest, *, graph_id: str
) -> ReconcilingItemPlan:
  """Preview a reconciling item's resolution. Writes nothing."""
  return plan_reconciling_item(session, body.event_id, graph_id=graph_id)


def _accepted_metadata(
  *, live: dict, accepted: dict, record: dict[str, Any]
) -> dict[str, Any]:
  """The accepted payload verbatim plus the disposition trail. Nothing is kept
  from the live payload: a key the adapter does not send would re-raise the
  item on every sync."""
  history = live.get(RECONCILIATION_HISTORY_KEY)
  history = list(history) if isinstance(history, list) else []
  history.append(record)
  round_trip = _round_trip(accepted)
  if round_trip is not None:
    # A written-back event keeps its own payload and QuickBooks markers; what
    # it accepts is the new baseline its next round trip is measured from.
    kept = {
      k: v for k, v in live.items() if k not in ("drift_payload", "drift_detected_at")
    }
    baselines = dict(kept.get(ROUND_TRIP_BASELINE_KEY) or {})
    baselines[str(round_trip["qb_id"])] = round_trip.get("accepted")
    return {
      **kept,
      ROUND_TRIP_BASELINE_KEY: baselines,
      RECONCILIATION_HISTORY_KEY: history,
    }
  return {**accepted, RECONCILIATION_HISTORY_KEY: history}


def _delete_event_gl_rows(
  session: Session, event_id: str, entry_ids: list[str]
) -> None:
  """Remove the event's line items, entries, and transactions, in FK order."""
  if entry_ids:
    session.query(LineItem).filter(LineItem.entry_id.in_(entry_ids)).delete(
      synchronize_session=False
    )
  session.query(Entry).filter(Entry.triggered_by_event_id == event_id).delete(
    synchronize_session=False
  )
  session.query(Transaction).filter(
    Transaction.triggered_by_event_id == event_id
  ).delete(synchronize_session=False)
  session.flush()


def resolve_reconciling_item(
  session: Session,
  body: ResolveReconcilingItemRequest,
  created_by: str,
  *,
  graph_id: str,
) -> ResolveReconcilingItemResponse:
  """Dispose of one reconciling item and clear its flag. Does not commit.

  Takes the period fence before any row lock (the order close uses).
  """
  plan, planned_stamp = _plan_with_stamp(session, body.event_id, graph_id=graph_id)
  # The preview the operator decided on, not only the one this request built.
  if (
    "expected_drift_detected_at" in body.model_fields_set
    and body.expected_drift_detected_at != plan.drift_detected_at
  ):
    raise RowLockedError(
      f"Event {body.event_id} was re-flagged with a newer payload after it was "
      f"previewed. Preview it again."
    )
  disposition: ReconcilingItemDisposition = body.disposition or plan.default_disposition

  catch_up_date: date | None = None
  if disposition == "catch_up":
    catch_up_date = body.posting_date or plan.default_posting_date
    if catch_up_date is None:
      raise ValueError(
        "No open period to post a catch-up entry into — pass posting_date "
        "explicitly, or initialize the fiscal calendar."
      )

  # Fence every period this touches before locking any row.
  entity_id = owner_entity_id(session, _load_event(session, body.event_id))
  fence_dates = list(plan.affected_posting_dates)
  if catch_up_date is not None:
    fence_dates.append(catch_up_date)
  if disposition == "restate":
    fence_dates.extend(
      summary.posting_date
      for summary in plan.accepted_entries
      if summary.posting_date is not None
    )
    assert_period_not_closed(session, *fence_dates, entity_id=entity_id)
  elif catch_up_date is not None:
    assert_period_not_closed(session, catch_up_date, entity_id=entity_id)

  with bounded_lock_wait(
    session,
    f"Event {body.event_id} is being written by another process. Retry in a moment.",
  ):
    event = _load_event(session, body.event_id, for_update=True)
  accepted = _require_flagged(event)
  live = dict(event.metadata_ or {})

  # A newer payload flagged since planning is one the operator never saw.
  if live.get("drift_detected_at") != planned_stamp:
    raise RowLockedError(
      f"Event {body.event_id} was re-flagged with a newer payload while this "
      f"resolution was being prepared. Preview it again."
    )

  entries = _event_entries(session, str(event.id))
  entry_ids = [str(e.id) for e in entries]
  if entry_ids:
    with bounded_lock_wait(
      session,
      f"Entries for event {body.event_id} are being written by another "
      "process. Retry in a moment.",
    ):
      session.query(Entry).filter(Entry.id.in_(entry_ids)).order_by(
        Entry.id.asc()
      ).populate_existing().with_for_update().all()

  now = datetime.now(UTC)
  record: dict[str, Any] = {
    "disposition": disposition,
    "resolved_at": now.isoformat(),
    "resolved_by": created_by,
    "drift_detected_at": live.get("drift_detected_at"),
    "note": body.note,
    "delta": [line.model_dump(mode="json") for line in plan.delta],
    "prior_entries": [
      summary.model_dump(mode="json") for summary in plan.prior_entries
    ],
  }
  dispatch_keys = {k: v for k, v in live.items() if k.startswith("dispatch_")}
  if dispatch_keys:
    record["prior_dispatch_state"] = dispatch_keys

  catch_up: ReconcilingItemCatchUp | None = None
  regenerated: ReconcilingItemRegenerated | None = None

  if disposition == "restate":
    blockers = _restate_blockers(session, event, entries, accepted)
    if blockers:
      raise RestateBlockedError(str(event.id), blockers)
    _delete_event_gl_rows(session, str(event.id), entry_ids)
    # Written before dispatch (the handler reads it) and again below with the
    # rebuilt ids; both build from the pre-mutation `live`, so the second
    # replaces rather than appends.
    event.metadata_ = _accepted_metadata(live=live, accepted=accepted, record=record)
    fire_handler_on_commit(session, event, created_by)
    session.flush()
    rebuilt = session.execute(
      select(Entry.id, Entry.transaction_id).where(
        Entry.triggered_by_event_id == str(event.id)
      )
    ).all()
    regenerated = ReconcilingItemRegenerated(
      entry_ids=[str(entry_id) for entry_id, _ in rebuilt],
      transaction_ids=sorted(
        {str(txn_id) for _, txn_id in rebuilt if txn_id is not None}
      ),
    )
    record["regenerated"] = regenerated.model_dump(mode="json")
    event.metadata_ = _accepted_metadata(live=live, accepted=accepted, record=record)

  elif disposition == "catch_up":
    assert catch_up_date is not None
    if not plan.no_gl_effect:
      if plan.unmapped_element_external_ids:
        raise ValueError(
          "Cannot post a catch-up entry: no mapping for source account(s) "
          f"{', '.join(plan.unmapped_element_external_ids)}. Map them first."
        )
      catch_up_event, _envelope = create_event_block_in_session(
        session,
        _catch_up_request(
          event=event,
          plan=plan,
          posting_date=catch_up_date,
          status=body.status,
          note=body.note,
        ),
        created_by,
        graph_id=graph_id,
        entity_id=entity_id,
      )
      session.flush()
      created = session.execute(
        select(Entry.id, Entry.transaction_id).where(
          Entry.triggered_by_event_id == str(catch_up_event.id)
        )
      ).first()
      catch_up = ReconcilingItemCatchUp(
        event_id=str(catch_up_event.id),
        entry_id=str(created[0]) if created else None,
        transaction_id=str(created[1]) if created and created[1] else None,
        posting_date=catch_up_date,
        status=body.status,
      )
      record["catch_up_event_id"] = str(catch_up_event.id)
      if _round_trip(accepted) is not None:
        record["settled_payload"] = accepted
    else:
      record["catch_up_event_id"] = None
    event.metadata_ = _accepted_metadata(live=live, accepted=accepted, record=record)

  else:  # acknowledge
    if body.reference_event_id is not None:
      reference = session.get(Event, body.reference_event_id)
      refusal = _reference_refusal(session, event, reference, body.reference_event_id)
      if refusal is None and reference is not None:
        refusal = _keep_local(event, reference)
      if refusal:
        raise InvalidSettlementReferenceError(
          str(event.id), body.reference_event_id, refusal
        )
      record["reference_event_id"] = body.reference_event_id
      if _round_trip(accepted) is not None:
        # Accepting moves the round trip's baseline; retracting the entry
        # has to move it back (`reopen_items_settled_by`).
        record["settled_payload"] = accepted
    event.metadata_ = _accepted_metadata(live=live, accepted=accepted, record=record)

  event.payload_drift = False
  session.flush()

  return ResolveReconcilingItemResponse(
    event_id=str(event.id),
    external_id=event.external_id,
    disposition=disposition,
    delta=plan.delta,
    no_gl_effect=plan.no_gl_effect,
    catch_up=catch_up,
    regenerated=regenerated,
    reference_event_id=body.reference_event_id,
    note=body.note,
    resolved_at=now,
    resolved_by=created_by,
  )


def reopen_items_settled_by(
  session: Session, event_id: str, reopened_by: str, *, reason: str
) -> list[str]:
  """Flag again every reconciling item ``event_id`` settled, as an
  acknowledged entry or as a catch-up, now that it is voided or reversed.
  Does not commit.

  The settlement stops counting once its entry is retracted, so an item
  flagged with the payload it accepted plans the difference that entry had
  booked. The stamp moves either way, so a preview taken while it still
  counted is refused. A ``reopened`` record ends the trail until the item is
  resolved again; the sync reads it as a difference still owed.
  """
  items = _lock_items_settled_by(session, event_id)

  now = datetime.now(UTC).isoformat()
  reopened: list[str] = []
  for item in items:
    live = dict(item.metadata_ or {})
    history = list(live.get(RECONCILIATION_HISTORY_KEY) or [])
    found = _settling_record(history, event_id)
    if found is None or _handed_on(history, event_id):
      continue
    position, record = found

    if live.get("qb_external_id"):
      # A round trip is measured from a stored baseline, which accepting
      # moved: put it back. Exact only while nothing has been decided since.
      settled = record.get("settled_payload")
      trip = settled.get("round_trip") if isinstance(settled, dict) else None
      decided_since = any(
        isinstance(r, dict) and r.get("disposition") != REOPENED
        for r in history[position + 1 :]
      )
      if not isinstance(trip, dict) or decided_since:
        raise SettlementNotReopenableError(event_id, str(item.id))
      qb_id = str(trip["qb_id"])
      baselines = dict(live.get(ROUND_TRIP_BASELINE_KEY) or {})
      baselines[qb_id] = trip.get("baseline")
      live[ROUND_TRIP_BASELINE_KEY] = baselines
      pending = live.get("drift_payload") if item.payload_drift else None
      if pending is None:
        live["drift_payload"] = settled
      else:
        pending_trip = pending.get("round_trip") if isinstance(pending, dict) else None
        if (
          not isinstance(pending_trip, dict) or str(pending_trip.get("qb_id")) != qb_id
        ):
          raise SettlementNotReopenableError(event_id, str(item.id))
        live["drift_payload"] = {
          **pending,
          "round_trip": {**pending_trip, "baseline": trip.get("baseline")},
        }
    elif not item.payload_drift:
      live["drift_payload"] = {
        k: v
        for k, v in live.items()
        if k not in ("drift_payload", "drift_detected_at", RECONCILIATION_HISTORY_KEY)
      }

    history.append(
      {
        "disposition": REOPENED,
        "reopened_at": now,
        "reopened_by": reopened_by,
        "reference_event_id": event_id,
        "reason": reason,
      }
    )
    live[RECONCILIATION_HISTORY_KEY] = history
    live["drift_detected_at"] = now
    item.metadata_ = live
    item.payload_drift = True
    reopened.append(str(item.id))

  session.flush()
  return reopened


def _event_nets(
  session: Session, event_id: str
) -> dict[tuple[str | None, str | None], int]:
  return _net_posted_lines(
    session,
    [
      str(entry_id)
      for entry_id in session.execute(
        select(Entry.id).where(Entry.triggered_by_event_id == event_id)
      ).scalars()
    ],
  )


def move_items_settled_by(
  session: Session, event_id: str, successor_id: str, moved_by: str
) -> list[str]:
  """Hand every reconciling item ``event_id`` settled to ``successor_id``, the
  event superseding it. Does not commit.

  The successor books what the original did, so each item stays settled and
  counted, now against the successor, and the trail records the move. A
  successor that books a different amount on an item's accounts refuses the
  supersede: counted at the original's figure it would misstate the books,
  and re-opening the item would leave what it books uncounted.
  """
  items = _lock_items_settled_by(session, event_id)
  if not items:
    return []
  successor = session.get(Event, successor_id)
  original_nets = _event_nets(session, event_id)
  successor_nets = _event_nets(session, successor_id)

  now = datetime.now(UTC).isoformat()
  moved: list[str] = []
  for item in items:
    live = dict(item.metadata_ or {})
    history = list(live.get(RECONCILIATION_HISTORY_KEY) or [])
    found = _settling_record(history, event_id)
    if found is None or _handed_on(history, event_id):
      continue
    _position, record = found

    refusal = _reference_refusal(session, item, successor, successor_id)
    if refusal is None:
      for line in record.get("delta") or []:
        element_id = line.get("element_id") if isinstance(line, dict) else None
        key = (element_id, None)
        if element_id and original_nets.get(key, 0) != successor_nets.get(key, 0):
          refusal = (
            "it books a different amount on "
            f"{line.get('element_name') or line.get('element_code') or element_id}"
          )
          break
    if refusal is None and successor is not None:
      refusal = _keep_local(item, successor)
    if refusal:
      raise SettlementNotMovableError(event_id, successor_id, str(item.id), refusal)

    history.append(
      {
        **{
          key: record[key]
          for key in ("drift_detected_at", "delta", "prior_entries", "settled_payload")
          if key in record
        },
        "disposition": "acknowledge",
        "resolved_at": now,
        "resolved_by": moved_by,
        "note": f"Settled by {successor_id}, which superseded {event_id}",
        "reference_event_id": successor_id,
        "superseded_event_id": event_id,
      }
    )
    live[RECONCILIATION_HISTORY_KEY] = history
    item.metadata_ = live
    moved.append(str(item.id))

  session.flush()
  return moved


def _catch_up_request(
  *,
  event: Event,
  plan: ReconcilingItemPlan,
  posting_date: date,
  status: str,
  note: str | None,
) -> CreateEventBlockRequest:
  """The alignment entry that levels the books without touching history. Never
  published: it mirrors a change the source already made."""
  line_items = []
  for line in plan.delta:
    if line.delta == 0 or line.element_id is None:
      continue
    if line.delta > 0:
      line_items.append(
        {
          "element_id": line.element_id,
          "debit_amount": line.delta,
          "credit_amount": 0,
          "description": line.element_name or line.element_code,
        }
      )
    else:
      line_items.append(
        {
          "element_id": line.element_id,
          "debit_amount": 0,
          "credit_amount": -line.delta,
          "description": line.element_name or line.element_code,
        }
      )

  label = event.external_id or str(event.id)
  memo = f"Reconciling item: {event.source} {label} changed after posting"
  if note:
    memo = f"{memo} — {note}"

  return CreateEventBlockRequest(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    event_action="transfer",
    source="system",
    occurred_at=datetime.combine(posting_date, datetime.min.time()),
    description=memo,
    apply_handlers=True,
    metadata={
      "posting_date": posting_date.isoformat(),
      "memo": memo,
      "type": "adjusting",
      "status": status,
      "line_items": line_items,
      PUBLISH_TO_SOURCE_KEY: False,
      "reconciles_event_id": str(event.id),
    },
  )
