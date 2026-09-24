"""Which RoboLedger-originated drafts publish to QuickBooks on period close.

Shared by the close path (which publishes) and the outbox read (which
previews), so the preview can't drift from the write. A draft publishes iff
the graph has a write-back QB connection (platform DB) and the draft is
eligible (extensions DB): in period, its event not retracted, not already in
QB, and the event publishes. ``metadata.publish_to_source`` decides that
when present; otherwise ``Event.source`` does. The explicit flag exists so an
entry mirroring a change already made upstream can stay local instead of
applying twice.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import ColumnElement, Row, and_, not_, or_
from sqlalchemy.orm import Session

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event

# Synced-in QB transactions already live in QB, so they are not here.
WRITEBACK_EVENT_SOURCES = ("schedule", "manual")

# Retracted events can keep leftover draft rows; close neither publishes
# nor posts them.
WRITEBACK_EXCLUDED_EVENT_STATUSES = ("voided", "superseded")

# ``native`` is absent: there RoboSystems is the system of record.
WRITEBACK_WRITE_POLICIES = ("qb_authoritative", "hybrid")

# Explicit publish decision on the event, overriding the source default
# either way.
PUBLISH_TO_SOURCE_KEY = "publish_to_source"


def writeback_source_clause() -> ColumnElement[bool]:
  """Explicit flag, else source default.

  ``->>`` yields SQL NULL for both an absent key and JSON ``null``, so
  ``IS NULL`` is exactly "no explicit answer".
  """
  flag = Event.metadata_[PUBLISH_TO_SOURCE_KEY].astext
  return or_(
    flag == "true",
    and_(flag.is_(None), Event.source.in_(WRITEBACK_EVENT_SOURCES)),
  )


@dataclass(frozen=True)
class WritebackConnection:
  """The QB connection (platform DB) that close will publish drafts to."""

  connection_id: str
  write_policy: str


def resolve_writeback_connection(
  platform_session: Session, graph_id: str
) -> WritebackConnection | None:
  """The QB connection close publishes to, or None. Newest wins if several."""
  from robosystems.models.core.connection.connection import Connection

  candidate = (
    platform_session.query(Connection)
    .filter(
      Connection.graph_id == graph_id,
      Connection.provider == "quickbooks",
      Connection.write_policy.in_(WRITEBACK_WRITE_POLICIES),
      Connection.deleted_at.is_(None),
    )
    .order_by(Connection.created_at.desc())
    .first()
  )
  if candidate is None:
    return None
  return WritebackConnection(
    connection_id=str(candidate.id),
    write_policy=str(candidate.write_policy),
  )


def _entry_not_yet_in_qb() -> ColumnElement[bool]:
  """The entry has no recorded QuickBooks id.

  Published entries are recorded per entry in ``metadata.qb_entry_ids``. An
  event published before that map existed carries only ``qb_external_id``,
  and all of its entries went together.
  """
  recorded = Event.metadata_["qb_entry_ids"]
  return or_(
    and_(recorded.is_(None), Event.metadata_["qb_external_id"].astext.is_(None)),
    and_(recorded.isnot(None), not_(recorded.has_key(Entry.id))),
  )


def select_writeback_eligible_entries(
  session: Session, period_start: date, period_end: date
) -> list[Row[tuple[Entry, Event]]]:
  """Entries that publish on close *if* a write-back connection exists."""
  return (
    session.query(Entry, Event)
    .join(Event, Event.id == Entry.triggered_by_event_id)
    .filter(
      Entry.posting_date >= period_start,
      Entry.posting_date <= period_end,
      Entry.status == "draft",
      writeback_source_clause(),
      Event.status.notin_(WRITEBACK_EXCLUDED_EVENT_STATUSES),
      _entry_not_yet_in_qb(),
    )
    .all()
  )


def writeback_eligible_entry_ids(
  session: Session, period_start: date, period_end: date
) -> set[str]:
  return {
    str(entry.id)
    for entry, _event in select_writeback_eligible_entries(
      session, period_start, period_end
    )
  }
