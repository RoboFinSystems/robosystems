"""Shared definition of which RoboLedger-originated drafts publish to
QuickBooks on period close.

Single source of truth for both the close path
(``close_service._publish_drafts_to_qb``, which actually publishes) and
the outbox read (``reads.period_drafts.list_period_drafts``, which
previews what *will* publish). Keeping the predicate here keeps the
preview from drifting from the actual write — if the two diverged, the
outbox would show a disclosure the close doesn't honor.

The predicate has two halves:

- **connection** (platform DB): the graph has a non-deleted QuickBooks
  ``Connection`` whose ``write_policy`` is qb_authoritative / hybrid.
- **eligible drafts** (extensions DB): in-period ``draft`` entries whose
  triggering ``Event`` publishes (see below), is not retracted (``status``
  not ``voided`` / ``superseded``), and is not already in QB (no
  ``qb_external_id``).

A draft publishes on close iff both hold.

Whether an event publishes is decided in two steps, and only here:

1. ``metadata.publish_to_source``, when the event carries it, is the
   answer — ``true`` publishes, ``false`` keeps the entry local.
2. Otherwise ``Event.source`` decides: RL-originated sources
   (``schedule``/``manual``) publish; everything else — synced-in QB
   transactions, which already live in QB, and ``system`` entries — does
   not.

The explicit flag exists because ``source`` alone conflates provenance
with destination. An alignment entry that mirrors a change already made
upstream must not travel back, or the change applies twice; before the
flag the only way to express that was to choose a source the predicate
happened to exclude.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import ColumnElement, Row, and_, or_
from sqlalchemy.orm import Session

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event

# RL-originated event sources whose draft GL rows write back to QB on
# close. Synced-in QB transactions (``source='quickbooks'``) already live
# in QB and are excluded.
WRITEBACK_EVENT_SOURCES = ("schedule", "manual")

# Retracted events keep leftover draft GL rows. Close must not publish
# or locally-post those drafts — void/supersede already said the work
# is off the books.
WRITEBACK_EXCLUDED_EVENT_STATUSES = ("voided", "superseded")

# Connection write policies that publish RL-originated drafts back to the
# source of truth on close (``native`` does not — RoboSystems is the SoR).
WRITEBACK_WRITE_POLICIES = ("qb_authoritative", "hybrid")

# Event metadata key carrying an explicit publish decision, overriding the
# source default in both directions. Written by
# ``JournalEntryRecordedMetadata.publish_to_source``.
PUBLISH_TO_SOURCE_KEY = "publish_to_source"


def writeback_source_clause() -> ColumnElement[bool]:
  """The publish half of the predicate: explicit flag, else source default.

  ``->>`` renders a JSON boolean as the text ``'true'`` / ``'false'``, and
  yields SQL NULL both when the key is absent and when it holds JSON
  ``null`` — so ``IS NULL`` is exactly "no explicit answer", which is the
  case that falls through to ``Event.source``.
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
  """Return the QB connection that close-period would publish to, or None.

  Mirrors the connection selection in
  ``PeriodCloseService._publish_drafts_to_qb``: the most-recently-created
  non-deleted QuickBooks connection on the graph whose ``write_policy`` is
  qb_authoritative / hybrid. Most graphs have at most one; when several
  exist the newest by ``created_at`` wins.
  """
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


def select_writeback_eligible_entries(
  session: Session, period_start: date, period_end: date
) -> list[Row[tuple[Entry, Event]]]:
  """In-period draft entries from publishing events not yet in QB.

  The extensions-side half of the publish predicate, shared by the close
  path (which publishes these) and the outbox read (which previews them).
  Connection ``write_policy`` is checked separately (platform DB) — these
  are the entries that publish *if* a writeback connection exists.
  """
  return (
    session.query(Entry, Event)
    .join(Event, Event.id == Entry.triggered_by_event_id)
    .filter(
      Entry.posting_date >= period_start,
      Entry.posting_date <= period_end,
      Entry.status == "draft",
      writeback_source_clause(),
      Event.status.notin_(WRITEBACK_EXCLUDED_EVENT_STATUSES),
      Event.metadata_["qb_external_id"].astext.is_(None),
    )
    .all()
  )


def writeback_eligible_entry_ids(
  session: Session, period_start: date, period_end: date
) -> set[str]:
  """Entry ids that publish to QB on close — given a writeback connection.

  The outbox read combines this with
  :func:`resolve_writeback_connection`: an entry's ``will_publish_to_qb``
  is true iff it is in this set *and* a writeback connection exists.
  """
  return {
    str(entry.id)
    for entry, _event in select_writeback_eligible_entries(
      session, period_start, period_end
    )
  }
