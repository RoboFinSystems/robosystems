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
  triggering ``Event`` is RL-originated (``source IN ('schedule',
  'manual')``) and not already in QB (no ``qb_external_id``).

A draft publishes on close iff both hold.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import Row
from sqlalchemy.orm import Session

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event

# RL-originated event sources whose draft GL rows write back to QB on
# close. Synced-in QB transactions (``source='quickbooks'``) already live
# in QB and are excluded.
WRITEBACK_EVENT_SOURCES = ("schedule", "manual")

# Connection write policies that publish RL-originated drafts back to the
# source of truth on close (``native`` does not — RoboSystems is the SoR).
WRITEBACK_WRITE_POLICIES = ("qb_authoritative", "hybrid")


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
  qb_authoritative / hybrid. Most graphs have at most one; v1 picks the
  first by ``created_at`` desc when several exist.
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
  """In-period draft entries from RL-originated events not yet in QB.

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
      Event.source.in_(WRITEBACK_EVENT_SOURCES),
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
