"""Which entries publish, decided by real SQL.

The publish predicate is now two rules: an explicit `publish_to_source` on
the event's metadata, falling back to the event's source. That first rule is
a JSONB comparison, and the mocked-session tests next door drive
`session.query(...)` through canned return values — they assert the call
shape, never the answer. A JSONB predicate that silently matches nothing
would pass every one of them and quietly send an alignment entry back to the
source system, applying an upstream change twice.

So this file runs the predicate against Postgres, over the five cases that
distinguish the two rules.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  select_writeback_eligible_entries,
  writeback_eligible_entry_ids,
)

pytestmark = pytest.mark.unit

PERIOD_START = date(2026, 8, 1)
PERIOD_END = date(2026, 8, 31)


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_wb_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  db.commit()
  db.execute(text(f'SET search_path TO "{schema}"'))
  try:
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _draft(db, *, label: str, source: str, metadata: dict) -> str:
  """A draft entry in the period, owned by an event with this metadata."""
  event = Event(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    source=source,
    external_id=label,
    occurred_at=datetime(2026, 8, 15),
    status="classified",
    metadata_=metadata,
    created_by="user_test",
  )
  db.add(event)
  db.flush()
  entry = Entry(
    posting_date=date(2026, 8, 15),
    memo=label,
    status="draft",
    provenance="manual_entry",
    triggered_by_event_id=event.id,
    created_by="user_test",
  )
  db.add(entry)
  db.flush()
  return str(entry.id)


def _publishing(db) -> set[str]:
  return writeback_eligible_entry_ids(db, PERIOD_START, PERIOD_END)


def test_source_decides_when_the_flag_is_absent(session):
  """The rule that held before the flag existed, unchanged."""
  manual = _draft(session, label="manual", source="manual", metadata={})
  schedule = _draft(session, label="schedule", source="schedule", metadata={})
  system = _draft(session, label="system", source="system", metadata={})

  publishing = _publishing(session)

  assert manual in publishing
  assert schedule in publishing
  assert system not in publishing


def test_the_flag_outranks_the_source_in_both_directions(session):
  held_back = _draft(
    session, label="held", source="manual", metadata={"publish_to_source": False}
  )
  sent_anyway = _draft(
    session, label="sent", source="system", metadata={"publish_to_source": True}
  )

  publishing = _publishing(session)

  assert held_back not in publishing
  assert sent_anyway in publishing


def test_a_json_null_flag_falls_back_to_the_source(session):
  """`->>` yields SQL NULL for JSON null and for a missing key alike, so
  both mean "no explicit answer" and defer to the source."""
  explicit_null = _draft(
    session, label="null", source="manual", metadata={"publish_to_source": None}
  )

  assert explicit_null in _publishing(session)


def test_an_alignment_entry_stays_local(session):
  """The shape `resolve-reconciling-item` writes: local source *and* the
  explicit flag. Either alone would do it; both together is the point —
  publishing this entry would apply an upstream change a second time."""
  alignment = _draft(
    session,
    label="alignment",
    source="system",
    metadata={"publish_to_source": False, "reconciles_event_id": "evt_x"},
  )

  assert alignment not in _publishing(session)


def test_entries_already_in_the_source_are_never_resent(session):
  """The other half of the predicate still holds with the flag present."""
  already_there = _draft(
    session,
    label="sent-already",
    source="manual",
    metadata={"publish_to_source": True, "qb_external_id": "JournalEntry_1858"},
  )

  assert already_there not in _publishing(session)


def test_the_row_query_and_the_id_query_agree(session):
  """`select_writeback_eligible_entries` is what close publishes from, and
  `writeback_eligible_entry_ids` is what the outbox preview shows. A
  disagreement between them is a disclosure the close would not honour."""
  _draft(session, label="manual", source="manual", metadata={})
  _draft(session, label="held", source="manual", metadata={"publish_to_source": False})
  _draft(session, label="sent", source="system", metadata={"publish_to_source": True})

  rows = select_writeback_eligible_entries(session, PERIOD_START, PERIOD_END)

  assert {str(entry.id) for entry, _event in rows} == _publishing(session)
