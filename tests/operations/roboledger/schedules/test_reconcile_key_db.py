"""The schedule reconcile key, against real SQL.

Two entries can share a `source_structure_id` and a posting-date window and mean
completely different things: the schedule's own entry for the period, and the
auto-reversal generated against the *previous* period's entry (which posts on
the first day of the next one). The reconcile has to tell them apart, and which
column it uses to do that has been wrong in both directions.

- Keyed on nothing, it found last period's reversal, judged it stale because its
  DR/CR are flipped, and deleted it — so the accrual never reversed.
- Keyed on `type != 'reversing'`, it stopped finding the schedule's *own* entry
  whenever the caller authored one with `entry_type="reversing"` — a legal value
  of `EntryType` — and drafted a fresh duplicate on every run.

The discriminator is the reversal link. These tests run the real predicate
against a real schema and pin both directions, because each fix on its own looks
correct and reintroduces the other defect.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.operations.roboledger.entry_status import (
  GENERATED_REVERSAL_SQL,
  PRIMARY_ENTRY_SQL,
)

pytestmark = pytest.mark.unit

STRUCTURE_ID = "struct_sched_01"
JAN_START, JAN_END = date(2026, 1, 1), date(2026, 1, 31)
FEB_START, FEB_END = date(2026, 2, 1), date(2026, 2, 28)

# The reconcile's own predicate, imported rather than retyped so this test
# cannot pass against a copy that has drifted from the query it guards.
RECONCILE_SQL = text(f"""
  SELECT id FROM entries
  WHERE source_structure_id = :sid
    AND {PRIMARY_ENTRY_SQL}
    AND posting_date >= :start AND posting_date <= :end
  ORDER BY created_at DESC LIMIT 1
""")


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_recon_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _entry(session, *, posting_date, type_, reversal_of=None):
  entry = Entry(
    posting_date=posting_date,
    status="draft",
    type=type_,
    reversal_of=reversal_of,
    source_structure_id=STRUCTURE_ID,
    provenance="schedule_derived",
    created_by="usr_test",
  )
  session.add(entry)
  session.flush()
  return entry


def _reconcile(session, start, end):
  return session.execute(
    RECONCILE_SQL, {"sid": STRUCTURE_ID, "start": start, "end": end}
  ).fetchone()


@pytest.mark.parametrize("authored_type", ["closing", "adjusting", "reversing"])
def test_reconcile_finds_the_schedules_own_entry_whatever_its_type(
  ext_session, authored_type
):
  """The regression: `entry_type` is caller-authored and `EntryType` admits
  "reversing", so a type filter makes the reconcile miss the entry it just
  wrote and draft a duplicate on the next run. Closing the period would then
  post both copies."""
  own = _entry(ext_session, posting_date=JAN_END, type_=authored_type)

  found = _reconcile(ext_session, JAN_START, JAN_END)

  assert found is not None, (
    f"a schedule entry typed {authored_type!r} must reconcile as this period's "
    "entry — missing it drafts a duplicate every run"
  )
  assert found.id == own.id


def test_reconcile_ignores_last_periods_generated_reversal(ext_session):
  """The original defect, still fixed. January's auto-reversal posts on 1 Feb
  carrying January's `source_structure_id`, so it sits in February's window. It
  must not be read as February's entry — that judged it stale and deleted it,
  and the accrual never reversed."""
  january = _entry(ext_session, posting_date=JAN_END, type_="closing")
  _entry(ext_session, posting_date=FEB_START, type_="reversing", reversal_of=january.id)

  assert _reconcile(ext_session, FEB_START, FEB_END) is None
  # January still reconciles to its own entry, not to the reversal.
  assert _reconcile(ext_session, JAN_START, JAN_END).id == january.id


def test_generated_reversal_predicate_is_the_exact_complement(ext_session):
  """The two halves must partition the schedule's entries with no row in both
  and none in neither — they are used by queries that assume exactly that."""
  january = _entry(ext_session, posting_date=JAN_END, type_="reversing")
  reversal = _entry(
    ext_session, posting_date=FEB_START, type_="reversing", reversal_of=january.id
  )

  primary = ext_session.execute(
    text(f"SELECT id FROM entries WHERE {PRIMARY_ENTRY_SQL}")
  ).fetchall()
  generated = ext_session.execute(
    text(f"SELECT id FROM entries WHERE {GENERATED_REVERSAL_SQL}")
  ).fetchall()

  assert [r.id for r in primary] == [january.id]
  assert [r.id for r in generated] == [reversal.id]
