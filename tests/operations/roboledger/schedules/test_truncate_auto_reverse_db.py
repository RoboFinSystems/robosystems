"""Truncating an auto-reverse schedule keeps the last kept period's reversal —
real DB, because the deletes are raw SQL.

An auto-reverse accrual for September posts on Sep 30 and its reversal is
drafted for Oct 1. Ending the schedule at Sep 30 keeps September, so that
reversal belongs to a kept period even though it is dated after the cutoff.
Deleting it by date left September's accrual unreversed: the expense counted
twice once the real invoice was booked, and the accrued liability never cleared.
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
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.schedules.service import ScheduleService

pytestmark = pytest.mark.unit

SEP_END = date(2026, 9, 30)
OCT_START, OCT_END = date(2026, 10, 1), date(2026, 10, 31)
NOV_START = date(2026, 11, 1)


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_trunc_{uuid.uuid4().hex[:12]}"
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


def _schedule(session) -> Structure:
  taxonomy = Taxonomy(name="Schedules", taxonomy_type="schedule")
  session.add(taxonomy)
  session.flush()
  structure = Structure(
    name="Accrued services",
    block_type="schedule",
    taxonomy_id=taxonomy.id,
    metadata_={"entry_template": {"auto_reverse": True}},
  )
  session.add(structure)
  session.flush()
  fact_set = create_fact_set(
    session,
    structure_id=structure.id,
    period_end=OCT_END,
    factset_type="report",
    entity_id="ent_1",
    provenance={"origin": "pivot", "mapping_id": "m", "period": "2026-09/2026-10"},
    created_by="usr_test",
  )
  session.flush()
  for start, end in ((date(2026, 9, 1), SEP_END), (OCT_START, OCT_END)):
    session.add(
      Fact(
        element_id="elem_expense",
        value=1000.0,
        period_start=start,
        period_end=end,
        period_type="duration",
        entity_id="ent_1",
        structure_id=structure.id,
        fact_set_id=fact_set.id,
      )
    )
  session.flush()
  return structure


def _entry(session, structure, *, posting_date, status="draft", reversal_of=None):
  entry = Entry(
    posting_date=posting_date,
    status=status,
    type="reversing" if reversal_of else "closing",
    reversal_of=reversal_of,
    source_structure_id=structure.id,
    provenance="schedule_derived",
    created_by="usr_test",
  )
  session.add(entry)
  session.flush()
  return entry


def _surviving_entry_ids(session, structure) -> set[str]:
  rows = session.execute(
    text("SELECT id FROM entries WHERE source_structure_id = :sid"),
    {"sid": structure.id},
  ).fetchall()
  return {r.id for r in rows}


def _truncate_to_september(session, structure):
  ScheduleService().truncate_schedule(
    session,
    structure_id=structure.id,
    new_end_date=SEP_END,
    reason="contract ended",
    updated_by="usr_test",
  )


def test_kept_periods_reversal_survives_truncation(ext_session):
  structure = _schedule(ext_session)
  september = _entry(ext_session, structure, posting_date=SEP_END, status="posted")
  sept_reversal = _entry(
    ext_session, structure, posting_date=OCT_START, reversal_of=september.id
  )
  october = _entry(ext_session, structure, posting_date=OCT_END)
  oct_reversal = _entry(
    ext_session, structure, posting_date=NOV_START, reversal_of=october.id
  )

  _truncate_to_september(ext_session, structure)

  surviving = _surviving_entry_ids(ext_session, structure)
  assert sept_reversal.id in surviving, (
    "September is kept, so its Oct 1 auto-reversal must stay — deleting it "
    "leaves September's accrual unreversed"
  )
  assert september.id in surviving
  assert october.id not in surviving
  assert oct_reversal.id not in surviving


def test_open_kept_period_keeps_its_draft_pair(ext_session):
  """September still open: its accrual and reversal are both drafts, and both
  belong to the kept period."""
  structure = _schedule(ext_session)
  september = _entry(ext_session, structure, posting_date=SEP_END)
  sept_reversal = _entry(
    ext_session, structure, posting_date=OCT_START, reversal_of=september.id
  )

  _truncate_to_september(ext_session, structure)

  assert _surviving_entry_ids(ext_session, structure) == {
    september.id,
    sept_reversal.id,
  }
