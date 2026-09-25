"""Close refuses a month that still holds uncommitted source events.

A captured bank line, or a QuickBooks bill whose automatic posting failed,
can never post once its month closes (commit is fenced out of it). Runs
against a throwaway schema in the real test database.
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
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"
BLOCKER = CloseableGateResult.UNPOSTED_SOURCE_EVENTS


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_gate_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  db.commit()
  db.execute(text(f'SET search_path TO "{schema}"'))
  try:
    fcs = FiscalCalendarService()
    fcs.initialize(db, GRAPH_ID, closed_through="2026-06", actor_id="usr_1")
    db.commit()
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _event(session, *, source, status, occurred, event_type="expense", metadata=None):
  session.add(
    Event(
      id=f"evt_{uuid.uuid4().hex[:10]}",
      event_type=event_type,
      event_category="purchase",
      source=source,
      status=status,
      occurred_at=occurred,
      metadata_=metadata or {},
      created_by="u",
    )
  )
  session.commit()


def _gate(session, **overrides):
  return FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, "2026-07", today=date(2026, 8, 15), **overrides
  )


def test_a_failed_quickbooks_bill_in_the_month_blocks(session):
  _event(
    session,
    source="quickbooks",
    status="captured",
    occurred=datetime(2026, 7, 20),
    metadata={"dispatch_error": "element_unmapped"},
  )
  gate = _gate(session)
  assert BLOCKER in gate.blockers
  assert gate.unposted_source_event_count == 1


def test_a_classified_bank_line_blocks_and_the_override_records_it(session):
  _event(session, source="plaid", status="classified", occurred=datetime(2026, 7, 3))
  assert BLOCKER in _gate(session).blockers

  overridden = _gate(session, allow_unposted_source_events=True)
  assert BLOCKER not in overridden.blockers
  assert overridden.unposted_source_event_count == 1


def test_lines_outside_the_month_and_obligations_do_not_block(session):
  _event(session, source="plaid", status="captured", occurred=datetime(2026, 8, 2))
  _event(session, source="plaid", status="committed", occurred=datetime(2026, 7, 9))
  _event(
    session,
    source="schedule",
    status="classified",
    occurred=datetime(2026, 7, 31),
    event_type="schedule_entry_due",
  )
  gate = _gate(session)
  assert BLOCKER not in gate.blockers
  assert gate.unposted_source_event_count == 0
