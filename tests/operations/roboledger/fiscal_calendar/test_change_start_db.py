"""Moving a calendar's start before its first close.

Earlier seeds open months, later removes empty leading ones, and nothing
moves once a month has closed or when activity would be stranded outside
the calendar. Runs against a throwaway schema in the real test database.
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
from robosystems.models.extensions.roboledger.fiscal_calendar import (
  FiscalCalendarEvent,
)
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CalendarStartBlockedError,
  CalendarStartLockedError,
  InvalidCloseTargetError,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"
_SERVICE = "robosystems.operations.roboledger.fiscal_calendar.service"


@pytest.fixture()
def session(monkeypatch):
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  monkeypatch.setattr(f"{_SERVICE}.current_month_period", lambda: "2026-10")
  schema = f"ext_start_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
  db.commit()
  db.execute(text(f'SET search_path TO "{schema}"'))
  try:
    fcs = FiscalCalendarService()
    fcs.initialize(db, GRAPH_ID, actor_id="usr_1")
    fcs.ensure_fiscal_periods(
      db, GRAPH_ID, start_period="2026-09", end_period="2026-10"
    )
    db.commit()
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _periods(session) -> list[tuple[str, str]]:
  return [
    (row.name, row.status)
    for row in session.query(FiscalPeriod).order_by(FiscalPeriod.start_date)
  ]


def _move(session, period: str):
  result = FiscalCalendarService().change_start(
    session, GRAPH_ID, period, actor_id="usr_1"
  )
  session.commit()
  return result


def test_moving_earlier_seeds_open_months_and_records_the_move(session):
  _, created, removed = _move(session, "2026-02")

  assert (created, removed) == (7, 0)
  periods = _periods(session)
  assert [name for name, _ in periods][:2] == ["2026-02", "2026-03"]
  assert {status for _, status in periods} == {"open"}
  assert len(periods) == 9
  event = session.query(FiscalCalendarEvent).filter_by(event_type="start_changed").one()
  assert (event.from_value, event.to_value) == ("2026-09", "2026-02")


def test_the_new_first_month_is_the_one_the_close_expects(session):
  _move(session, "2026-02")
  gate = FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, "2026-03", today=date(2026, 10, 9)
  )
  assert "sequence_violation" in gate.blockers
  gate = FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, "2026-02", today=date(2026, 10, 9)
  )
  assert "sequence_violation" not in gate.blockers


def test_moving_later_removes_empty_leading_months(session):
  _move(session, "2026-02")
  _, created, removed = _move(session, "2026-08")

  assert (created, removed) == (0, 6)
  assert [name for name, _ in _periods(session)] == ["2026-08", "2026-09", "2026-10"]


def test_moving_later_is_refused_while_the_dropped_months_hold_activity(session):
  _move(session, "2026-02")
  session.add(
    Entry(
      entity_id=PARENT_ENTITY_ID,
      posting_date=date(2026, 3, 18),
      status="draft",
      type="standard",
      provenance="manual_entry",
      created_by="usr",
    )
  )
  session.add(
    Event(
      entity_id=PARENT_ENTITY_ID,
      event_type="bank_transaction",
      event_category="sales",
      source="plaid",
      status="captured",
      occurred_at=datetime(2026, 4, 2),
      created_by="usr",
    )
  )
  session.commit()

  with pytest.raises(CalendarStartBlockedError) as refused:
    _move(session, "2026-09")
  assert (refused.value.entries, refused.value.unposted) == (1, 1)
  assert _periods(session)[0][0] == "2026-02"


def test_nothing_moves_once_a_month_has_closed(session):
  _move(session, "2026-02")
  session.query(FiscalPeriod).filter_by(name="2026-02").one().status = "closed"
  session.commit()

  with pytest.raises(CalendarStartLockedError):
    _move(session, "2026-01")


def test_the_same_month_is_a_no_op_and_a_future_month_is_refused(session):
  calendar, created, removed = _move(session, "2026-09")
  assert (created, removed) == (0, 0)
  assert (
    session.query(FiscalCalendarEvent).filter_by(event_type="start_changed").count()
    == 0
  )

  with pytest.raises(InvalidCloseTargetError):
    _move(session, "2026-11")
