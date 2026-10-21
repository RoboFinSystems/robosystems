"""A bank line a sync held out of a closed month blocks the next close until
someone decides on it: reopening its month gives it back, and closing over it
records the decision on the line. Runs against a throwaway schema in the real
test database.
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
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  CloseGateFailed,
  PeriodCloseService,
)
from robosystems.operations.roboledger.fiscal_calendar.held_feed_lines import (
  FEED_IN_CLOSED_MONTH,
  hold,
  restore_held_lines,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"
BLOCKER = CloseableGateResult.HELD_BANK_LINES


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_held_{uuid.uuid4().hex[:12]}"
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
    FiscalCalendarService().initialize(
      db, GRAPH_ID, closed_through="2026-06", actor_id="usr_1"
    )
    db.commit()
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _held_line(session, occurred: datetime, *, description="Late deposit") -> str:
  event = Event(
    entity_id=PARENT_ENTITY_ID,
    id=f"evt_{uuid.uuid4().hex[:10]}",
    event_type="bank_transaction",
    event_category="treasury",
    source="plaid",
    external_id=f"plaid_txn_{uuid.uuid4().hex[:8]}",
    status="captured",
    occurred_at=occurred,
    amount=12_500,
    description=description,
    metadata_={"suggested_element_id": "elem_x"},
    created_by="u",
  )
  session.add(event)
  hold(event)
  session.commit()
  return str(event.id)


def _gate(session, period="2026-07", today=date(2026, 8, 15), **overrides):
  return FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, period, today=today, **overrides
  )


def test_a_held_line_blocks_the_next_close_and_is_named(session):
  event_id = _held_line(session, datetime(2026, 6, 28))
  gate = _gate(session)
  assert BLOCKER in gate.blockers
  assert gate.held_bank_line_count == 1
  assert gate.held_bank_line_sample == [f"2026-06-28: Late deposit 125.00 [{event_id}]"]


def test_the_override_lifts_the_gate_but_still_counts_the_lines(session):
  _held_line(session, datetime(2026, 6, 28))
  gate = _gate(session, allow_held_bank_lines=True)
  assert BLOCKER not in gate.blockers
  assert gate.held_bank_line_count == 1


def test_closing_over_held_lines_records_the_decision_on_each(session):
  event_id = _held_line(session, datetime(2026, 6, 28))
  close = PeriodCloseService(FiscalCalendarService())
  with pytest.raises(CloseGateFailed) as refused:
    close.close(
      session,
      GRAPH_ID,
      "2026-07",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )
  assert BLOCKER in refused.value.blockers
  session.rollback()

  result = close.close(
    session,
    GRAPH_ID,
    "2026-07",
    actor_id="usr_1",
    has_sync_connection=False,
    last_sync_at=None,
    allow_held_bank_lines=True,
  )
  session.commit()
  line = session.get(Event, event_id)
  assert line.status == "voided"
  assert line.metadata_["held_acknowledged_in"] == "2026-07"
  assert line.metadata_["held_acknowledged_by"] == "usr_1"
  audit = session.execute(
    text(
      "SELECT note FROM fiscal_calendar_events "
      "WHERE event_type = 'period_closed' AND period = '2026-07'"
    )
  ).scalar_one()
  assert "held-bank-line gate overridden" in audit
  assert result.period == "2026-07"
  # Decided once: the next month is not held by it.
  assert BLOCKER not in _gate(session, "2026-08", today=date(2026, 9, 2)).blockers


def test_a_reopen_gives_back_only_its_months_held_lines(session):
  june = _held_line(session, datetime(2026, 6, 28))
  may = _held_line(session, datetime(2026, 5, 3))

  restored = restore_held_lines(
    session, PARENT_ENTITY_ID, date(2026, 6, 1), date(2026, 6, 30)
  )
  session.commit()

  assert restored == 1
  line = session.get(Event, june)
  assert line.status == "captured"
  assert "void_reason" not in line.metadata_
  assert line.metadata_["suggested_element_id"] == "elem_x"
  still_held = session.get(Event, may)
  assert still_held.status == "voided"
  assert still_held.metadata_["void_reason"] == FEED_IN_CLOSED_MONTH


def test_a_line_voided_for_another_reason_never_blocks(session):
  event_id = _held_line(session, datetime(2026, 6, 28))
  line = session.get(Event, event_id)
  line.metadata_ = {**line.metadata_, "void_reason": "duplicate"}
  session.commit()
  assert BLOCKER not in _gate(session).blockers


def test_reopening_the_month_gives_its_held_lines_back_to_post(session):
  from unittest.mock import MagicMock, patch

  from robosystems.operations.roboledger.commands.fiscal_calendar import (
    reopen_period,
  )

  PeriodCloseService(FiscalCalendarService()).close(
    session,
    GRAPH_ID,
    "2026-07",
    actor_id="usr_1",
    has_sync_connection=False,
    last_sync_at=None,
  )
  session.commit()
  # A sync after the close holds a July line out of July.
  event_id = _held_line(session, datetime(2026, 7, 30))
  with patch(
    "robosystems.operations.roboledger.commands.fiscal_calendar.entity_sync_state",
    return_value=(False, None),
  ):
    result = reopen_period(
      session,
      MagicMock(),
      GRAPH_ID,
      "2026-07",
      actor_id="usr_1",
      reason="late deposit",
      note=None,
      service=FiscalCalendarService(),
    )
  assert result.held_bank_lines_restored == 1
  assert session.get(Event, event_id).status == "captured"
  # Back in its month, it is that month's to post: the reclose waits on it.
  reclose = _gate(session)
  assert BLOCKER not in reclose.blockers
  assert CloseableGateResult.UNPOSTED_SOURCE_EVENTS in reclose.blockers
