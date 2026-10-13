"""Moving a calendar's start before its first close, and the rule beneath it.

Earlier seeds open months, later removes empty leading ones, and nothing
moves once a month has closed or when activity would be stranded outside
the calendar. Underneath: nothing is written dated before the entity's first
period, whichever path writes it. Runs against a throwaway schema in the real
test database.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  ReverseJournalEntryRequest,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.models.extensions.roboledger.fiscal_calendar import (
  FiscalCalendarEvent,
)
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  update_event_block,
)
from robosystems.operations.event_block.engine import apply_handler
from robosystems.operations.event_block.promotion import promote_pending_obligations
from robosystems.operations.roboledger.commands._guards import (
  BeforeCalendarStartError,
  baseline_import,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
  reverse_journal_entry,
  update_journal_entry,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CalendarStartBlockedError,
  CalendarStartLockedError,
  InvalidCloseTargetError,
)
from robosystems.operations.roboledger.schedules.service import ScheduleService
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"
_SERVICE = "robosystems.operations.roboledger.fiscal_calendar.service"
# The fixture's calendar starts at 2026-09.
BEFORE_START = date(2026, 8, 31)
INSIDE = date(2026, 9, 15)


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


@pytest.fixture(autouse=True)
def _skip_platform_db_checks(monkeypatch):
  """Source and connection registration live in the platform database; the
  event commit path below is about the tenant schema, so both are stubbed."""
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


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


def _accounts(session) -> tuple[str, str]:
  ids = []
  for code, name in (("6100", "Software"), ("1400", "Prepaid Expenses")):
    element = Element(name=name, code=code, created_by="test")
    session.add(element)
    session.flush()
    ids.append(str(element.id))
  return ids[0], ids[1]


def _schedule(session, start: date, end: date) -> str:
  expense, prepaid = _accounts(session)
  return create_schedule(
    session,
    CreateScheduleRequest(
      name="Annual software prepaid",
      element_ids=[expense, prepaid],
      period_start=start,
      period_end=end,
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=expense, credit_element_id=prepaid
      ),
    ),
    created_by="usr",
  ).structure_id


def test_moving_later_is_refused_while_the_dropped_months_hold_obligations(session):
  """A schedule's pending obligations are activity too: dropping their months
  would leave them to draft into months no close ever posts."""
  _move(session, "2026-01")
  _schedule(session, date(2026, 1, 1), date(2026, 6, 30))
  session.commit()

  with pytest.raises(CalendarStartBlockedError) as refused:
    _move(session, "2026-03")
  assert (refused.value.entries, refused.value.unposted) == (0, 2)
  assert _periods(session)[0][0] == "2026-01"


# ── Nothing is written dated before the first period ─────────────────────


def _lines(debit: str, credit: str) -> list[JournalEntryLineItemInput]:
  return [
    JournalEntryLineItemInput(element_id=debit, debit_amount=10_000, credit_amount=0),
    JournalEntryLineItemInput(element_id=credit, debit_amount=0, credit_amount=10_000),
  ]


def _journal_entry(session, posting_date: date, *, status: str = "draft") -> str:
  expense, prepaid = _accounts(session)
  return create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=posting_date,
      memo="Software",
      line_items=_lines(expense, prepaid),
      status=status,
    ),
    "usr",
  ).id


def _create_journal_entry(session, posting_date: date) -> None:
  _journal_entry(session, posting_date)


def _redate_a_draft(session, posting_date: date) -> None:
  entry_id = _journal_entry(session, INSIDE)
  update_journal_entry(
    session, UpdateJournalEntryRequest(entry_id=entry_id, posting_date=posting_date)
  )


def _reverse_a_posted_entry(session, posting_date: date) -> None:
  entry_id = _journal_entry(session, INSIDE, status="posted")
  reverse_journal_entry(
    session,
    ReverseJournalEntryRequest(entry_id=entry_id, posting_date=posting_date),
    "usr",
  )


def _as_generated_before_the_calendar_rule(session, structure_id: str) -> None:
  """A schedule generated before months ahead of the calendar were history:
  its facts in scope and its obligations pending there. A schedule made now
  retires those months; the write-time guard still covers rows like these."""
  session.execute(
    text(
      "UPDATE facts SET fact_scope = 'in_scope' WHERE structure_id = :sid; "
      "UPDATE events SET status = 'pending' "
      "WHERE event_type = 'schedule_entry_due' "
      "AND metadata->>'schedule_id' = :sid"
    ),
    {"sid": structure_id},
  )


def _schedule_entry(session, posting_date: date) -> None:
  month_start = posting_date.replace(day=1)
  structure_id = _schedule(session, month_start, posting_date)
  _as_generated_before_the_calendar_rule(session, structure_id)
  ScheduleService().create_closing_entry(
    session,
    structure_id=structure_id,
    posting_date=posting_date,
    period_start=month_start,
    period_end=posting_date,
    created_by="usr",
  )


def _manual_closing_entry(session, posting_date: date) -> None:
  expense, prepaid = _accounts(session)
  ScheduleService().create_manual_closing_entry(
    session,
    posting_date=posting_date,
    line_items=[
      {"element_id": expense, "debit_amount": 10_000},
      {"element_id": prepaid, "credit_amount": 10_000},
    ],
    memo="Write-off",
    created_by="usr",
  )


def _tenant_rule(session, posting_date: date) -> None:
  expense, prepaid = _accounts(session)
  event = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="invoice_received",
    event_category="purchase",
    occurred_at=datetime.combine(posting_date, datetime.min.time(), tzinfo=UTC),
    source="native",
    amount=10_000,
    currency="USD",
    metadata_={},
    created_by="usr",
  )
  session.add(event)
  session.flush()
  handler = EventHandler(
    name="Software rule",
    event_type="invoice_received",
    transaction_template={
      "transactions": [
        {
          "entry_template": {
            "debit": {"element_id": expense, "amount": "{{ event.amount }}"},
            "credit": {"element_id": prepaid, "amount": "{{ event.amount }}"},
          }
        }
      ]
    },
  )
  apply_handler(session, event, handler, created_by="usr")


def _commit_an_event(session, posting_date: date) -> None:
  expense, prepaid = _accounts(session)
  event, _envelope = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      event_class="economic",
      source="manual",
      occurred_at=datetime.combine(posting_date, datetime.min.time()),
      amount=10_000,
      apply_handlers=False,
      metadata={
        "posting_date": posting_date.isoformat(),
        "memo": "Software accrual",
        "type": "adjusting",
        "status": "draft",
        "line_items": [
          {"element_id": expense, "debit_amount": 10_000, "credit_amount": 0},
          {"element_id": prepaid, "debit_amount": 0, "credit_amount": 10_000},
        ],
      },
    ),
    "usr",
    graph_id=GRAPH_ID,
  )
  session.flush()
  update_event_block(
    session,
    UpdateEventBlockRequest(event_id=str(event.id), transition_to="committed"),
    "usr",
    graph_id=GRAPH_ID,
  )


WRITE_PATHS: dict[str, Callable[..., None]] = {
  "create_journal_entry": _create_journal_entry,
  "update_journal_entry": _redate_a_draft,
  "reverse_journal_entry": _reverse_a_posted_entry,
  "schedule_closing_entry": _schedule_entry,
  "manual_closing_entry": _manual_closing_entry,
  "tenant_rule": _tenant_rule,
  "event_commit": _commit_an_event,
}


def _entries_before_start(session) -> int:
  return session.query(Entry).filter(Entry.posting_date < date(2026, 9, 1)).count()


@pytest.mark.parametrize("write", WRITE_PATHS.values(), ids=WRITE_PATHS.keys())
def test_nothing_is_written_dated_before_the_first_period(session, write):
  with pytest.raises(BeforeCalendarStartError, match="2026-09"):
    write(session, BEFORE_START)
  assert _entries_before_start(session) == 0


@pytest.mark.parametrize("write", WRITE_PATHS.values(), ids=WRITE_PATHS.keys())
def test_the_same_write_inside_the_calendar_lands(session, write):
  write(session, date(2026, 9, 30))
  assert session.query(Entry).count() >= 1


def test_the_promoter_skips_an_obligation_dated_before_the_first_period(session):
  structure_id = _schedule(session, date(2026, 8, 1), date(2026, 9, 30))
  _as_generated_before_the_calendar_rule(session, structure_id)
  session.commit()

  result = promote_pending_obligations(
    session,
    GRAPH_ID,
    as_of=datetime(2026, 10, 15, tzinfo=UTC),
    dispatch_handlers=True,
  )

  drafted = (
    session.query(Entry.posting_date)
    .filter(Entry.source_structure_id == structure_id)
    .all()
  )
  assert [posting_date for (posting_date,) in drafted] == [date(2026, 9, 30)]
  assert len(result.errors) == 1
  assert "2026-09" in result.errors[0][1]


def test_an_entity_with_no_periods_yet_has_no_start_to_hold_writes_to(session):
  session.query(FiscalPeriod).delete()
  session.commit()

  _create_journal_entry(session, BEFORE_START)
  assert _entries_before_start(session) == 1


def test_a_baseline_import_lands_before_the_start(session):
  """A source ledger's own history arriving on empty books is the books, and
  posts past any calendar already in place."""
  with baseline_import():
    _journal_entry(session, BEFORE_START, status="posted")
  assert _entries_before_start(session) == 1
