"""Nothing is written dated outside an entity's calendar: where the calendar
is first set up, where a schedule started before it, while its start moves,
and between an event's preview and its commit.

Runs against throwaway schemas in the real test database. Each test gets its
own graph id too: the period fence is a database-wide advisory lock keyed by
graph and month, so a shared id would contend with other suites.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.api.extensions.fiscal_calendar import InitializeLedgerRequest
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  RebuildScheduleRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  preview_event_block,
  update_event_block,
)
from robosystems.operations.event_block.promotion import promote_pending_obligations
from robosystems.operations.roboledger.commands._guards import (
  BeforeCalendarStartError,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  initialize_ledger,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule,
  rebuild_schedule,
)
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CalendarStartBlockedError,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

_SERVICE = "robosystems.operations.roboledger.fiscal_calendar.service"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
TODAY = date(2026, 10, 2)


@dataclass
class Ledger:
  session: Session
  graph_id: str
  new_session: Callable[[], Session]

  def start_calendar(self, first_period: str) -> None:
    fcs = FiscalCalendarService()
    fcs.initialize(self.session, self.graph_id, actor_id="usr_1")
    fcs.ensure_fiscal_periods(
      self.session, self.graph_id, start_period=first_period, end_period="2026-10"
    )
    self.session.commit()

  def set_up(self, **request) -> None:
    """The setup every first-calendar path runs (REST, MCP, the app)."""
    initialize_ledger(
      self.session,
      None,  # type: ignore[arg-type]  (sync state is stubbed below)
      self.graph_id,
      InitializeLedgerRequest(**request),
      "usr_1",
      FiscalCalendarService(),
    )


@pytest.fixture()
def ledger(monkeypatch) -> Iterator[Ledger]:
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  monkeypatch.setattr(f"{_SERVICE}.current_month_period", lambda: "2026-10")
  monkeypatch.setattr(f"{_COMMANDS}.current_month_period", lambda: "2026-10")
  monkeypatch.setattr(
    f"{_COMMANDS}.entity_sync_state", lambda *args, **kwargs: (False, None)
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )

  suffix = uuid.uuid4().hex[:12]
  schema = f"ext_bounds_{suffix}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  factory = sessionmaker(bind=engine)
  opened: list[Session] = []

  def new_session() -> Session:
    s = factory()
    # A safety net: nothing here may wait on a lock for long.
    s.execute(text("SET lock_timeout = '8s'"))
    s.execute(text(f'SET search_path TO "{schema}"'))
    opened.append(s)
    return s

  db = new_session()
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
  db.commit()
  db.execute(text(f'SET search_path TO "{schema}"'))
  try:
    yield Ledger(session=db, graph_id=f"kgbounds{suffix}", new_session=new_session)
  finally:
    for s in opened:
      s.rollback()
      s.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _accounts(session: Session) -> tuple[str, str]:
  ids = []
  for code, name in (("6100", "Software"), ("1400", "Prepaid Expenses")):
    element = Element(name=name, code=code, created_by="test")
    session.add(element)
    session.flush()
    ids.append(str(element.id))
  return ids[0], ids[1]


def _draft(session: Session, posting_date: date, accounts: tuple[str, str]) -> None:
  debit, credit = accounts
  create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=posting_date,
      memo="Accrual",
      line_items=[
        JournalEntryLineItemInput(element_id=debit, debit_amount=500, credit_amount=0),
        JournalEntryLineItemInput(element_id=credit, debit_amount=0, credit_amount=500),
      ],
      status="draft",
    ),
    "usr",
  )


def _captured_line(session: Session, on: date, *, source: str = "plaid") -> None:
  session.add(
    Event(
      entity_id=PARENT_ENTITY_ID,
      event_type="bank_transaction",
      event_category="sales",
      source=source,
      status="captured",
      occurred_at=datetime.combine(on, datetime.min.time()),
      created_by="usr",
    )
  )
  session.flush()


def _schedule(session: Session, start: date) -> str:
  expense, prepaid = _accounts(session)
  return create_schedule(
    session,
    CreateScheduleRequest(
      name="Annual software prepaid",
      element_ids=[expense, prepaid],
      period_start=start,
      period_end=date(2027, 5, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=expense, credit_element_id=prepaid
      ),
    ),
    created_by="usr",
  ).structure_id


def _obligations(session: Session, structure_id: str) -> dict[str, str]:
  """``{period_end month: status}`` for the schedule's obligations."""
  rows = (
    session.query(Event)
    .filter(
      Event.event_type == "schedule_entry_due",
      Event.metadata_["schedule_id"].astext == structure_id,
    )
    .all()
  )
  return {row.metadata_["period_end"][:7]: row.status for row in rows}


def _calendar_initialized(session: Session) -> bool:
  return (
    session.query(FiscalCalendar)
    .filter(FiscalCalendar.initialized_at.isnot(None))
    .count()
    > 0
  )


def _first_close_gate(ledger: Ledger, period: str = "2026-09"):
  return FiscalCalendarService().closeable_gate(
    ledger.session, ledger.graph_id, period, today=TODAY
  )


# ── BCF-1: the first calendar holds to the rule change_start keeps ───────


def test_setup_is_refused_while_a_draft_sits_before_the_first_open_month(ledger):
  _draft(ledger.session, date(2026, 8, 15), _accounts(ledger.session))
  ledger.session.commit()

  with pytest.raises(CalendarStartBlockedError) as refused:
    ledger.set_up(earliest_data_period="2026-09")
  ledger.session.rollback()

  assert (refused.value.entries, refused.value.unposted) == (1, 0)
  assert "2026-08" in str(refused.value)
  assert not _calendar_initialized(ledger.session)


def test_setup_closed_through_a_draft_is_refused_the_same_way(ledger):
  """A month on or before ``closed_through`` is closed at setup: a draft in
  it would never post either."""
  _draft(ledger.session, date(2026, 8, 15), _accounts(ledger.session))
  ledger.session.commit()

  with pytest.raises(CalendarStartBlockedError):
    ledger.set_up(closed_through="2026-08")
  ledger.session.rollback()
  assert not _calendar_initialized(ledger.session)


def test_setup_is_refused_while_a_bank_line_sits_before_the_first_open_month(ledger):
  _captured_line(ledger.session, date(2026, 7, 3))
  ledger.session.commit()

  with pytest.raises(CalendarStartBlockedError) as refused:
    ledger.set_up(earliest_data_period="2026-09")
  ledger.session.rollback()
  assert (refused.value.entries, refused.value.unposted) == (0, 1)


def test_a_clean_entity_sets_up_its_calendar(ledger):
  """Controls: a draft inside the calendar, and a QuickBooks mirror before
  it (posted by its sync, never by a close), leave setup alone."""
  _draft(ledger.session, date(2026, 9, 15), _accounts(ledger.session))
  _captured_line(ledger.session, date(2026, 7, 3), source="quickbooks")
  ledger.session.commit()

  ledger.set_up(earliest_data_period="2026-09")

  assert _calendar_initialized(ledger.session)


# ── BCF-2: a schedule's months before the calendar are history ──────────


def test_a_schedule_starting_before_the_calendar_does_not_block_the_first_close(
  ledger,
):
  ledger.start_calendar("2026-09")
  structure_id = _schedule(ledger.session, date(2026, 6, 1))
  ledger.session.commit()

  obligations = _obligations(ledger.session, structure_id)
  assert [obligations[m] for m in ("2026-06", "2026-07", "2026-08")] == ["voided"] * 3
  assert obligations["2026-09"] == "pending"

  result = promote_pending_obligations(
    ledger.session,
    ledger.graph_id,
    as_of=datetime(2026, 10, 2, tzinfo=UTC),
    dispatch_handlers=True,
  )
  ledger.session.commit()
  assert result.errors == []
  drafted = [
    d
    for (d,) in ledger.session.query(Entry.posting_date).filter(
      Entry.source_structure_id == structure_id
    )
  ]
  assert drafted == [date(2026, 9, 30)]
  assert "pending_obligations" not in _first_close_gate(ledger).blockers


def test_a_calendar_set_up_after_a_schedule_retires_the_months_before_it(ledger):
  structure_id = _schedule(ledger.session, date(2026, 6, 1))
  ledger.session.commit()

  ledger.set_up(earliest_data_period="2026-09")

  obligations = _obligations(ledger.session, structure_id)
  assert [obligations[m] for m in ("2026-06", "2026-07", "2026-08")] == ["voided"] * 3
  assert obligations["2026-09"] == "pending"
  scopes = {
    fact.period_end: fact.fact_scope
    for fact in ledger.session.query(Fact).filter(
      Fact.structure_id == structure_id, Fact.period_start.isnot(None)
    )
  }
  assert scopes[date(2026, 8, 31)] == "historical"
  assert scopes[date(2026, 9, 30)] == "in_scope"
  # The first close waits on September's obligation only, which promotes.
  gate = _first_close_gate(ledger)
  assert (gate.pending_obligation_count, gate.earliest_pending_period) == (
    1,
    "2026-09",
  )


# ── BCF-3: a later start and a draft in flight serialize ────────────────


def _in_thread(work: Callable[[], object]) -> tuple[threading.Thread, dict]:
  outcome: dict = {}

  def run() -> None:
    try:
      outcome["value"] = work()
    except Exception as exc:
      outcome["error"] = exc

  thread = threading.Thread(target=run, daemon=True)
  thread.start()
  return thread, outcome


def test_a_draft_in_flight_is_counted_by_a_later_start(ledger):
  ledger.start_calendar("2026-01")
  accounts = _accounts(ledger.session)
  ledger.session.commit()

  writer = ledger.new_session()
  _draft(writer, date(2026, 2, 15), accounts)
  writer.flush()  # through the guards, not yet committed

  def move() -> object:
    mover = ledger.new_session()
    result = FiscalCalendarService().change_start(
      mover, ledger.graph_id, "2026-03", actor_id="usr_1"
    )
    mover.commit()
    return result

  thread, outcome = _in_thread(move)
  time.sleep(0.5)
  writer.commit()
  thread.join(15)

  assert not thread.is_alive()
  assert isinstance(outcome.get("error"), CalendarStartBlockedError), outcome
  assert outcome["error"].entries == 1


def test_a_draft_written_while_the_start_moves_is_refused(ledger):
  ledger.start_calendar("2026-01")
  accounts = _accounts(ledger.session)
  ledger.session.commit()

  mover = ledger.new_session()
  FiscalCalendarService().change_start(
    mover, ledger.graph_id, "2026-03", actor_id="usr_1"
  )  # months removed, not yet committed

  def write() -> object:
    writer = ledger.new_session()
    _draft(writer, date(2026, 2, 15), accounts)
    writer.commit()
    return "written"

  thread, outcome = _in_thread(write)
  time.sleep(0.5)
  mover.commit()
  thread.join(15)

  assert not thread.is_alive()
  assert isinstance(outcome.get("error"), BeforeCalendarStartError), outcome
  stranded = (
    ledger.session.query(Entry).filter(Entry.posting_date < date(2026, 3, 1)).count()
  )
  assert stranded == 0


# ── BCF-4: preview and commit check the date the entry posts on ─────────


def _recorded(
  session: Session, occurred: date, posting: date
) -> CreateEventBlockRequest:
  expense, prepaid = _accounts(session)
  return CreateEventBlockRequest(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    source="manual",
    occurred_at=datetime.combine(occurred, datetime.min.time()),
    amount=10_000,
    apply_handlers=False,
    metadata={
      "posting_date": posting.isoformat(),
      "memo": "Accrual",
      "type": "adjusting",
      "status": "draft",
      "line_items": [
        {"element_id": expense, "debit_amount": 10_000, "credit_amount": 0},
        {"element_id": prepaid, "debit_amount": 0, "credit_amount": 10_000},
      ],
    },
  )


def _commit(ledger: Ledger, body: CreateEventBlockRequest) -> None:
  event, _envelope = create_event_block_in_session(
    ledger.session, body, "usr", graph_id=ledger.graph_id
  )
  ledger.session.flush()
  update_event_block(
    ledger.session,
    UpdateEventBlockRequest(event_id=str(event.id), transition_to="committed"),
    "usr",
    graph_id=ledger.graph_id,
  )


def test_an_event_dated_before_the_calendar_that_posts_inside_it_commits(ledger):
  ledger.start_calendar("2026-09")
  body = _recorded(ledger.session, date(2026, 8, 31), date(2026, 9, 1))

  assert preview_event_block(ledger.session, body, "usr").would_succeed
  _commit(ledger, body)

  posted_on = [d for (d,) in ledger.session.query(Entry.posting_date)]
  assert posted_on == [date(2026, 9, 1)]


def test_an_event_dated_inside_that_posts_before_the_calendar_is_refused_by_both(
  ledger,
):
  ledger.start_calendar("2026-09")
  body = _recorded(ledger.session, date(2026, 9, 1), date(2026, 8, 31))

  assert not preview_event_block(ledger.session, body, "usr").would_succeed
  with pytest.raises(BeforeCalendarStartError):
    _commit(ledger, body)


# ── R2-1: moving the start earlier gives back the months it retired ──────


def _promote(ledger: Ledger, *, dispatch: bool = True):
  result = promote_pending_obligations(
    ledger.session,
    ledger.graph_id,
    as_of=datetime(2026, 10, 2, tzinfo=UTC),
    dispatch_handlers=dispatch,
  )
  ledger.session.commit()
  assert result.errors == []
  return result


def _drafted(session: Session, structure_id: str) -> list[date]:
  return sorted(
    d
    for (d,) in session.query(Entry.posting_date).filter(
      Entry.source_structure_id == structure_id
    )
  )


def _scopes(session: Session, structure_id: str) -> dict[date, str]:
  return {
    fact.period_end: fact.fact_scope
    for fact in session.query(Fact).filter(
      Fact.structure_id == structure_id, Fact.period_start.isnot(None)
    )
  }


def _move_start(ledger: Ledger, first_open_period: str) -> None:
  FiscalCalendarService().change_start(
    ledger.session, ledger.graph_id, first_open_period, actor_id="usr_1"
  )
  ledger.session.commit()


def _void(ledger: Ledger, structure_id: str, month: str) -> None:
  (obligation,) = [
    row
    for row in ledger.session.query(Event).filter(
      Event.event_type == "schedule_entry_due",
      Event.metadata_["schedule_id"].astext == structure_id,
    )
    if row.metadata_["period_end"].startswith(month)
  ]
  update_event_block(
    ledger.session,
    UpdateEventBlockRequest(event_id=str(obligation.id), transition_to="voided"),
    "usr",
    graph_id=ledger.graph_id,
  )
  ledger.session.commit()


def test_moving_the_start_earlier_returns_months_setup_retired(ledger):
  structure_id = _schedule(ledger.session, date(2026, 6, 1))
  ledger.session.commit()
  _void(ledger, structure_id, "2026-07")  # the user's own void
  ledger.set_up(earliest_data_period="2026-09")
  assert _obligations(ledger.session, structure_id)["2026-08"] == "voided"

  _move_start(ledger, "2026-06")

  obligations = _obligations(ledger.session, structure_id)
  assert [obligations[m] for m in ("2026-06", "2026-07", "2026-08")] == [
    "pending",
    "voided",
    "pending",
  ]
  scopes = _scopes(ledger.session, structure_id)
  assert scopes[date(2026, 6, 30)] == scopes[date(2026, 8, 31)] == "in_scope"
  assert scopes[date(2026, 7, 31)] == "historical"
  # The close drafts what the moved calendar now holds.
  _promote(ledger)
  assert _drafted(ledger.session, structure_id) == [
    date(2026, 6, 30),
    date(2026, 8, 31),
    date(2026, 9, 30),
  ]
  assert "pending_obligations" not in _first_close_gate(ledger, "2026-06").blockers


def test_moving_the_start_earlier_returns_months_a_new_schedule_retired(ledger):
  ledger.set_up(earliest_data_period="2026-09")
  structure_id = _schedule(ledger.session, date(2026, 6, 1))
  ledger.session.commit()
  assert _obligations(ledger.session, structure_id)["2026-06"] == "voided"

  _move_start(ledger, "2026-07")

  obligations = _obligations(ledger.session, structure_id)
  assert [obligations[m] for m in ("2026-06", "2026-07", "2026-08")] == [
    "voided",
    "pending",
    "pending",
  ]
  _promote(ledger)
  assert _drafted(ledger.session, structure_id) == [
    date(2026, 7, 31),
    date(2026, 8, 31),
    date(2026, 9, 30),
  ]


def test_months_booked_by_an_explicit_closed_through_stay_history(ledger):
  """A month the caller said is already booked is not "before the calendar":
  moving the start earlier leaves it voided."""
  ledger.set_up(earliest_data_period="2026-09")
  expense, prepaid = _accounts(ledger.session)
  structure_id = create_schedule(
    ledger.session,
    CreateScheduleRequest(
      name="Booked through June",
      element_ids=[expense, prepaid],
      period_start=date(2026, 6, 1),
      period_end=date(2027, 5, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=expense, credit_element_id=prepaid
      ),
      closed_through=date(2026, 6, 30),
    ),
    created_by="usr",
  ).structure_id
  ledger.session.commit()

  _move_start(ledger, "2026-06")

  obligations = _obligations(ledger.session, structure_id)
  assert [obligations[m] for m in ("2026-06", "2026-07", "2026-08")] == [
    "voided",
    "pending",
    "pending",
  ]


def test_a_rebuild_refused_after_a_close_names_where_the_retired_months_went(ledger):
  ledger.set_up(earliest_data_period="2026-09")
  structure_id = _schedule(ledger.session, date(2026, 6, 1))
  ledger.session.commit()
  _promote(ledger)
  ledger.session.execute(
    text("UPDATE entries SET status = 'posted' WHERE source_structure_id = :sid"),
    {"sid": structure_id},
  )
  ledger.session.commit()

  with pytest.raises(ValueError) as refused:
    rebuild_schedule(
      ledger.session, RebuildScheduleRequest(structure_id=structure_id), "usr"
    )
  ledger.session.rollback()

  assert "change-calendar-start" in str(refused.value)
  assert "2026-06" in str(refused.value)


# ── R2-2: setup retires a schedule the sweep already touched ─────────────


@pytest.mark.parametrize("dispatch", [False, True])
def test_setup_retires_promoted_obligations_and_their_drafts(ledger, dispatch):
  structure_id = _schedule(ledger.session, date(2026, 6, 1))
  ledger.session.commit()
  _promote(ledger, dispatch=dispatch)  # no calendar yet: the sweep still runs
  assert _obligations(ledger.session, structure_id)["2026-06"] == "classified"

  ledger.set_up(earliest_data_period="2026-09")

  obligations = _obligations(ledger.session, structure_id)
  assert [obligations[m] for m in ("2026-06", "2026-07", "2026-08")] == ["voided"] * 3
  assert obligations["2026-09"] == "classified"
  assert _drafted(ledger.session, structure_id) == (
    [date(2026, 9, 30)] if dispatch else []
  )

  # And they come back with the start.
  _move_start(ledger, "2026-06")
  assert _obligations(ledger.session, structure_id)["2026-06"] == "pending"
  _promote(ledger)
  assert _drafted(ledger.session, structure_id) == [
    date(2026, 6, 30),
    date(2026, 7, 31),
    date(2026, 8, 31),
    date(2026, 9, 30),
  ]


def test_setup_still_refuses_on_a_draft_the_schedule_did_not_write(ledger):
  """Only a schedule's own rows are its history; the refusal names the rest."""
  structure_id = _schedule(ledger.session, date(2026, 6, 1))
  _draft(ledger.session, date(2026, 8, 15), _accounts(ledger.session))
  ledger.session.commit()
  _promote(ledger)

  with pytest.raises(CalendarStartBlockedError) as refused:
    ledger.set_up(earliest_data_period="2026-09")
  ledger.session.rollback()

  assert (refused.value.entries, refused.value.unposted) == (1, 0)
  assert _obligations(ledger.session, structure_id)["2026-06"] == "classified"
