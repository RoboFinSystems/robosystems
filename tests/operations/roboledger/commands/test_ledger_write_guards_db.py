"""Ledger writers outside the journal-entry commands, against real Postgres.

Each of these writes rows the journal-entry commands would have refused: a
draft on a retired account, a schedule deleted from under its posted entries,
a voided event's leftover draft counted as work a close will post.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  EntryTemplateRequest,
  RebuildScheduleRequest,
)
from robosystems.models.extensions import Fact, Structure
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.engine import apply_handler
from robosystems.operations.event_block.promotion import promote_pending_obligations
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  InactiveAccountError,
)
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule,
  delete_schedule,
  rebuild_schedule,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  drafts_close_posts,
)
from robosystems.operations.roboledger.schedules.service import ScheduleService
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef02"
AS_OF = datetime(2026, 2, 15, tzinfo=UTC)
JAN_START, JAN_END = date(2026, 1, 1), date(2026, 1, 31)
_COMMANDS = "robosystems.operations.roboledger.commands.schedules"
_SERVICE = "robosystems.operations.roboledger.schedules.service"


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_guards_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  seed_parent_entity(session)
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


def _element(session, name: str, *, is_active: bool = True) -> str:
  element = Element(name=name, code=name[:8], is_active=is_active, created_by="test")
  session.add(element)
  session.flush()
  return str(element.id)


def _schedule(session) -> tuple[str, str, str]:
  """A three-month depreciation schedule; returns (structure, debit, credit)."""
  debit = _element(session, "Depreciation Expense")
  credit = _element(session, "Accumulated Depreciation")
  created = create_schedule(
    session,
    CreateScheduleRequest(
      name="Depreciation",
      element_ids=[debit, credit],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 3, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=debit, credit_element_id=credit
      ),
    ),
    created_by="usr",
  )
  return created.structure_id, debit, credit


def _schedule_entries(session, structure_id: str) -> list[Entry]:
  return session.query(Entry).filter(Entry.source_structure_id == structure_id).all()


def _event(session, *, status: str) -> str:
  event = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    occurred_at=datetime(2026, 1, 15, tzinfo=UTC),
    status=status,
    source="manual",
    created_by="usr",
  )
  session.add(event)
  session.flush()
  return str(event.id)


def _draft(session, *, triggered_by_event_id: str | None = None) -> None:
  session.add(
    Entry(
      entity_id=PARENT_ENTITY_ID,
      posting_date=date(2026, 1, 20),
      status="draft",
      type="standard",
      provenance="manual_entry",
      triggered_by_event_id=triggered_by_event_id,
      created_by="usr",
    )
  )
  session.flush()


def test_a_schedule_stops_drafting_against_a_retired_account(ext_session):
  session = ext_session
  structure_id, debit, _ = _schedule(session)
  session.get(Element, debit).is_active = False
  session.commit()

  result = promote_pending_obligations(
    session, GRAPH_ID, as_of=AS_OF, dispatch_handlers=True
  )
  session.commit()

  assert _schedule_entries(session, structure_id) == []
  assert len(result.errors) == 1
  assert "inactive account" in result.errors[0][1]


def test_a_manual_draft_refuses_a_retired_account(ext_session):
  session = ext_session
  cash = _element(session, "Operating Cash")
  retired = _element(session, "Old Equipment", is_active=False)

  with pytest.raises(InactiveAccountError, match="Old Equipment"):
    ScheduleService().create_manual_closing_entry(
      session,
      posting_date=date(2026, 1, 31),
      line_items=[
        {"element_id": cash, "debit_amount": 5_000},
        {"element_id": retired, "credit_amount": 5_000},
      ],
      memo="Disposal",
      created_by="usr",
    )

  assert session.query(Entry).count() == 0
  assert session.query(LineItem).count() == 0


def test_a_tenant_rule_refuses_a_retired_account(ext_session):
  session = ext_session
  cash = _element(session, "Operating Cash")
  retired = _element(session, "Old Revenue", is_active=False)
  event = Event(
    entity_id=PARENT_ENTITY_ID,
    id="evt_sale",
    event_type="invoice_issued",
    event_category="sales",
    occurred_at=datetime(2026, 1, 15, tzinfo=UTC),
    source="native",
    amount=12_500,
    currency="USD",
    metadata_={},
    created_by="usr",
  )
  handler = EventHandler(
    name="Invoice rule",
    event_type="invoice_issued",
    transaction_template={
      "transactions": [
        {
          "entry_template": {
            "debit": {"element_id": cash, "amount": "{{ event.amount }}"},
            "credit": {"element_id": retired, "amount": "{{ event.amount }}"},
          }
        }
      ]
    },
  )

  with pytest.raises(InactiveAccountError, match="Old Revenue"):
    apply_handler(session, event, handler, created_by="usr")

  assert session.query(Entry).count() == 0


def test_delete_refuses_once_an_entry_has_posted(ext_session):
  session = ext_session
  structure_id, _, _ = _schedule(session)
  promote_pending_obligations(session, GRAPH_ID, as_of=AS_OF, dispatch_handlers=True)
  for entry in _schedule_entries(session, structure_id):
    entry.status = "posted"
  session.commit()
  facts_before = session.query(Fact).filter(Fact.structure_id == structure_id).count()

  with pytest.raises(ValueError, match="terminate-schedule"):
    delete_schedule(session, DeleteScheduleRequest(structure_id=structure_id))
  session.rollback()

  assert session.get(Structure, structure_id) is not None
  assert facts_before > 0
  assert (
    session.query(Fact).filter(Fact.structure_id == structure_id).count()
    == facts_before
  )


def test_delete_takes_its_drafts_and_obligations_with_it(ext_session):
  session = ext_session
  structure_id, _, _ = _schedule(session)
  promote_pending_obligations(session, GRAPH_ID, as_of=AS_OF, dispatch_handlers=True)
  session.commit()
  assert len(_schedule_entries(session, structure_id)) == 1

  delete_schedule(session, DeleteScheduleRequest(structure_id=structure_id))

  assert session.get(Structure, structure_id) is None
  assert _schedule_entries(session, structure_id) == []
  assert session.query(LineItem).count() == 0
  live_obligations = (
    session.query(Event)
    .filter(Event.event_type == "schedule_entry_due", Event.status != "voided")
    .count()
  )
  assert live_obligations == 0


def _drafted(session, *, as_of: datetime = AS_OF) -> str:
  structure_id, _, _ = _schedule(session)
  promote_pending_obligations(session, GRAPH_ID, as_of=as_of, dispatch_handlers=True)
  session.commit()
  return structure_id


def _close_lands_the_drafts(session, structure_id: str):
  """Stands in for a close that finished just before the caller took the
  period fence: by the time the fence is held, the drafts are posted."""

  def _land(*_args, **_kwargs) -> None:
    session.execute(
      text(
        "UPDATE entries SET status = 'posted' "
        "WHERE source_structure_id = :sid AND status = 'draft'"
      ),
      {"sid": structure_id},
    )

  return _land


def test_delete_takes_a_voided_obligations_leftover_draft_in_a_closed_month(
  ext_session,
):
  """A close leaves a voided obligation's draft behind as a draft. It is not
  part of the closed month's books, so it must not hold the schedule."""
  session = ext_session
  structure_id = _drafted(session)
  (leftover,) = _schedule_entries(session, structure_id)
  session.get(Event, leftover.triggered_by_event_id).status = "voided"
  session.add(
    FiscalCalendar(
      entity_id=PARENT_ENTITY_ID, graph_id=GRAPH_ID, closed_through_period="2026-01"
    )
  )
  session.commit()

  delete_schedule(session, DeleteScheduleRequest(structure_id=structure_id))

  assert session.get(Structure, structure_id) is None
  assert _schedule_entries(session, structure_id) == []


def test_delete_still_refuses_a_live_draft_in_a_closed_month(ext_session):
  session = ext_session
  structure_id = _drafted(session)
  session.add(
    FiscalCalendar(
      entity_id=PARENT_ENTITY_ID, graph_id=GRAPH_ID, closed_through_period="2026-01"
    )
  )
  session.commit()

  with pytest.raises(ClosedPeriodError):
    delete_schedule(session, DeleteScheduleRequest(structure_id=structure_id))
  session.rollback()

  assert session.get(Structure, structure_id) is not None


def test_ending_early_takes_a_leftover_draft_in_a_closed_month(ext_session):
  """The same leftover, past the cutoff of a schedule being ended early."""
  session = ext_session
  structure_id = _drafted(session, as_of=datetime(2026, 4, 15, tzinfo=UTC))
  january, february, march = sorted(
    _schedule_entries(session, structure_id), key=lambda e: e.posting_date
  )
  january.status = "posted"
  session.get(Event, february.triggered_by_event_id).status = "voided"
  session.add(
    FiscalCalendar(
      entity_id=PARENT_ENTITY_ID, graph_id=GRAPH_ID, closed_through_period="2026-02"
    )
  )
  session.commit()
  kept, open_month = january.id, march.posting_date

  ScheduleService().truncate_schedule(
    session,
    structure_id=structure_id,
    new_end_date=date(2026, 1, 31),
    reason="Sold",
    updated_by="usr",
  )

  assert open_month == date(2026, 3, 31)
  assert [e.id for e in _schedule_entries(session, structure_id)] == [kept]


def test_rebuild_counts_entries_a_close_landed_before_the_fence(ext_session):
  session = ext_session
  structure_id = _drafted(session)
  facts_before = session.query(Fact).filter(Fact.structure_id == structure_id).count()

  with (
    patch(
      f"{_COMMANDS}._fence_draft_periods",
      side_effect=_close_lands_the_drafts(session, structure_id),
    ),
    pytest.raises(ValueError, match="posted closing entries exist"),
  ):
    rebuild_schedule(session, RebuildScheduleRequest(structure_id=structure_id))
  session.rollback()

  assert (
    session.query(Fact).filter(Fact.structure_id == structure_id).count()
    == facts_before
  )


def test_truncate_counts_entries_a_close_landed_before_the_fence(ext_session):
  session = ext_session
  structure_id = _drafted(session, as_of=datetime(2026, 4, 15, tzinfo=UTC))
  facts_before = session.query(Fact).filter(Fact.structure_id == structure_id).count()

  with (
    patch(
      f"{_SERVICE}.assert_period_not_closed",
      side_effect=_close_lands_the_drafts(session, structure_id),
    ),
    pytest.raises(ValueError, match="Cannot truncate"),
  ):
    ScheduleService().truncate_schedule(
      session,
      structure_id=structure_id,
      new_end_date=date(2026, 1, 31),
      reason="Sold",
      updated_by="usr",
    )
  session.rollback()

  assert (
    session.query(Fact).filter(Fact.structure_id == structure_id).count()
    == facts_before
  )


def test_a_voided_events_draft_is_not_work_a_close_will_post(ext_session):
  session = ext_session
  _draft(session)
  _draft(session, triggered_by_event_id=_event(session, status="committed"))
  _draft(session, triggered_by_event_id=_event(session, status="voided"))
  _draft(session, triggered_by_event_id=_event(session, status="superseded"))
  session.commit()

  assert (
    drafts_close_posts(session, JAN_START, JAN_END, entity_id=PARENT_ENTITY_ID).count()
    == 2
  )
  assert (
    drafts_close_posts(
      session, date(2026, 2, 1), date(2026, 2, 28), entity_id=PARENT_ENTITY_ID
    ).count()
    == 0
  )
