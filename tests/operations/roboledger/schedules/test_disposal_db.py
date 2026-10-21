"""Disposing of a scheduled asset, against a real database.

The disposal entry removes accumulated depreciation through the last month-end
on or before the disposal date, so every later month is no longer the
schedule's to post, and every earlier month still is. The preview refuses what
the write refuses.
"""

from __future__ import annotations

import os
import uuid
from calendar import monthrange
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  ScheduleMetadataRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  preview_event_block,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.schedules.service import ScheduleService
from tests.ledger_entity import seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_dispose_{uuid.uuid4().hex[:12]}"
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
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture(autouse=True)
def _skip_platform_db_checks(monkeypatch):
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


@pytest.fixture()
def accounts(session) -> dict[str, str]:
  ids = {}
  for key, code, name, balance in (
    ("equipment", "1500", "Equipment", "debit"),
    # Credit-balanced, so the schedule accumulates it rather than drawing it
    # down.
    ("accumulated", "1590", "Accumulated Depreciation", "credit"),
    ("depreciation", "6400", "Depreciation Expense", "debit"),
    ("cash", "1000", "Checking", "debit"),
    ("gain_loss", "7100", "Gain or Loss on Disposal", "credit"),
  ):
    element = Element(name=name, code=code, balance_type=balance, created_by="test")
    session.add(element)
    session.flush()
    ids[key] = str(element.id)
  session.commit()
  return ids


def _roaster(
  session,
  accounts,
  *,
  original: int = 120_000,
  monthly: int = 10_000,
  end: date = date(2026, 12, 31),
):
  created = create_schedule(
    session,
    CreateScheduleRequest(
      name="Roaster",
      element_ids=[accounts["depreciation"], accounts["accumulated"]],
      period_start=date(2026, 1, 1),
      period_end=end,
      monthly_amount=monthly,
      entry_template=EntryTemplateRequest(
        debit_element_id=accounts["depreciation"],
        credit_element_id=accounts["accumulated"],
      ),
      schedule_metadata=ScheduleMetadataRequest(
        original_amount=original, asset_element_id=accounts["equipment"]
      ),
    ),
    created_by="usr",
  )
  session.commit()
  return created.structure_id


def _draft_month(session, structure_id: str, month: int, *, status: str = "draft"):
  end = date(2026, month, monthrange(2026, month)[1])
  result = ScheduleService().create_closing_entry(
    session,
    structure_id=structure_id,
    posting_date=end,
    period_start=date(2026, month, 1),
    period_end=end,
    created_by="usr",
  )
  session.get(Entry, result.entry_id).status = status
  session.commit()


def _disposal(structure_id: str, on: date, **metadata) -> CreateEventBlockRequest:
  return CreateEventBlockRequest(
    event_type="asset_disposed",
    event_category="adjustment",
    event_class="economic",
    source="manual",
    occurred_at=datetime(on.year, on.month, on.day, tzinfo=UTC),
    apply_handlers=True,
    metadata={"schedule_id": structure_id, **metadata},
  )


def _dispose(session, body: CreateEventBlockRequest) -> Event:
  event, _ = create_event_block_in_session(session, body, "usr", graph_id=GRAPH_ID)
  session.flush()
  return event


def _schedule_drafts(session, structure_id: str) -> list[date]:
  return sorted(
    session.execute(
      select(Entry.posting_date).where(
        Entry.source_structure_id == structure_id, Entry.status == "draft"
      )
    ).scalars()
  )


def _obligations(session, structure_id: str) -> dict[str, str]:
  rows = session.execute(
    select(Event.metadata_["period_start"].astext, Event.status).where(
      Event.event_type == "schedule_entry_due",
      Event.metadata_["schedule_id"].astext == structure_id,
    )
  ).all()
  return dict(rows)


def test_a_backdated_disposal_retires_only_the_months_after_it(session, accounts):
  structure_id = _roaster(session, accounts)
  for month in (1, 2):
    _draft_month(session, structure_id, month, status="posted")
  # March and April were already drafted when the March disposal is recorded.
  for month in (3, 4):
    _draft_month(session, structure_id, month)
  before = _obligations(session, structure_id)

  _dispose(
    session,
    _disposal(
      structure_id,
      date(2026, 3, 15),
      proceeds=90_000,
      proceeds_element_id=accounts["cash"],
      gain_loss_element_id=accounts["gain_loss"],
    ),
  )

  # The disposal removes depreciation through February: March and April
  # would have left a charge the entry already took off the books.
  assert _schedule_drafts(session, structure_id) == []
  after = _obligations(session, structure_id)
  for period_start, status in after.items():
    if period_start > "2026-02-28":
      assert status == "voided", period_start
    else:
      assert status == before[period_start], period_start


def test_a_disposal_in_the_first_month_posts_with_nothing_depreciated(
  session, accounts
):
  structure_id = _roaster(session, accounts)

  event = _dispose(
    session,
    _disposal(
      structure_id,
      date(2026, 1, 15),
      proceeds=120_000,
      proceeds_element_id=accounts["cash"],
    ),
  )

  [entry] = session.execute(
    select(Entry).where(Entry.triggered_by_event_id == event.id)
  ).scalars()
  lines = session.execute(
    select(LineItem.element_id, LineItem.debit_amount, LineItem.credit_amount).where(
      LineItem.entry_id == entry.id
    )
  ).all()
  assert sorted(lines) == sorted(
    [(accounts["equipment"], 0, 120_000), (accounts["cash"], 120_000, 0)]
  )


def test_a_fully_depreciated_sale_needs_its_gain_account(session, accounts):
  structure_id = _roaster(session, accounts, original=20_000, end=date(2026, 2, 28))
  for month in (1, 2):
    _draft_month(session, structure_id, month, status="posted")
  body = _disposal(
    structure_id,
    date(2026, 2, 28),
    proceeds=5_000,
    proceeds_element_id=accounts["cash"],
  )

  preview = preview_event_block(session, body, "usr")

  assert not preview.would_succeed
  assert any("gain_loss_element_id" in e for e in preview.validation_errors)
  with pytest.raises(ValueError, match="gain_loss_element_id"):
    _dispose(session, body)


def test_the_preview_refuses_a_disposal_before_a_posted_month(session, accounts):
  structure_id = _roaster(session, accounts)
  for month in (1, 2, 3):
    _draft_month(session, structure_id, month, status="posted")
  body = _disposal(
    structure_id,
    date(2026, 2, 10),
    proceeds=100_000,
    proceeds_element_id=accounts["cash"],
    gain_loss_element_id=accounts["gain_loss"],
  )

  preview = preview_event_block(session, body, "usr")

  assert not preview.would_succeed
  assert any("posted or shadowed" in e for e in preview.validation_errors)
  with pytest.raises(ValueError, match="posted or shadowed"):
    _dispose(session, body)


def _plan(session, structure_id: str, on: date, accounts, **kwargs):
  from robosystems.operations.event_block.python_handlers._disposal_plan import (
    compute_disposal_plan,
  )

  return compute_disposal_plan(
    session,
    structure_id=structure_id,
    disposal_date=on,
    sale_proceeds=kwargs.get("proceeds", 0),
    proceeds_element_id=accounts["cash"],
    gain_loss_element_id=accounts["gain_loss"],
  )


def test_a_classified_month_after_the_disposal_is_voided_with_its_draft(
  session, accounts
):
  structure_id = _roaster(session, accounts)
  _draft_month(session, structure_id, 1, status="posted")
  _draft_month(session, structure_id, 2)
  # A promotion sweep classified February and drafted it before the disposal.
  session.execute(
    text(
      "UPDATE events SET status = 'classified' "
      "WHERE event_type = 'schedule_entry_due' "
      "AND metadata->>'period_start' = '2026-02-01'"
    )
  )
  session.commit()

  _dispose(
    session,
    _disposal(
      structure_id,
      date(2026, 2, 10),
      proceeds=100_000,
      proceeds_element_id=accounts["cash"],
      gain_loss_element_id=accounts["gain_loss"],
    ),
  )

  assert _schedule_drafts(session, structure_id) == []
  assert _obligations(session, structure_id)["2026-02-01"] == "voided"


def test_a_month_end_disposal_keeps_that_months_draft(session, accounts):
  structure_id = _roaster(session, accounts)
  _draft_month(session, structure_id, 1, status="posted")
  for month in (2, 3):
    _draft_month(session, structure_id, month)

  _dispose(
    session,
    _disposal(
      structure_id,
      date(2026, 2, 28),
      proceeds=100_000,
      proceeds_element_id=accounts["cash"],
      gain_loss_element_id=accounts["gain_loss"],
    ),
  )

  # The plan counts February's depreciation, so February's entry still posts.
  assert _schedule_drafts(session, structure_id) == [date(2026, 2, 28)]


def test_a_terminated_schedule_disposes_at_its_starting_cost(session, accounts):
  from robosystems.models.api.extensions.schedules import TerminateScheduleRequest
  from robosystems.operations.roboledger.commands.schedules import (
    terminate_schedule,
  )

  structure_id = _roaster(session, accounts)
  terminate_schedule(
    session,
    TerminateScheduleRequest(
      structure_id=structure_id, new_end_date=date(2026, 6, 30), reason="Idle"
    ),
    created_by="usr",
  )

  plan = _plan(session, structure_id, date(2026, 7, 15), accounts)

  # Terminate re-anchors original_amount to the 60,000 it recognized; the
  # equipment still cost 120,000.
  assert plan.original_amount == 120_000
  assert plan.accumulated_depreciation == 60_000
  assert plan.nbv == 60_000


def test_months_before_the_calendar_count_as_depreciated(session, accounts):
  created = create_schedule(
    session,
    CreateScheduleRequest(
      name="Roaster",
      element_ids=[accounts["depreciation"], accounts["accumulated"]],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 12, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=accounts["depreciation"],
        credit_element_id=accounts["accumulated"],
      ),
      schedule_metadata=ScheduleMetadataRequest(
        original_amount=120_000, asset_element_id=accounts["equipment"]
      ),
      closed_through=date(2026, 3, 31),
    ),
    created_by="usr",
  )
  session.commit()

  plan = _plan(session, created.structure_id, date(2026, 4, 15), accounts)

  assert plan.accumulated_depreciation == 30_000


def test_a_drawn_down_credit_account_is_read_as_the_remaining_balance(
  session, accounts
):
  """The generator writes a debit-balanced, non-contra credit account's facts
  as the balance left, whichever account the asset is; the plan must read them
  the same way, or a separate prepaid asset disposes at the wrong value."""
  prepaid = Element(
    name="Prepaid Lease", code="1400", balance_type="debit", created_by="t"
  )
  session.add(prepaid)
  session.commit()
  created = create_schedule(
    session,
    CreateScheduleRequest(
      name="Lease",
      element_ids=[accounts["depreciation"], str(prepaid.id)],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 12, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=accounts["depreciation"], credit_element_id=str(prepaid.id)
      ),
      schedule_metadata=ScheduleMetadataRequest(
        original_amount=120_000, asset_element_id=accounts["equipment"]
      ),
    ),
    created_by="usr",
  )
  session.commit()

  plan = _plan(session, created.structure_id, date(2026, 3, 31), accounts)

  assert plan.nbv == 90_000
  assert plan.accumulated_depreciation == 30_000
  # The remaining balance leaves the account that carried it.
  (derecognized,) = [li for li in plan.line_items if li["credit_amount"]]
  assert derecognized["element_id"] == str(prepaid.id)
  assert derecognized["credit_amount"] == 90_000
