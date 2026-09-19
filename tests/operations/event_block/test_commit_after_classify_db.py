"""Approving an event whose handler already ran, against a real database.

A journal entry recorded as a draft runs its handler when it is created: the
draft entry is written then and the event arrives ``classified``. The inbox
still offers Approve on a classified event, and approval used to fire the same
handler again, writing a second draft that close would post beside the first.
A mocked session can only show which branch was taken; this counts the rows.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  update_event_block,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
POSTING_DATE = date(2026, 7, 31)


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_commit_{uuid.uuid4().hex[:12]}"
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


@pytest.fixture(autouse=True)
def _skip_platform_db_checks(monkeypatch):
  """Source and connection registration live in the platform database; this
  suite is about the tenant schema, so both checks are stubbed."""
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


def _seed(db) -> tuple[str, str]:
  ids = []
  for code, name in (("6100", "Software"), ("2100", "Accrued Liabilities")):
    element = Element(name=name, code=code, created_by="test")
    db.add(element)
    db.flush()
    ids.append(str(element.id))
  db.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      name="2026-07",
      start_date=date(2026, 7, 1),
      end_date=POSTING_DATE,
      period_type="month",
      status="open",
    )
  )
  db.flush()
  return ids[0], ids[1]


def _record_accrual(db, *, apply_handlers: bool) -> str:
  expense, accrued = _seed(db)
  event, _envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      event_class="economic",
      source="manual",
      occurred_at=datetime(2026, 7, 31),
      amount=120000,
      apply_handlers=apply_handlers,
      metadata={
        "posting_date": POSTING_DATE.isoformat(),
        "memo": "July software accrual",
        "type": "adjusting",
        "status": "draft",
        "line_items": [
          {"element_id": expense, "debit_amount": 120000, "credit_amount": 0},
          {"element_id": accrued, "debit_amount": 0, "credit_amount": 120000},
        ],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  db.flush()
  return str(event.id)


def _entries(db, event_id: str) -> int:
  return db.execute(
    select(func.count())
    .select_from(Entry)
    .where(Entry.triggered_by_event_id == event_id)
  ).scalar_one()


def _approve(db, event_id: str):
  schema = db.execute(text("SELECT current_schema()")).scalar_one()
  envelope = update_event_block(
    db,
    UpdateEventBlockRequest(event_id=event_id, transition_to="committed"),
    "user_test",
    graph_id=GRAPH_ID,
  )
  # The command commits, which hands the connection back to the pool.
  db.execute(text(f'SET search_path TO "{schema}"'))
  return envelope


def test_approving_a_recorded_draft_writes_no_second_entry(session):
  event_id = _record_accrual(session, apply_handlers=True)
  assert _entries(session, event_id) == 1

  envelope = _approve(session, event_id)

  assert envelope.status == "committed"
  assert _entries(session, event_id) == 1


def test_approving_a_captured_event_still_writes_its_entry(session):
  event_id = _record_accrual(session, apply_handlers=False)
  assert _entries(session, event_id) == 0

  envelope = _approve(session, event_id)

  assert envelope.status == "committed"
  assert _entries(session, event_id) == 1
