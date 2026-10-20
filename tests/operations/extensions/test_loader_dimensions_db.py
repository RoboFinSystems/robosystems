"""A QuickBooks line's class and location reach the posted line, never as drift.

A tag moves no balance, so the loader carries it to the line the handler
posts, re-tags a posted line in place when QuickBooks changes it, fills the
tags an earlier sync never carried whatever the SyncToken says, and still
flags a real edit as drift.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Dimension
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.extensions.loader import OLTPLoader, _upsert_dimensions
from robosystems.operations.roboledger.line_dimensions import line_dimensions
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
CONN = "conn_test"
AR, REVENUE = "11", "40"
INVOICE = "Invoice_42"
CATALOG = [
  {"dimension_type": "class", "name": "Retail", "value": "7"},
  {"dimension_type": "class", "name": "Wholesale", "value": "8"},
  {"dimension_type": "location", "name": "Downtown", "value": "1"},
]


@pytest.fixture()
def db():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_dims_{uuid.uuid4().hex[:12]}"
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  seed_parent_entity(session)
  for ext, code, name in ((AR, "1100", "Receivables"), (REVENUE, "4000", "Sales")):
    session.add(
      Element(
        name=name,
        code=code,
        external_id=ext,
        external_source="quickbooks",
        connection_id=CONN,
        created_by="t",
      )
    )
  session.add(
    FiscalPeriod(
      entity_id=PARENT_ENTITY_ID,
      graph_id=GRAPH_ID,
      name="2026-07",
      start_date=date(2026, 7, 1),
      end_date=date(2026, 7, 31),
      period_type="month",
      status="open",
    )
  )
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


@pytest.fixture(autouse=True)
def _no_platform(monkeypatch):
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


def _invoice(token: str, revenue_tags: list[dict], amount: int = 120000) -> dict:
  """Invoice 42 as the dbt mart hands it to the loader, its revenue line
  tagged."""
  return {
    "transactions": [
      {
        "external_id": INVOICE,
        "type": "Invoice",
        "event_type": "invoice_issued",
        "event_category": "sales",
        "amount": amount,
        "date": date(2026, 7, 10),
        "sync_token": token,
        "description": "Consulting",
      }
    ],
    "entries": [
      {
        "external_id": INVOICE,
        "external_transaction_id": INVOICE,
        "type": "standard",
        "posting_date": date(2026, 7, 10),
        "memo": "Consulting",
      }
    ],
    "line_items": [
      {
        "entry_external_id": INVOICE,
        "element_external_id": AR,
        "debit_amount": amount,
        "credit_amount": 0,
        "line_order": 1,
      },
      {
        "entry_external_id": INVOICE,
        "element_external_id": REVENUE,
        "debit_amount": 0,
        "credit_amount": amount,
        "line_order": 2,
        "dimension_refs": revenue_tags or None,
      },
    ],
  }


RETAIL = [{"type": "class", "value": "7"}, {"type": "location", "value": "1"}]
WHOLESALE = [{"type": "class", "value": "8"}, {"type": "location", "value": "1"}]


def _sync(db, data):
  _upsert_dimensions(db, CATALOG, source="quickbooks", now=datetime.now(UTC))
  OLTPLoader()._capture_transactions_as_events(
    db,
    data,
    source="quickbooks",
    connection_id=CONN,
    created_by="sync",
    now=datetime.now(UTC),
  )
  db.flush()


def _event(db) -> Event:
  event = db.query(Event).filter(Event.external_id == INVOICE).one()
  db.refresh(event)
  return event


def _revenue_tags(db) -> list[str]:
  line = (
    db.query(LineItem)
    .join(Entry, Entry.id == LineItem.entry_id)
    .join(Element, Element.id == LineItem.element_id)
    .filter(
      Entry.triggered_by_event_id == _event(db).id, Element.external_id == REVENUE
    )
    .one()
  )
  return sorted(d.name for d in line_dimensions(db, [line.id]).get(str(line.id), []))


def test_a_new_invoice_posts_its_revenue_line_tagged(db):
  _sync(db, _invoice("0", RETAIL))

  assert _event(db).status in ("committed", "fulfilled")
  assert _revenue_tags(db) == ["Downtown", "Retail"]


def test_a_class_changed_in_quickbooks_re_tags_the_posted_line(db):
  _sync(db, _invoice("0", RETAIL))

  _sync(db, _invoice("1", WHOLESALE))

  event = _event(db)
  assert event.payload_drift is False
  assert _revenue_tags(db) == ["Downtown", "Wholesale"]


def test_tags_an_earlier_sync_missed_are_filled_at_the_same_version(db):
  _sync(db, _invoice("0", []))
  assert _revenue_tags(db) == []

  _sync(db, _invoice("0", RETAIL))

  assert _event(db).payload_drift is False
  assert _revenue_tags(db) == ["Downtown", "Retail"]


def test_a_member_added_by_hand_on_another_axis_stays(db):
  _sync(db, _invoice("0", RETAIL))
  project = Dimension(dimension_type="project", name="Launch", value="launch")
  db.add(project)
  db.flush()
  line_id = (
    db.query(LineItem.id)
    .join(Entry, Entry.id == LineItem.entry_id)
    .join(Element, Element.id == LineItem.element_id)
    .filter(
      Entry.triggered_by_event_id == _event(db).id, Element.external_id == REVENUE
    )
    .scalar()
  )
  from robosystems.models.extensions.roboledger.dimension_junctions import (
    line_item_dimensions,
  )

  db.execute(
    line_item_dimensions.insert(),
    [{"line_item_id": line_id, "dimension_id": project.id}],
  )

  _sync(db, _invoice("1", WHOLESALE))

  assert _revenue_tags(db) == ["Downtown", "Launch", "Wholesale"]


def test_a_real_edit_still_drifts(db):
  _sync(db, _invoice("0", RETAIL))

  _sync(db, _invoice("1", WHOLESALE, amount=150000))

  assert _event(db).payload_drift is True
  assert _revenue_tags(db) == ["Downtown", "Retail"]
