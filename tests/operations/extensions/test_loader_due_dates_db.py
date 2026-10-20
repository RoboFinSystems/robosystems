"""A QuickBooks due date reaches the obligation's event, and never as drift.

A due date moves no ledger row, so the loader applies it to ``events.due_date``
in place: on a new event, on an existing one an earlier sync never gave one
(whatever its SyncToken), and on a newer version of a posted invoice, where a
change to the due date alone flags nothing.
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
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.extensions.loader import OLTPLoader
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
CONN = "conn_test"
AR, REVENUE = "11", "40"
INVOICE = "Invoice_42"


@pytest.fixture()
def db():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_due_{uuid.uuid4().hex[:12]}"
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


def _invoice(token: str, due: date | None, amount: int = 120000) -> dict:
  """Invoice 42 as the dbt mart hands it to the loader."""
  return {
    "transactions": [
      {
        "external_id": INVOICE,
        "type": "Invoice",
        "event_type": "invoice_issued",
        "event_category": "sales",
        "amount": amount,
        "date": date(2026, 7, 10),
        "due_date": due,
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
      },
    ],
  }


def _sync(db, data):
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


def test_a_new_invoice_carries_its_due_date_to_the_event_and_transaction(db):
  _sync(db, _invoice("0", date(2026, 8, 9)))

  event = _event(db)
  assert event.due_date == date(2026, 8, 9)
  txn = (
    db.query(Transaction).filter(Transaction.triggered_by_event_id == event.id).one()
  )
  assert txn.due_date == date(2026, 8, 9)


def test_a_due_date_an_earlier_sync_missed_is_filled_at_the_same_version(db):
  _sync(db, _invoice("0", None))
  assert _event(db).due_date is None

  _sync(db, _invoice("0", date(2026, 8, 9)))

  event = _event(db)
  assert event.due_date == date(2026, 8, 9)
  assert event.payload_drift is False


def test_an_extension_granted_in_quickbooks_is_not_a_reconciling_item(db):
  _sync(db, _invoice("0", date(2026, 8, 9)))
  assert _event(db).status in ("committed", "fulfilled")

  _sync(db, _invoice("1", date(2026, 9, 8)))

  event = _event(db)
  assert event.due_date == date(2026, 9, 8)
  assert event.payload_drift is False


def test_a_real_edit_still_drifts_and_takes_the_new_due_date(db):
  _sync(db, _invoice("0", date(2026, 8, 9)))

  _sync(db, _invoice("1", date(2026, 9, 8), amount=150000))

  event = _event(db)
  assert event.payload_drift is True
  assert event.due_date == date(2026, 9, 8)
