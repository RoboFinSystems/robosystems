"""A QuickBooks payment settles each document it names by the amount it
applied to that document.

The loader writes the LinkedTxn lines as ``event_discharges`` rows once the
documents are loaded: a payment naming one not loaded yet is left for the next
sync, a same-version sync fills rows an earlier one never wrote, and a newer
version of the payment replaces them.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.event_discharge import EventDischarge
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.extensions.loader import OLTPLoader
from robosystems.operations.roboledger.reads.ar_ap import list_open_items
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
CONN = "conn_test"
CASH, AR, REVENUE = "10", "11", "40"
AS_OF = date(2026, 7, 31)


@pytest.fixture()
def db():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_dis_{uuid.uuid4().hex[:12]}"
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  seed_parent_entity(session)
  for ext, code, name in (
    (CASH, "1000", "Checking"),
    (AR, "1100", "Receivables"),
    (REVENUE, "4000", "Sales"),
  ):
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


def _txn(
  ext_id: str,
  event_type: str,
  amount: int,
  debit: str,
  credit: str,
  *,
  token: str = "0",
  linked: list[dict] | None = None,
) -> dict:
  """One transaction as the dbt mart hands it to the loader, with its entry."""
  return {
    "transaction": {
      "external_id": ext_id,
      "type": ext_id.split("_")[0],
      "event_type": event_type,
      "event_category": "sales",
      "amount": amount,
      "date": date(2026, 7, 10),
      "sync_token": token,
      "linked_txns": json.dumps(linked or []),
    },
    "entry": {
      "external_id": ext_id,
      "external_transaction_id": ext_id,
      "type": "standard",
      "posting_date": date(2026, 7, 10),
    },
    "lines": [
      {
        "entry_external_id": ext_id,
        "element_external_id": debit,
        "debit_amount": amount,
        "credit_amount": 0,
        "line_order": 1,
      },
      {
        "entry_external_id": ext_id,
        "element_external_id": credit,
        "debit_amount": 0,
        "credit_amount": amount,
        "line_order": 2,
      },
    ],
  }


def _invoice(number: int, amount: int) -> dict:
  return _txn(f"Invoice_{number}", "invoice_issued", amount, AR, REVENUE)


def _payment(applied: list[tuple[int, str]], *, token: str = "0") -> dict:
  return _txn(
    "Payment_7",
    "payment_received",
    150000,
    CASH,
    AR,
    token=token,
    linked=[
      {"txn_id": str(number), "txn_type": "Invoice", "amount": amount}
      for number, amount in applied
    ],
  )


def _sync(db, *txns: dict) -> None:
  OLTPLoader()._capture_transactions_as_events(
    db,
    {
      "transactions": [t["transaction"] for t in txns],
      "entries": [t["entry"] for t in txns],
      "line_items": [line for t in txns for line in t["lines"]],
    },
    source="quickbooks",
    connection_id=CONN,
    created_by="sync",
    now=datetime.now(UTC),
  )
  db.flush()


def _allocations(db) -> dict[str, int]:
  rows = db.execute(
    select(Event.external_id, EventDischarge.amount).join(
      Event, Event.id == EventDischarge.obligation_event_id
    )
  ).all()
  return dict(rows)


def _open(db) -> dict[str, int]:
  ids = dict(db.execute(select(Event.id, Event.external_id)).all())
  return {
    ids[item.event_id]: item.open_cents
    for item in list_open_items(db, "receivables", as_of=AS_OF)
  }


def test_one_payment_settles_each_document_by_what_it_applied(db):
  _sync(db, _invoice(42, 120000), _invoice(43, 50000))
  _sync(db, _payment([(42, "1000.00"), (43, "500.00")]))

  assert _allocations(db) == {"Invoice_42": 100000, "Invoice_43": 50000}
  # Without the rows the first invoice took the whole 1,500.00.
  assert _open(db) == {"Invoice_42": 20000}


def test_a_payment_ahead_of_its_documents_is_allocated_on_a_later_sync(db):
  _sync(db, _invoice(42, 120000), _payment([(42, "1000.00"), (43, "500.00")]))
  assert _allocations(db) == {}

  _sync(db, _invoice(43, 50000), _payment([(42, "1000.00"), (43, "500.00")]))

  assert _allocations(db) == {"Invoice_42": 100000, "Invoice_43": 50000}


def test_a_newer_version_reapplies_the_payment(db):
  _sync(db, _invoice(42, 120000), _invoice(43, 50000))
  _sync(db, _payment([(42, "1000.00"), (43, "500.00")]))

  _sync(db, _payment([(42, "1200.00"), (43, "300.00")], token="1"))

  assert _allocations(db) == {"Invoice_42": 120000, "Invoice_43": 30000}


def test_a_same_version_sync_keeps_the_rows_held(db):
  _sync(db, _invoice(42, 120000), _invoice(43, 50000))
  _sync(db, _payment([(42, "1000.00"), (43, "500.00")]))

  _sync(db, _payment([(42, "1200.00"), (43, "300.00")]))

  assert _allocations(db) == {"Invoice_42": 100000, "Invoice_43": 50000}


def test_a_line_that_does_not_say_how_it_splits_leaves_the_single_link(db):
  _sync(db, _invoice(42, 120000), _invoice(43, 50000))
  _sync(db, _payment([(42, ""), (43, "")]))

  assert _allocations(db) == {}
  payment = db.query(Event).filter(Event.external_id == "Payment_7").one()
  assert payment.discharges_event_id is not None
