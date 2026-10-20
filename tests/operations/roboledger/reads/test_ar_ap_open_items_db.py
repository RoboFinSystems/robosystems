"""Open receivables and payables as of a date, per entity and per group.

A settlement dated after the as-of date has not happened yet. A bank line that
pays a bill settles it by its magnitude, whatever its sign. A sales receipt is
paid at the sale and is never open. A group sums its parent and the members it
consolidates in full for the month, and sets apart the items its members owe
each other, beside the other side's books.
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
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.reads.ar_ap import (
  list_group_open_items,
  list_open_items,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

SUB = "ent_sub"
ASSOCIATE = "ent_associate"
ACME = "agt_acme"


@pytest.fixture()
def session():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_arap_{uuid.uuid4().hex[:12]}"
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
  for entity_id, method, joined in (
    (SUB, "full", None),
    (ASSOCIATE, "equity", None),
  ):
    db.add(
      Entity(
        id=entity_id,
        name=entity_id,
        is_parent=False,
        entity_role="operating",
        consolidation_method=method,
        joined_on=joined,
        source="native",
        created_by="t",
      )
    )
  db.add(Agent(id=ACME, agent_type="customer", name="Acme", created_by="t"))
  for entity_id in (PARENT_ENTITY_ID, SUB, ASSOCIATE):
    db.add(
      Agent(
        id=f"agt_self_{entity_id}",
        agent_type="self",
        name=entity_id,
        external_id=entity_id,
        created_by="t",
      )
    )
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


def _event(
  session,
  event_type: str,
  amount: int,
  on: date,
  *,
  entity: str = PARENT_ENTITY_ID,
  agent: str | None = ACME,
  settles: str | None = None,
  status: str = "committed",
) -> str:
  event = Event(
    id=f"evt_{uuid.uuid4().hex[:12]}",
    entity_id=entity,
    event_type=event_type,
    event_category="sales",
    occurred_at=datetime.combine(on, datetime.min.time()),
    source="native",
    amount=amount,
    agent_id=agent,
    discharges_event_id=settles,
    status=status,
    created_by="t",
  )
  session.add(event)
  session.flush()
  return str(event.id)


def _open(items) -> dict[str, int]:
  return {item.event_id: item.open_cents for item in items}


def test_a_payment_after_the_date_has_not_happened_yet(session):
  invoice = _event(session, "invoice_issued", 100000, date(2026, 9, 10))
  _event(session, "payment_received", 40000, date(2026, 9, 20), settles=invoice)
  _event(session, "payment_received", 60000, date(2026, 10, 5), settles=invoice)

  september = list_open_items(session, "receivables", as_of=date(2026, 9, 30))
  october = list_open_items(session, "receivables", as_of=date(2026, 10, 31))

  assert _open(september) == {invoice: 60000}
  assert september[0].settled_cents == 40000
  assert october == []


def test_an_invoice_issued_after_the_date_is_not_open_yet(session):
  _event(session, "invoice_issued", 100000, date(2026, 10, 2))

  assert list_open_items(session, "receivables", as_of=date(2026, 9, 30)) == []


def test_a_bank_line_settles_a_bill_by_its_magnitude(session):
  """Money out is negative on a bank line; it settles the bill, never
  doubles it."""
  bill = _event(session, "bill_received", 420000, date(2026, 9, 1))
  _event(session, "bank_transaction", -420000, date(2026, 9, 15), settles=bill)

  assert list_open_items(session, "payables", as_of=date(2026, 9, 30)) == []


def test_an_overpayment_is_an_unapplied_credit(session):
  invoice = _event(session, "invoice_issued", 50000, date(2026, 9, 1))
  _event(session, "payment_received", 55000, date(2026, 9, 2), settles=invoice)

  assert _open(list_open_items(session, "receivables", as_of=date(2026, 9, 30))) == {
    invoice: -5000
  }


def test_a_sales_receipt_is_never_open(session):
  _event(session, "sales_receipt_recorded", 25000, date(2026, 9, 1))

  assert list_open_items(session, "receivables", as_of=date(2026, 9, 30)) == []


def test_an_uncommitted_invoice_or_payment_does_not_count(session):
  invoice = _event(session, "invoice_issued", 100000, date(2026, 9, 1))
  _event(session, "invoice_issued", 70000, date(2026, 9, 1), status="captured")
  _event(
    session,
    "payment_received",
    100000,
    date(2026, 9, 2),
    settles=invoice,
    status="captured",
  )

  assert _open(list_open_items(session, "receivables", as_of=date(2026, 9, 30))) == {
    invoice: 100000
  }


def test_one_counterpartys_items(session):
  acme = _event(session, "invoice_issued", 100000, date(2026, 9, 1))
  _event(session, "invoice_issued", 30000, date(2026, 9, 1), agent=None)

  items = list_open_items(
    session, "receivables", as_of=date(2026, 9, 30), agent_id=ACME
  )

  assert _open(items) == {acme: 100000}


def test_the_group_sets_intercompany_items_apart_beside_the_other_side(session):
  external = _event(session, "invoice_issued", 100000, date(2026, 9, 1))
  to_sub = _event(
    session, "invoice_issued", 50000, date(2026, 9, 1), agent=f"agt_self_{SUB}"
  )
  # The subsidiary booked 450.00 of the parent's 500.00 charge.
  _event(
    session,
    "bill_received",
    45000,
    date(2026, 9, 1),
    entity=SUB,
    agent=f"agt_self_{PARENT_ENTITY_ID}",
  )
  # Owed by a member on the equity method: not eliminated, so external.
  to_associate = _event(
    session, "invoice_issued", 20000, date(2026, 9, 1), agent=f"agt_self_{ASSOCIATE}"
  )
  _event(session, "invoice_issued", 99000, date(2026, 9, 1), entity=ASSOCIATE)

  group = list_group_open_items(session, "receivables", as_of=date(2026, 9, 30))

  assert set(group.member_entity_ids) == {PARENT_ENTITY_ID, SUB}
  assert _open(group.external) == {external: 100000, to_associate: 20000}
  assert _open(group.intercompany) == {to_sub: 50000}
  assert [
    (p.creditor_entity_id, p.debtor_entity_id, p.difference_cents) for p in group.pairs
  ] == [(PARENT_ENTITY_ID, SUB, 5000)]


def test_a_member_not_yet_in_the_group_is_not_summed(session):
  session.query(Entity).filter(Entity.id == SUB).update(
    {Entity.joined_on: date(2026, 11, 1)}
  )
  _event(session, "invoice_issued", 70000, date(2026, 9, 1), entity=SUB)

  group = list_group_open_items(session, "receivables", as_of=date(2026, 9, 30))

  assert group.member_entity_ids == [PARENT_ENTITY_ID]
  assert group.external == []
