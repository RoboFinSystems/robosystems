"""Receivables and payables aged at a date.

An item buckets by how far past its due date it is at the as-of date; one with
no due date of its own is due on receipt, and says so. An overpayment is an
unapplied credit, never netted into a bucket. Each entity's open items are
tied out against its control accounts, the difference named and never
plugged: the accounts are those mapped to the receivables or payables concept,
or QuickBooks' own A/R or A/P account.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.config import env
from robosystems.db.extensions import (
  ExtensionsBase,
  extensions_session,
  provision_tenant_schema,
)
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.reads.ar_ap import (
  get_aged_balances,
  list_open_items,
)
from tests.ledger_entity import PARENT_ENTITY_ID, entity_account, seed_parent_entity

pytestmark = pytest.mark.unit

SUB = "ent_sub"
ACME = "agt_acme"
GLOBEX = "agt_globex"
AS_OF = date(2026, 9, 30)


@pytest.fixture()
def session():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_aging_{uuid.uuid4().hex[:12]}"
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
  db.add(
    Entity(
      id=SUB,
      name=SUB,
      is_parent=False,
      entity_role="operating",
      consolidation_method="full",
      source="native",
      created_by="t",
    )
  )
  db.add(Agent(id=ACME, agent_type="customer", name="Acme", created_by="t"))
  db.add(Agent(id=GLOBEX, agent_type="customer", name="Globex", created_by="t"))
  for entity_id in (PARENT_ENTITY_ID, SUB):
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
  due: date | None = None,
  entity: str = PARENT_ENTITY_ID,
  agent: str | None = ACME,
  settles: str | None = None,
) -> str:
  event = Event(
    id=f"evt_{uuid.uuid4().hex[:12]}",
    entity_id=entity,
    event_type=event_type,
    event_category="sales",
    occurred_at=datetime.combine(on, datetime.min.time()),
    due_date=due,
    source="native",
    amount=amount,
    agent_id=agent,
    discharges_event_id=settles,
    status="committed",
    created_by="t",
  )
  session.add(event)
  session.flush()
  return str(event.id)


def _post(
  session,
  account: str,
  *,
  debit: int = 0,
  credit: int = 0,
  on: date = date(2026, 9, 1),
  entity: str = PARENT_ENTITY_ID,
  status: str = "posted",
) -> None:
  entry = Entry(
    entity_id=entity,
    posting_date=on,
    status=status,
    created_by="t",
  )
  session.add(entry)
  session.flush()
  session.add(
    LineItem(
      entry_id=entry.id,
      element_id=account,
      debit_amount=debit,
      credit_amount=credit,
    )
  )
  session.flush()


def _qb_account(session, entity_id: str, name: str, account_type: str) -> str:
  account = entity_account(session, entity_id, name)
  session.get(Element, account).metadata_ = {"account_type": account_type}
  session.flush()
  return account


def test_items_bucket_by_days_past_due(session):
  _event(session, "invoice_issued", 10000, date(2026, 9, 1), due=date(2026, 10, 1))
  _event(session, "invoice_issued", 20000, date(2026, 8, 1), due=date(2026, 9, 30))
  _event(session, "invoice_issued", 30000, date(2026, 8, 1), due=date(2026, 8, 31))
  _event(session, "invoice_issued", 40000, date(2026, 7, 1), due=date(2026, 8, 15))
  _event(session, "invoice_issued", 50000, date(2026, 6, 1), due=date(2026, 7, 15))
  _event(session, "invoice_issued", 60000, date(2026, 5, 1), due=date(2026, 6, 1))

  aged = get_aged_balances(session, "receivables", as_of=AS_OF)

  assert aged.buckets.current_cents == 30000
  assert aged.buckets.one_to_thirty_cents == 30000
  assert aged.buckets.thirty_one_to_sixty_cents == 40000
  assert aged.buckets.sixty_one_to_ninety_cents == 50000
  assert aged.buckets.over_ninety_cents == 60000
  assert aged.buckets.total_cents == 210000
  assert aged.defaulted_due_date_count == 0


def test_an_item_with_no_due_date_is_due_on_receipt_and_says_so(session):
  invoice = _event(session, "invoice_issued", 10000, date(2026, 8, 15))

  [item] = list_open_items(session, "receivables", as_of=AS_OF)
  aged = get_aged_balances(session, "receivables", as_of=AS_OF)

  assert item.event_id == invoice
  assert item.due_date == date(2026, 8, 15)
  assert item.due_date_basis == "defaulted"
  assert item.days_past_due == 46
  assert item.bucket == "thirty_one_to_sixty"
  assert aged.defaulted_due_date_count == 1


def test_an_overpayment_is_unapplied_credit_never_a_bucket(session):
  old = _event(session, "invoice_issued", 50000, date(2026, 5, 1), due=date(2026, 5, 1))
  _event(session, "payment_received", 55000, date(2026, 9, 2), settles=old)
  _event(session, "invoice_issued", 10000, date(2026, 9, 1), due=date(2026, 10, 1))

  aged = get_aged_balances(session, "receivables", as_of=AS_OF)

  assert aged.buckets.over_ninety_cents == 0
  assert aged.buckets.unapplied_credit_cents == -5000
  assert aged.buckets.current_cents == 10000
  assert aged.buckets.total_cents == 5000


def test_a_row_per_counterparty_largest_first(session):
  _event(session, "invoice_issued", 10000, date(2026, 9, 1), due=date(2026, 10, 1))
  _event(session, "invoice_issued", 20000, date(2026, 6, 1), due=date(2026, 7, 15))
  _event(
    session,
    "invoice_issued",
    90000,
    date(2026, 9, 1),
    due=date(2026, 10, 1),
    agent=GLOBEX,
  )

  aged = get_aged_balances(session, "receivables", as_of=AS_OF)

  assert [(r.agent_name, r.buckets.total_cents) for r in aged.agents] == [
    ("Globex", 90000),
    ("Acme", 30000),
  ]
  acme = aged.agents[1]
  assert acme.open_item_count == 2
  assert acme.oldest_due_date == date(2026, 7, 15)
  assert acme.buckets.sixty_one_to_ninety_cents == 20000


def test_the_tie_out_names_what_moved_the_account_without_a_document(session):
  receivable = _qb_account(session, PARENT_ENTITY_ID, "A/R", "Accounts Receivable")
  _event(session, "invoice_issued", 100000, date(2026, 9, 1), due=date(2026, 10, 1))
  _post(session, receivable, debit=100000)
  # A write-off straight to the account, with no document behind it.
  _post(session, receivable, credit=10000, on=date(2026, 9, 20))
  # Neither a draft nor an entry after the date has moved the books yet.
  _post(session, receivable, debit=7000, status="draft")
  _post(session, receivable, debit=3000, on=date(2026, 10, 2))

  [tie_out] = get_aged_balances(session, "receivables", as_of=AS_OF).tie_outs

  assert [a.element_id for a in tie_out.accounts] == [receivable]
  assert tie_out.control_balance_cents == 90000
  assert tie_out.open_items_cents == 100000
  assert tie_out.difference_cents == 10000


def test_payables_tie_out_in_their_natural_sign(session):
  payable = _qb_account(session, PARENT_ENTITY_ID, "A/P", "Accounts Payable")
  _event(
    session, "bill_received", 42000, date(2026, 9, 1), due=date(2026, 9, 30), agent=None
  )
  _post(session, payable, credit=42000)

  [tie_out] = get_aged_balances(session, "payables", as_of=AS_OF).tie_outs

  assert tie_out.control_balance_cents == 42000
  assert tie_out.difference_cents == 0


def test_no_control_account_leaves_the_difference_unstated(session):
  _event(session, "invoice_issued", 10000, date(2026, 9, 1))

  [tie_out] = get_aged_balances(session, "receivables", as_of=AS_OF).tie_outs

  assert tie_out.accounts == []
  assert tie_out.control_balance_cents is None
  assert tie_out.difference_cents is None
  assert tie_out.open_items_cents == 10000


def test_the_group_ages_external_items_and_ties_out_each_member(session):
  parent_ar = _qb_account(session, PARENT_ENTITY_ID, "A/R", "Accounts Receivable")
  sub_ar = _qb_account(session, SUB, "A/R", "Accounts Receivable")
  _event(session, "invoice_issued", 100000, date(2026, 9, 1), due=date(2026, 10, 1))
  _event(
    session,
    "invoice_issued",
    25000,
    date(2026, 6, 1),
    due=date(2026, 7, 15),
    entity=SUB,
    agent=GLOBEX,
  )
  to_sub = _event(
    session,
    "invoice_issued",
    50000,
    date(2026, 9, 1),
    due=date(2026, 9, 1),
    agent=f"agt_self_{SUB}",
  )
  _post(session, parent_ar, debit=150000)
  _post(session, sub_ar, debit=25000, entity=SUB)

  aged = get_aged_balances(session, "receivables", as_of=AS_OF, scope="group")

  assert aged.scope == "group"
  assert aged.entity_id == PARENT_ENTITY_ID
  assert set(aged.member_entity_ids) == {PARENT_ENTITY_ID, SUB}
  assert aged.buckets.current_cents == 100000
  assert aged.buckets.sixty_one_to_ninety_cents == 25000
  assert aged.buckets.total_cents == 125000
  assert [i.event_id for i in aged.intercompany] == [to_sub]
  assert {r.agent_id for r in aged.agents} == {ACME, GLOBEX}
  tie_outs = {t.entity_id: t for t in aged.tie_outs}
  # The parent's books carry its receivable from the subsidiary too.
  assert tie_outs[PARENT_ENTITY_ID].open_items_cents == 150000
  assert tie_outs[PARENT_ENTITY_ID].difference_cents == 0
  assert tie_outs[SUB].control_balance_cents == 25000
  assert tie_outs[SUB].difference_cents == 0


# ---- The control account through the chart's mapping -----------------------

GRAPH = "kgdddddddddddddddd65"
PARENT = "ent_cedar"


@pytest.fixture()
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  with engine.begin() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
  try:
    provision_tenant_schema(GRAPH)
    with extensions_session(GRAPH) as session:
      session.add(
        Entity(
          id=PARENT,
          name="Cedar Advisory LLC",
          entity_type="llc",
          is_parent=True,
          created_by="usr_1",
        )
      )
      session.flush()
      chart = initialize_chart_of_accounts(
        session, InitializeChartOfAccountsRequest(template="services"), "usr_1"
      ).taxonomy_id
      session.commit()
    yield chart
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def test_the_control_account_is_the_one_the_chart_maps_to_receivables(tenant):
  with (
    patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
    extensions_session(GRAPH) as session,
  ):
    receivable = session.execute(
      select(Element.id).where(Element.taxonomy_id == tenant, Element.code == "1100")
    ).scalar_one()
    _event(
      session,
      "invoice_issued",
      80000,
      date(2026, 9, 1),
      due=date(2026, 10, 1),
      entity=PARENT,
      agent=None,
    )
    _post(session, str(receivable), debit=80000, entity=PARENT)

    [tie_out] = get_aged_balances(session, "receivables", as_of=AS_OF).tie_outs

    assert [a.code for a in tie_out.accounts] == ["1100"]
    assert tie_out.control_balance_cents == 80000
    assert tie_out.difference_cents == 0
    session.rollback()
