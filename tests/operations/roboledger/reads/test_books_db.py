"""The books predicates behind the provider guard, against real SQL.

A QuickBooks tenant can hold an account someone added in RoboLedger, and a
schedule drafting against it puts line items on it before anything posts.
Those drafts are not books, so reconnecting the same company is not refused
over them. A first connect over a chart QuickBooks did not build is refused,
since the first sync would merge its accounts into it.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Element, Taxonomy
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.reads.books import (
  chart_built_elsewhere,
  graph_has_native_line_items,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

QB = "quickbooks"


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_books_{uuid.uuid4().hex[:12]}"
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


def _chart(session) -> Taxonomy:
  chart = Taxonomy(
    name="Chart of Accounts", taxonomy_type="chart_of_accounts", is_active=True
  )
  session.add(chart)
  session.flush()
  return chart


def _account(session, *, name, source, chart=None) -> Element:
  element = Element(
    name=name,
    balance_type="debit",
    source=source,
    taxonomy_id=chart.id if chart is not None else None,
  )
  session.add(element)
  session.flush()
  return element


def _entry(session, debit, credit, *, status) -> Entry:
  entry = Entry(
    entity_id=PARENT_ENTITY_ID,
    posting_date=date(2026, 9, 30),
    status=status,
    type="adjusting",
    provenance="schedule_derived",
    created_by="usr_test",
  )
  session.add(entry)
  session.flush()
  session.add_all(
    [
      LineItem(
        entry_id=entry.id, element_id=debit.id, debit_amount=10_000, credit_amount=0
      ),
      LineItem(
        entry_id=entry.id, element_id=credit.id, debit_amount=0, credit_amount=10_000
      ),
    ]
  )
  session.flush()
  return entry


@pytest.mark.parametrize(
  ("status", "native"),
  [("draft", False), ("shadowed", False), ("posted", True), ("reversed", True)],
)
def test_only_a_posted_line_on_an_added_account_is_native_books(
  ext_session, status, native
):
  chart = _chart(ext_session)
  expense = _account(ext_session, name="Amortization Expense", source=QB, chart=chart)
  added = _account(
    ext_session, name="Accumulated Amortization", source="native", chart=chart
  )
  _entry(ext_session, expense, added, status=status)

  for entity_id in (PARENT_ENTITY_ID, None):
    assert (
      graph_has_native_line_items(ext_session, synced_source=QB, entity_id=entity_id)
      is native
    )


def test_a_template_chart_is_built_elsewhere(ext_session):
  chart = _chart(ext_session)
  _account(ext_session, name="Cash", source="native", chart=chart)

  assert chart_built_elsewhere(ext_session, synced_source=QB)


def test_the_providers_chart_is_its_own(ext_session):
  chart = _chart(ext_session)
  _account(ext_session, name="Cash", source=QB, chart=chart)

  assert not chart_built_elsewhere(ext_session, synced_source=QB)


def test_an_account_added_to_the_providers_chart_keeps_it_the_providers(
  ext_session,
):
  chart = _chart(ext_session)
  _account(ext_session, name="Cash", source=QB, chart=chart)
  _account(ext_session, name="Accumulated Amortization", source="native", chart=chart)

  assert not chart_built_elsewhere(ext_session, synced_source=QB)


@pytest.mark.parametrize("with_empty_chart", [False, True])
def test_no_accounts_is_nothing_to_merge_into(ext_session, with_empty_chart):
  if with_empty_chart:
    _chart(ext_session)

  assert not chart_built_elsewhere(ext_session, synced_source=QB)
