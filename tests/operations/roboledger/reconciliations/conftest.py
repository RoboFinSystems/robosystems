"""A synced ledger on real Postgres, shared by the reconciliation tests."""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.adapters.quickbooks.reports import (
  TrialBalanceAccount,
  TrialBalanceReport,
)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem

GRAPH_ID = "kg0123456789abcdef03"
LIVE_CONNECTION = "conn_live"
SYNCED_AT = datetime(2026, 9, 2, 8, 0)


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_recs_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
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


def account(
  session,
  name: str,
  *,
  period_type: str,
  source_id: str | None,
  sub_type: str | None = None,
  connection_id: str = LIVE_CONNECTION,
) -> str:
  element = Element(
    name=name,
    code=name[:8],
    period_type=period_type,
    external_id=source_id,
    external_source="quickbooks" if source_id else None,
    connection_id=connection_id if source_id else None,
    metadata_={"account_sub_type": sub_type} if sub_type else {},
    created_by="test",
  )
  session.add(element)
  session.flush()
  return str(element.id)


def classified_account(
  session, name: str, trait: str, *, balance_type: str = "debit"
) -> str:
  """A chart account carrying its financial-statement element as a trait,
  the way a synced or initialized chart does."""
  from robosystems.models.extensions import ElementTrait, Trait

  trait_row = (
    session.query(Trait)
    .filter(
      Trait.category == "elementsOfFinancialStatements", Trait.identifier == trait
    )
    .first()
  )
  if trait_row is None:
    trait_row = Trait(category="elementsOfFinancialStatements", identifier=trait)
    session.add(trait_row)
    session.flush()
  element = Element(
    name=name,
    code=name[:8],
    balance_type=balance_type,
    period_type="instant"
    if trait in ("asset", "contraAsset", "liability")
    else "duration",
    created_by="test",
  )
  session.add(element)
  session.flush()
  session.add(
    ElementTrait(element_id=element.id, trait_id=trait_row.id, is_primary=True)
  )
  session.flush()
  return str(element.id)


def entry(
  session, posting_date: date, debit: str, credit: str, cents: int, *, status="posted"
):
  entry = Entry(
    posting_date=posting_date,
    status=status,
    type="standard",
    provenance="manual_entry",
    created_by="test",
  )
  session.add(entry)
  session.flush()
  session.add(
    LineItem(entry_id=entry.id, element_id=debit, debit_amount=cents, line_order=1)
  )
  session.add(
    LineItem(entry_id=entry.id, element_id=credit, credit_amount=cents, line_order=2)
  )
  session.flush()


@pytest.fixture()
def books(ext_session):
  """A synced ledger with one earlier year of activity. At 2026-08-31:
  cash 1,380.00 DR, revenue 500.00 CR and software 120.00 DR for the year,
  retained earnings 1,000.00 CR from 2025."""
  session = ext_session
  accounts = {
    "cash": account(session, "Checking", period_type="instant", source_id="35"),
    "revenue": account(session, "Services", period_type="duration", source_id="50"),
    "software": account(session, "Software", period_type="duration", source_id="70"),
    "retained": account(
      session,
      "Retained Earnings",
      period_type="instant",
      source_id="3",
      sub_type="RetainedEarnings",
    ),
  }
  entry(session, date(2025, 6, 15), accounts["cash"], accounts["revenue"], 100_000)
  entry(session, date(2026, 3, 10), accounts["cash"], accounts["revenue"], 50_000)
  entry(session, date(2026, 4, 2), accounts["software"], accounts["cash"], 12_000)
  # Neither is in the books at the period end: a draft, and a later month.
  entry(
    session,
    date(2026, 8, 20),
    accounts["software"],
    accounts["cash"],
    5_000,
    status="draft",
  )
  entry(session, date(2026, 9, 5), accounts["cash"], accounts["revenue"], 1_000)
  session.commit()
  return accounts


def source_report(*rows: tuple[str, str, int]) -> TrialBalanceReport:
  """A source trial balance from (account id, name, debit-positive cents)."""
  return TrialBalanceReport(
    basis="Accrual",
    start_date="2026-01-01",
    end_date="2026-08-31",
    accounts=[
      TrialBalanceAccount(
        account_id=account_id,
        name=name,
        debit_cents=max(cents, 0),
        credit_cents=max(-cents, 0),
      )
      for account_id, name, cents in rows
    ],
  )


TIED = (
  ("35", "Checking", 138_000),
  ("50", "Services", -50_000),
  ("70", "Software", 12_000),
  ("3", "Retained Earnings", -100_000),
)
