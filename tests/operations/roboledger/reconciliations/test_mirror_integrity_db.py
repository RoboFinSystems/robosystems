"""The source-ledger comparison against real Postgres.

The ledger side is read by the trial balance's own SQL, so these run it for
real: which entries have landed, which window each kind of account is summed
over, and where earlier years' results go.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.adapters.quickbooks.reports import (
  TrialBalanceAccount,
  TrialBalanceReport,
)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.commands.reconciliations import (
  preview_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import (
  SourceLedgerResolver,
  reconciliation_window,
)

pytestmark = pytest.mark.unit

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


def _account(
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


def _entry(
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
    "cash": _account(session, "Checking", period_type="instant", source_id="35"),
    "revenue": _account(session, "Services", period_type="duration", source_id="50"),
    "software": _account(session, "Software", period_type="duration", source_id="70"),
    "retained": _account(
      session,
      "Retained Earnings",
      period_type="instant",
      source_id="3",
      sub_type="RetainedEarnings",
    ),
  }
  _entry(session, date(2025, 6, 15), accounts["cash"], accounts["revenue"], 100_000)
  _entry(session, date(2026, 3, 10), accounts["cash"], accounts["revenue"], 50_000)
  _entry(session, date(2026, 4, 2), accounts["software"], accounts["cash"], 12_000)
  # Neither is in the books at the period end: a draft, and a later month.
  _entry(
    session,
    date(2026, 8, 20),
    accounts["software"],
    accounts["cash"],
    5_000,
    status="draft",
  )
  _entry(session, date(2026, 9, 5), accounts["cash"], accounts["revenue"], 1_000)
  session.commit()
  return accounts


def _source(*rows: tuple[str, str, int]) -> TrialBalanceReport:
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


_TIED = (
  ("35", "Checking", 138_000),
  ("50", "Services", -50_000),
  ("70", "Software", 12_000),
  ("3", "Retained Earnings", -100_000),
)


def _preview(session, report: TrialBalanceReport, *, include_tied: bool = False):
  with patch.object(
    SourceLedgerResolver,
    "_fetch",
    return_value=(report, LIVE_CONNECTION, SYNCED_AT),
  ) as fetch:
    result = preview_reconciliations(
      session,
      PreviewReconciliationsRequest(period="2026-08", include_tied=include_tied),
      graph_id=GRAPH_ID,
    )
  return result, fetch


def test_a_faithful_mirror_ties_on_every_account(ext_session, books):
  result, fetch = _preview(ext_session, _source(*_TIED), include_tied=True)

  assert (result.accounts_compared, result.accounts_tied) == (4, 4)
  assert result.accounts_different == 0
  assert result.total_difference == 0
  assert {row.account_name: row.ledger_balance for row in result.rows} == {
    "Checking": 1380.00,
    "Services": -500.00,
    "Software": 120.00,
    "Retained Earnings": -1000.00,
  }
  assert result.as_of == date(2026, 8, 31)
  assert result.report_basis == "Accrual"
  # Asked from the fiscal year's first day.
  window = fetch.call_args.args[0]
  assert window.fiscal_year_start == date(2026, 1, 1)


def test_tied_accounts_stay_out_of_the_rows_unless_asked_for(ext_session, books):
  result, _ = _preview(ext_session, _source(*_TIED))

  assert result.rows == []
  assert result.accounts_tied == 4


def test_a_transaction_removed_at_the_source_shows_on_both_its_accounts(
  ext_session, books
):
  """QuickBooks no longer has the April software bill; the mirror still does."""
  source = _source(
    ("35", "Checking", 150_000),
    ("50", "Services", -50_000),
    ("3", "Retained Earnings", -100_000),
  )

  result, _ = _preview(ext_session, source)

  assert [(r.account_name, r.status, r.difference) for r in result.rows] == [
    ("Checking", "different", -120.00),
    ("Software", "different", 120.00),
  ]
  assert result.accounts_tied == 2
  assert result.total_difference == 240.00


def test_a_source_account_the_ledger_never_received(ext_session, books):
  result, _ = _preview(ext_session, _source(*_TIED, ("88", "Undeposited Funds", 2_500)))

  (row,) = result.rows
  assert (row.status, row.element_id, row.source_account_id) == (
    "not_in_ledger",
    None,
    "88",
  )
  assert (row.ledger_balance, row.independent_balance) == (0, 25.00)


def test_a_ledger_account_the_source_does_not_have(ext_session, books):
  accrual = _account(
    ext_session, "Accrued Bonus", period_type="instant", source_id=None
  )
  _entry(ext_session, date(2026, 8, 31), books["software"], accrual, 7_500)
  ext_session.commit()

  result, _ = _preview(ext_session, _source(*_TIED))

  by_name = {row.account_name: row for row in result.rows}
  assert by_name["Accrued Bonus"].status == "not_in_source"
  assert by_name["Accrued Bonus"].ledger_balance == -75.00
  assert by_name["Software"].status == "different"


def test_the_live_connections_element_is_the_mirror(ext_session, books):
  """A reconnect left an earlier copy of the account under the same source id."""
  stale = _account(
    ext_session,
    "Checking (old)",
    period_type="instant",
    source_id="35",
    connection_id="conn_before",
  )
  ext_session.commit()

  result, _ = _preview(ext_session, _source(*_TIED), include_tied=True)

  checking = [row for row in result.rows if row.source_account_id == "35"]
  assert [row.element_id for row in checking] == [books["cash"]]
  assert stale not in {row.element_id for row in result.rows}


def test_earlier_years_with_no_retained_earnings_account_say_so(ext_session, books):
  """The account is there but the sync never marked it, so 2025's result has
  nowhere to go and the note names the cause."""
  ext_session.get(Element, books["retained"]).metadata_ = {}
  ext_session.commit()

  result, _ = _preview(ext_session, _source(*_TIED))

  (row,) = result.rows
  assert (row.account_name, row.status, row.difference) == (
    "Retained Earnings",
    "different",
    1000.00,
  )
  assert any("no chart account is marked" in note for note in result.notes)


def test_a_stale_sync_is_called_out(ext_session, books):
  with patch.object(
    SourceLedgerResolver,
    "_fetch",
    return_value=(_source(*_TIED), LIVE_CONNECTION, datetime(2026, 8, 12)),
  ):
    result = preview_reconciliations(
      ext_session, PreviewReconciliationsRequest(period="2026-08"), graph_id=GRAPH_ID
    )

  assert any("older than the period end" in note for note in result.notes)


def test_income_accounts_are_summed_over_the_ledgers_own_fiscal_year(
  ext_session, books
):
  """A July year start makes March and April 2026 an earlier year: their
  result leaves the income accounts and lands in retained earnings."""
  ext_session.add(FiscalCalendar(graph_id=GRAPH_ID, fiscal_year_start_month=7))
  ext_session.commit()
  source = _source(("35", "Checking", 138_000), ("3", "Retained Earnings", -138_000))

  result, fetch = _preview(ext_session, source, include_tied=True)

  assert fetch.call_args.args[0].fiscal_year_start == date(2026, 7, 1)
  assert result.fiscal_year_start == date(2026, 7, 1)
  assert result.accounts_different == 0
  assert {row.account_name: row.ledger_balance for row in result.rows} == {
    "Checking": 1380.00,
    "Retained Earnings": -1380.00,
  }


@pytest.mark.parametrize(
  ("period", "start_month", "expected"),
  [
    ("2026-08", 1, date(2026, 1, 1)),
    ("2026-01", 1, date(2026, 1, 1)),
    ("2026-08", 7, date(2026, 7, 1)),
    ("2026-06", 7, date(2025, 7, 1)),
    ("2026-12", 12, date(2026, 12, 1)),
  ],
)
def test_the_fiscal_year_holding_the_period(period, start_month, expected):
  assert reconciliation_window(period, start_month).fiscal_year_start == expected


def test_a_malformed_period_is_refused():
  with pytest.raises(ValueError, match="YYYY-MM"):
    reconciliation_window("2026-8", 1)
