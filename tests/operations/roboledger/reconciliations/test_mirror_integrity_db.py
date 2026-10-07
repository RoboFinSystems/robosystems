"""The source-ledger comparison against real Postgres.

The ledger side is read by the trial balance's own SQL, so these run it for
real: which entries have landed, which window each kind of account is summed
over, and where earlier years' results go.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import patch

import pytest

from robosystems.adapters.quickbooks.reports import TrialBalanceReport
from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.operations.roboledger.commands.reconciliations import (
  preview_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import (
  SourceLedgerResolver,
  reconciliation_window,
)
from tests.ledger_entity import PARENT_ENTITY_ID

from .conftest import (
  GRAPH_ID,
  LIVE_CONNECTION,
  SYNCED_AT,
  TIED,
  account,
  entry,
  source_report,
)

pytestmark = pytest.mark.unit


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


def test_a_faithful_mirror_ties_on_everyaccount(ext_session, books):
  result, fetch = _preview(ext_session, source_report(*TIED), include_tied=True)

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
  result, _ = _preview(ext_session, source_report(*TIED))

  assert result.rows == []
  assert result.accounts_tied == 4


def test_a_transaction_removed_at_the_source_shows_on_both_its_accounts(
  ext_session, books
):
  """QuickBooks no longer has the April software bill; the mirror still does."""
  source = source_report(
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
  result, _ = _preview(
    ext_session, source_report(*TIED, ("88", "Undeposited Funds", 2_500))
  )

  (row,) = result.rows
  assert (row.status, row.element_id, row.source_account_id) == (
    "not_in_ledger",
    None,
    "88",
  )
  assert (row.ledger_balance, row.independent_balance) == (0, 25.00)


def test_a_ledger_account_the_source_does_not_have(ext_session, books):
  accrual = account(ext_session, "Accrued Bonus", period_type="instant", source_id=None)
  entry(ext_session, date(2026, 8, 31), books["software"], accrual, 7_500)
  ext_session.commit()

  result, _ = _preview(ext_session, source_report(*TIED))

  by_name = {row.account_name: row for row in result.rows}
  assert by_name["Accrued Bonus"].status == "not_in_source"
  assert by_name["Accrued Bonus"].ledger_balance == -75.00
  assert by_name["Software"].status == "different"


def test_the_live_connections_element_is_the_mirror(ext_session, books):
  """A reconnect left an earlier copy of the account under the same source id."""
  stale = account(
    ext_session,
    "Checking (old)",
    period_type="instant",
    source_id="35",
    connection_id="conn_before",
  )
  ext_session.commit()

  result, _ = _preview(ext_session, source_report(*TIED), include_tied=True)

  checking = [row for row in result.rows if row.source_account_id == "35"]
  assert [row.element_id for row in checking] == [books["cash"]]
  assert stale not in {row.element_id for row in result.rows}


def test_earlier_years_with_no_retained_earnings_account_say_so(ext_session, books):
  """The account is there but the sync never marked it, so 2025's result has
  nowhere to go and the note names the cause."""
  ext_session.get(Element, books["retained"]).metadata_ = {}
  ext_session.commit()

  result, _ = _preview(ext_session, source_report(*TIED))

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
    return_value=(source_report(*TIED), LIVE_CONNECTION, datetime(2026, 8, 12)),
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
  ext_session.add(
    FiscalCalendar(
      entity_id=PARENT_ENTITY_ID, graph_id=GRAPH_ID, fiscal_year_start_month=7
    )
  )
  ext_session.commit()
  source = source_report(
    ("35", "Checking", 138_000), ("3", "Retained Earnings", -138_000)
  )

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
