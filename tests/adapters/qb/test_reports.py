"""The QuickBooks TrialBalance report parser."""

from __future__ import annotations

import pytest

from robosystems.adapters.quickbooks.reports import (
  TrialBalanceReportError,
  parse_trial_balance_report,
)

pytestmark = pytest.mark.unit

_COLUMNS = {
  "Column": [
    {"ColTitle": "", "ColType": "Account"},
    {"ColTitle": "Debit", "ColType": "Money"},
    {"ColTitle": "Credit", "ColType": "Money"},
  ]
}


def _row(account_id: str | None, name: str, debit: str, credit: str) -> dict:
  first = {"value": name}
  if account_id is not None:
    first["id"] = account_id
  return {"ColData": [first, {"value": debit}, {"value": credit}], "type": "Data"}


def _report(rows: list[dict], columns: dict | None = None) -> dict:
  return {
    "Header": {
      "ReportName": "TrialBalance",
      "ReportBasis": "Accrual",
      "StartPeriod": "2026-01-01",
      "EndPeriod": "2026-08-31",
    },
    "Columns": columns or _COLUMNS,
    "Rows": {"Row": rows},
  }


def test_reads_each_account_in_cents_with_its_source_id():
  report = parse_trial_balance_report(
    _report(
      [
        _row("35", "Checking", "11,110.60", ""),
        _row("24", "Prepaid expenses", "4316.16", ""),
        _row("61", "Notes Payable", "", "10000.00"),
      ]
    )
  )

  assert report.basis == "Accrual"
  assert (report.start_date, report.end_date) == ("2026-01-01", "2026-08-31")
  assert [(a.account_id, a.net_cents) for a in report.accounts] == [
    ("35", 1_111_060),
    ("24", 431_616),
    ("61", -1_000_000),
  ]


def test_the_total_row_is_not_an_account():
  total = {
    "Summary": {"ColData": [{"value": "TOTAL"}, {"value": "5.00"}, {"value": "5.00"}]},
    "type": "Section",
    "group": "GrandTotal",
  }
  report = parse_trial_balance_report(
    _report(
      [_row("35", "Checking", "5.00", ""), _row(None, "TOTAL", "5.00", "5.00"), total]
    )
  )

  assert [a.account_id for a in report.accounts] == ["35"]


def test_reads_accounts_nested_under_a_section():
  section = {
    "Header": {
      "ColData": [{"value": "Expenses", "id": "70"}, {"value": ""}, {"value": ""}]
    },
    "Rows": {
      "Row": [
        _row("70", "Expenses", "10.00", ""),
        _row("71", "Expenses:Software", "25.50", ""),
      ]
    },
    "Summary": {
      "ColData": [{"value": "Total Expenses"}, {"value": "35.50"}, {"value": ""}]
    },
    "type": "Section",
  }
  report = parse_trial_balance_report(_report([section]))

  assert [(a.account_id, a.debit_cents) for a in report.accounts] == [
    ("70", 1_000),
    ("71", 2_550),
  ]


def test_columns_are_found_by_title_not_position():
  columns = {
    "Column": [
      {"ColTitle": "", "ColType": "Account"},
      {"ColTitle": "Credit", "ColType": "Money"},
      {"ColTitle": "Debit", "ColType": "Money"},
    ]
  }
  report = parse_trial_balance_report(
    _report([_row("35", "Checking", "1.00", "9.00")], columns)
  )

  assert (report.accounts[0].credit_cents, report.accounts[0].debit_cents) == (100, 900)


def test_a_half_cent_never_arrives_but_fractions_keep_their_cents():
  report = parse_trial_balance_report(_report([_row("35", "Checking", "0.07", "0.1")]))

  assert (report.accounts[0].debit_cents, report.accounts[0].credit_cents) == (7, 10)


@pytest.mark.parametrize("report", [None, {}])
def test_an_empty_response_is_refused(report):
  with pytest.raises(TrialBalanceReportError, match="no TrialBalance"):
    parse_trial_balance_report(report)


def test_a_report_without_money_columns_is_refused():
  columns = {"Column": [{"ColTitle": "", "ColType": "Account"}]}
  with pytest.raises(TrialBalanceReportError, match="Debit/Credit"):
    parse_trial_balance_report(_report([], columns))


def test_rows_without_account_ids_are_refused_not_read_as_zero():
  with pytest.raises(TrialBalanceReportError, match="no account ids"):
    parse_trial_balance_report(_report([_row(None, "Checking", "5.00", "")]))


def test_a_company_with_no_activity_has_no_accounts():
  assert parse_trial_balance_report(_report([])).accounts == []


def test_an_amount_that_is_not_a_number_is_refused():
  with pytest.raises(TrialBalanceReportError, match="Checking"):
    parse_trial_balance_report(_report([_row("35", "Checking", "n/a", "")]))
