"""Parsers for QuickBooks reports read outside the sync pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


class TrialBalanceReportError(ValueError):
  """The TrialBalance report is not in the shape this parser reads."""


@dataclass(frozen=True)
class TrialBalanceAccount:
  account_id: str
  name: str
  debit_cents: int
  credit_cents: int

  @property
  def net_cents(self) -> int:
    return self.debit_cents - self.credit_cents


@dataclass(frozen=True)
class TrialBalanceReport:
  basis: str | None
  start_date: str | None
  end_date: str | None
  accounts: list[TrialBalanceAccount]


def _cents(value: Any, *, account: str, column: str) -> int:
  text = str(value or "").replace(",", "").strip()
  if not text:
    return 0
  try:
    return int((Decimal(text) * 100).quantize(Decimal(1)))
  except InvalidOperation:
    raise TrialBalanceReportError(
      f"TrialBalance {column} for account {account!r} is not a number: {value!r}"
    ) from None


def _money_columns(report: dict[str, Any]) -> tuple[int, int]:
  """Positions of the Debit and Credit columns, by title rather than order."""
  columns = (report.get("Columns") or {}).get("Column") or []
  titles = [str(col.get("ColTitle") or "").strip().lower() for col in columns]
  try:
    return titles.index("debit"), titles.index("credit")
  except ValueError:
    raise TrialBalanceReportError(
      f"TrialBalance has no Debit/Credit columns (columns: {titles})"
    ) from None


def _account_rows(rows: list[dict[str, Any]]):
  """Every data row naming an account, at any depth. Section headers and
  totals are skipped: a parent's own balance is a data row of its own."""
  for row in rows:
    cells = row.get("ColData")
    if cells and cells[0].get("id"):
      yield cells
    yield from _account_rows((row.get("Rows") or {}).get("Row") or [])


def parse_trial_balance_report(report: dict[str, Any] | None) -> TrialBalanceReport:
  """One row per account: its QuickBooks id, name and debit/credit in cents.

  Raises `TrialBalanceReportError` when the report has no Debit/Credit
  columns, rows but no account ids, or an amount that is not a number.
  """
  if not report:
    raise TrialBalanceReportError("QuickBooks returned no TrialBalance report")
  header = report.get("Header") or {}
  debit_col, credit_col = _money_columns(report)

  rows = (report.get("Rows") or {}).get("Row") or []
  accounts: list[TrialBalanceAccount] = []
  for cells in _account_rows(rows):
    account_id = str(cells[0]["id"])
    name = str(cells[0].get("value") or "")

    def cell(index: int, cells=cells) -> Any:
      return cells[index].get("value") if index < len(cells) else None

    accounts.append(
      TrialBalanceAccount(
        account_id=account_id,
        name=name,
        debit_cents=_cents(cell(debit_col), account=name, column="debit"),
        credit_cents=_cents(cell(credit_col), account=name, column="credit"),
      )
    )

  # Rows with no account among them would read as every account at zero.
  if rows and not accounts:
    raise TrialBalanceReportError(
      "TrialBalance rows carry no account ids; the report shape has changed"
    )

  return TrialBalanceReport(
    basis=header.get("ReportBasis"),
    start_date=header.get("StartPeriod"),
    end_date=header.get("EndPeriod"),
    accounts=accounts,
  )
