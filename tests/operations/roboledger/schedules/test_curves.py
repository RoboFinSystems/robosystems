"""The effective-interest curve's arithmetic, to the cent."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from robosystems.operations.roboledger.schedules.curves import (
  LoanTerms,
  accrual_days,
  amortization_table,
  reclass_cents,
)
from robosystems.operations.roboledger.schedules.service import (
  _generate_monthly_periods,
)

pytestmark = pytest.mark.unit


def _periods(start: date, end: date) -> list[tuple[date, date]]:
  return _generate_monthly_periods(start, end)


def test_a_line_of_credit_on_actual_360_reproduces_the_bank_interest():
  # $80,000 drawn on the 5th at 9.9%: the lender billed 594.00 for the 27
  # days of the first month and 660.00 for the 30 of the next.
  terms = LoanTerms(
    principal=8_000_000,
    annual_rate=Decimal("0.099"),
    start_date=date(2026, 8, 5),
    day_count="actual/360",
    interest_only=True,
  )
  table = amortization_table(terms, _periods(date(2026, 8, 1), date(2027, 1, 31)))

  assert table["interest"][:3] == [59_400, 66_000, 68_200]
  assert table["principal"] == [0] * 6
  assert table["balance"] == [8_000_000] * 6
  # No maturity, so nothing is due within twelve months.
  assert table["current_portion"] == [0] * 6


@pytest.mark.parametrize(
  ("convention", "expected"),
  [("30/360", 26), ("actual/365", 27), ("actual/360", 27)],
)
def test_a_part_month_counts_by_its_convention(convention, expected):
  assert accrual_days(date(2026, 8, 5), date(2026, 9, 1), convention)[0] == expected


def test_30_360_counts_every_whole_month_as_30_days():
  for month in (1, 2, 7):
    start = date(2026, month, 1)
    end = date(2026, month + 1, 1)
    assert accrual_days(start, end, "30/360") == (30, 360)


def test_a_level_payment_retires_the_principal_at_maturity():
  terms = LoanTerms(
    principal=10_000_000,
    annual_rate=Decimal("0.06"),
    start_date=date(2026, 1, 1),
    first_payment_date=date(2026, 1, 31),
    maturity_date=date(2030, 12, 31),
  )
  table = amortization_table(terms, _periods(date(2026, 1, 1), date(2030, 12, 31)))

  assert table["payment"][0] == 193_328  # the textbook $1,933.28
  assert table["interest"][0] == 50_000
  assert table["balance"][-1] == 0
  assert sum(table["principal"]) == 10_000_000
  # Each payment is interest plus principal, and the balance rolls.
  for i in range(len(table["payment"])):
    assert table["payment"][i] == table["interest_paid"][i] + table["principal"][i]
    assert table["balance"][i] == table["opening"][i] - table["principal"][i]
  # The current portion is the next twelve months' principal.
  assert table["current_portion"][0] == sum(table["principal"][1:13])
  assert table["current_portion"][-1] == 0


def test_the_reclass_moves_the_change_in_the_current_portion():
  columns = {"current_portion": [500, 520, 480]}
  assert [reclass_cents(columns, i) for i in range(3)] == [500, 20, -40]


def test_quarterly_payments_settle_the_interest_accrued_since_the_last():
  terms = LoanTerms(
    principal=1_000_000,
    annual_rate=Decimal("0.20"),
    start_date=date(2026, 8, 1),
    day_count="actual/365",
    payment_frequency="quarterly",
    first_payment_date=date(2026, 11, 10),
    interest_only=True,
  )
  table = amortization_table(terms, _periods(date(2026, 8, 1), date(2027, 1, 31)))

  assert table["payment"][:3] == [0, 0, 0]
  assert table["interest_paid"][3] == sum(table["interest"][:4])


def test_a_payment_short_of_the_interest_is_refused_with_its_month():
  terms = LoanTerms(
    principal=10_000_000,
    annual_rate=Decimal("0.12"),
    start_date=date(2026, 1, 1),
    first_payment_date=date(2026, 1, 31),
    payment_amount=50_000,
    maturity_date=date(2027, 12, 31),
  )
  with pytest.raises(ValueError, match="2026-01: the payment"):
    amortization_table(terms, _periods(date(2026, 1, 1), date(2027, 12, 31)))


def test_capitalized_interest_grows_the_balance():
  terms = LoanTerms(
    principal=1_000_000,
    annual_rate=Decimal("0.12"),
    start_date=date(2026, 1, 1),
    capitalize_interest=True,
    payment_frequency="at_maturity",
    maturity_date=date(2026, 3, 31),
  )
  table = amortization_table(terms, _periods(date(2026, 1, 1), date(2026, 3, 31)))

  assert table["balance"][:2] == [1_010_000, 1_020_100]
  assert table["interest_paid"] == [0, 0, 0]
  assert table["balance"][-1] == 0


def test_the_draw_must_fall_in_the_first_month():
  terms = LoanTerms(
    principal=1_000_000,
    annual_rate=Decimal("0.05"),
    start_date=date(2026, 3, 1),
    interest_only=True,
  )
  with pytest.raises(ValueError, match="first month"):
    amortization_table(terms, _periods(date(2026, 1, 1), date(2026, 6, 30)))


def test_an_amortizing_loan_needs_a_maturity_or_a_payment():
  terms = LoanTerms(
    principal=1_000_000, annual_rate=Decimal("0.05"), start_date=date(2026, 1, 1)
  )
  with pytest.raises(ValueError, match="maturity_date or payment_amount"):
    amortization_table(terms, _periods(date(2026, 1, 1), date(2026, 6, 30)))
