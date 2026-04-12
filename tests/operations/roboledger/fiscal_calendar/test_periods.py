"""Tests for period utility functions."""

from __future__ import annotations

from datetime import date

import pytest

from robosystems.operations.roboledger.fiscal_calendar.periods import (
  add_months,
  current_month_period,
  last_completed_period,
  last_day_of_month,
  next_period,
  parse_period,
  period_date_range,
  period_from_date,
  period_name,
  previous_period,
)


class TestParsePeriod:
  def test_valid(self):
    assert parse_period("2026-03") == (2026, 3)
    assert parse_period("2025-12") == (2025, 12)
    assert parse_period("2026-01") == (2026, 1)

  @pytest.mark.parametrize(
    "bad",
    [
      "2026-3",  # month not zero-padded
      "2026-13",  # invalid month
      "2026-00",  # invalid month
      "26-03",  # year not 4 digits
      "2026/03",  # wrong separator
      "",
      "march-2026",
      "2026",
    ],
  )
  def test_invalid_raises(self, bad):
    with pytest.raises(ValueError):
      parse_period(bad)


class TestPeriodName:
  def test_basic(self):
    assert period_name(2026, 3) == "2026-03"
    assert period_name(2026, 12) == "2026-12"
    assert period_name(1999, 1) == "1999-01"

  def test_invalid_month_raises(self):
    with pytest.raises(ValueError):
      period_name(2026, 13)
    with pytest.raises(ValueError):
      period_name(2026, 0)


class TestLastDayOfMonth:
  def test_31_day_months(self):
    assert last_day_of_month(2026, 1) == 31
    assert last_day_of_month(2026, 3) == 31
    assert last_day_of_month(2026, 12) == 31

  def test_30_day_months(self):
    assert last_day_of_month(2026, 4) == 30
    assert last_day_of_month(2026, 6) == 30
    assert last_day_of_month(2026, 9) == 30
    assert last_day_of_month(2026, 11) == 30

  def test_february_non_leap(self):
    assert last_day_of_month(2026, 2) == 28

  def test_february_leap(self):
    assert last_day_of_month(2028, 2) == 29
    assert last_day_of_month(2000, 2) == 29  # divisible by 400
    assert last_day_of_month(2100, 2) == 28  # divisible by 100 but not 400


class TestAddMonths:
  def test_forward(self):
    assert add_months("2026-03", 1) == "2026-04"
    assert add_months("2026-03", 9) == "2026-12"
    assert add_months("2026-03", 10) == "2027-01"
    assert add_months("2026-03", 24) == "2028-03"

  def test_backward(self):
    assert add_months("2026-03", -1) == "2026-02"
    assert add_months("2026-03", -3) == "2025-12"
    assert add_months("2026-03", -15) == "2024-12"

  def test_zero(self):
    assert add_months("2026-03", 0) == "2026-03"

  def test_year_wrap(self):
    assert add_months("2025-12", 1) == "2026-01"
    assert add_months("2026-01", -1) == "2025-12"


class TestNextPreviousPeriod:
  def test_next(self):
    assert next_period("2026-03") == "2026-04"
    assert next_period("2025-12") == "2026-01"

  def test_previous(self):
    assert previous_period("2026-03") == "2026-02"
    assert previous_period("2026-01") == "2025-12"


class TestPeriodDateRange:
  def test_march(self):
    start, end = period_date_range("2026-03")
    assert start == date(2026, 3, 1)
    assert end == date(2026, 3, 31)

  def test_february_non_leap(self):
    start, end = period_date_range("2026-02")
    assert start == date(2026, 2, 1)
    assert end == date(2026, 2, 28)

  def test_february_leap(self):
    start, end = period_date_range("2028-02")
    assert start == date(2028, 2, 1)
    assert end == date(2028, 2, 29)


class TestCurrentLastPeriod:
  def test_current_month_period(self):
    today = date(2026, 4, 12)
    assert current_month_period(today) == "2026-04"

  def test_last_completed_period(self):
    today = date(2026, 4, 12)
    assert last_completed_period(today) == "2026-03"

  def test_last_completed_january(self):
    today = date(2026, 1, 15)
    assert last_completed_period(today) == "2025-12"


class TestPeriodFromDate:
  def test_basic(self):
    assert period_from_date(date(2026, 3, 15)) == "2026-03"
    assert period_from_date(date(2026, 12, 31)) == "2026-12"
