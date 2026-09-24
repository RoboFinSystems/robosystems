"""`YYYY-MM` period-name parsing, arithmetic, and date ranges."""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import date

PERIOD_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parse_period(period: str) -> tuple[int, int]:
  """Parse `YYYY-MM` into (year, month); `ValueError` if malformed."""
  match = PERIOD_PATTERN.match(period)
  if not match:
    raise ValueError(
      f"Invalid period format {period!r}. Expected YYYY-MM (e.g. '2026-03')."
    )
  return int(match.group(1)), int(match.group(2))


def period_name(year: int, month: int) -> str:
  if not 1 <= month <= 12:
    raise ValueError(f"Invalid month {month}; must be 1-12.")
  return f"{year:04d}-{month:02d}"


def last_day_of_month(year: int, month: int) -> int:
  return monthrange(year, month)[1]


def add_months(period: str, months: int) -> str:
  """Shift `period` by `months` (negative = backward)."""
  year, month = parse_period(period)
  total = month - 1 + months
  new_year = year + total // 12
  new_month = total % 12 + 1
  return period_name(new_year, new_month)


def next_period(period: str) -> str:
  return add_months(period, 1)


def previous_period(period: str) -> str:
  return add_months(period, -1)


def period_date_range(period: str) -> tuple[date, date]:
  """(first day, last day) of the period, both inclusive."""
  year, month = parse_period(period)
  start = date(year, month, 1)
  end = date(year, month, last_day_of_month(year, month))
  return start, end


def period_from_date(d: date) -> str:
  return period_name(d.year, d.month)


def current_month_period(today: date | None = None) -> str:
  today = today or date.today()
  return period_name(today.year, today.month)


def last_completed_period(today: date | None = None) -> str:
  """Previous calendar month: the latest period that can be a close target."""
  today = today or date.today()
  return previous_period(current_month_period(today))
