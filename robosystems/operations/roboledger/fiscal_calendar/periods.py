"""Period-name utilities for the fiscal calendar.

Periods are identified by `YYYY-MM` strings (e.g., "2026-03"). This module
provides parsing, formatting, arithmetic, and date-range conversion used
across the service and router layers.
"""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import date

PERIOD_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parse_period(period: str) -> tuple[int, int]:
  """Parse a `YYYY-MM` period string into (year, month).

  Raises:
      ValueError: if the period is not in YYYY-MM format with month 01-12.
  """
  match = PERIOD_PATTERN.match(period)
  if not match:
    raise ValueError(
      f"Invalid period format {period!r}. Expected YYYY-MM (e.g. '2026-03')."
    )
  return int(match.group(1)), int(match.group(2))


def period_name(year: int, month: int) -> str:
  """Format a (year, month) pair as a `YYYY-MM` period string."""
  if not 1 <= month <= 12:
    raise ValueError(f"Invalid month {month}; must be 1-12.")
  return f"{year:04d}-{month:02d}"


def last_day_of_month(year: int, month: int) -> int:
  """Return the last day of the given month (handles leap years)."""
  return monthrange(year, month)[1]


def add_months(period: str, months: int) -> str:
  """Return the period `months` away from `period` (negative = backward).

  Example: `add_months("2026-03", 2)` → "2026-05".
           `add_months("2026-03", -4)` → "2025-11".
  """
  year, month = parse_period(period)
  total = month - 1 + months
  new_year = year + total // 12
  new_month = total % 12 + 1
  return period_name(new_year, new_month)


def next_period(period: str) -> str:
  """Return the period one month after `period`."""
  return add_months(period, 1)


def previous_period(period: str) -> str:
  """Return the period one month before `period`."""
  return add_months(period, -1)


def period_date_range(period: str) -> tuple[date, date]:
  """Return the (start_date, end_date) for a period.

  Example: `period_date_range("2026-03")` → (date(2026, 3, 1), date(2026, 3, 31)).
  """
  year, month = parse_period(period)
  start = date(year, month, 1)
  end = date(year, month, last_day_of_month(year, month))
  return start, end


def period_from_date(d: date) -> str:
  """Return the period containing the given date."""
  return period_name(d.year, d.month)


def current_month_period(today: date | None = None) -> str:
  """Return the period for the current calendar month."""
  today = today or date.today()
  return period_name(today.year, today.month)


def last_completed_period(today: date | None = None) -> str:
  """Return the most recently completed calendar month.

  This is `current_month - 1`, i.e., the latest period that can ever be
  a valid close target (the current month is in progress).
  """
  today = today or date.today()
  return previous_period(current_month_period(today))
