"""Fiscal calendar operations — close cadence state management.

The fiscal calendar tracks where a graph's books stand: which periods are
closed, which is the next target, what's required to close the next one.

See `local/docs/specs/ledger-close-workflow.md` for the full design.
"""

from .periods import (
  PERIOD_PATTERN,
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
from .service import CloseableGateResult, FiscalCalendarService

__all__ = [
  "PERIOD_PATTERN",
  "CloseableGateResult",
  "FiscalCalendarService",
  "add_months",
  "current_month_period",
  "last_completed_period",
  "last_day_of_month",
  "next_period",
  "parse_period",
  "period_date_range",
  "period_from_date",
  "period_name",
  "previous_period",
]
