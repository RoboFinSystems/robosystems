"""An annual statement with no fiscal year named reads the fiscal year that
contains today, never one that has not started."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.reads import reports
from robosystems.operations.roboledger.reads.reports import resolve_reporting_window

pytestmark = pytest.mark.unit


class _October2026(date):
  @classmethod
  def today(cls):
    return cls(2026, 10, 21)


@pytest.mark.parametrize(
  ("start_month", "expected"),
  [
    (1, (date(2026, 1, 1), date(2026, 12, 31))),
    (7, (date(2026, 7, 1), date(2027, 6, 30))),
    (10, (date(2026, 10, 1), date(2027, 9, 30))),
    # Starts after October: still last calendar year's fiscal year.
    (11, (date(2025, 11, 1), date(2026, 10, 31))),
  ],
)
def test_the_default_annual_window_contains_today(start_month, expected):
  with (
    patch.object(reports, "date", _October2026),
    patch.object(reports, "get_fiscal_year_start_month", return_value=start_month),
  ):
    window = resolve_reporting_window(
      MagicMock(),
      period_start=None,
      period_end=None,
      period_type="annual",
      fiscal_year=None,
    )
  assert window == expected


def test_a_named_fiscal_year_is_the_one_starting_that_year():
  with patch.object(reports, "get_fiscal_year_start_month", return_value=11):
    window = resolve_reporting_window(
      MagicMock(),
      period_start=None,
      period_end=None,
      period_type="annual",
      fiscal_year=2026,
    )
  assert window == (date(2026, 11, 1), date(2027, 10, 31))
