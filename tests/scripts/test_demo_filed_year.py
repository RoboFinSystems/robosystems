"""The showcase demos file their annual report over closed months only.

A filed report refuses a column the books have not closed through, and the
showcase leaves its close target open for the live close. The filed year is
therefore the trailing twelve months ending one month before it.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from examples._scenario.engine import trailing_year
from examples._scenario.runner import filed_year


def _scenario(months: int, start: date) -> MagicMock:
  scenario = MagicMock()
  scenario.months = months
  scenario.report_period = trailing_year(start, months)
  return scenario


@pytest.mark.unit
def test_the_filed_year_ends_on_the_month_before_the_close_target():
  start = date(2025, 7, 1)
  scenario = _scenario(16, start)
  assert scenario.report_period == (date(2025, 10, 1), date(2026, 9, 30))
  with patch("examples._scenario.engine.get_demo_start_date", return_value=start):
    assert filed_year(scenario) == (date(2025, 9, 1), date(2026, 8, 31))


@pytest.mark.unit
def test_the_filed_year_never_starts_before_the_books():
  start = date(2026, 1, 1)
  scenario = _scenario(12, start)
  assert scenario.report_period[0] == start
  with patch("examples._scenario.engine.get_demo_start_date", return_value=start):
    first, last = filed_year(scenario)
  assert first == start
  assert last == date(2026, 10, 31)
