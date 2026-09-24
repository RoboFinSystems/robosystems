"""A schedule's date range is bounded before any facts are built."""

from datetime import date

import pytest
from pydantic import ValidationError

from robosystems.models.api.extensions.schedules import CreateScheduleRequest

BASE = {
  "name": "Depreciation",
  "element_ids": ["elem_a", "elem_b"],
  "monthly_amount": 100,
  "entry_template": {"debit_element_id": "elem_a", "credit_element_id": "elem_b"},
}


@pytest.mark.unit
def test_fifty_years_is_accepted():
  CreateScheduleRequest(
    period_start=date(2026, 1, 1), period_end=date(2075, 12, 31), **BASE
  )


@pytest.mark.unit
@pytest.mark.parametrize(
  ("start", "end"),
  [
    (date(2026, 1, 1), date(2076, 1, 31)),
    (date(1, 1, 1), date(9999, 12, 31)),
    (date(2026, 6, 1), date(2026, 5, 31)),
  ],
)
def test_an_unbounded_or_inverted_range_is_refused(start, end):
  with pytest.raises(ValidationError):
    CreateScheduleRequest(period_start=start, period_end=end, **BASE)
