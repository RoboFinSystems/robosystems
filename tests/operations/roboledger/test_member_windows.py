"""The column a group read opens the day after a member's last month: read
for the cash and capital that leave with the member on the cash flow and
equity statement, never on the balance sheet or income statement, and never
for an entity the sum did not hold."""

from __future__ import annotations

from datetime import date

import pytest

from robosystems.operations.roboledger.entity_scope import GroupMember
from robosystems.operations.roboledger.reads.reports import member_windows
from robosystems.operations.roboledger.reports.fact_grid import PeriodSpec

pytestmark = pytest.mark.unit

JULY = PeriodSpec(start=date(2026, 7, 1), end=date(2026, 7, 31), label="Jul")
AUGUST = PeriodSpec(start=date(2026, 8, 1), end=date(2026, 8, 31), label="Aug")
LEFT_IN_JULY = GroupMember("m", "operating", None, date(2026, 7, 15))


def test_the_month_after_a_members_last_is_read_for_what_leaves_with_it():
  windows, partial = member_windows(LEFT_IN_JULY, "cash_flow_statement", [JULY, AUGUST])
  assert windows == [JULY, AUGUST] and partial == set()
  windows, _ = member_windows(LEFT_IN_JULY, "equity_statement", [AUGUST])
  assert windows == [AUGUST]


def test_balances_and_results_of_the_month_after_are_not_the_groups():
  for statement_type in ("balance_sheet", "income_statement"):
    windows, _ = member_windows(LEFT_IN_JULY, statement_type, [JULY, AUGUST])
    assert windows == [JULY, None], statement_type


def test_an_entity_outside_the_sum_gets_no_leaving_column():
  windows, _ = member_windows(
    LEFT_IN_JULY, "cash_flow_statement", [JULY, AUGUST], leaving=False
  )
  assert windows == [JULY, None]
