"""A fact the Style presents no row for takes a row under the calculation
ancestor the statement does present.

The case that found it: an LLC subsidiary's members' equity on the corporate
balance sheet of the group parent. The equity subtotal already counted it, so
the subtotal did not foot and the amount showed on no line.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from robosystems.operations.roboledger.reports.fact_grid import (
  FactRow,
  PeriodSpec,
  ReportFact,
  _place_strays_by_calculation,
  _roll_up_facts_to_structure,
)
from robosystems.operations.roboledger.reports.guard_rails import validate_report

JULY = PeriodSpec(start=date(2026, 7, 1), end=date(2026, 7, 31), label="Current")
JUNE = PeriodSpec(start=date(2026, 6, 1), end=date(2026, 6, 30), label="Prior")


def _row(
  element_id: str, value: float, *, depth: int, subtotal: bool = False
) -> FactRow:
  return FactRow(
    element_id=element_id,
    element_qname=f"rs-gaap:{element_id}",
    element_name=element_id,
    classification="equity",
    balance_type="credit",
    values=[value],
    is_subtotal=subtotal,
    depth=depth,
  )


def _fact(element_id: str, value: float, period: PeriodSpec = JULY) -> ReportFact:
  return ReportFact(
    element_id=element_id,
    element_qname=f"rs-gaap:{element_id}",
    element_name=element_id,
    classification="equity",
    balance_type="credit",
    value=value,
    period_start=period.start,
    period_end=period.end,
    period_type="instant",
  )


def _corporate_equity() -> list[FactRow]:
  """APIC and retained earnings under an equity subtotal that already holds
  100 of members' equity the Style has no row for."""
  return [
    _row("apic", 50.0, depth=2),
    _row("re", 30.0, depth=2),
    _row("equity", 180.0, depth=1, subtotal=True),
    _row("total", 180.0, depth=0, subtotal=True),
  ]


# ``{summand: [subtotals that sum it]}``, the shape ``_calculation_ancestry`` loads.
EQUITY_PARENTS = {"apic": ["equity"], "re": ["equity"], "members": ["equity"]}


def test_a_stray_becomes_the_last_child_of_its_presented_calculation_parent():
  rows = _place_strays_by_calculation(
    _corporate_equity(), [_fact("members", 100.0)], [JULY], EQUITY_PARENTS
  )

  assert [r.element_id for r in rows] == ["apic", "re", "members", "equity", "total"]
  members = rows[2]
  assert (members.values, members.depth, members.is_subtotal) == ([100.0], 2, False)
  assert members.balance_type == "credit"


def test_the_placed_row_makes_the_subtotal_foot():
  before = validate_report("balance_sheet", _corporate_equity(), ["Current"])
  assert any("does not match sum of children" in w for w in before.warnings)

  rows = _place_strays_by_calculation(
    _corporate_equity(), [_fact("members", 100.0)], [JULY], EQUITY_PARENTS
  )
  after = validate_report("balance_sheet", rows, ["Current"])

  assert not any("does not match sum of children" in w for w in after.warnings)


def test_a_stray_with_no_presented_ancestor_stays_off_the_statement():
  """An income-statement concept while the balance sheet renders: its
  calculation parents are not on this statement, so it is not placed."""
  rows = _place_strays_by_calculation(
    _corporate_equity(),
    [_fact("rent_expense", 40.0)],
    [JULY],
    {**EQUITY_PARENTS, "rent_expense": ["net_income"]},
  )

  assert [r.element_id for r in rows] == ["apic", "re", "equity", "total"]


def test_a_stray_climbs_past_an_unpresented_calculation_parent():
  """Members' equity sums into a members' total the Style does not present
  either; the row goes under the nearest presented ancestor."""
  parents = {
    "members": ["members_total"],
    "members_total": ["equity"],
    "apic": ["equity"],
    "re": ["equity"],
  }
  rows = _place_strays_by_calculation(
    _corporate_equity(), [_fact("members", 100.0)], [JULY], parents
  )

  assert [r.element_id for r in rows] == ["apic", "re", "members", "equity", "total"]
  assert rows[2].depth == 2


def test_a_stray_under_a_valued_stray_is_covered_by_that_row():
  """When the unpresented parent has its own fact (a calc target's subtotal
  fact), only the parent takes a row, or the child would count twice."""
  parents = {
    "members": ["members_total"],
    "members_total": ["equity"],
    "apic": ["equity"],
    "re": ["equity"],
  }
  rows = _place_strays_by_calculation(
    _corporate_equity(),
    [_fact("members_total", 100.0), _fact("members", 100.0)],
    [JULY],
    parents,
  )

  assert [r.element_id for r in rows] == [
    "apic",
    "re",
    "members_total",
    "equity",
    "total",
  ]
  assert rows[2].is_subtotal is True


def test_the_deeper_of_two_presented_parents_wins():
  """An identity arrangement also sums members' equity into the grand total;
  the row belongs under the specific subtotal, not the total."""
  parents = {**EQUITY_PARENTS, "members": ["total", "equity"]}
  rows = _place_strays_by_calculation(
    _corporate_equity(), [_fact("members", 100.0)], [JULY], parents
  )

  assert [r.element_id for r in rows] == ["apic", "re", "members", "equity", "total"]


def test_a_stray_with_no_value_in_any_column_is_not_placed():
  rows = _place_strays_by_calculation(
    _corporate_equity(), [_fact("members", 0.0)], [JULY], EQUITY_PARENTS
  )

  assert [r.element_id for r in rows] == ["apic", "re", "equity", "total"]


def test_values_follow_the_period_columns():
  rows = _place_strays_by_calculation(
    _corporate_equity(),
    [_fact("members", 100.0, JULY), _fact("members", 60.0, JUNE)],
    [JULY, JUNE],
    EQUITY_PARENTS,
  )

  assert rows[2].values == [100.0, 60.0]


def test_the_rollup_collects_only_the_facts_it_drops():
  """``dropped`` gets the facts with no renderable ancestor: not the ones
  rolled up, not the audit-only ones, not the ones an authoritative
  ancestor already covers."""
  parents = {"orphan": ["dead_end"], "detail": ["in_struct"]}

  def execute(_stmt, params):
    result = MagicMock()
    result.fetchall.return_value = [
      MagicMock(parent_id=p) for c in params["children"] for p in parents.get(c, [])
    ]
    return result

  session = MagicMock()
  session.execute.side_effect = execute
  audit = _fact("hidden", 5.0)
  audit.audit_only = True
  facts = [
    _fact("in_struct", 100.0),
    _fact("detail", 20.0),
    _fact("orphan", 50.0),
    audit,
  ]
  dropped: list[ReportFact] = []

  rolled = _roll_up_facts_to_structure(session, facts, {"in_struct"}, dropped=dropped)

  assert [f.element_id for f in rolled] == ["in_struct"]
  assert [f.element_id for f in dropped] == ["orphan"]


def test_the_rollup_signature_without_a_collector_is_unchanged():
  session = MagicMock()
  assert _roll_up_facts_to_structure(session, [], {"a"}) == []
