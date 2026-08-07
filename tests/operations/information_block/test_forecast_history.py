"""Lever back-solve tests — the realized side of the Assumptions grid.

Exercises the rule inversion against the REAL seeded driver expressions
(growth via ``[t-1]``, rate-on-base, days-on-flow) with the data loads
patched, so the assertions target the math and the binding rules rather
than the queries: which months solve, which blank, and that an
unsolvable rule never takes the grid down with it.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.information_block import (
  ForecastMechanics,
  LeverAssertionLite,
  LineAssertionLite,
)
from robosystems.operations.information_block import forecast_history as fh


def _element(element_id: str, qname: str) -> SimpleNamespace:
  return SimpleNamespace(id=element_id, qname=qname)


EL_REV = _element("el_rev", "rs-gaap:Revenues")
EL_COGS = _element("el_cogs", "rs-gaap:CostOfRevenue")
EL_AR = _element("el_ar", "rs-gaap:ReceivablesNetCurrent")
EL_RENT = _element("el_rent", "rs-gaap:RentExpense")
EL_GROWTH = _element("el_growth", "rs-driver:RevenueGrowthRate")
EL_RATE = _element("el_rate", "rs-driver:CostOfRevenueRate")
EL_DSO = _element("el_dso", "rs-driver:DaysSalesOutstanding")

BY_QNAME = {
  el.qname: el for el in (EL_REV, EL_COGS, EL_AR, EL_RENT, EL_GROWTH, EL_RATE, EL_DSO)
}
BY_ID = {el.id: el for el in BY_QNAME.values()}


def _rule(rule_id: str, target: SimpleNamespace, expression: str, variables) -> Any:
  return SimpleNamespace(
    id=rule_id,
    rule_expression=expression,
    rule_variables=[
      {"variable_name": name, "variable_qname": qname} for name, qname in variables
    ],
    target_element_id=target.id,
  )


# The rs-driver/v1 expressions, verbatim.
GROWTH_RULE = _rule(
  "rule_growth",
  EL_REV,
  "$Revenues = ($Revenues[t-1] * (1 + $RevenueGrowthRate))",
  [
    ("Revenues", "rs-gaap:Revenues"),
    ("RevenueGrowthRate", "rs-driver:RevenueGrowthRate"),
  ],
)
COGS_RULE = _rule(
  "rule_cogs",
  EL_COGS,
  "$CostOfRevenue = ($Revenues * $CostOfRevenueRate)",
  [
    ("CostOfRevenue", "rs-gaap:CostOfRevenue"),
    ("Revenues", "rs-gaap:Revenues"),
    ("CostOfRevenueRate", "rs-driver:CostOfRevenueRate"),
  ],
)
DSO_RULE = _rule(
  "rule_dso",
  EL_AR,
  "$ReceivablesNetCurrent = (($Revenues / 30) * $DaysSalesOutstanding)",
  [
    ("ReceivablesNetCurrent", "rs-gaap:ReceivablesNetCurrent"),
    ("Revenues", "rs-gaap:Revenues"),
    ("DaysSalesOutstanding", "rs-driver:DaysSalesOutstanding"),
  ],
)


def _mechanics(
  levers: list[LeverAssertionLite],
  assertions: list[LineAssertionLite] | None = None,
) -> ForecastMechanics:
  return ForecastMechanics(
    scenario_kind="budget",
    horizon_months=3,
    base_period="2026-06",
    levers=levers,
    line_assertions=assertions or [],
  )


def _lever(qname: str, element_id: str) -> LeverAssertionLite:
  return LeverAssertionLite(
    qname=qname,
    element_id=element_id,
    item_type="percent",
    values_by_period={"2026-07": 0.03},
  )


GROWTH_LEVER = _lever("rs-driver:RevenueGrowthRate", "el_growth")
RATE_LEVER = _lever("rs-driver:CostOfRevenueRate", "el_rate")
DSO_LEVER = _lever("rs-driver:DaysSalesOutstanding", "el_dso")


def _session() -> MagicMock:
  session = MagicMock()
  session.get.side_effect = lambda _model, element_id: BY_ID.get(element_id)
  return session


def _run(
  mechanics: ForecastMechanics,
  rules: list[Any],
  months: list[str],
  actuals: dict[str, dict[str, float]],
) -> fh.LeverHistory:
  with (
    patch.object(fh, "driver_rules", return_value=rules),
    patch.object(fh, "_lookup_element", lambda _s, qname: BY_QNAME.get(qname)),
    patch.object(fh, "_actual_monthly_values", return_value=(months, actuals)),
  ):
    return fh.back_solve_lever_history(_session(), mechanics)


class TestBackSolve:
  def test_growth_inverts_against_the_prior_month(self) -> None:
    history = _run(
      _mechanics([GROWTH_LEVER]),
      [GROWTH_RULE],
      ["2026-04", "2026-05"],
      {
        "2026-04": {"el_rev": 100.0},
        "2026-05": {"el_rev": 104.0},
      },
    )
    solved = history.lever_values["rs-driver:RevenueGrowthRate"]
    # April has no prior month in the series → nothing to solve against.
    assert "2026-04" not in solved
    assert solved["2026-05"] == pytest.approx(0.04)

  def test_rate_and_days_levers_invert_from_the_same_month(self) -> None:
    history = _run(
      _mechanics([RATE_LEVER, DSO_LEVER]),
      [COGS_RULE, DSO_RULE],
      ["2026-05"],
      {"2026-05": {"el_rev": 300.0, "el_cogs": 186.0, "el_ar": 310.0}},
    )
    assert history.lever_values["rs-driver:CostOfRevenueRate"][
      "2026-05"
    ] == pytest.approx(0.62)
    # AR / (Revenues / 30) = 310 / 10 = 31 days.
    assert history.lever_values["rs-driver:DaysSalesOutstanding"][
      "2026-05"
    ] == pytest.approx(31.0)

  def test_a_month_missing_an_operand_blanks_only_that_cell(self) -> None:
    history = _run(
      _mechanics([RATE_LEVER]),
      [COGS_RULE],
      ["2026-04", "2026-05", "2026-06"],
      {
        "2026-04": {"el_rev": 100.0, "el_cogs": 62.0},
        # May never closed its cost line — no rate to recover.
        "2026-05": {"el_rev": 110.0},
        "2026-06": {"el_rev": 200.0, "el_cogs": 130.0},
      },
    )
    solved = history.lever_values["rs-driver:CostOfRevenueRate"]
    assert sorted(solved) == ["2026-04", "2026-06"]
    assert solved["2026-06"] == pytest.approx(0.65)

  def test_a_zero_base_leaves_the_cell_blank_rather_than_dividing(self) -> None:
    # Revenues[t-1] = 0 ⇒ the growth rule's slope is 0: no rate explains
    # a jump off a zero base (the restart-from-zero case).
    history = _run(
      _mechanics([GROWTH_LEVER]),
      [GROWTH_RULE],
      ["2026-04", "2026-05"],
      {"2026-04": {"el_rev": 0.0}, "2026-05": {"el_rev": 5000.0}},
    )
    assert history.lever_values.get("rs-driver:RevenueGrowthRate", {}) == {}
    assert history.months == ["2026-04", "2026-05"]

  def test_only_levers_this_scenario_asserts_are_solved(self) -> None:
    history = _run(
      _mechanics([RATE_LEVER]),
      [GROWTH_RULE, COGS_RULE, DSO_RULE],
      ["2026-05"],
      {"2026-05": {"el_rev": 300.0, "el_cogs": 186.0, "el_ar": 310.0}},
    )
    assert list(history.lever_values) == ["rs-driver:CostOfRevenueRate"]

  def test_line_assertions_take_the_actual_line_itself(self) -> None:
    mechanics = _mechanics(
      [RATE_LEVER],
      [
        LineAssertionLite(
          qname="rs-gaap:RentExpense",
          element_id="el_rent",
          item_type=None,
          period_type="duration",
          values_by_period={"2026-07": 0.0},
        )
      ],
    )
    history = _run(
      mechanics,
      [COGS_RULE],
      ["2026-04", "2026-05"],
      {
        "2026-04": {"el_rev": 100.0, "el_cogs": 62.0, "el_rent": 900.0},
        "2026-05": {"el_rev": 110.0, "el_cogs": 68.2},
      },
    )
    # No inversion for a line assertion — its realized value IS the line.
    assert history.line_values["el_rent"] == {"2026-04": 900.0}

  def test_no_closed_months_yields_an_empty_history(self) -> None:
    history = _run(_mechanics([GROWTH_LEVER]), [GROWTH_RULE], [], {})
    assert history.months == []
    assert history.lever_values == {}

  def test_no_driver_rules_yields_an_empty_history(self) -> None:
    history = _run(_mechanics([GROWTH_LEVER]), [], ["2026-05"], {})
    assert history.months == []

  def test_an_unparseable_rule_is_skipped_not_raised(self) -> None:
    broken = _rule(
      "rule_broken",
      EL_REV,
      "$Revenues = ($Revenues[t-1] * ",
      [
        ("Revenues", "rs-gaap:Revenues"),
        ("RevenueGrowthRate", "rs-driver:RevenueGrowthRate"),
      ],
    )
    history = _run(
      _mechanics([GROWTH_LEVER, RATE_LEVER]),
      [broken, COGS_RULE],
      ["2026-05"],
      {"2026-05": {"el_rev": 300.0, "el_cogs": 186.0}},
    )
    assert list(history.lever_values) == ["rs-driver:CostOfRevenueRate"]


class TestActualMonthlyValues:
  def test_reads_only_the_elements_asked_for_across_the_series(self) -> None:
    session = MagicMock()
    series = [
      SimpleNamespace(id="fs_apr", period_end=date(2026, 4, 30)),
      SimpleNamespace(id="fs_may", period_end=date(2026, 5, 31)),
    ]
    facts = [
      SimpleNamespace(fact_set_id="fs_apr", element_id="el_rev", value=100.0),
      SimpleNamespace(fact_set_id="fs_may", element_id="el_rev", value=104.0),
    ]
    facts_result = MagicMock()
    facts_result.scalars.return_value.all.return_value = facts
    empty_result = MagicMock()
    empty_result.scalars.return_value.all.return_value = []
    session.execute.side_effect = [facts_result, empty_result]

    with (
      patch.object(fh, "newest_actual_structure_id", side_effect=["struct_is", None]),
      patch.object(fh, "load_statement_fact_set_series", return_value=series),
    ):
      months, actuals = fh._actual_monthly_values(session, {"el_rev"})

    assert months == ["2026-04", "2026-05"]
    assert actuals == {"2026-04": {"el_rev": 100.0}, "2026-05": {"el_rev": 104.0}}

  def test_a_month_with_no_binding_fact_is_still_a_column(self) -> None:
    session = MagicMock()
    series = [
      SimpleNamespace(id="fs_apr", period_end=date(2026, 4, 30)),
      SimpleNamespace(id="fs_may", period_end=date(2026, 5, 31)),
    ]
    facts_result = MagicMock()
    facts_result.scalars.return_value.all.return_value = [
      SimpleNamespace(fact_set_id="fs_apr", element_id="el_rev", value=100.0)
    ]
    session.execute.return_value = facts_result

    with (
      patch.object(fh, "newest_actual_structure_id", side_effect=["struct_is", None]),
      patch.object(fh, "load_statement_fact_set_series", return_value=series),
    ):
      months, actuals = fh._actual_monthly_values(session, {"el_rev"})

    assert months == ["2026-04", "2026-05"]
    assert "2026-05" not in actuals

  def test_no_actual_structures_means_no_months(self) -> None:
    with patch.object(fh, "newest_actual_structure_id", return_value=None):
      months, actuals = fh._actual_monthly_values(MagicMock(), {"el_rev"})
    assert months == []
    assert actuals == {}
