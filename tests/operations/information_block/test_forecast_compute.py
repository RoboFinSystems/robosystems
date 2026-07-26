"""compute-forecast cascade tests.

Exercises the month-by-month walk with the persistence seams patched
(``_upsert_month_set`` recorded, data loads stubbed) so the assertions
target the cascade math itself: compounding via ``[t-1]``, rate-on-base
same-month ordering, days-based instants, carry-forward, calc-DAG
subtotal derivation, skip-and-fall-back semantics. ``_upsert_month_set``
has its own persistence tests at the bottom; the full end-to-end path
runs in the Driftline showcase harness.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.information_block import (
  ComputeForecastRequest,
  ForecastMechanics,
  LeverAssertionLite,
  LineAssertionLite,
  LineGrowthLite,
)
from robosystems.operations.information_block import (
  forecast_compute as fc,
)


def _element(
  element_id: str,
  qname: str,
  *,
  period_type: str = "duration",
  is_monetary: bool = True,
  item_type: str | None = None,
) -> SimpleNamespace:
  return SimpleNamespace(
    id=element_id,
    qname=qname,
    period_type=period_type,
    is_monetary=is_monetary,
    item_type=item_type,
  )


EL_REV = _element("el_rev", "rs-gaap:Revenues")
EL_COGS = _element("el_cogs", "rs-gaap:CostOfRevenue")
EL_AR = _element("el_ar", "rs-gaap:ReceivablesNetCurrent", period_type="instant")
EL_RENT = _element("el_rent", "rs-gaap:RentExpense")
EL_GP = _element("el_gp", "rs-gaap:GrossProfit")
EL_REV_A = _element("el_rev_a", "rs-gaap:RevenueStreamA")
EL_REV_B = _element("el_rev_b", "rs-gaap:RevenueStreamB")
EL_LOAN = _element("el_loan", "rs-gaap:NotesPayable", period_type="instant")
EL_MKT = _element("el_mkt", "rs-gaap:MarketingExpense")
EL_REVC = _element(
  "el_revc", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
)

ELEMENTS = [
  EL_REV,
  EL_COGS,
  EL_AR,
  EL_RENT,
  EL_GP,
  EL_REV_A,
  EL_REV_B,
  EL_LOAN,
  EL_MKT,
  EL_REVC,
]
BY_ID = {e.id: e for e in ELEMENTS}
BY_QNAME = {e.qname: e for e in ELEMENTS}


def _rule(rule_id: str, target: SimpleNamespace, expression: str, variables) -> Any:
  return SimpleNamespace(
    id=rule_id,
    rule_expression=expression,
    rule_variables=[
      {"variable_name": name, "variable_qname": qname} for name, qname in variables
    ],
    target_element_id=target.id,
  )


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

# Base month actuals: the June IS monthly set (subtotal included).
BASE_IS_FACTS = [
  SimpleNamespace(element_id="el_rev", value=100.0),
  SimpleNamespace(element_id="el_cogs", value=62.0),
  SimpleNamespace(element_id="el_rent", value=10.0),
  SimpleNamespace(element_id="el_gp", value=38.0),
]

CALC_DAG = {"el_gp": [("el_rev", 1.0), ("el_cogs", -1.0)]}


def _mechanics(
  levers: list[LeverAssertionLite],
  horizon: int = 3,
  assertions: list[LineAssertionLite] | None = None,
  growth: list[LineGrowthLite] | None = None,
) -> dict:
  return ForecastMechanics(
    scenario_kind="budget",
    horizon_months=horizon,
    base_period="2026-06",
    levers=levers,
    line_assertions=assertions or [],
    line_growth=growth or [],
  ).model_dump(mode="json")


def _growth(
  qname: str, element_id: str, values_by_period: dict[str, float]
) -> LineGrowthLite:
  return LineGrowthLite(
    qname=qname, element_id=element_id, values_by_period=values_by_period
  )


def _lever(qname: str, element_id: str, value: float, months: list[str]) -> Any:
  return LeverAssertionLite(
    qname=qname,
    element_id=element_id,
    item_type="percent",
    values_by_period=dict.fromkeys(months, value),
  )


def _assertion(
  qname: str,
  element_id: str,
  value: float,
  months: list[str],
  *,
  period_type: str = "duration",
) -> LineAssertionLite:
  return LineAssertionLite(
    qname=qname,
    element_id=element_id,
    item_type=None,
    period_type=period_type,
    values_by_period=dict.fromkeys(months, value),
  )


ALL_MONTHS = ["2026-07", "2026-08", "2026-09"]


def _lever_facts(levers: list[LeverAssertionLite]) -> list[SimpleNamespace]:
  facts = []
  for lever in levers:
    for month, value in lever.values_by_period.items():
      year, mm = int(month[:4]), int(month[5:7])
      end = date(year, mm, 28)  # any day in the month works for period_from_date
      facts.append(
        SimpleNamespace(element_id=lever.element_id, value=value, period_end=end)
      )
  return facts


class _Recorder:
  """Captures _upsert_month_set calls; returns deterministic set ids."""

  def __init__(self) -> None:
    self.calls: list[dict] = []

  def __call__(self, session: Any, **kwargs: Any) -> str | None:
    if not kwargs["facts"]:
      return None
    self.calls.append(kwargs)
    return f"fs_{kwargs['structure_id']}_{kwargs['period_end']}"

  def month(self, structure_id: str, period_end: date) -> dict | None:
    for call in self.calls:
      if call["structure_id"] == structure_id and call["period_end"] == period_end:
        return call
    return None

  def value(self, structure_id: str, period_end: date, element_id: str) -> float:
    call = self.month(structure_id, period_end)
    assert call is not None, f"no emission for {structure_id} @ {period_end}"
    for element, value in call["facts"]:
      if element.id == element_id:
        return value
    raise AssertionError(f"{element_id} not emitted @ {period_end}")


def _session(structure: Any) -> MagicMock:
  session = MagicMock()

  def _get(model: Any, obj_id: str) -> Any:
    if obj_id == structure.id:
      return structure
    return BY_ID.get(obj_id)

  session.get.side_effect = _get

  def _execute(stmt: Any) -> Any:
    # Only _element_by_qname reaches session.execute with the load
    # seams patched — resolve the qname from the compiled params.
    params = stmt.compile().params
    qname = next((v for v in params.values() if isinstance(v, str)), None)
    result = MagicMock()
    result.scalar_one_or_none.return_value = BY_QNAME.get(qname)
    return result

  session.execute.side_effect = _execute
  return session


def _structure(mechanics: dict) -> SimpleNamespace:
  return SimpleNamespace(
    id="struct_budget",
    block_type="forecast",
    artifact_mechanics=mechanics,
  )


def _run(
  mechanics: dict,
  rules: list[Any],
  levers: list[LeverAssertionLite],
  *,
  months: int | None = None,
  bs_structure: str | None = "struct_bs",
  assertions: list[LineAssertionLite] | None = None,
  calc_dag: dict | None = None,
  base_is_facts: list[Any] | None = None,
):
  structure = _structure(mechanics)
  recorder = _Recorder()

  def _structures(session: Any, block_type: str) -> str | None:
    return {"income_statement": "struct_is", "balance_sheet": bs_structure}.get(
      block_type
    )

  authored_facts = _lever_facts(levers) + _lever_facts(assertions or [])
  facts_by_set = {
    "fs_base_is": base_is_facts if base_is_facts is not None else BASE_IS_FACTS,
    "fs_lever": authored_facts,
  }

  with (
    patch.object(fc, "newest_actual_structure_id", side_effect=_structures),
    patch.object(
      fc,
      "_actual_set_at",
      side_effect=lambda session, sid, *a, **k: (
        SimpleNamespace(id="fs_base_is") if sid == "struct_is" else None
      ),
    ),
    patch.object(
      fc, "numeric_facts", side_effect=lambda session, fs_id: facts_by_set[fs_id]
    ),
    patch.object(fc, "driver_rules", return_value=rules),
    patch.object(fc, "_local_calc_arcs", return_value={}),
    patch.object(
      fc,
      "load_rs_gaap_calculations",
      return_value=dict(calc_dag if calc_dag is not None else CALC_DAG),
    ),
    patch.object(
      fc,
      "_load_lever_fact_set",
      return_value=SimpleNamespace(id="fs_lever", entity_id="ent_1"),
    ),
    patch.object(fc, "_upsert_month_set", recorder),
    patch.object(fc, "_verify_month_sets", return_value=(None, [])),
  ):
    response = fc.cmd_compute_forecast(
      _session(structure),
      ComputeForecastRequest(structure_id="struct_budget", months=months),
      "usr_test",
    )
  return response, recorder


FULL_LEVERS = [
  _lever("rs-driver:RevenueGrowthRate", "el_growth", 0.03, ALL_MONTHS),
  _lever("rs-driver:CostOfRevenueRate", "el_rate", 0.62, ALL_MONTHS),
  _lever("rs-driver:DaysSalesOutstanding", "el_dso", 45.0, ALL_MONTHS),
]
FULL_RULES = [GROWTH_RULE, COGS_RULE, DSO_RULE]

JUL = date(2026, 7, 31)
AUG = date(2026, 8, 31)
SEP = date(2026, 9, 30)


class TestCascade:
  def test_growth_compounds_month_over_month(self) -> None:
    _, rec = _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS)
    assert rec.value("struct_is", JUL, "el_rev") == pytest.approx(103.0)
    assert rec.value("struct_is", AUG, "el_rev") == pytest.approx(106.09)
    assert rec.value("struct_is", SEP, "el_rev") == pytest.approx(100 * 1.03**3)

  def test_rate_on_base_binds_the_same_months_grown_revenue(self) -> None:
    """Topo order: Revenues resolves before CostOfRevenue, so the rate
    applies to THIS month's grown revenue, not the prior month's."""
    _, rec = _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS)
    assert rec.value("struct_is", JUL, "el_cogs") == pytest.approx(103.0 * 0.62)

  def test_days_based_instant_lands_in_the_bs_set(self) -> None:
    _, rec = _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS)
    assert rec.value("struct_bs", JUL, "el_ar") == pytest.approx(103.0 / 30 * 45)
    # Instants never leak into the IS emission.
    is_call = rec.month("struct_is", JUL)
    assert is_call is not None
    assert all(el.id != "el_ar" for el, _ in is_call["facts"])

  def test_unmodeled_leaf_carries_forward(self) -> None:
    _, rec = _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS)
    for month_end in (JUL, AUG, SEP):
      assert rec.value("struct_is", month_end, "el_rent") == pytest.approx(10.0)

  def test_subtotal_is_derived_never_carried(self) -> None:
    """GrossProfit re-derives from the month's Revenues - CostOfRevenue
    — carrying the base month's 38.0 would silently break articulation."""
    _, rec = _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS)
    expected = 103.0 - (103.0 * 0.62)
    assert rec.value("struct_is", JUL, "el_gp") == pytest.approx(expected)
    assert rec.value("struct_is", JUL, "el_gp") != pytest.approx(38.0)

  def test_inactive_rule_falls_to_carry_forward(self) -> None:
    """No growth lever asserted → the GROWTH rule never activates and
    Revenues carries; no DSO lever → no BS emission at all."""
    levers = [_lever("rs-driver:CostOfRevenueRate", "el_rate", 0.62, ALL_MONTHS)]
    _, rec = _run(_mechanics(levers), FULL_RULES, levers)
    assert rec.value("struct_is", JUL, "el_rev") == pytest.approx(100.0)
    # Rate-on-base still runs against the carried revenue.
    assert rec.value("struct_is", JUL, "el_cogs") == pytest.approx(62.0)
    assert rec.month("struct_bs", JUL) is None

  def test_lever_month_gap_skips_and_falls_back(self) -> None:
    """Growth asserted only for July: August/September record a skip and
    Revenues holds at the last computed value (carry fallback)."""
    levers = [_lever("rs-driver:RevenueGrowthRate", "el_growth", 0.03, ["2026-07"])]
    response, rec = _run(_mechanics(levers), [GROWTH_RULE], levers)
    assert rec.value("struct_is", JUL, "el_rev") == pytest.approx(103.0)
    assert rec.value("struct_is", AUG, "el_rev") == pytest.approx(103.0)
    assert rec.value("struct_is", SEP, "el_rev") == pytest.approx(103.0)
    skip_periods = {s.period for s in response.skipped}
    assert skip_periods == {"2026-08", "2026-09"}
    assert all("lever not asserted" in s.missing[0] for s in response.skipped)

  def test_response_shape(self) -> None:
    response, _ = _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS)
    assert response.scenario_id == "struct_budget"
    assert response.base_period == "2026-06"
    assert response.months == 3
    assert [m.period for m in response.months_computed] == ALL_MONTHS
    july = response.months_computed[0]
    assert july.period_start == date(2026, 7, 1)
    assert july.period_end == JUL
    assert july.income_statement_fact_set_id is not None
    assert july.balance_sheet_fact_set_id is not None
    assert july.computed_count == 5  # 4 IS facts + 1 BS instant

  def test_provenance_carries_month_index_and_drivers(self) -> None:
    _, rec = _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS)
    call = rec.month("struct_is", AUG)
    assert call is not None
    provenance = call["provenance"]
    assert provenance.origin == "forecast"
    assert provenance.scenario_structure_id == "struct_budget"
    assert provenance.month_index == 2
    assert "rs-driver:RevenueGrowthRate" in provenance.drivers
    assert call["scenario_id"] == "struct_budget"

  def test_months_capped_at_horizon(self) -> None:
    with pytest.raises(ValueError, match="exceeds the block's horizon"):
      _run(_mechanics(FULL_LEVERS), FULL_RULES, FULL_LEVERS, months=12)

  def test_wrong_block_type_rejected(self) -> None:
    structure = SimpleNamespace(id="struct_metric", block_type="metric")
    session = MagicMock()
    session.get.return_value = structure
    with pytest.raises(ValueError, match="block_type='forecast'"):
      fc.cmd_compute_forecast(
        session,
        ComputeForecastRequest(structure_id="struct_metric"),
        "usr_test",
      )


class TestLineAssertions:
  """The manual-override path (fpa §11 #10): assertions win over carry
  and driver rules for the months they name, articulate through the
  calc DAG, and pin their values against the rule-delta push-down."""

  def test_assertion_overrides_carry_forward(self) -> None:
    """The Harbinger case: zero out a base-month one-off so carry stops
    replicating it into every forward month."""
    assertions = [_assertion("rs-gaap:RentExpense", "el_rent", 0.0, ALL_MONTHS)]
    _, rec = _run(
      _mechanics(FULL_LEVERS, assertions=assertions),
      FULL_RULES,
      FULL_LEVERS,
      assertions=assertions,
    )
    for month_end in (JUL, AUG, SEP):
      assert rec.value("struct_is", month_end, "el_rent") == pytest.approx(0.0)

  def test_assertion_displaces_rule_with_legible_skip(self) -> None:
    """An asserted month wins over the driver rule (skip recorded);
    unasserted months fall back to the rule."""
    assertions = [_assertion("rs-gaap:CostOfRevenue", "el_cogs", 50.0, ["2026-07"])]
    response, rec = _run(
      _mechanics(FULL_LEVERS, assertions=assertions),
      FULL_RULES,
      FULL_LEVERS,
      assertions=assertions,
    )
    assert rec.value("struct_is", JUL, "el_cogs") == pytest.approx(50.0)
    assert rec.value("struct_is", AUG, "el_cogs") == pytest.approx(106.09 * 0.62)
    displaced = [
      s for s in response.skipped if s.reason == "displaced by line assertion"
    ]
    assert [(s.rule_id, s.period) for s in displaced] == [("rule_cogs", "2026-07")]

  def test_asserted_leaf_articulates_through_subtotals(self) -> None:
    """GrossProfit re-derives against the asserted CostOfRevenue — the
    manual line still articulates, never breaks the calc DAG."""
    assertions = [_assertion("rs-gaap:CostOfRevenue", "el_cogs", 50.0, ["2026-07"])]
    _, rec = _run(
      _mechanics(FULL_LEVERS, assertions=assertions),
      FULL_RULES,
      FULL_LEVERS,
      assertions=assertions,
    )
    assert rec.value("struct_is", JUL, "el_gp") == pytest.approx(103.0 - 50.0)

  def test_asserted_child_is_pinned_against_rule_delta_push_down(self) -> None:
    """A driven calc parent's remainder distributes over the UNPINNED
    children only — the asserted child never rescales."""
    calc_dag = {
      "el_gp": [("el_rev", 1.0), ("el_cogs", -1.0)],
      "el_rev": [("el_rev_a", 1.0), ("el_rev_b", 1.0)],
    }
    base_facts = [
      *BASE_IS_FACTS,
      SimpleNamespace(element_id="el_rev_a", value=60.0),
      SimpleNamespace(element_id="el_rev_b", value=40.0),
    ]
    levers = [_lever("rs-driver:RevenueGrowthRate", "el_growth", 0.03, ALL_MONTHS)]
    assertions = [_assertion("rs-gaap:RevenueStreamA", "el_rev_a", 30.0, ["2026-07"])]
    _, rec = _run(
      _mechanics(levers, assertions=assertions),
      [GROWTH_RULE],
      levers,
      assertions=assertions,
      calc_dag=calc_dag,
      base_is_facts=base_facts,
    )
    assert rec.value("struct_is", JUL, "el_rev") == pytest.approx(103.0)
    assert rec.value("struct_is", JUL, "el_rev_a") == pytest.approx(30.0)
    # Remainder over the unpinned child: (103 - 30) / 40 scales 40 → 73.
    assert rec.value("struct_is", JUL, "el_rev_b") == pytest.approx(73.0)

  def test_instant_assertion_lands_in_the_bs_set(self) -> None:
    assertions = [
      _assertion(
        "rs-gaap:NotesPayable", "el_loan", 500.0, ALL_MONTHS, period_type="instant"
      )
    ]
    _, rec = _run(
      _mechanics(FULL_LEVERS, assertions=assertions),
      FULL_RULES,
      FULL_LEVERS,
      assertions=assertions,
    )
    assert rec.value("struct_bs", JUL, "el_loan") == pytest.approx(500.0)
    # Instant assertions never leak into the IS emission.
    is_call = rec.month("struct_is", JUL)
    assert is_call is not None
    assert all(el.id != "el_loan" for el, _ in is_call["facts"])

  def test_asserted_line_absent_from_base_still_emits(self) -> None:
    assertions = [_assertion("rs-gaap:MarketingExpense", "el_mkt", 5.0, ["2026-07"])]
    _, rec = _run(
      _mechanics(FULL_LEVERS, assertions=assertions),
      FULL_RULES,
      FULL_LEVERS,
      assertions=assertions,
    )
    assert rec.value("struct_is", JUL, "el_mkt") == pytest.approx(5.0)

  def test_unasserted_months_resume_normal_derivation(self) -> None:
    """After the asserted window ends, the line carries the last emitted
    value (the engine's normal carry-forward, seeded by the assertion)."""
    assertions = [_assertion("rs-gaap:RentExpense", "el_rent", 2.0, ["2026-07"])]
    _, rec = _run(
      _mechanics(FULL_LEVERS, assertions=assertions),
      FULL_RULES,
      FULL_LEVERS,
      assertions=assertions,
    )
    assert rec.value("struct_is", JUL, "el_rent") == pytest.approx(2.0)
    assert rec.value("struct_is", AUG, "el_rent") == pytest.approx(2.0)


class TestAssertionRollupIntoEmptySubtree:
  """The Harbinger restart-ramp repro (prod, 2026-07-25): a leaf asserted
  into a month whose BASE has no revenue subtree at all. The derived
  ancestors must emit (else the set can't roll up and verification fails
  with a residual equal to the assertion), same-month operands must bind
  the derived parent (DSO), the parent-driving growth rule must be
  displaced while the subtree is fully pinned, and after the ramp the
  rule takes over from the carried assertion with the child scaling in
  register."""

  # Revenues is a PARENT of the asserted contract-revenue leaf; the base
  # month carries only rent — no revenue subtree anywhere.
  CALC_DAG_CHAIN = {
    "el_rev": [("el_revc", 1.0)],
    "el_gp": [("el_rev", 1.0), ("el_cogs", -1.0)],
  }
  BASE_RENT_ONLY = [SimpleNamespace(element_id="el_rent", value=10.0)]

  def _ramp_run(self):
    levers = [
      _lever("rs-driver:RevenueGrowthRate", "el_growth", 0.03, ALL_MONTHS),
      _lever("rs-driver:DaysSalesOutstanding", "el_dso", 45.0, ALL_MONTHS),
    ]
    assertion = LineAssertionLite(
      qname="rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
      element_id="el_revc",
      item_type=None,
      period_type="duration",
      values_by_period={"2026-07": 4000.0, "2026-08": 6000.0},
    )
    return _run(
      _mechanics(levers, assertions=[assertion]),
      [GROWTH_RULE, DSO_RULE],
      levers,
      assertions=[assertion],
      calc_dag=dict(self.CALC_DAG_CHAIN),
      base_is_facts=list(self.BASE_RENT_ONLY),
    )

  def test_derived_ancestors_emit_with_the_asserted_leaf(self) -> None:
    _, rec = self._ramp_run()
    assert rec.value("struct_is", JUL, "el_revc") == pytest.approx(4000.0)
    assert rec.value("struct_is", JUL, "el_rev") == pytest.approx(4000.0)
    assert rec.value("struct_is", JUL, "el_gp") == pytest.approx(4000.0)
    assert rec.value("struct_is", AUG, "el_rev") == pytest.approx(6000.0)

  def test_parent_driving_rule_displaced_while_subtree_fully_pinned(self) -> None:
    response, _ = self._ramp_run()
    pinned_skips = [
      (s.rule_id, s.period)
      for s in response.skipped
      if s.reason == "displaced by line assertion (contributing children pinned)"
    ]
    assert ("rule_growth", "2026-07") in pinned_skips
    assert ("rule_growth", "2026-08") in pinned_skips
    assert all(period != "2026-09" for _, period in pinned_skips)

  def test_same_month_operand_derives_from_asserted_child(self) -> None:
    """DSO binds $Revenues from the asserted leaf the same month — no
    skip, no stale prior."""
    _, rec = self._ramp_run()
    assert rec.value("struct_bs", JUL, "el_ar") == pytest.approx(4000.0 / 30 * 45)
    assert rec.value("struct_bs", AUG, "el_ar") == pytest.approx(6000.0 / 30 * 45)

  def test_rule_takes_over_after_the_ramp_and_scales_the_child(self) -> None:
    """September (unasserted): the carried assertion seeds [t-1], the
    growth rule drives the parent, and the push-down keeps the child in
    register — the rollup stays exact past the ramp."""
    _, rec = self._ramp_run()
    assert rec.value("struct_is", SEP, "el_rev") == pytest.approx(6000.0 * 1.03)
    assert rec.value("struct_is", SEP, "el_revc") == pytest.approx(6000.0 * 1.03)
    assert rec.value("struct_bs", SEP, "el_ar") == pytest.approx(
      6000.0 * 1.03 / 30 * 45
    )

  def test_partially_pinned_subtree_keeps_the_rule_active(self) -> None:
    """Two revenue streams, one asserted: the growth rule still drives
    the parent and the remainder lands on the unpinned stream — the
    displacement only fires when the WHOLE valued subtree is pinned."""
    calc_dag = {
      "el_gp": [("el_rev", 1.0), ("el_cogs", -1.0)],
      "el_rev": [("el_rev_a", 1.0), ("el_rev_b", 1.0)],
    }
    base_facts = [
      *BASE_IS_FACTS,
      SimpleNamespace(element_id="el_rev_a", value=60.0),
      SimpleNamespace(element_id="el_rev_b", value=40.0),
    ]
    levers = [_lever("rs-driver:RevenueGrowthRate", "el_growth", 0.03, ALL_MONTHS)]
    assertions = [_assertion("rs-gaap:RevenueStreamA", "el_rev_a", 30.0, ["2026-07"])]
    response, rec = _run(
      _mechanics(levers, assertions=assertions),
      [GROWTH_RULE],
      levers,
      assertions=assertions,
      calc_dag=calc_dag,
      base_is_facts=base_facts,
    )
    assert rec.value("struct_is", JUL, "el_rev") == pytest.approx(103.0)
    assert not any(
      s.reason == "displaced by line assertion (contributing children pinned)"
      for s in response.skipped
    )


class TestLineGrowth:
  """The generic per-line trajectory: line[t] = line[t-1] * (1 + rate[t]),
  binding rates from the mechanics (never facts), compounding through
  the prior-values roll, yielding to catalog rules and to the carry on
  rate-less months."""

  def test_growth_compounds_from_the_base_value(self) -> None:
    growth = [_growth("rs-gaap:RentExpense", "el_rent", dict.fromkeys(ALL_MONTHS, 0.1))]
    _, rec = _run(_mechanics([], growth=growth), [], [])
    assert rec.value("struct_is", JUL, "el_rent") == pytest.approx(11.0)
    assert rec.value("struct_is", AUG, "el_rent") == pytest.approx(12.1)
    assert rec.value("struct_is", SEP, "el_rent") == pytest.approx(13.31)

  def test_rateless_months_keep_the_carry(self) -> None:
    """Grow-then-hold: a sparse values_by_period grows July only; the
    later months carry the grown value instead of reverting or vanishing."""
    growth = [_growth("rs-gaap:RentExpense", "el_rent", {"2026-07": 0.1})]
    _, rec = _run(_mechanics([], growth=growth), [], [])
    assert rec.value("struct_is", JUL, "el_rent") == pytest.approx(11.0)
    assert rec.value("struct_is", AUG, "el_rent") == pytest.approx(11.0)
    assert rec.value("struct_is", SEP, "el_rent") == pytest.approx(11.0)

  def test_catalog_rule_wins_over_stale_growth_overlap(self) -> None:
    """Authoring rejects the overlap, but a stored growth entry can be
    overtaken when levers change — the rule owns the line and the growth
    skip is legible."""
    growth = [_growth("rs-gaap:Revenues", "el_rev", dict.fromkeys(ALL_MONTHS, 0.5))]
    response, rec = _run(
      _mechanics(FULL_LEVERS, growth=growth), FULL_RULES, FULL_LEVERS
    )
    # The rule's 3% — not the growth entry's 50%.
    assert rec.value("struct_is", JUL, "el_rev") == pytest.approx(103.0)
    displaced = [
      s
      for s in response.skipped
      if s.reason == "line growth displaced by catalog driver rule"
    ]
    assert [s.period for s in displaced] == ALL_MONTHS
    assert all(s.element_qname == "rs-gaap:Revenues" for s in displaced)

  def test_out_of_base_growth_grows_from_zero_with_diagnostic(self) -> None:
    growth = [
      _growth("rs-gaap:MarketingExpense", "el_mkt", dict.fromkeys(ALL_MONTHS, 0.2))
    ]
    response, rec = _run(_mechanics([], growth=growth), [], [])
    assert any(
      "line growth on rs-gaap:MarketingExpense" in d and "grows from 0" in d
      for d in response.diagnostics
    )
    assert rec.value("struct_is", JUL, "el_mkt") == pytest.approx(0.0)

  def test_growth_articulates_through_subtotals(self) -> None:
    """A grown COGS re-derives GrossProfit — the grown leaf participates
    in the calc DAG exactly like a carried or rule-driven one."""
    growth = [
      _growth("rs-gaap:CostOfRevenue", "el_cogs", dict.fromkeys(ALL_MONTHS, 0.1))
    ]
    _, rec = _run(_mechanics([], growth=growth), [], [])
    assert rec.value("struct_is", JUL, "el_cogs") == pytest.approx(68.2)
    # GP = carried revenue 100 - grown COGS 68.2.
    assert rec.value("struct_is", JUL, "el_gp") == pytest.approx(31.8)


class TestUpsertMonthSet:
  def test_creates_scenario_keyed_set_with_typed_facts(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    added: list[Any] = []
    session.add.side_effect = added.append

    from robosystems.models.api.fact_provenance import ForecastProvenance

    provenance = ForecastProvenance(
      scenario_structure_id="struct_budget",
      base_period="2026-06",
      month_index=1,
      drivers=[],
    )
    with patch.object(
      fc, "create_fact_set", return_value=SimpleNamespace(id="fs_new")
    ) as create_fs:
      set_id = fc._upsert_month_set(
        session,
        structure_id="struct_is",
        entity_id="ent_1",
        scenario_id="struct_budget",
        period_start=date(2026, 7, 1),
        period_end=JUL,
        provenance=provenance,
        created_by="usr_test",
        facts=[(EL_REV, 103.0), (EL_AR, 154.5)],
      )

    assert set_id == "fs_new"
    kwargs = create_fs.call_args.kwargs
    assert kwargs["scenario_id"] == "struct_budget"
    assert kwargs["factset_type"] == "report"
    assert kwargs["provenance"] is provenance

    by_element = {f.element_id: f for f in added}
    duration = by_element["el_rev"]
    assert duration.period_type == "duration"
    assert duration.period_start == date(2026, 7, 1)
    instant = by_element["el_ar"]
    assert instant.period_type == "instant"
    assert instant.period_start is None

  def test_rerun_replaces_facts_and_restamps_provenance(self) -> None:
    standing = MagicMock()
    standing.id = "fs_existing"
    old_facts = [MagicMock(), MagicMock()]

    session = MagicMock()
    first = MagicMock()
    first.scalar_one_or_none.return_value = standing
    second = MagicMock()
    second.scalars.return_value.all.return_value = old_facts
    session.execute.side_effect = [first, second]

    from robosystems.models.api.fact_provenance import ForecastProvenance

    provenance = ForecastProvenance(
      scenario_structure_id="struct_budget",
      base_period="2026-06",
      month_index=1,
      drivers=["rs-driver:RevenueGrowthRate"],
    )
    set_id = fc._upsert_month_set(
      session,
      structure_id="struct_is",
      entity_id="ent_1",
      scenario_id="struct_budget",
      period_start=date(2026, 7, 1),
      period_end=JUL,
      provenance=provenance,
      created_by="usr_test",
      facts=[(EL_REV, 104.0)],
    )

    assert set_id == "fs_existing"
    deleted = [call.args[0] for call in session.delete.call_args_list]
    assert deleted == old_facts
    assert standing.provenance == provenance.model_dump(mode="json")

  def test_empty_facts_emit_nothing(self) -> None:
    session = MagicMock()

    from robosystems.models.api.fact_provenance import ForecastProvenance

    result = fc._upsert_month_set(
      session,
      structure_id="struct_bs",
      entity_id="ent_1",
      scenario_id="struct_budget",
      period_start=date(2026, 7, 1),
      period_end=JUL,
      provenance=ForecastProvenance(
        scenario_structure_id="struct_budget",
        base_period="2026-06",
        month_index=1,
      ),
      created_by="usr_test",
      facts=[],
    )
    assert result is None
    session.execute.assert_not_called()
