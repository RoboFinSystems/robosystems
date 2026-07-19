"""Tests for the arc-derived RollUp validator.

Regression coverage for the false-failure classes found on live reports:
(1) a Balance Sheet subtotal footing over a sibling concept
(``...ExcludingGoodwill`` is the actual calc child, where the frozen rule
enumerated ``...IncludingGoodwill``); (2) a Cash Flow operating subtotal that
reconciles only when BOTH facts for one element+period are summed; and (3) an
income-statement decomposition rule (``NetIncomeLoss = ContinuingOps +
DiscontinuedOps``) landing on the Cash Flow statement, which must be SKIPPED —
not resolved through a different statement's calc path.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.information_block.rules.engine import (
  _evaluate_rollup_arc_derived,
  _load_period_balances,
  evaluate_rules_for_structure,
)

_P1 = (date(2025, 1, 1), date(2025, 12, 31))
_P2 = (date(2026, 1, 1), date(2026, 12, 31))


def _rule(parent_id: str, parent_qname: str, metadata: dict | None = None) -> MagicMock:
  rule = MagicMock()
  rule.rule_pattern = "RollUp"
  rule.rule_variables = [
    {
      "variable_name": parent_qname.split(":")[-1],
      "variable_qname": parent_qname,
      "variable_element_id": parent_id,  # avoids a session qname lookup
    }
  ]
  rule.metadata_ = metadata or {}
  return rule


def _eval(rule, by_period, calcs):
  return _evaluate_rollup_arc_derived(MagicMock(), rule, by_period, calcs, {})


class TestEvaluateRollupArcDerived:
  def test_balance_sheet_sibling_concept_passes(self) -> None:
    # The calc arc names the actual mapped leaf (ExcludingGoodwill); the frozen
    # rule enumerated IncludingGoodwill and failed by exactly 5966.27.
    calcs = {
      "AssetsNoncurrent": [
        ("PPE", 1.0),
        ("ExcludingGoodwill", 1.0),
        ("Other", 1.0),
        ("Goodwill", 1.0),  # absent → 0
      ],
    }
    balances = {
      "PPE": 8901.31,
      "ExcludingGoodwill": 5966.27,
      "Other": 83569.82,
      "AssetsNoncurrent": 98437.40,
    }
    outcome = _eval(
      _rule("AssetsNoncurrent", "rs-gaap:AssetsNoncurrent"),
      {_P1: (balances, set(balances))},
      calcs,
    )
    assert outcome is not None
    assert outcome.status == "pass"
    assert outcome.detail["source"] == "calc-dag"

  def test_cash_flow_operating_sums_multiple_facts(self) -> None:
    # by_period balances already carry the SUMMED OtherOpCap
    # (+4782.70 + -1601.30 = 3181.40); the operating subtotal foots.
    calcs = {
      "NetCashOperating": [
        ("NetIncome", 1.0),
        ("DDA", 1.0),
        ("AR", 1.0),
        ("Prepaid", 1.0),
        ("OtherOpCap", 1.0),
      ]
    }
    balances = {
      "NetIncome": -22356.10,
      "DDA": 1124.84,
      "AR": 20156.25,
      "Prepaid": -348.0,
      "OtherOpCap": 3181.40,
      "NetCashOperating": 1758.39,
    }
    outcome = _eval(
      _rule("NetCashOperating", "rs-gaap:NetCashProvidedByUsedInOperatingActivities"),
      {_P2: (balances, set(balances))},
      calcs,
    )
    assert outcome is not None
    assert outcome.status == "pass"

  def test_cross_statement_rule_skipped_not_failed(self) -> None:
    # NetIncomeLoss = ContinuingOps + DiscontinuedOps is an IS rule; on the CF
    # statement NetIncomeLoss is present (an add-back) but neither child is —
    # so it must skip, NOT fail by resolving DDA through the IS expense path.
    calcs = {"NetIncomeLoss": [("ContinuingOps", 1.0), ("DiscontinuedOps", 1.0)]}
    balances = {"NetIncomeLoss": 13823.36, "DDA": 1216.64}  # children absent
    outcome = _eval(
      _rule("NetIncomeLoss", "rs-gaap:NetIncomeLoss"),
      {_P1: (balances, set(balances))},
      calcs,
    )
    assert outcome is not None
    assert outcome.status == "skipped"
    assert "children are present" in (outcome.message or "")

  def test_real_discrepancy_fails_not_tautology(self) -> None:
    # Parent reported independently at 100000 but children sum to 92471.13 —
    # a genuine mismatch must still fail (the check is not vacuous).
    calcs = {"AssetsNoncurrent": [("PPE", 1.0), ("Other", 1.0)]}
    balances = {"PPE": 8901.31, "Other": 83569.82, "AssetsNoncurrent": 100000.0}
    outcome = _eval(
      _rule("AssetsNoncurrent", "rs-gaap:AssetsNoncurrent"),
      {_P1: (balances, set(balances))},
      calcs,
    )
    assert outcome is not None
    assert outcome.status == "fail"
    assert "residual" in (outcome.message or "")

  def test_returns_none_for_non_calc_parent(self) -> None:
    # Parent not in the rs-gaap calc DAG → fall back to the frozen evaluator.
    calcs = {"AssetsNoncurrent": [("PPE", 1.0)]}
    outcome = _eval(
      _rule("CustomThing", "acme:CustomThing"),
      {_P1: ({"CustomThing": 5.0}, {"CustomThing"})},
      calcs,
    )
    assert outcome is None

  def test_skipped_when_parent_absent_all_periods(self) -> None:
    calcs = {"LiabilitiesNoncurrent": [("LTDebt", 1.0)]}
    outcome = _eval(
      _rule("LiabilitiesNoncurrent", "rs-gaap:LiabilitiesNoncurrent"),
      {_P1: ({"LTDebt": 100.0}, {"LTDebt"})},
      calcs,
    )
    assert outcome is not None
    assert outcome.status == "skipped"
    assert "LiabilitiesNoncurrent" in (outcome.message or "")

  def test_multi_period_fails_if_any_period_fails(self) -> None:
    calcs = {"AssetsNoncurrent": [("PPE", 1.0)]}
    by_period = {
      _P1: ({"PPE": 100.0, "AssetsNoncurrent": 100.0}, {"PPE", "AssetsNoncurrent"}),
      _P2: ({"PPE": 100.0, "AssetsNoncurrent": 150.0}, {"PPE", "AssetsNoncurrent"}),
    }
    outcome = _eval(
      _rule("AssetsNoncurrent", "rs-gaap:AssetsNoncurrent"), by_period, calcs
    )
    assert outcome is not None
    assert outcome.status == "fail"
    assert "50" in (outcome.message or "")

  def test_tolerance_from_metadata_allows_rounding(self) -> None:
    calcs = {"AssetsNoncurrent": [("PPE", 1.0)]}
    balances = {"PPE": 100.0, "AssetsNoncurrent": 100.004}  # 0.004 drift
    outcome = _eval(
      _rule(
        "AssetsNoncurrent", "rs-gaap:AssetsNoncurrent", metadata={"tolerance": 0.01}
      ),
      {_P1: (balances, set(balances))},
      calcs,
    )
    assert outcome is not None
    assert outcome.status == "pass"


class TestLoadPeriodBalances:
  def test_sums_multiple_facts_per_element_period(self) -> None:
    session = MagicMock()
    rows = [
      SimpleNamespace(
        element_id="OtherOpCap", value=4782.70, period_start=_P2[0], period_end=_P2[1]
      ),
      SimpleNamespace(
        element_id="OtherOpCap", value=-1601.30, period_start=_P2[0], period_end=_P2[1]
      ),
      SimpleNamespace(
        element_id="NetIncome", value=-22356.10, period_start=_P2[0], period_end=_P2[1]
      ),
    ]
    exec_result = MagicMock()
    exec_result.all.return_value = rows
    session.execute.return_value = exec_result

    by_period = _load_period_balances(session, "struct_cf", None, None, None)
    balances, present = by_period[_P2]
    assert balances["OtherOpCap"] == pytest.approx(3181.40)
    assert "NetIncome" in present

  def test_unpinned_scopes_to_latest_fact_set_not_structure(self) -> None:
    """Unpinned evaluation resolves the latest FactSet and scopes facts to it,
    rather than a bare structure_id scope that would sum facts across every
    coexisting report that stamped the structure (doubling every balance)."""
    session = MagicMock()
    captured: list[str] = []

    fs_result = MagicMock()
    fs_result.scalar.return_value = "fs_latest"
    facts_result = MagicMock()
    facts_result.all.return_value = [
      SimpleNamespace(
        element_id="Cash", value=100.0, period_start=_P1[0], period_end=_P1[1]
      ),
    ]

    def _execute(stmt, *args, **kwargs):
      captured.append(str(stmt))
      return fs_result if len(captured) == 1 else facts_result

    session.execute.side_effect = _execute

    by_period = _load_period_balances(session, "struct_bs", None, None, None)

    # First query resolves the latest FactSet for the structure.
    assert "fact_sets" in captured[0]
    # Facts query is scoped to the resolved fact_set_id, not the structure.
    assert "fact_set_id" in captured[1]
    assert "structure_id" not in captured[1]
    assert by_period[_P1][0]["Cash"] == pytest.approx(100.0)

  def test_pinned_skips_fact_set_resolution(self) -> None:
    """A pinned ``fact_set_id`` scopes directly — no latest-FactSet lookup."""
    session = MagicMock()
    captured: list[str] = []

    facts_result = MagicMock()
    facts_result.all.return_value = []

    def _execute(stmt, *args, **kwargs):
      captured.append(str(stmt))
      return facts_result

    session.execute.side_effect = _execute

    _load_period_balances(session, "struct_bs", "fs_pinned", None, None)

    assert len(captured) == 1
    assert "fact_set_id" in captured[0]


class TestRollupRoutingInEngine:
  def test_rollup_rule_routes_to_arc_derived_path(self) -> None:
    session = MagicMock()
    session.get.return_value = MagicMock(id="struct_bs", block_type="balance_sheet")

    rule_lite = MagicMock()
    rule_lite.id = "rule_rollup"
    rule_orm = _rule("ANC", "rs-gaap:AssetsNoncurrent")
    rule_orm.id = "rule_rollup"

    calcs = {"ANC": [("PPE", 1.0)]}
    by_period = {_P1: ({"PPE": 100.0, "ANC": 100.0}, {"PPE", "ANC"})}

    assoc = MagicMock()
    assoc.scalars.return_value.all.return_value = []
    rules_res = MagicMock()
    rules_res.scalars.return_value.all.return_value = [rule_orm]
    session.execute.side_effect = [assoc, rules_res]

    with (
      patch(
        "robosystems.operations.information_block.rules.engine.load_rules_for_structure",
        return_value=[rule_lite],
      ),
      patch(
        "robosystems.operations.roboledger.reports.calc_dag.load_rs_gaap_calculations",
        return_value=calcs,
      ),
      patch(
        "robosystems.operations.information_block.rules.engine._load_period_balances",
        return_value=by_period,
      ),
    ):
      results = evaluate_rules_for_structure(
        session, "struct_bs", period_start=_P1[0], period_end=_P1[1]
      )

    assert len(results) == 1
    added = session.add.call_args_list[0][0][0]
    assert added.status == "pass"
    assert added.detail.get("source") == "calc-dag"

  def test_structure_local_calc_arcs_overlay_the_global_dag(self) -> None:
    """A tenant-authored roll_up note foots against its OWN calc arcs.

    The note's parent (a BS leaf, e.g. the inventory total) has no
    children in the global rs-gaap-calculations DAG — before the overlay
    the arc-derived evaluator returned None and the auto RollUp rule fell
    to the frozen evaluator with nothing to bind. The engine overlays the
    structure's local calculation associations, so footing derives from
    live arcs."""
    session = MagicMock()
    session.get.return_value = MagicMock(
      id="struct_note", block_type="regulatory_disclosure"
    )

    rule_lite = MagicMock()
    rule_lite.id = "rule_note_rollup"
    rule_orm = _rule("elem_total", "rs-gaap:InventoryNet")
    rule_orm.id = "rule_note_rollup"

    def _calc_assoc(child_id: str, order: float) -> MagicMock:
      a = MagicMock()
      a.id = f"assoc_{child_id}"
      a.association_type = "calculation"
      a.from_element_id = "elem_total"
      a.to_element_id = child_id
      a.weight = 1.0
      a.order_value = order
      return a

    assoc = MagicMock()
    assoc.scalars.return_value.all.return_value = [
      _calc_assoc("elem_raw", 1.0),
      _calc_assoc("elem_fg", 2.0),
    ]
    rules_res = MagicMock()
    rules_res.scalars.return_value.all.return_value = [rule_orm]
    session.execute.side_effect = [assoc, rules_res]

    by_period = {
      _P1: (
        {"elem_raw": 8000.0, "elem_fg": 5000.0, "elem_total": 13000.0},
        {"elem_raw", "elem_fg", "elem_total"},
      )
    }

    with (
      patch(
        "robosystems.operations.information_block.rules.engine.load_rules_for_structure",
        return_value=[rule_lite],
      ),
      patch(
        # Global DAG has NO entry for the note's parent.
        "robosystems.operations.roboledger.reports.calc_dag.load_rs_gaap_calculations",
        return_value={"rs-gaap:Assets": [("rs-gaap:AssetsCurrent", 1.0)]},
      ),
      patch(
        "robosystems.operations.information_block.rules.engine._load_period_balances",
        return_value=by_period,
      ),
    ):
      results = evaluate_rules_for_structure(
        session, "struct_note", period_start=_P1[0], period_end=_P1[1]
      )

    assert len(results) == 1
    added = session.add.call_args_list[0][0][0]
    assert added.status == "pass"
    assert added.detail.get("source") == "calc-dag"

  def test_global_dag_wins_over_local_arcs_for_shared_parent(self) -> None:
    """When a parent exists in BOTH the global DAG and the structure's
    local arcs, the global definition wins (statement structures mirror
    it; overlay must not shadow #883's semantics)."""
    session = MagicMock()
    session.get.return_value = MagicMock(id="struct_bs", block_type="balance_sheet")

    rule_lite = MagicMock()
    rule_lite.id = "rule_bs"
    rule_orm = _rule("ANC", "rs-gaap:AssetsNoncurrent")
    rule_orm.id = "rule_bs"

    local = MagicMock()
    local.id = "assoc_local"
    local.association_type = "calculation"
    local.from_element_id = "ANC"
    local.to_element_id = "WrongChild"
    local.weight = 1.0
    local.order_value = 1.0

    assoc = MagicMock()
    assoc.scalars.return_value.all.return_value = [local]
    rules_res = MagicMock()
    rules_res.scalars.return_value.all.return_value = [rule_orm]
    session.execute.side_effect = [assoc, rules_res]

    # Global children foot; the local WrongChild (0 balance) would fail.
    by_period = {_P1: ({"PPE": 100.0, "ANC": 100.0}, {"PPE", "ANC"})}

    with (
      patch(
        "robosystems.operations.information_block.rules.engine.load_rules_for_structure",
        return_value=[rule_lite],
      ),
      patch(
        "robosystems.operations.roboledger.reports.calc_dag.load_rs_gaap_calculations",
        return_value={"ANC": [("PPE", 1.0)]},
      ),
      patch(
        "robosystems.operations.information_block.rules.engine._load_period_balances",
        return_value=by_period,
      ),
    ):
      results = evaluate_rules_for_structure(
        session, "struct_bs", period_start=_P1[0], period_end=_P1[1]
      )

    assert len(results) == 1
    added = session.add.call_args_list[0][0][0]
    assert added.status == "pass"
