"""Unit tests for ``_emit_subtotal_facts`` — persisting calc-DAG subtotals.

The function walks the rs-gaap-calculations DAG bottom-up over the
already-emitted leaf facts and emits one fact per subtotal, so verification
rules scoped to a subtotal can bind (info-block §6.1). The session is mocked
so the rollup math is tested without a database.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  ReportFact,
  _emit_subtotal_facts,
)

_P = PeriodSpec(start=date(2025, 1, 1), end=date(2025, 12, 31), label="FY2025")


def _leaf(eid: str, value: float) -> ReportFact:
  return ReportFact(
    element_id=eid,
    element_qname=f"rs-gaap:{eid}",
    element_name=eid,
    classification="asset",
    balance_type="debit",
    value=value,
    period_start=_P.start,
    period_end=_P.end,
    period_type="instant",
  )


def _dag_row(parent: str, child: str, weight: float) -> SimpleNamespace:
  return SimpleNamespace(parent=parent, child=child, weight=weight)


def _meta_row(eid: str) -> SimpleNamespace:
  return SimpleNamespace(
    id=eid,
    qname=f"rs-gaap:{eid}",
    name=eid,
    balance_type="debit",
    period_type="instant",
  )


def _session(dag_rows: list, target_ids: list[str]) -> MagicMock:
  """Mock session: 1st execute → DAG rows, 2nd execute → element meta."""
  session = MagicMock()
  dag_result = MagicMock()
  dag_result.fetchall.return_value = dag_rows
  meta_result = MagicMock()
  meta_result.fetchall.return_value = [_meta_row(t) for t in target_ids]
  session.execute.side_effect = [dag_result, meta_result]
  return session


# DAG: AssetsCurrent = Cash + AR; AssetsNoncurrent = PPE;
#      Assets = AssetsCurrent + AssetsNoncurrent
_DAG = [
  _dag_row("AssetsCurrent", "Cash", 1.0),
  _dag_row("AssetsCurrent", "AR", 1.0),
  _dag_row("AssetsNoncurrent", "PPE", 1.0),
  _dag_row("Assets", "AssetsCurrent", 1.0),
  _dag_row("Assets", "AssetsNoncurrent", 1.0),
]
_TARGETS = ["AssetsCurrent", "AssetsNoncurrent", "Assets"]


class TestEmitSubtotalFacts:
  def test_emits_subtotals_bottom_up(self) -> None:
    facts = [_leaf("Cash", 100.0), _leaf("AR", 20.0), _leaf("PPE", 30.0)]
    _emit_subtotal_facts(_session(_DAG, _TARGETS), facts, [_P])
    by_id = {f.element_id: f.value for f in facts}
    assert by_id["AssetsCurrent"] == 120.0  # 100 + 20
    assert by_id["AssetsNoncurrent"] == 30.0
    assert by_id["Assets"] == 150.0  # 120 + 30 (depends on the computed children)

  def test_weighted_subtraction(self) -> None:
    dag = [
      _dag_row("GrossProfit", "Revenues", 1.0),
      _dag_row("GrossProfit", "CostOfRevenue", -1.0),
    ]
    facts = [_leaf("Revenues", 175000.0), _leaf("CostOfRevenue", 60000.0)]
    _emit_subtotal_facts(_session(dag, ["GrossProfit"]), facts, [_P])
    gp = next(f for f in facts if f.element_id == "GrossProfit")
    assert gp.value == 115000.0  # 175000 - 60000

  def test_direct_fact_wins_not_overwritten(self) -> None:
    """A subtotal that already has a fact (e.g. NetIncomeLoss from the
    close) is left untouched — no duplicate, original value preserved."""
    dag = [
      _dag_row("Assets", "AssetsCurrent", 1.0),
      _dag_row("Assets", "AssetsNoncurrent", 1.0),
    ]
    facts = [
      _leaf("AssetsCurrent", 120.0),
      _leaf("AssetsNoncurrent", 30.0),
      _leaf("Assets", 999.0),  # authoritative direct fact
    ]
    _emit_subtotal_facts(_session(dag, ["Assets"]), facts, [_P])
    assets = [f for f in facts if f.element_id == "Assets"]
    assert len(assets) == 1
    assert assets[0].value == 999.0

  def test_zero_subtotal_not_emitted(self) -> None:
    dag = [_dag_row("AssetsNoncurrent", "PPE", 1.0)]
    facts = [_leaf("PPE", 0.0)]
    _emit_subtotal_facts(_session(dag, ["AssetsNoncurrent"]), facts, [_P])
    assert all(f.element_id != "AssetsNoncurrent" for f in facts)

  def test_zero_direct_fact_wins_over_calc_sum_for_downstream(self) -> None:
    """A calc target with a zero-valued DIRECT fact must contribute its 0
    to a downstream subtotal — 'direct wins' keys off presence, not a
    non-zero test. Regression for the zero-value bypass: with a non-zero
    test, SubA would wrongly resolve to its calc sum (100) and Parent
    would be 150 instead of 50."""
    dag = [
      _dag_row("SubA", "Leaf1", 1.0),  # SubA is a target with a direct 0.0 fact
      _dag_row("Parent", "SubA", 1.0),
      _dag_row("Parent", "SubB", 1.0),
    ]
    facts = [
      _leaf("SubA", 0.0),  # authoritative direct fact = 0 (present)
      _leaf("Leaf1", 100.0),  # SubA's calc sum would be 100 — must NOT win
      _leaf("SubB", 50.0),
    ]
    _emit_subtotal_facts(_session(dag, ["SubA", "Parent"]), facts, [_P])
    parent = next(f for f in facts if f.element_id == "Parent")
    assert parent.value == 50.0  # SubA contributes its direct 0, not the 100 sum

  def test_subtotals_computed_per_period_independently(self) -> None:
    """Each period's subtotal sums only that period's leaves — no bleed."""
    p2 = PeriodSpec(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY2024")

    def leaf_p(eid: str, value: float, period: PeriodSpec) -> ReportFact:
      return ReportFact(
        element_id=eid,
        element_qname=f"rs-gaap:{eid}",
        element_name=eid,
        classification="asset",
        balance_type="debit",
        value=value,
        period_start=period.start,
        period_end=period.end,
        period_type="instant",
      )

    dag = [_dag_row("AssetsCurrent", "Cash", 1.0)]
    facts = [leaf_p("Cash", 100.0, _P), leaf_p("Cash", 200.0, p2)]
    _emit_subtotal_facts(_session(dag, ["AssetsCurrent"]), facts, [_P, p2])
    ac = {f.period_start: f.value for f in facts if f.element_id == "AssetsCurrent"}
    assert ac[_P.start] == 100.0
    assert ac[p2.start] == 200.0
