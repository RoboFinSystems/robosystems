"""Unit tests for the shared calc-DAG resolver (``calc_dag.py``).

These pin the resolution semantics the fact producer and the rollup validator
now share: direct-fact-wins-on-presence, absent-summand-is-zero, weighted sums,
and — the load-bearing one — transitive resolution through an intermediate
subtotal, which is what lets a subtotal foot over a sibling concept.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from robosystems.operations.roboledger.reports.calc_dag import (
  load_rs_gaap_calculations,
  merge_calculations,
  resolve_calc_dag,
  topo_sort_calculations,
)


class TestResolveCalcDag:
  def test_direct_fact_wins_over_calc_sum(self) -> None:
    calcs = {"Assets": [("AC", 1.0), ("ANC", 1.0)]}
    balances = {"AC": 120.0, "ANC": 30.0, "Assets": 999.0}
    computed = resolve_calc_dag(balances, {"AC", "ANC", "Assets"}, calcs)
    assert computed["Assets"] == 999.0  # authoritative direct fact, not 150

  def test_absent_subtotal_computed_from_children(self) -> None:
    calcs = {"AC": [("Cash", 1.0), ("AR", 1.0)]}
    computed = resolve_calc_dag({"Cash": 100.0, "AR": 20.0}, {"Cash", "AR"}, calcs)
    assert computed["AC"] == 120.0

  def test_weighted_subtraction(self) -> None:
    calcs = {"GrossProfit": [("Rev", 1.0), ("COGS", -1.0)]}
    computed = resolve_calc_dag(
      {"Rev": 175000.0, "COGS": 60000.0}, {"Rev", "COGS"}, calcs
    )
    assert computed["GrossProfit"] == 115000.0

  def test_absent_summand_is_zero(self) -> None:
    calcs = {"AC": [("Cash", 1.0), ("AR", 1.0)]}
    computed = resolve_calc_dag({"Cash": 100.0}, {"Cash"}, calcs)
    assert computed["AC"] == 100.0

  def test_transitive_including_excluding_goodwill(self) -> None:
    """The Balance Sheet false-failure case. The subtotal rolls up
    ``...IncludingGoodwill``, but the tenant's fact is ``...ExcludingGoodwill``
    (a child of Including). Transitive resolution collapses Including to the
    present Excluding leaf, so AssetsNoncurrent foots — where the frozen
    enumeration failed by exactly 5966.27."""
    calcs = {
      "AssetsNoncurrent": [("PPE", 1.0), ("IncludingGoodwill", 1.0), ("Other", 1.0)],
      "IncludingGoodwill": [("ExcludingGoodwill", 1.0), ("Goodwill", 1.0)],
    }
    balances = {"PPE": 8901.31, "ExcludingGoodwill": 5966.27, "Other": 83569.82}
    computed = resolve_calc_dag(balances, set(balances), calcs)
    assert computed["IncludingGoodwill"] == pytest.approx(5966.27)
    assert computed["AssetsNoncurrent"] == pytest.approx(98437.40)

  def test_chaining_resolves_regardless_of_dict_order(self) -> None:
    calcs = {
      "NetIncome": [("OperatingIncome", 1.0), ("Tax", -1.0)],
      "OperatingIncome": [("GrossProfit", 1.0), ("OpEx", -1.0)],
      "GrossProfit": [("Rev", 1.0), ("COGS", -1.0)],
    }
    balances = {"Rev": 1000.0, "COGS": 400.0, "OpEx": 150.0, "Tax": 90.0}
    computed = resolve_calc_dag(balances, set(balances), calcs)
    assert computed["GrossProfit"] == 600.0
    assert computed["OperatingIncome"] == 450.0
    assert computed["NetIncome"] == 360.0

  def test_zero_direct_fact_wins_over_calc_sum(self) -> None:
    """Presence, not non-zero: a direct 0 subtotal isn't overwritten by its
    calc sum, so a downstream parent inherits the authoritative 0."""
    calcs = {"Parent": [("SubA", 1.0), ("SubB", 1.0)], "SubA": [("Leaf1", 1.0)]}
    balances = {"SubA": 0.0, "Leaf1": 100.0, "SubB": 50.0}
    computed = resolve_calc_dag(balances, {"SubA", "Leaf1", "SubB"}, calcs)
    assert computed["Parent"] == 50.0

  def test_precomputed_order_is_honored(self) -> None:
    calcs = {"AC": [("Cash", 1.0)]}
    order = topo_sort_calculations(calcs)
    computed = resolve_calc_dag({"Cash": 5.0}, {"Cash"}, calcs, order)
    assert computed["AC"] == 5.0


class TestLoadRsGaapCalculations:
  def test_builds_parent_children_map_with_default_weight(self) -> None:
    session = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = [
      SimpleNamespace(parent="Assets", child="AC", weight=1.0),
      SimpleNamespace(parent="Assets", child="ANC", weight=None),  # → default 1.0
      SimpleNamespace(parent="GP", child="COGS", weight=-1.0),
    ]
    session.execute.return_value = result
    calcs = load_rs_gaap_calculations(session)
    assert calcs["Assets"] == [("AC", 1.0), ("ANC", 1.0)]
    assert calcs["GP"] == [("COGS", -1.0)]


class TestTopoSort:
  def test_dependencies_ordered_before_dependents(self) -> None:
    calcs = {
      "NetIncome": [("OperatingIncome", 1.0)],
      "OperatingIncome": [("GrossProfit", 1.0)],
      "GrossProfit": [("Rev", 1.0)],
    }
    order = topo_sort_calculations(calcs)
    assert order.index("GrossProfit") < order.index("OperatingIncome")
    assert order.index("OperatingIncome") < order.index("NetIncome")

  def test_empty(self) -> None:
    assert topo_sort_calculations({}) == []


class TestMergeCalculations:
  def test_local_wins_per_parent(self) -> None:
    """A structure's own arcs ARE its footing spec — a note decomposing a
    global calc parent foots against its own members, not the statement
    children absent from its FactSet."""
    global_calcs = {"Revenues": [("StmtChildA", 1.0), ("StmtChildB", 1.0)]}
    local_calcs = {"Revenues": [("MemProduct", 1.0), ("MemService", 1.0)]}
    merged = merge_calculations(global_calcs, local_calcs)
    assert merged["Revenues"] == [("MemProduct", 1.0), ("MemService", 1.0)]

  def test_global_fallback_for_unarced_parents(self) -> None:
    global_calcs = {"Assets": [("AC", 1.0), ("ANC", 1.0)]}
    local_calcs = {"InventoryNet": [("Raw", 1.0), ("Finished", 1.0)]}
    merged = merge_calculations(global_calcs, local_calcs)
    assert merged["Assets"] == [("AC", 1.0), ("ANC", 1.0)]
    assert merged["InventoryNet"] == [("Raw", 1.0), ("Finished", 1.0)]

  def test_pure_does_not_mutate_inputs(self) -> None:
    global_calcs = {"Assets": [("AC", 1.0)]}
    local_calcs = {"Assets": [("Local", 1.0)]}
    merge_calculations(global_calcs, local_calcs)
    assert global_calcs == {"Assets": [("AC", 1.0)]}
    assert local_calcs == {"Assets": [("Local", 1.0)]}
