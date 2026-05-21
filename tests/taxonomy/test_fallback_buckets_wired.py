"""Every MappingOperator fallback bucket must be a wired calc leaf.

``FAC_TO_RS_GAAP_FALLBACK`` is the per-FAC "Other" bucket the
MappingOperator falls back to when AI refinement can't pick a specific
rs-gaap concept. If a fallback target is NOT a child in the rs-gaap
calculation DAG (or is itself a rollup parent), a CoA account routed
there silently drops out of its subtotal at render — the exact failure
that motivated wiring the revenue/COGS/OpEx ``Other`` leaves
(info-block §3.7 / mapping leaves-only model). This test pins the
invariant so the constant and the calc package can't drift apart.
"""

from __future__ import annotations

import json

import pytest

from robosystems.operations.operators.implementations.mapping.constants import (
  FAC_TO_RS_GAAP_FALLBACK,
)
from robosystems.taxonomy.discovery import framework_root

_CALC_PATH = (
  framework_root("rs-gaap")
  / "packages"
  / "rs-gaap-calculations"
  / "v1"
  / "taxonomy.jsonld"
)


@pytest.fixture(scope="module")
def calc_sets() -> tuple[set[str], set[str]]:
  """(calc children, calc parents) for ``calculation`` arcs."""
  graph = json.loads(_CALC_PATH.read_text())["@graph"]
  children = {
    n["arcTo"]["@id"] for n in graph if n.get("arcAssociationType") == "calculation"
  }
  parents = {
    n["arcFrom"]["@id"] for n in graph if n.get("arcAssociationType") == "calculation"
  }
  return children, parents


class TestFallbackBucketsWired:
  def test_every_fallback_target_is_a_wired_calc_leaf(
    self, calc_sets: tuple[set[str], set[str]]
  ) -> None:
    children, parents = calc_sets
    broken = {
      fac: tgt
      for fac, tgt in FAC_TO_RS_GAAP_FALLBACK.items()
      if tgt not in children or tgt in parents
    }
    assert not broken, (
      "fallback targets that don't roll up (not a calc child, or are a "
      f"rollup parent) — a CoA routed here would silently drop: {broken}"
    )

  def test_revenue_and_cogs_buckets_are_the_operating_catch_alls(self) -> None:
    """Regression for the QB revenue/COGS gap: the fallback must point at
    the operating-revenue / operating-cost catch-all leaves wired under
    Revenues / CostOfRevenue — NOT OtherIncome (non-operating, unwired)."""
    assert FAC_TO_RS_GAAP_FALLBACK["fac:Revenues"] == "rs-gaap:OtherSalesRevenueNet"
    assert (
      FAC_TO_RS_GAAP_FALLBACK["fac:CostOfRevenue"]
      == "rs-gaap:OtherCostOfOperatingRevenue"
    )
