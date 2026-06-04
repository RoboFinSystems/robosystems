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

import pytest

from robosystems.operations.operators.implementations.mapping.constants import (
  FAC_TO_RS_GAAP_FALLBACK,
)
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.loader import load_taxonomy_package

_CALC_PATH = (
  framework_root("rs-gaap")
  / "packages"
  / "rs-gaap-calculations"
  / "v1"
  / "taxonomy.jsonld"
)


@pytest.fixture(scope="module")
def calc_sets() -> tuple[set[str], set[str]]:
  """(calc children, calc parents) for ``calculation`` arcs.

  Read through the loader so this is agnostic to the RDF encoding — the
  canonical seed reifies calc arcs as ``rs:Association`` nodes.
  """
  pkg = load_taxonomy_package(_CALC_PATH)
  calc = [a for a in pkg.associations if a.association_type == "calculation"]
  children = {a.to_qname for a in calc}
  parents = {a.from_qname for a in calc}
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
    """Regression for the QB revenue/COGS gap: the fallback must point at an
    operating-revenue / operating-cost leaf wired under Revenues /
    CostOfRevenue — NOT OtherIncome (non-operating, unwired). After the
    deprecated-leaf swap, RevenueFromContractWithCustomerExcludingAssessedTax
    is the sole (ASC 606 primary) revenue leaf, so it doubles as the fallback;
    OtherCostOfOperatingRevenue remains the COGS catch-all leaf."""
    assert (
      FAC_TO_RS_GAAP_FALLBACK["fac:Revenues"]
      == "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
    )
    assert (
      FAC_TO_RS_GAAP_FALLBACK["fac:CostOfRevenue"]
      == "rs-gaap:OtherCostOfOperatingRevenue"
    )
