"""Shape tests for the rs-gaap BS + CF calculation structures.

The Income Statement got its 5 calc structures on the prior branch;
this round adds 8 BS structures and 4 CF structures. Tests guard
against arc loss or hub rewiring.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from robosystems.taxonomy.loaders import load_taxonomy_package
from robosystems.taxonomy.model import TaxonomyPackage

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PACKAGES = _REPO_ROOT / "robosystems" / "taxonomy" / "packages"


@pytest.fixture(scope="module")
def calculations() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-calculations" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def fac() -> TaxonomyPackage:
  return load_taxonomy_package(_PACKAGES / "fac" / "v1" / "taxonomy.jsonld")


class TestBalanceSheetCalcs:
  EXPECTED_NAMES = (
    "rs-gaap calc BS-Assets — fac:Assets = fac:CurrentAssets + fac:NoncurrentAssets",
    "rs-gaap calc BS-CurrentAssets — fac:CurrentAssets = Σ rs-gaap current-asset leaves",
    "rs-gaap calc BS-NoncurrentAssets — fac:NoncurrentAssets = Σ rs-gaap noncurrent-asset leaves",
    "rs-gaap calc BS-LiabilitiesAndEquity — fac:LiabilitiesAndEquity = fac:Liabilities + fac:Equity",
    "rs-gaap calc BS-Liabilities — fac:Liabilities = fac:CurrentLiabilities + fac:NoncurrentLiabilities",
    "rs-gaap calc BS-CurrentLiabilities — fac:CurrentLiabilities = Σ rs-gaap current-liability leaves",
    "rs-gaap calc BS-NoncurrentLiabilities — fac:NoncurrentLiabilities = Σ rs-gaap noncurrent-liability leaves",
    "rs-gaap calc BS-Equity — fac:Equity = Σ rs-gaap equity leaves",
  )

  def test_all_eight_structures_present(self, calculations: TaxonomyPackage) -> None:
    names = {s.name for s in calculations.structures}
    missing = [n for n in self.EXPECTED_NAMES if n not in names]
    assert not missing, f"missing BS calc structures: {missing}"

  def test_arithmetic_pattern(self, calculations: TaxonomyPackage) -> None:
    for s in calculations.structures:
      if s.name in self.EXPECTED_NAMES:
        assert s.structure_type == "validation_rules"
        assert s.concept_arrangement == "arithmetic"

  def test_top_level_identity_uses_fac_subhubs(
    self, calculations: TaxonomyPackage
  ) -> None:
    """fac:Assets = fac:CurrentAssets + fac:NoncurrentAssets — both children
    must be fac concepts, not rs-gaap leaves."""
    role = (
      "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/BS-Assets"
    )
    targets = {a.to_qname for a in calculations.associations if a.role == role}
    assert targets == {"fac:CurrentAssets", "fac:NoncurrentAssets"}

  def test_equity_has_treasury_with_negative_weight(
    self, calculations: TaxonomyPackage
  ) -> None:
    """TreasuryStock is a contra-equity account — must roll up with weight=-1."""
    role = (
      "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/BS-Equity"
    )
    treasury_arc = next(
      a
      for a in calculations.associations
      if a.role == role and a.to_qname == "rs-gaap:TreasuryStockValue"
    )
    assert treasury_arc.weight == -1.0


class TestCashFlowCalcs:
  EXPECTED_NAMES = (
    "rs-gaap calc CF-Operating — fac:NetCashFlowFromOperatingActivities = rs-gaap operating CF top-line",
    "rs-gaap calc CF-Investing — fac:NetCashFlowFromInvestingActivities = rs-gaap investing CF top-line",
    "rs-gaap calc CF-Financing — fac:NetCashFlowFromFinancingActivities = rs-gaap financing CF top-line",
    "rs-gaap calc CF-NetChange — fac:NetCashFlow = Op + Inv + Fin",
  )

  def test_all_four_structures_present(self, calculations: TaxonomyPackage) -> None:
    names = {s.name for s in calculations.structures}
    missing = [n for n in self.EXPECTED_NAMES if n not in names]
    assert not missing, f"missing CF calc structures: {missing}"

  def test_net_change_sums_three_subactivities(
    self, calculations: TaxonomyPackage
  ) -> None:
    role = (
      "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/CF-NetChange"
    )
    targets = {a.to_qname for a in calculations.associations if a.role == role}
    assert targets == {
      "fac:NetCashFlowFromOperatingActivities",
      "fac:NetCashFlowFromInvestingActivities",
      "fac:NetCashFlowFromFinancingActivities",
    }
