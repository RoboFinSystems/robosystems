"""Shape tests for the rs-gaap BS + CF calculation structures.

The Income Statement chain (Revenues → CostOfRevenue → GrossProfit →
OperatingExpenses → OperatingIncomeLoss → NonoperatingIncomeExpense →
PreTax → ContinuingOps → NetIncomeLoss) lives alongside BS + CF here.
All parents and children are rs-gaap concepts — FAC is out of the
reporting layer entirely per the rs-gaap-anchored architecture.
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy.loaders import load_taxonomy_package
from robosystems.taxonomy.loaders.discovery import framework_root
from robosystems.taxonomy.model import TaxonomyPackage

_PACKAGES = framework_root("rs-gaap", "v1") / "packages"


@pytest.fixture(scope="module")
def calculations() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-calculations" / "v1" / "taxonomy.jsonld"
  )


class TestBalanceSheetCalcs:
  EXPECTED_NAMES = (
    "rs-gaap calc BS-Assets — rs-gaap:Assets = rs-gaap:AssetsCurrent + rs-gaap:AssetsNoncurrent",
    "rs-gaap calc BS-CurrentAssets — rs-gaap:AssetsCurrent = Σ rs-gaap current-asset leaves",
    "rs-gaap calc BS-NoncurrentAssets — rs-gaap:AssetsNoncurrent = Σ rs-gaap noncurrent-asset leaves",
    "rs-gaap calc BS-Liabilities — rs-gaap:Liabilities = rs-gaap:LiabilitiesCurrent + rs-gaap:LiabilitiesNoncurrent",
    "rs-gaap calc BS-CurrentLiabilities — rs-gaap:LiabilitiesCurrent = Σ rs-gaap current-liability leaves",
    "rs-gaap calc BS-NoncurrentLiabilities — rs-gaap:LiabilitiesNoncurrent = Σ rs-gaap noncurrent-liability leaves",
    "rs-gaap calc BS-Equity — rs-gaap:StockholdersEquity = Σ rs-gaap equity leaves (Treasury weight=-1)",
    "rs-gaap calc BS-LiabilitiesAndStockholdersEquity — rs-gaap:LiabilitiesAndStockholdersEquity = rs-gaap:Liabilities + rs-gaap:StockholdersEquity",
  )

  def test_all_eight_structures_present(self, calculations: TaxonomyPackage) -> None:
    names = {s.name for s in calculations.structures}
    missing = [n for n in self.EXPECTED_NAMES if n not in names]
    assert not missing, f"missing BS calc structures: {missing}"

  def test_arithmetic_pattern(self, calculations: TaxonomyPackage) -> None:
    for s in calculations.structures:
      if s.name in self.EXPECTED_NAMES:
        assert s.block_type == "validation_rules"
        assert s.concept_arrangement == "arithmetic"

  def test_top_level_identity_uses_rs_gaap_subhubs(
    self, calculations: TaxonomyPackage
  ) -> None:
    """rs-gaap:Assets = rs-gaap:AssetsCurrent + rs-gaap:AssetsNoncurrent —
    parent and children are all rs-gaap concepts."""
    role = (
      "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/BS-Assets"
    )
    targets = {a.to_qname for a in calculations.associations if a.role == role}
    assert targets == {"rs-gaap:AssetsCurrent", "rs-gaap:AssetsNoncurrent"}

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
    "rs-gaap calc CF-NetChange — rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease = Op + Inv + Fin + Exchange",
  )

  def test_net_change_present(self, calculations: TaxonomyPackage) -> None:
    names = {s.name for s in calculations.structures}
    missing = [n for n in self.EXPECTED_NAMES if n not in names]
    assert not missing, f"missing CF calc structures: {missing}"

  def test_net_change_sums_three_subactivities_and_exchange(
    self, calculations: TaxonomyPackage
  ) -> None:
    """CashAndCashEquivalentsPeriodIncreaseDecrease = Op + Inv + Fin + Exchange,
    all rs-gaap concepts."""
    role = (
      "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/CF-NetChange"
    )
    targets = {a.to_qname for a in calculations.associations if a.role == role}
    assert targets == {
      "rs-gaap:NetCashProvidedByUsedInOperatingActivities",
      "rs-gaap:NetCashProvidedByUsedInInvestingActivities",
      "rs-gaap:NetCashProvidedByUsedInFinancingActivities",
      "rs-gaap:EffectOfExchangeRateOnCash",
    }


class TestIncomeStatementCalcs:
  EXPECTED_NAMES = (
    "rs-gaap calc IS-Rev — rs-gaap:Revenues = Σ rs-gaap revenue leaves",
    "rs-gaap calc IS-COR — rs-gaap:CostOfRevenue = Σ rs-gaap COGS leaves",
    "rs-gaap calc IS-GrossProfit — rs-gaap:GrossProfit = rs-gaap:Revenues − rs-gaap:CostOfRevenue",
    "rs-gaap calc IS-OpEx — rs-gaap:OperatingExpenses = Σ rs-gaap operating expense leaves",
    "rs-gaap calc IS-OperatingIncome — rs-gaap:OperatingIncomeLoss = rs-gaap:GrossProfit − rs-gaap:OperatingExpenses",
    "rs-gaap calc IS-NonOp — rs-gaap:NonoperatingIncomeExpense = Σ rs-gaap nonoperating leaves",
    "rs-gaap calc IS-PreTax — rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes... = OperatingIncomeLoss + NonoperatingIncomeExpense − InterestExpense",
    "rs-gaap calc IS-ContinuingOps — rs-gaap:IncomeLossFromContinuingOperations = PreTax − IncomeTaxExpenseBenefit",
    "rs-gaap calc IS-NetIncome — rs-gaap:NetIncomeLoss = ContinuingOps + DiscontinuedOps",
  )

  def test_full_is_chain_present(self, calculations: TaxonomyPackage) -> None:
    """Full multi-step IS chain: Revenues → CostOfRevenue → GrossProfit →
    OperatingExpenses → OperatingIncomeLoss → NonOp → InterestExp →
    PreTax → Tax → ContinuingOps → DiscontinuedOps → NetIncomeLoss."""
    names = {s.name for s in calculations.structures}
    missing = [n for n in self.EXPECTED_NAMES if n not in names]
    assert not missing, f"missing IS calc structures: {missing}"

  def test_gross_profit_subtracts_cost_of_revenue(
    self, calculations: TaxonomyPackage
  ) -> None:
    role = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/IS-GrossProfit"
    arcs = {a.to_qname: a.weight for a in calculations.associations if a.role == role}
    assert arcs == {"rs-gaap:Revenues": 1.0, "rs-gaap:CostOfRevenue": -1.0}

  def test_operating_income_subtracts_opex(self, calculations: TaxonomyPackage) -> None:
    role = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/IS-OperatingIncome"
    arcs = {a.to_qname: a.weight for a in calculations.associations if a.role == role}
    assert arcs == {
      "rs-gaap:GrossProfit": 1.0,
      "rs-gaap:OperatingExpenses": -1.0,
    }

  def test_net_income_sums_continuing_and_discontinued(
    self, calculations: TaxonomyPackage
  ) -> None:
    role = (
      "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/IS-NetIncome"
    )
    arcs = {a.to_qname: a.weight for a in calculations.associations if a.role == role}
    assert arcs == {
      "rs-gaap:IncomeLossFromContinuingOperations": 1.0,
      "rs-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax": 1.0,
    }
