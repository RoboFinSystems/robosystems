"""Shape tests for the rs-gaap calculation structures.

The calc package collapses to THREE per-statement calculation structures
(Balance Sheet / Income Statement / Cash Flow), one per statement, mirroring
the presentation layer. Each arc carries the per-statement ``role``; an
individual roll-up equation is isolated by its parent ``from_qname``. The calc
DAG itself (from/to/weight) is unchanged from the prior per-equation layout —
only the structure grouping and the arc roles differ. All parents and children
are rs-gaap concepts; FAC is out of the reporting layer entirely.
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.model import TaxonomyPackage

_PACKAGES = framework_root("rs-gaap") / "packages"
_ROLE_BASE = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/"


@pytest.fixture(scope="module")
def calculations() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-calculations" / "v1" / "taxonomy.jsonld"
  )


def _equation(
  pkg: TaxonomyPackage, statement: str, parent_qname: str
) -> dict[str, float | None]:
  """The children (``to_qname`` → ``weight``) of one roll-up equation: arcs
  under the per-statement ``role`` whose parent is ``parent_qname``."""
  role = _ROLE_BASE + statement
  return {
    a.to_qname: a.weight
    for a in pkg.associations
    if a.role == role and a.from_qname == parent_qname
  }


class TestStatementStructures:
  """The package exposes exactly the three per-statement calc structures —
  one Network per statement, mirroring the presentation layer (the prior
  per-equation structures were collapsed into these)."""

  EXPECTED = {
    "rs-gaap — Balance Sheet calculation": "balance_sheet",
    "rs-gaap — Income Statement calculation": "income_statement",
    "rs-gaap — Cash Flow Statement calculation": "cash_flow_statement",
  }

  def test_exactly_three_statement_structures(
    self, calculations: TaxonomyPackage
  ) -> None:
    assert {s.name for s in calculations.structures} == set(self.EXPECTED)

  def test_block_type_is_the_statement_type(
    self, calculations: TaxonomyPackage
  ) -> None:
    by_name = {s.name: s for s in calculations.structures}
    for name, block_type in self.EXPECTED.items():
      assert by_name[name].block_type == block_type

  def test_arithmetic_pattern(self, calculations: TaxonomyPackage) -> None:
    for s in calculations.structures:
      assert s.concept_arrangement == "arithmetic"


class TestBalanceSheetCalcs:
  STATEMENT = "BalanceSheet"

  def test_top_level_identity_uses_rs_gaap_subhubs(
    self, calculations: TaxonomyPackage
  ) -> None:
    """rs-gaap:Assets = rs-gaap:AssetsCurrent + rs-gaap:AssetsNoncurrent —
    parent and children are all rs-gaap concepts."""
    assert _equation(calculations, self.STATEMENT, "rs-gaap:Assets") == {
      "rs-gaap:AssetsCurrent": 1.0,
      "rs-gaap:AssetsNoncurrent": 1.0,
    }

  def test_balance_sheet_identity(self, calculations: TaxonomyPackage) -> None:
    """rs-gaap:LiabilitiesAndStockholdersEquity = Liabilities + StockholdersEquity."""
    assert _equation(
      calculations, self.STATEMENT, "rs-gaap:LiabilitiesAndStockholdersEquity"
    ) == {
      "rs-gaap:Liabilities": 1.0,
      "rs-gaap:StockholdersEquity": 1.0,
    }

  def test_equity_has_treasury_with_negative_weight(
    self, calculations: TaxonomyPackage
  ) -> None:
    """TreasuryStock is a contra-equity account — must roll up with weight=-1."""
    equity = _equation(calculations, self.STATEMENT, "rs-gaap:StockholdersEquity")
    assert equity.get("rs-gaap:TreasuryStockValue") == -1.0


class TestCashFlowCalcs:
  STATEMENT = "CashFlowStatement"

  def test_net_change_sums_three_subactivities_and_exchange(
    self, calculations: TaxonomyPackage
  ) -> None:
    """CashAndCashEquivalentsPeriodIncreaseDecrease = Op + Inv + Fin + Exchange,
    all rs-gaap concepts."""
    assert _equation(
      calculations,
      self.STATEMENT,
      "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease",
    ) == {
      "rs-gaap:NetCashProvidedByUsedInOperatingActivities": 1.0,
      "rs-gaap:NetCashProvidedByUsedInInvestingActivities": 1.0,
      "rs-gaap:NetCashProvidedByUsedInFinancingActivities": 1.0,
      "rs-gaap:EffectOfExchangeRateOnCash": 1.0,
    }


class TestIncomeStatementCalcs:
  STATEMENT = "IncomeStatement"

  def test_gross_profit_subtracts_cost_of_revenue(
    self, calculations: TaxonomyPackage
  ) -> None:
    assert _equation(calculations, self.STATEMENT, "rs-gaap:GrossProfit") == {
      "rs-gaap:Revenues": 1.0,
      "rs-gaap:CostOfRevenue": -1.0,
    }

  def test_operating_income_subtracts_opex(self, calculations: TaxonomyPackage) -> None:
    assert _equation(calculations, self.STATEMENT, "rs-gaap:OperatingIncomeLoss") == {
      "rs-gaap:GrossProfit": 1.0,
      "rs-gaap:OperatingExpenses": -1.0,
    }

  def test_net_income_sums_continuing_and_discontinued(
    self, calculations: TaxonomyPackage
  ) -> None:
    assert _equation(calculations, self.STATEMENT, "rs-gaap:NetIncomeLoss") == {
      "rs-gaap:IncomeLossFromContinuingOperations": 1.0,
      "rs-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax": 1.0,
    }
