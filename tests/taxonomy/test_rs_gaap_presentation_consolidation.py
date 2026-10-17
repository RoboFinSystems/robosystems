"""Shape tests for the consolidation rows in the rs-gaap presentation package.

Migration 0046 presented noncontrolling interest, the parent's investment in
its subsidiaries and the intercompany pair in every Reporting Style, and
0052 the equity-method investment and its share of income. Each row is a
leaf beside an existing subtotal in each of the three balance sheets and
cash flows (and the one income statement), so a Style that gains a form or
loses a row shows up here rather than in a tenant's render.
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.model import TaxonomyPackage

_PRES = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation"
_FORMS = ("", "-LLC", "-PART")


@pytest.fixture(scope="module")
def presentation() -> TaxonomyPackage:
  return load_taxonomy_package(
    framework_root("rs-gaap")
    / "packages"
    / "rs-gaap-presentation"
    / "v1"
    / "taxonomy.jsonld"
  )


def _children(pkg: TaxonomyPackage, role: str, parent: str) -> set[str]:
  return {
    a.to_qname
    for a in pkg.associations
    if a.role == f"{_PRES}/{role}" and a.from_qname == parent
  }


@pytest.mark.unit
class TestConsolidationRowsInEveryStyle:
  @pytest.mark.parametrize("form", _FORMS)
  def test_the_balance_sheet_rows(
    self, presentation: TaxonomyPackage, form: str
  ) -> None:
    role = f"BS-classified{form}"
    assert "rs-gaap:DueFromAffiliateCurrent" in _children(
      presentation, role, "rs-gaap:AssetsCurrent"
    )
    noncurrent = _children(presentation, role, "rs-gaap:AssetsNoncurrent")
    assert {
      "rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures",
      "rs-gaap:EquityMethodInvestments",
    } <= noncurrent
    assert "rs-gaap:DueToAffiliateCurrent" in _children(
      presentation, role, "rs-gaap:LiabilitiesCurrent"
    )
    assert "rs-gaap:MinorityInterest" in _children(
      presentation, role, "rs-gaap:LiabilitiesAndStockholdersEquity"
    )

  def test_the_income_statement_rows(self, presentation: TaxonomyPackage) -> None:
    """One income statement serves every form: the share of income to the
    noncontrolling interest before net income, the share of an associate's
    result in the nonoperating section."""
    assert "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest" in _children(
      presentation, "IS-multistep", "rs-gaap:IncomeStatementAbstract"
    )
    assert "rs-gaap:IncomeLossFromEquityMethodInvestments" in _children(
      presentation, "IS-multistep", "rs-gaap:NonoperatingIncomeExpense"
    )

  @pytest.mark.parametrize("form", _FORMS)
  def test_the_cash_flow_rows(self, presentation: TaxonomyPackage, form: str) -> None:
    role = f"CashFlow-indirect{form}"
    operating = _children(
      presentation, role, "rs-gaap:NetCashProvidedByUsedInOperatingActivities"
    )
    assert {
      "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest",
      "rs-gaap:IncomeLossFromEquityMethodInvestments",
      "rs-gaap:IncreaseDecreaseInDueFromAffiliatesCurrent",
      "rs-gaap:IncreaseDecreaseInDueToAffiliatesCurrent",
    } <= operating
    investing = _children(
      presentation, role, "rs-gaap:NetCashProvidedByUsedInInvestingActivities"
    )
    assert {
      "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates",
      "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates",
      "rs-gaap:PaymentsToAcquireEquityMethodInvestments",
      "rs-gaap:ProceedsFromSaleOfEquityMethodInvestments",
    } <= investing
    financing = _children(
      presentation, role, "rs-gaap:NetCashProvidedByUsedInFinancingActivities"
    )
    assert {
      "rs-gaap:ProceedsFromMinorityShareholders",
      "rs-gaap:PaymentsToMinorityShareholders",
    } <= financing
