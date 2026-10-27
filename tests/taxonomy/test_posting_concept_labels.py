"""The concepts the ledger posts to, pinned by label.

A concept's qname outlives its meaning: us-gaap narrowed
``GainLossOnDispositionOfAssets`` to property, plant and equipment, and the
consolidation engine went on booking a subsidiary's sale to it, so every
consolidated statement called that sale an equipment sale. The numbers tied,
so nothing failed.

Every concept the consolidation engine posts to, and every concept a chart
template maps an account to, is pinned here with the label it renders under.
A relabel in the library, or a new concept in the engine or a template,
fails until someone reads the label and pins it.
"""

from __future__ import annotations

import pytest

from robosystems.operations.roboledger.commands import consolidation as commands
from robosystems.operations.roboledger.reads import consolidation as reads
from robosystems.operations.taxonomy_block.chart_templates import CHART_TEMPLATES
from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root

PINNED = {
  "rs-gaap:AccountsPayableCurrent": "Accounts Payable, Current",
  "rs-gaap:AccruedLiabilitiesCurrent": "Accrued Liabilities, Current",
  "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment": (
    "Accumulated Depreciation, Depletion and Amortization, Property, Plant, "
    "and Equipment"
  ),
  "rs-gaap:AdditionalPaidInCapital": "Additional Paid in Capital",
  "rs-gaap:BusinessCombinationBargainPurchaseGainRecognizedAmount": (
    "Business Combination, Bargain Purchase, Gain Recognized, Amount"
  ),
  "rs-gaap:CashAndCashEquivalentsAtCarryingValue": (
    "Cash and Cash Equivalents, at Carrying Value"
  ),
  "rs-gaap:CostOfGoodsAndServicesSold": "Cost of Product and Service Sold",
  "rs-gaap:DeferredRevenueCurrent": "Deferred Revenue, Current",
  "rs-gaap:DepreciationDepletionAndAmortization": (
    "Depreciation, Depletion and Amortization"
  ),
  "rs-gaap:DividendIncomeOperating": "Dividend Income, Operating",
  "rs-gaap:DueFromAffiliateCurrent": "Due from Affiliate, Current",
  "rs-gaap:DueToAffiliateCurrent": "Due to Affiliate, Current",
  "rs-gaap:EquityMethodInvestments": "Equity Method Investments",
  "rs-gaap:GainOrLossOnSaleOfStockInSubsidiary": (
    "Gain (Loss) on Disposition of Stock in Subsidiary"
  ),
  "rs-gaap:GeneralAndAdministrativeExpense": "General and Administrative Expense",
  "rs-gaap:Goodwill": "Goodwill",
  "rs-gaap:IncomeLossFromEquityMethodInvestments": (
    "Income (Loss) from Equity Method Investments"
  ),
  "rs-gaap:InterestExpense": "Interest Expense, Operating and Nonoperating",
  "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings": (
    "Inventory, Net of Allowances, Customer Advances and Progress Billings"
  ),
  "rs-gaap:InvestmentIncomeDividend": "Investment Income, Dividend",
  "rs-gaap:InvestmentIncomeInterest": "Investment Income, Interest",
  "rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures": (
    "Investments in and Advance to Affiliates, Subsidiaries, Associates, and "
    "Joint Ventures"
  ),
  "rs-gaap:LongTermDebtAndCapitalLeaseObligations": (
    "Long-Term Debt and Lease Obligation"
  ),
  "rs-gaap:LongTermInvestmentsAndReceivablesNet": (
    "Long-Term Investments and Receivables, Net"
  ),
  "rs-gaap:MembersEquity": "Members' Equity",
  "rs-gaap:MinorityInterest": (
    "Stockholders' Equity Attributable to Noncontrolling Interest"
  ),
  "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest": (
    "Net Income (Loss) Attributable to Noncontrolling Interest"
  ),
  "rs-gaap:OtherNonoperatingIncome": "Other Nonoperating Income",
  "rs-gaap:OtherNonoperatingIncomeExpense": "Other Nonoperating Income (Expense)",
  "rs-gaap:PartnersCapital": "Partners' Capital",
  "rs-gaap:PaymentsToMinorityShareholders": "Payments to Noncontrolling Interests",
  "rs-gaap:PrepaidExpenseCurrent": "Prepaid Expense, Current",
  "rs-gaap:ProceedsFromMinorityShareholders": (
    "Proceeds from Noncontrolling Interests"
  ),
  "rs-gaap:PropertyPlantAndEquipmentGross": "Property, Plant and Equipment, Gross",
  "rs-gaap:ReceivablesNetCurrent": "Receivables, Net, Current",
  "rs-gaap:ResearchAndDevelopmentExpense": "Research and Development Expense",
  "rs-gaap:RetainedEarningsAccumulatedDeficit": (
    "Retained Earnings (Accumulated Deficit)"
  ),
  "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": (
    "Revenue from Contract with Customer, Excluding Assessed Tax"
  ),
  "rs-gaap:SellingAndMarketingExpense": "Selling and Marketing Expense",
}


def _engine_concepts() -> set[str]:
  return {
    reads.INVESTMENT_IN_SUBSIDIARIES,
    reads.EQUITY_METHOD_INVESTMENT,
    reads.EQUITY_METHOD_INCOME,
    reads.NONCONTROLLING_INTEREST,
    reads.NCI_SHARE_OF_INCOME,
    reads.GOODWILL,
    *reads.DISTRIBUTION_INCOME,
    commands.DECONSOLIDATION_GAIN,
    commands.BARGAIN_PURCHASE_GAIN,
    *commands._PARENT_EQUITY.keys(),
    *commands._PARENT_EQUITY.values(),
    *commands._NCI_FLOWS,
  }


def _template_concepts() -> set[str]:
  concepts: set[str] = set()
  for template in CHART_TEMPLATES.values():
    mapping = template.mapping_set("rs-gaap")
    if mapping is None:
      continue
    concepts.update(q for _, q in mapping.arcs)
    for arcs in mapping.variants.values():
      concepts.update(q for _, q in arcs)
  return concepts


@pytest.fixture(scope="module")
def labels() -> dict[str, str | None]:
  package = load_taxonomy_package(
    framework_root("rs-gaap") / "packages" / "rs-gaap" / "v1" / "taxonomy.jsonld"
  )
  return {
    element.qname: next(
      (label.text for label in element.labels if label.role == "standard"), None
    )
    for element in package.elements
  }


@pytest.mark.unit
def test_every_engine_concept_is_pinned() -> None:
  assert sorted(_engine_concepts() - PINNED.keys()) == []


@pytest.mark.unit
def test_every_template_concept_is_pinned() -> None:
  assert sorted(_template_concepts() - PINNED.keys()) == []


@pytest.mark.unit
def test_pinned_labels_match_the_library(labels: dict[str, str | None]) -> None:
  drifted = {q: labels.get(q) for q, label in PINNED.items() if labels.get(q) != label}
  assert drifted == {}, (
    "A posting concept's label changed; read what it means now before "
    f"re-pinning: {drifted}"
  )
