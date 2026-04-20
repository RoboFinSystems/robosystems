"""Generate fac-presentation/v1/taxonomy.jsonld from FAC's statement scaffolds.

FAC's structural abstracts (*Abstract, *Table, *LineItems, *RollUp)
form the presentation linkbase — the scaffold that organizes how a
statement is rendered. This seed makes those relationships explicit
as `parent-child` arcs, exactly mirroring how fac-calculations
captured the identity rules as `summationOf` arcs.

Structure:

  BalanceSheetAbstract
  ├── BalanceSheetClassifiedTable
  │   └── BalanceSheetLineItems
  │       ├── AssetsRollUp  (groups Assets with its calc children)
  │       ├── LiabilitiesRollUp
  │       ├── EquityRollUp
  │       └── LiabilitiesEquityRollUp
  └── BalanceSheetUnclassifiedTable
      └── BalanceSheetLineItems

Each RollUp abstract then has parent-child arcs to its total and
constituents — the presentation-side companion to the calc-side
`summationOf` arcs.

One structure per statement / hierarchy; arcs carry the presentation
topology.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PresentationStructure:
  code: str
  name: str
  structure_type: str
  # parent → [children] — ordered
  edges: tuple[tuple[str, tuple[str, ...]], ...]


# ────────────────────────────────────────────────────────────────────────
# Balance sheet — classified & unclassified variants share LineItems
# ────────────────────────────────────────────────────────────────────────

_BS_CLASSIFIED = PresentationStructure(
  code="BS-classified",
  name="Balance Sheet — Classified",
  structure_type="balance_sheet",
  edges=(
    ("fac:BalanceSheetAbstract", ("fac:BalanceSheetClassifiedTable",)),
    ("fac:BalanceSheetClassifiedTable", ("fac:BalanceSheetLineItems",)),
    (
      "fac:BalanceSheetLineItems",
      ("fac:AssetsRollUp", "fac:LiabilitiesEquityRollUp"),
    ),
    (
      "fac:AssetsRollUp",
      ("fac:Assets", "fac:CurrentAssets", "fac:NoncurrentAssets"),
    ),
    (
      "fac:NoncurrentAssets",
      (
        "fac:FixedAssets",
        "fac:OtherThanFixedNoncurrentAssets",
        "fac:PublicUtilitiesPropertyPlantAndEquipmentNet",
        "fac:OtherNoncurrentAssetsOfRegulatedEntity",
      ),
    ),
    (
      "fac:LiabilitiesEquityRollUp",
      (
        "fac:LiabilitiesAndEquity",
        "fac:LiabilitiesRollUp",
        "fac:CommitmentsAndContingencies",
        "fac:TemporaryEquity",
        "fac:RedeemableNoncontrollingInterest",
        "fac:EquityRollUp",
      ),
    ),
    (
      "fac:LiabilitiesRollUp",
      ("fac:Liabilities", "fac:CurrentLiabilities", "fac:NoncurrentLiabilities"),
    ),
    (
      "fac:NoncurrentLiabilities",
      ("fac:LongTermDebt", "fac:OtherNoncurrentLiabilitiesOfRegulatedEntity"),
    ),
    (
      "fac:TemporaryEquity",
      ("fac:TemporaryEquityAttributableToParent",),
    ),
    (
      "fac:RedeemableNoncontrollingInterest",
      (
        "fac:RedeemableNoncontrollingInterestCommon",
        "fac:RedeemableNoncontrollingInterestPreferred",
        "fac:RedeemableNoncontrollingInterestOther",
      ),
    ),
    (
      "fac:EquityRollUp",
      (
        "fac:Equity",
        "fac:EquityAttributableToParent",
        "fac:EquityAttributableToNoncontrollingInterest",
        "fac:Capitalization",
      ),
    ),
  ),
)

_BS_UNCLASSIFIED = PresentationStructure(
  code="BS-unclassified",
  name="Balance Sheet — Unclassified",
  structure_type="balance_sheet",
  edges=(
    ("fac:BalanceSheetAbstract", ("fac:BalanceSheetUnclassifiedTable",)),
    ("fac:BalanceSheetUnclassifiedTable", ("fac:BalanceSheetLineItems",)),
  ),
)

# ────────────────────────────────────────────────────────────────────────
# Income statement — multi-step + single-step + industry variants
# ────────────────────────────────────────────────────────────────────────

_IS_MULTISTEP = PresentationStructure(
  code="IS-multistep",
  name="Income Statement — Multi-step",
  structure_type="income_statement",
  edges=(
    ("fac:IncomeStatementAbstract", ("fac:IncomeStatementMultiStepTable",)),
    ("fac:IncomeStatementMultiStepTable", ("fac:IncomeStatementLineItems",)),
    (
      "fac:IncomeStatementLineItems",
      (
        "fac:RevenuesRollUp",
        "fac:CostRevenueRollUp",
        "fac:GrossProfitRollUp",
        "fac:OperatingIncomeLossRollUp",
        "fac:IncomeLossFromContinuingOperationsBeforeTaxRollUp",
        "fac:IncomeLossFromContinuingOperationsAfterTaxRollUp",
        "fac:NetIncomeLossRollUp",
      ),
    ),
    ("fac:RevenuesRollUp", ("fac:Revenues",)),
    (
      "fac:CostRevenueRollUp",
      ("fac:CostOfRevenue", "fac:CostOfRevenueGoods", "fac:CostOfRevenueServices"),
    ),
    ("fac:GrossProfitRollUp", ("fac:GrossProfit", "fac:Revenues", "fac:CostOfRevenue")),
    (
      "fac:OperatingIncomeLossRollUp",
      (
        "fac:OperatingIncomeLoss",
        "fac:GrossProfit",
        "fac:OperatingExpenses",
        "fac:OtherOperatingIncomeExpenses",
      ),
    ),
    (
      "fac:IncomeLossFromContinuingOperationsBeforeTaxRollUp",
      (
        "fac:IncomeLossFromContinuingOperationsBeforeTax",
        "fac:IncomeLossBeforeEquityMethodInvestments",
        "fac:IncomeLossFromEquityMethodInvestments",
        "fac:NonoperatingIncomeLoss",
        "fac:InterestAndDebtExpense",
      ),
    ),
    (
      "fac:IncomeLossFromContinuingOperationsAfterTaxRollUp",
      (
        "fac:IncomeLossFromContinuingOperationsAfterTax",
        "fac:IncomeLossFromContinuingOperationsBeforeTax",
        "fac:IncomeTaxExpenseBenefit",
      ),
    ),
    (
      "fac:IncomeTaxExpenseBenefit",
      ("fac:IncomeTaxExpenseBenefitCurrent", "fac:IncomeTaxExpenseBenefitDeferred"),
    ),
    (
      "fac:NetIncomeLossRollUp",
      (
        "fac:NetIncomeLoss",
        "fac:IncomeLossFromContinuingOperationsAfterTax",
        "fac:IncomeLossFromDiscontinuedOperationsNetOfTax",
      ),
    ),
    (
      "fac:IncomeLossFromDiscontinuedOperationsNetOfTax",
      (
        "fac:IncomeLossFromDiscontinuedOperationsNetOfTaxAdjustmentToPriorYearGainLossOnDisposal",
        "fac:IncomeLossFromDiscontinuedOperationsNetOfTaxDuringPhaseOut",
        "fac:IncomeLossFromDiscontinuedOperationsNetOfTaxGainLossOnDisposal",
        "fac:IncomeLossFromDiscontinuedOperationsNetOfTaxProvisionForGainLossOnDisposal",
      ),
    ),
  ),
)

_IS_SINGLESTEP = PresentationStructure(
  code="IS-singlestep",
  name="Income Statement — Single-step",
  structure_type="income_statement",
  edges=(
    ("fac:IncomeStatementAbstract", ("fac:IncomeStatementSingleStepTable",)),
    ("fac:IncomeStatementSingleStepTable", ("fac:IncomeStatementLineItems",)),
    (
      "fac:IncomeStatementLineItems",
      (
        "fac:Revenues",
        "fac:CostsExpensesRollUp",
        "fac:OtherOperatingIncomeExpenses",
        "fac:OperatingIncomeLoss",
      ),
    ),
    (
      "fac:CostsExpensesRollUp",
      (
        "fac:CostsAndExpenses",
        "fac:CostOfRevenue",
        "fac:OperatingExpenses",
        "fac:BenefitsCostsExpenses",
      ),
    ),
  ),
)

_IS_INSURANCE = PresentationStructure(
  code="IS-insurance",
  name="Income Statement — Insurance-based",
  structure_type="income_statement",
  edges=(
    (
      "fac:IncomeStatementAbstract",
      ("fac:IncomeStatementInsuranceBasedRevenuesTable",),
    ),
    (
      "fac:IncomeStatementInsuranceBasedRevenuesTable",
      ("fac:IncomeStatementLineItems",),
    ),
  ),
)

_IS_INTEREST = PresentationStructure(
  code="IS-interest",
  name="Income Statement — Interest-based (Banking)",
  structure_type="income_statement",
  edges=(
    ("fac:IncomeStatementAbstract", ("fac:IncomeStatementInterestBasedRevenuesTable",)),
    (
      "fac:IncomeStatementInterestBasedRevenuesTable",
      ("fac:IncomeStatementLineItems",),
    ),
    (
      "fac:InterestIncomeExpenseNetRollUp",
      (
        "fac:InterestIncomeExpenseOperatingNet",
        "fac:InterestAndDividendIncomeOperating",
        "fac:InterestExpenseOperating",
      ),
    ),
    (
      "fac:InterestIncomeExpenseAfterProvisionForLossesRollUp",
      (
        "fac:InterestIncomeExpenseAfterProvisionForLosses",
        "fac:InterestIncomeExpenseOperatingNet",
        "fac:ProvisionForLoanLeaseAndOtherLosses",
      ),
    ),
    (
      "fac:RevenuesNetInterestExpenseRollUp",
      (
        "fac:RevenuesNetInterestExpense",
        "fac:NoninterestIncome",
        "fac:InterestIncomeExpenseAfterProvisionForLosses",
      ),
    ),
  ),
)

_IS_SECURITIES = PresentationStructure(
  code="IS-securities",
  name="Income Statement — Securities-based",
  structure_type="income_statement",
  edges=(
    ("fac:IncomeStatementAbstract", ("fac:IncomeStatementSecuritiesBasedIncomeTable",)),
    (
      "fac:IncomeStatementSecuritiesBasedIncomeTable",
      ("fac:IncomeStatementLineItems",),
    ),
  ),
)

# ────────────────────────────────────────────────────────────────────────
# Statement of Comprehensive Income
# ────────────────────────────────────────────────────────────────────────

_COMPREHENSIVE = PresentationStructure(
  code="ComprehensiveIncome",
  name="Statement of Comprehensive Income",
  structure_type="equity_statement",
  edges=(
    (
      "fac:StatementComprehensiveIncomeAbstract",
      ("fac:StatementComprehensiveIncomeLossTable",),
    ),
    (
      "fac:StatementComprehensiveIncomeLossTable",
      ("fac:StatementComprehensiveIncomeLossLineItems",),
    ),
    (
      "fac:StatementComprehensiveIncomeLossLineItems",
      ("fac:ComprehensiveIncomeLossRollUp",),
    ),
    (
      "fac:ComprehensiveIncomeLossRollUp",
      (
        "fac:ComprehensiveIncomeLoss",
        "fac:NetIncomeLoss",
        "fac:OtherComprehensiveIncomeLoss",
      ),
    ),
    (
      "fac:OtherComprehensiveIncomeLoss",
      (
        "fac:OtherComprehensiveIncomeLossAttributableToParent",
        "fac:OtherComprehensiveIncomeLossAttributableToNoncontrollingInterest",
      ),
    ),
    (
      "fac:ComprehensiveIncomeLoss",
      (
        "fac:ComprehensiveIncomeLossAttributableToParent",
        "fac:ComprehensiveIncomeLossAttributableToNoncontrollingInterest",
      ),
    ),
  ),
)

# ────────────────────────────────────────────────────────────────────────
# Cash flow statement
# ────────────────────────────────────────────────────────────────────────

_CASH_FLOW = PresentationStructure(
  code="CashFlow",
  name="Cash Flow Statement",
  structure_type="cash_flow_statement",
  edges=(
    ("fac:CashFlowStatementAbstract", ("fac:CashFlowStatementTable",)),
    ("fac:CashFlowStatementTable", ("fac:CashFlowStatementLineItems",)),
    (
      "fac:CashFlowStatementLineItems",
      ("fac:NetCashFlowRollUp",),
    ),
    (
      "fac:NetCashFlowRollUp",
      (
        "fac:NetCashFlow",
        "fac:NetCashFlowFromOperatingActivitiesRollUp",
        "fac:NetCashFlowFromInvestingActivitiesRollUp",
        "fac:NetCashFlowFromFinancingActivitiesRollUp",
        "fac:ExchangeGainsLosses",
      ),
    ),
    (
      "fac:NetCashFlowFromOperatingActivitiesRollUp",
      (
        "fac:NetCashFlowFromOperatingActivities",
        "fac:NetCashFlowFromOperatingActivitiesContinuing",
        "fac:NetCashFlowFromOperatingActivitiesDiscontinued",
      ),
    ),
    (
      "fac:NetCashFlowFromInvestingActivitiesRollUp",
      (
        "fac:NetCashFlowFromInvestingActivities",
        "fac:NetCashFlowFromInvestingActivitiesContinuing",
        "fac:NetCashFlowFromInvestingActivitiesDiscontinued",
      ),
    ),
    (
      "fac:NetCashFlowFromFinancingActivitiesRollUp",
      (
        "fac:NetCashFlowFromFinancingActivities",
        "fac:NetCashFlowFromFinancingActivitiesContinuing",
        "fac:NetCashFlowFromFinancingActivitiesDiscontinued",
      ),
    ),
    (
      "fac:NetCashFlowContinuingRollUp",
      (
        "fac:NetCashFlowContinuing",
        "fac:NetCashFlowFromOperatingActivitiesContinuing",
        "fac:NetCashFlowFromInvestingActivitiesContinuing",
        "fac:NetCashFlowFromFinancingActivitiesContinuing",
      ),
    ),
    (
      "fac:NetCashFlowDiscontinuedRollUp",
      (
        "fac:NetCashFlowDiscontinued",
        "fac:NetCashFlowFromOperatingActivitiesDiscontinued",
        "fac:NetCashFlowFromInvestingActivitiesDiscontinued",
        "fac:NetCashFlowFromFinancingActivitiesDiscontinued",
      ),
    ),
  ),
)

# ────────────────────────────────────────────────────────────────────────
# Earnings-per-share breakdown + NIAC
# ────────────────────────────────────────────────────────────────────────

_NIAC_BREAKDOWN = PresentationStructure(
  code="NIAC-breakdown",
  name="Net Income Available to Common Stockholders — Breakdown",
  structure_type="income_statement",
  edges=(
    (
      "fac:NetIncomeLossAvailableToCommonBreakdownTable",
      ("fac:NetIncomeLossAvailableToCommonBreakdownLineItems",),
    ),
    (
      "fac:NetIncomeLossAvailableToCommonBreakdownLineItems",
      (
        "fac:NetIncomeLossAvailableToCommonStockholdersBasicRollUp",
        "fac:ParticipatingSecuritiesDistributedAndUndistributedEarningsLossBasic",
        "fac:UndistributedEarningsLossAllocatedToParticipatingSecuritiesBasic",
      ),
    ),
    (
      "fac:NetIncomeLossAvailableToCommonStockholdersBasicRollUp",
      (
        "fac:NetIncomeLossAvailableToCommonStockholdersBasic",
        "fac:NetIncomeLossAttributableToParent",
        "fac:PreferredStockDividendsAndOtherAdjustments",
      ),
    ),
  ),
)

_NETINCOME_BREAKDOWN = PresentationStructure(
  code="NetIncome-breakdown",
  name="Net Income — Parent/Noncontrolling Breakdown",
  structure_type="income_statement",
  edges=(
    (
      "fac:NetIncomeLossBreakdownTable",
      ("fac:NetIncomeLossBreakdownLineItems",),
    ),
    (
      "fac:NetIncomeLossBreakdownLineItems",
      (
        "fac:NetIncomeLossAttributableToParent",
        "fac:NetIncomeLossAttributableToNoncontrollingInterest",
        "fac:NetIncomeLossAttributableToNonredeemableNoncontrollingInterest",
        "fac:NetIncomeLossAttributableToRedeemableNoncontrollingInterest",
        "fac:NetIncomeLossAttributableToNoncontrollingInterestPlusPreferredStockDividendsAndOtherAdjustments",
      ),
    ),
  ),
)

_COMPREHENSIVE_BREAKDOWN = PresentationStructure(
  code="ComprehensiveIncome-breakdown",
  name="Comprehensive Income — Parent/Noncontrolling Breakdown",
  structure_type="equity_statement",
  edges=(
    (
      "fac:ComprehensiveIncomeLossBreakdownTable",
      ("fac:ComprehensiveIncomeLossBreakdownLineItems",),
    ),
    (
      "fac:ComprehensiveIncomeLossBreakdownLineItems",
      (
        "fac:ComprehensiveIncomeLossAttributableToParent",
        "fac:ComprehensiveIncomeLossAttributableToNoncontrollingInterest",
      ),
    ),
  ),
)

_CASHFLOW_BREAKDOWN = PresentationStructure(
  code="CashFlow-breakdown",
  name="Cash Flow — Continuing/Discontinued Breakdown",
  structure_type="cash_flow_statement",
  edges=(
    (
      "fac:NetCashFlowBreakdownTable",
      ("fac:NetCashFlowBreakdownLineItems",),
    ),
    (
      "fac:NetCashFlowBreakdownLineItems",
      (
        "fac:NetCashFlowContinuingRollUp",
        "fac:NetCashFlowDiscontinuedRollUp",
        "fac:ExchangeGainsLosses",
      ),
    ),
  ),
)

# ────────────────────────────────────────────────────────────────────────
# Supplementary hierarchies
# ────────────────────────────────────────────────────────────────────────

_GENERAL_INFO = PresentationStructure(
  code="GeneralInformation",
  name="General Information Hierarchy",
  structure_type="custom",
  edges=(
    ("fac:GeneralInformationHierarchy", ("fac:GeneralInformationTable",)),
    ("fac:GeneralInformationTable", ("fac:GeneralInformationLineItems",)),
    (
      "fac:GeneralInformationLineItems",
      (
        "fac:EntityRegistrantName",
        "fac:EntityCentralIndexKey",
        "fac:EntityFilerCategory",
        "fac:TradingSymbol",
        "fac:DocumentType",
        "fac:FiscalYearEnd",
        "fac:FiscalPeriodFocus",
        "fac:FiscalYearFocus",
        "fac:BalanceSheetDate",
        "fac:BalanceSheetFormat",
        "fac:IncomeStatementFormat",
        "fac:IncomeStatementStartPeriodYearToDate",
      ),
    ),
  ),
)

_BASIC_INFO = PresentationStructure(
  code="BasicInformation",
  name="Basic Information Hierarchy",
  structure_type="custom",
  edges=(("fac:BasicInformationTable", ("fac:BasicInformationLineItems",)),),
)

_KEY_RATIOS = PresentationStructure(
  code="KeyRatios",
  name="Key Ratios Hierarchy",
  structure_type="custom",
  edges=(
    ("fac:KeyRatiosHierarchy", ("fac:KeyRatiosTable",)),
    ("fac:KeyRatiosTable", ("fac:KeyRatiosLineItems",)),
    (
      "fac:KeyRatiosLineItems",
      (
        "fac:ReturnOnAssets",
        "fac:ReturnOnEquity",
        "fac:ReturnOnSales",
        "fac:SustainableGrowthRate",
      ),
    ),
  ),
)

_OTHER_INFO = PresentationStructure(
  code="OtherInformation",
  name="Other Information Hierarchy",
  structure_type="custom",
  edges=(
    ("fac:OtherInformationHierarchy", ("fac:OtherInformationTable",)),
    ("fac:OtherInformationTable", ("fac:OtherInformationLineItems",)),
  ),
)

_VALIDATION = PresentationStructure(
  code="ValidationResults",
  name="Validation Results Hierarchy",
  structure_type="custom",
  edges=(
    ("fac:ValidationResultsHierarchy", ("fac:ValidationResultsTable",)),
    ("fac:ValidationResultsTable", ("fac:ValidationResultsLineItems",)),
  ),
)


STRUCTURES: tuple[PresentationStructure, ...] = (
  _BS_CLASSIFIED,
  _BS_UNCLASSIFIED,
  _IS_MULTISTEP,
  _IS_SINGLESTEP,
  _IS_INSURANCE,
  _IS_INTEREST,
  _IS_SECURITIES,
  _COMPREHENSIVE,
  _CASH_FLOW,
  _NIAC_BREAKDOWN,
  _NETINCOME_BREAKDOWN,
  _COMPREHENSIVE_BREAKDOWN,
  _CASHFLOW_BREAKDOWN,
  _GENERAL_INFO,
  _BASIC_INFO,
  _KEY_RATIOS,
  _OTHER_INFO,
  _VALIDATION,
)


SEED_PATH = (
  Path(__file__).resolve().parent.parent
  / "taxonomy/seeds/fac-presentation/v1/taxonomy.jsonld"
)

RS_VOCAB = "https://robosystems.ai/vocab/"
CM_ROLES = "http://www.xbrlsite.com/seattlemethod/conceptual-model/cm-roles/roles/"


def _context() -> dict:
  return {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "fac": "http://www.xbrlsite.com/fac#",
    "rs": RS_VOCAB,
    "parent": {"@id": f"{RS_VOCAB}parent", "@type": "@id"},
    "structureName": {"@id": f"{RS_VOCAB}structureName"},
    "structureType": {"@id": f"{RS_VOCAB}structureType"},
    "roleUri": {"@id": f"{RS_VOCAB}roleUri"},
  }


def build() -> dict:
  structure_nodes: list[dict] = []
  for s in STRUCTURES:
    structure_nodes.append(
      {
        "@id": f"_:fac-pres-{s.code.lower()}",
        "roleUri": f"{CM_ROLES}fac-presentation/{s.code}",
        "structureName": f"FAC — {s.name}",
        "structureType": s.structure_type,
      }
    )

  # Merge parent→children across structures. We emit `parent` on each
  # child pointing UP to its parent rather than a downward children list
  # on the parent; rdflib's JSON-LD parsing treats both shapes equivalently
  # but the inverted form keeps each node self-contained.
  child_to_parents: dict[str, list[str]] = {}
  for s in STRUCTURES:
    for parent, children in s.edges:
      for child in children:
        parents = child_to_parents.setdefault(child, [])
        if parent not in parents:
          parents.append(parent)

  arc_nodes: list[dict] = [
    {"@id": child, "parent": [{"@id": parent} for parent in parents]}
    for child, parents in sorted(child_to_parents.items())
  ]

  doc = {
    "@context": _context(),
    "standard": "fac-presentation",
    "version": "v1",
    "namespace_uri": "http://www.xbrlsite.com/fac/presentation/",
    "taxonomy_type": "mapping",
    "description": (
      "FAC's presentation scaffold — the Abstract → Table → LineItems → "
      "RollUp → primitives hierarchy that organizes how each statement "
      "renders. Parent-child arcs for the 4 primary statements (BS, IS, "
      "CF, Comprehensive Income), their industry variants (Insurance, "
      "Interest, Securities), the dimensional breakdowns, and the "
      "supplementary information hierarchies."
    ),
    "@graph": structure_nodes + arc_nodes,
  }
  return doc


def main() -> int:
  doc = build()
  SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
  SEED_PATH.write_text(json.dumps(doc, indent=2))
  n_arcs = sum(len(n["parent"]) for n in doc["@graph"] if "parent" in n)
  n_structs = sum(1 for n in doc["@graph"] if "roleUri" in n)
  print(f"Wrote {SEED_PATH}")
  print(f"  structures: {n_structs}  arcs: {n_arcs}")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
