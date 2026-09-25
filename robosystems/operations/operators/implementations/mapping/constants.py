"""Hand-curated rs-gaap concept sets for CoA → rs-gaap mapping."""

from __future__ import annotations

# Statement-level rollups: rendered from their children, so a CoA arc to one
# would double-count. Adding a non-rollup here strands it as un-mappable.
RS_GAAP_SUBTOTAL_DENYLIST: frozenset[str] = frozenset(
  {
    # Balance Sheet — Assets
    "rs-gaap:Assets",
    "rs-gaap:AssetsCurrent",
    "rs-gaap:AssetsNoncurrent",
    # Balance Sheet — Liabilities
    "rs-gaap:Liabilities",
    "rs-gaap:LiabilitiesCurrent",
    "rs-gaap:LiabilitiesNoncurrent",
    "rs-gaap:LiabilitiesAndStockholdersEquity",
    # Balance Sheet — Equity
    "rs-gaap:StockholdersEquity",
    "rs-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    # Income Statement — Top-line revenue rollups
    "rs-gaap:Revenues",
    "rs-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
    # Income Statement — Cost / margin rollups
    "rs-gaap:CostOfRevenue",
    "rs-gaap:CostsAndExpenses",
    "rs-gaap:GrossProfit",
    # Income Statement — Operating rollups
    "rs-gaap:OperatingExpenses",
    "rs-gaap:OperatingIncomeLoss",
    # Income Statement — Bottom-line rollups
    "rs-gaap:NonoperatingIncomeExpense",
    "rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "rs-gaap:NetIncomeLoss",
    "rs-gaap:NetIncomeLossAvailableToCommonStockholdersBasic",
  }
)


# Leaves the renderer folds into a synthesized parent (PP&E Net), so they are
# absent from the BS presentation network, yet they are the right mapping
# grain: CF Investing reads capex from the change in Gross. The candidate
# suggester admits them regardless of presentation membership.
RS_GAAP_SYNTHESIZED_DETAIL_ALLOW: frozenset[str] = frozenset(
  {
    "rs-gaap:PropertyPlantAndEquipmentGross",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  }
)


# Per-FAC catch-all "Other" leaf (non-rollup) for each FAC category.
FAC_TO_RS_GAAP_FALLBACK: dict[str, str] = {
  # Assets
  "fac:Assets": "rs-gaap:OtherAssetsNoncurrent",
  "fac:CurrentAssets": "rs-gaap:OtherAssetsCurrent",
  "fac:NoncurrentAssets": "rs-gaap:OtherAssetsNoncurrent",
  "fac:FixedAssets": "rs-gaap:OtherAssetsNoncurrent",
  # Liabilities
  "fac:Liabilities": "rs-gaap:OtherLiabilitiesNoncurrent",
  "fac:CurrentLiabilities": "rs-gaap:OtherLiabilitiesCurrent",
  "fac:NoncurrentLiabilities": "rs-gaap:OtherLiabilitiesNoncurrent",
  "fac:LongTermDebt": "rs-gaap:OtherLiabilitiesNoncurrent",
  # Equity — rs-gaap has no "OtherEquity"; APIC is the broadest non-rollup.
  "fac:Equity": "rs-gaap:AdditionalPaidInCapital",
  "fac:EquityAttributableToParent": "rs-gaap:AdditionalPaidInCapital",
  # Revenues — the sole operating-revenue leaf under rs-gaap:Revenues
  # (OtherIncome is non-operating and would drop out of the rollup).
  "fac:Revenues": "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
  "fac:OtherOperatingIncomeExpenses": "rs-gaap:OtherCostAndExpenseOperating",
  "fac:NonoperatingIncomeLoss": "rs-gaap:OtherNonoperatingIncomeExpense",
  # Cost of Revenue
  "fac:CostOfRevenue": "rs-gaap:OtherCostOfOperatingRevenue",
  "fac:CostOfRevenueGoods": "rs-gaap:OtherCostOfOperatingRevenue",
  "fac:CostOfRevenueServices": "rs-gaap:OtherCostOfOperatingRevenue",
  # Operating expenses
  "fac:OperatingExpenses": "rs-gaap:OtherCostAndExpenseOperating",
  "fac:ExciseAndSalesTaxes": "rs-gaap:OtherCostAndExpenseOperating",
}


# (regex, qname) overrides for synthesized-detail accounts the model collapses
# into the net parent; matched case-insensitively on name + code. Only names
# that are unambiguous belong here (accumulated amortization is not: it maps to
# IntangibleAssetsNetIncludingGoodwill).
RS_GAAP_NAME_PATTERN_OVERRIDES: tuple[tuple[str, str], ...] = (
  (
    r"accumulated\s+deprec",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  ),
)
