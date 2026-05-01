"""Constants for the CoA → rs-gaap auto-mapping refinement pass.

Two collaborating tables:

- ``RS_GAAP_SUBTOTAL_DENYLIST``: rs-gaap qnames that are statement-level
  rollups. CoA accounts must NEVER map to these — their values are
  computed by the report renderer summing component leaves. A CoA arc
  pointing to ``rs-gaap:Assets`` would land a leaf fact on the rollup
  and double-count when rendered.

- ``FAC_TO_RS_GAAP_FALLBACK``: per-FAC-concept "Other" bucket used as
  the deterministic fallback when the AI refinement returns nothing
  with sufficient confidence (or its parent equivalent is denylisted).
  Picks a non-rollup, broad-but-specific rs-gaap concept that fits the
  FAC concept's classification — preserves auto-map coverage without
  silently writing a subtotal arc.

Both are intentionally hand-curated. The set of statement-level rollups
is small and stable; growing it accidentally would silently break
report rendering. New FAC concepts that appear in mappings need a
fallback entry — without one, accounts under that concept fall back
to ``needs_review`` (low confidence stamp, surfaced in the CoA UI).
"""

from __future__ import annotations

# rs-gaap concepts that are statement-level rollups / subtotals. Their
# values come from rendering (sum of children + calc associations), not
# from CoA-mapped facts. Keep this list canonical — adding non-rollup
# concepts here will silently strand them as un-mappable.
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
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
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


# Per-FAC "Other" bucket. When the rs-gaap refinement AI fails (low
# confidence or wide-equivalence dead end), pick the canonical Other
# concept for that FAC category as a safe non-rollup fallback at
# ``confidence=0.40`` — visible to the user as a low-confidence
# auto-map that they can correct via the CoA UI.
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
  # Equity — no clean "OtherEquity" in rs-gaap, APIC is the broadest
  # paid-in catch-all that isn't a rollup.
  "fac:Equity": "rs-gaap:AdditionalPaidInCapital",
  "fac:EquityAttributableToParent": "rs-gaap:AdditionalPaidInCapital",
  # Revenues
  "fac:Revenues": "rs-gaap:OtherIncome",
  "fac:OtherOperatingIncomeExpenses": "rs-gaap:OtherIncome",
  "fac:NonoperatingIncomeLoss": "rs-gaap:OtherNonoperatingIncomeExpense",
  # Cost of Revenue — note: rs-gaap:CostOfRevenue itself is a rollup;
  # CostOfGoodsAndServicesSold is the catch-all leaf.
  "fac:CostOfRevenue": "rs-gaap:CostOfGoodsAndServicesSold",
  "fac:CostOfRevenueGoods": "rs-gaap:CostOfGoodsAndServicesSold",
  "fac:CostOfRevenueServices": "rs-gaap:CostOfGoodsAndServicesSold",
  # Operating expenses
  "fac:OperatingExpenses": "rs-gaap:OtherCostAndExpenseOperating",
  "fac:ExciseAndSalesTaxes": "rs-gaap:TaxesExcludingIncomeAndExciseTaxes",
}

# Confidence stamp for fallback-driven mappings. Below
# CONFIDENCE_AUTO_APPROVE — the user sees these as low-confidence in
# the CoA UI and can correct them.
FALLBACK_CONFIDENCE: float = 0.40
