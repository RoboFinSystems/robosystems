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


# rs-gaap leaf concepts that the report renderer synthesizes a parent
# from rather than presenting directly, so they're absent from the BS
# presentation network — yet they ARE the correct CoA mapping grain.
# ``_synthesize_ppe_net_facts`` computes PropertyPlantAndEquipmentNet as
# Gross minus AccumulatedDepreciation and marks the two details
# ``audit_only`` (no extra BS rows), but mapping fixed-asset accounts to
# Gross + the accumulated-depreciation contra is what lets CF Investing
# read the change in Gross as capex (the change in Net conflates
# purchases with depreciation). The candidate
# suggester admits these regardless of presentation-set membership.
RS_GAAP_SYNTHESIZED_DETAIL_ALLOW: frozenset[str] = frozenset(
  {
    "rs-gaap:PropertyPlantAndEquipmentGross",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
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
  # Revenues — OtherSalesRevenueNet is the operating-revenue catch-all
  # leaf wired under rs-gaap:Revenues (NOT OtherIncome, which is
  # non-operating and isn't a child of the Revenues rollup → would drop).
  "fac:Revenues": "rs-gaap:OtherSalesRevenueNet",
  "fac:OtherOperatingIncomeExpenses": "rs-gaap:OtherCostAndExpenseOperating",
  "fac:NonoperatingIncomeLoss": "rs-gaap:OtherNonoperatingIncomeExpense",
  # Cost of Revenue — rs-gaap:CostOfRevenue itself is a rollup;
  # OtherCostOfOperatingRevenue is the catch-all leaf wired under it.
  "fac:CostOfRevenue": "rs-gaap:OtherCostOfOperatingRevenue",
  "fac:CostOfRevenueGoods": "rs-gaap:OtherCostOfOperatingRevenue",
  "fac:CostOfRevenueServices": "rs-gaap:OtherCostOfOperatingRevenue",
  # Operating expenses — OtherCostAndExpenseOperating is the catch-all
  # leaf wired under rs-gaap:OperatingExpenses.
  "fac:OperatingExpenses": "rs-gaap:OtherCostAndExpenseOperating",
  "fac:ExciseAndSalesTaxes": "rs-gaap:OtherCostAndExpenseOperating",
}

# Confidence stamp for fallback-driven mappings. Below
# CONFIDENCE_AUTO_APPROVE — the user sees these as low-confidence in
# the CoA UI and can correct them.
FALLBACK_CONFIDENCE: float = 0.40


# Deterministic CoA-name → rs-gaap overrides for synthesized-detail
# concepts (see RS_GAAP_SYNTHESIZED_DETAIL_ALLOW). The AI refinement
# tends to collapse these into the net parent (e.g. an "Accumulated
# Depreciation" account → PropertyPlantAndEquipmentNet via the
# FixedAssets parent fallback) because the contra is absorbed into the
# synthesized net at render time and isn't the highest-confidence
# semantic match. The account NAME is the unambiguous signal, so we
# route it deterministically before the AI pass. Each entry is a
# (compiled-at-use regex pattern, rs-gaap qname) pair; the pattern is
# matched case-insensitively against the CoA element's name + code.
# Intentionally narrow — only accounts whose meaning is unambiguous
# from the name belong here (accumulated depreciation, NOT amortization,
# which maps to the single IntangibleAssetsNetIncludingGoodwill concept).
RS_GAAP_NAME_PATTERN_OVERRIDES: tuple[tuple[str, str], ...] = (
  (
    r"accumulated\s+deprec",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  ),
)
