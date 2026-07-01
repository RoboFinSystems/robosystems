"""CoA → rs-gaap mappings for Cadence Labs, Inc.

Every target is a verified leaf of the Default Reporting Style's Networks
(Classified BS / Multi-step IS). The SaaS-shaped lines this episode leans on:

- **Deferred Revenue** → ``rs-gaap:DeferredRevenueCurrent`` (the annual-prepay
  liability at the heart of the runway reveal).
- **R&D** → ``rs-gaap:ResearchAndDevelopmentExpense`` and **S&M** →
  ``rs-gaap:SellingAndMarketingExpense`` (the burn, rendered as their own
  Multi-step IS lines below Gross Profit).
- **Cost of Revenue** → ``rs-gaap:CostOfGoodsAndServicesSold`` (hosting +
  support; drives the ~78% gross margin and the GrossProfit subtotal).
"""

# (coa_code, rs_gaap_qname)
MAPPINGS: list[tuple[str, str]] = [
  # Assets
  ("1000", "rs-gaap:CashAndCashEquivalentsAtCarryingValue"),
  ("1100", "rs-gaap:ReceivablesNetCurrent"),
  ("1200", "rs-gaap:PrepaidExpenseCurrent"),
  ("1210", "rs-gaap:PrepaidExpenseCurrent"),
  ("1300", "rs-gaap:PropertyPlantAndEquipmentGross"),
  (
    "1350",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  ),
  # Liabilities
  ("2000", "rs-gaap:AccountsPayableCurrent"),
  ("2100", "rs-gaap:AccruedLiabilitiesCurrent"),
  ("2300", "rs-gaap:DeferredRevenueCurrent"),  # annual-prepay float — the reveal
  # Equity
  ("3000", "rs-gaap:AdditionalPaidInCapital"),
  ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),
  # Revenue — subscription + services, both ASC 606
  ("4000", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  ("4100", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  # Cost of revenue
  ("5000", "rs-gaap:CostOfGoodsAndServicesSold"),
  # Operating expenses — the SaaS burn, by function
  ("6000", "rs-gaap:ResearchAndDevelopmentExpense"),
  ("6100", "rs-gaap:SellingAndMarketingExpense"),
  ("6200", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6300", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6400", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("7000", "rs-gaap:DepreciationDepletionAndAmortization"),
]


_EQUITY_BY_FORM: dict[str, list[tuple[str, str]]] = {
  "corporation": [
    ("3000", "rs-gaap:AdditionalPaidInCapital"),
    ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),
  ],
  "partnership": [
    ("3000", "rs-gaap:PartnersCapital"),
    ("3100", "rs-gaap:PartnersCapital"),
  ],
  "llc": [
    ("3000", "rs-gaap:MembersEquity"),
    ("3100", "rs-gaap:MembersEquity"),
  ],
}


def mappings_for(entity_type: str = "corporation") -> list[tuple[str, str]]:
  """CoA→rs-gaap mappings with equity rows tuned to the entity legal form."""
  form = (entity_type or "corporation").strip().lower()
  equity = _EQUITY_BY_FORM.get(form, _EQUITY_BY_FORM["corporation"])
  base = [m for m in MAPPINGS if m[0] not in ("3000", "3100")]
  return base + equity
