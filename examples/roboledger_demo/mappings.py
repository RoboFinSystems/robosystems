"""CoA to rs-gaap mapping definitions for Cascade Advisory Group LLC.

CoA elements map to rs-gaap detail concepts that are admitted by the
**Default Reporting Style's Networks**. Only concepts that appear in the
rs-gaap Balance Sheet — Classified or Income Statement — Multi-step
Networks render through the Default Style; mapping to a concept outside
those Networks drops the fact as "out of structure".

The Multi-step IS deliberately presents a curated set of leaves (Sales
Revenue / COGS / SG&A / R&D / D&A / Interest / Other Nonoperating /
Tax). For a services firm like Cascade with no goods sold or R&D, the
common rollups are:

- **Revenue** → ``rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax``
- **Operating expenses (everything not D&A or interest)** →
  ``rs-gaap:GeneralAndAdministrativeExpense`` (its sibling
  ``rs-gaap:SellingAndMarketingExpense`` is unused by this back-office firm)
- **Depreciation** → ``rs-gaap:DepreciationDepletionAndAmortization``

The Classified BS admits a richer set of rs-gaap rollups:

- **Cash** → ``rs-gaap:CashAndCashEquivalentsAtCarryingValue`` (its sibling
  ``rs-gaap:ShortTermInvestments`` is unused here)
- **AR** → ``rs-gaap:ReceivablesNetCurrent``
- **Prepaids** → ``rs-gaap:PrepaidExpenseCurrent``
- **PP&E gross** → ``rs-gaap:PropertyPlantAndEquipmentGross``
  (separate from the contra-asset so the CF Investing derivation reads
  ΔGross = purchases, not ΔNet which would conflate purchases with
  depreciation activity)
- **Accumulated Depreciation** → ``rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment``
  (BS Net = Gross - AD synthesized at fact-generation time)
- **AP** → ``rs-gaap:AccountsPayableCurrent``; **Accrued / Payroll Taxes** →
  ``rs-gaap:AccruedLiabilitiesCurrent``
- **APIC** → ``rs-gaap:AdditionalPaidInCapital``
- **Retained Earnings** → ``rs-gaap:RetainedEarningsAccumulatedDeficit``

The Default Style renders at standard SMB granularity: Cash and Short-Term
Investments, Goodwill and Intangibles, Accounts Payable and Accrued
Liabilities, and G&A and Selling & Marketing each get their own line.
Finer concepts (PrepaidInsurance, SalariesAndWages, and the like) exist in
the broader rs-gaap library but sit outside the Default Style's Networks —
they render only under a richer Reporting Style.

The demo resolves rs-gaap qnames → element IDs at runtime against the
library in the entity graph.
"""

# (coa_code, rs_gaap_qname)
MAPPINGS: list[tuple[str, str]] = [
  # Assets
  ("1000", "rs-gaap:CashAndCashEquivalentsAtCarryingValue"),  # Cash
  ("1100", "rs-gaap:ReceivablesNetCurrent"),  # Accounts Receivable
  ("1200", "rs-gaap:PrepaidExpenseCurrent"),  # Prepaid Insurance
  ("1210", "rs-gaap:PrepaidExpenseCurrent"),  # Prepaid Software
  ("1220", "rs-gaap:PrepaidExpenseCurrent"),  # Prepaid Cloud Hosting
  ("1300", "rs-gaap:PropertyPlantAndEquipmentGross"),  # Computer Equipment
  ("1310", "rs-gaap:PropertyPlantAndEquipmentGross"),  # Office Furniture
  (
    "1350",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  ),  # Accumulated Depreciation (contra-asset)
  # Liabilities
  ("2000", "rs-gaap:AccountsPayableCurrent"),  # Accounts Payable
  ("2100", "rs-gaap:AccruedLiabilitiesCurrent"),  # Accrued Liabilities
  ("2200", "rs-gaap:AccruedLiabilitiesCurrent"),  # Payroll Taxes Payable
  # Equity
  ("3000", "rs-gaap:AdditionalPaidInCapital"),  # Owner's Equity
  ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),  # Retained Earnings
  # Revenue — services firm, no goods sold; everything is revenue from
  # contracts with customers (ASC 606)
  (
    "4000",
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
  ),  # Consulting Revenue
  (
    "4100",
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
  ),  # Strategy Advisory Revenue
  (
    "4200",
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
  ),  # Implementation Services Revenue
  # Operating expenses — this back-office services firm's costs are all
  # General & Administrative (Selling & Marketing is a sibling line, unused
  # here). Depreciation gets its own line.
  ("5000", "rs-gaap:GeneralAndAdministrativeExpense"),  # Salaries & Wages
  ("5100", "rs-gaap:GeneralAndAdministrativeExpense"),  # Payroll Taxes
  ("5200", "rs-gaap:GeneralAndAdministrativeExpense"),  # Health Insurance
  ("6000", "rs-gaap:GeneralAndAdministrativeExpense"),  # Office Rent
  ("6100", "rs-gaap:GeneralAndAdministrativeExpense"),  # Software Subscriptions
  ("6200", "rs-gaap:GeneralAndAdministrativeExpense"),  # Cloud Hosting
  ("6300", "rs-gaap:GeneralAndAdministrativeExpense"),  # Professional Development
  ("6400", "rs-gaap:GeneralAndAdministrativeExpense"),  # Business Insurance
  ("6500", "rs-gaap:GeneralAndAdministrativeExpense"),  # Office Supplies
  ("6600", "rs-gaap:GeneralAndAdministrativeExpense"),  # Travel & Entertainment
  ("7000", "rs-gaap:DepreciationDepletionAndAmortization"),  # Depreciation Expense
]


# Equity CoA (3000 Owner's Equity, 3100 Retained Earnings) maps to the
# rs-gaap capital concept appropriate to the entity's legal form, so the
# equity-form Reporting Style (BSC-{CORP,PART,LLC}-…) renders its native
# capital line. Non-equity mappings are identical across forms.
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
