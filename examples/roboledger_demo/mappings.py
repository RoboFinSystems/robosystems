"""CoA to rs-gaap mapping definitions for Cascade Advisory Group LLC.

CoA elements map to rs-gaap detail concepts that are admitted by the
**Default Reporting Style's Networks** (§3.2). Only concepts that appear
in the rs-gaap Balance Sheet — Classified or Income Statement —
Multi-step Networks render through the Default Style; mapping to a
concept outside those Networks drops the fact as "out of structure".

The Multi-step IS deliberately presents a curated set of leaves (Sales
Revenue / COGS / SG&A / R&D / D&A / Interest / Other Nonoperating /
Tax). For a services firm like Cascade with no goods sold or R&D, the
common rollups are:

- **Revenue** → ``rs-gaap:SalesRevenueNet``
- **Operating expenses (everything not D&A or interest)** →
  ``rs-gaap:SellingGeneralAndAdministrativeExpense``
- **Depreciation** → ``rs-gaap:DepreciationDepletionAndAmortization``

The Classified BS admits a richer set of rs-gaap rollups:

- **Cash** → ``rs-gaap:CashCashEquivalentsAndShortTermInvestments``
- **AR** → ``rs-gaap:ReceivablesNetCurrent``
- **Prepaids** → ``rs-gaap:PrepaidExpenseCurrent``
- **PP&E gross** → ``rs-gaap:PropertyPlantAndEquipmentGross``
  (separate from the contra-asset so the CF Investing derivation reads
  ΔGross = purchases, not ΔNet which would conflate purchases with
  depreciation activity)
- **Accumulated Depreciation** → ``rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment``
  (BS Net = Gross - AD synthesized at fact-generation time)
- **AP / Accrued** → ``rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent``
- **APIC** → ``rs-gaap:AdditionalPaidInCapital``
- **Retained Earnings** → ``rs-gaap:RetainedEarningsAccumulatedDeficit``

Finer rs-gaap concepts (CashAndCashEquivalentsAtCarryingValue,
AccountsReceivableNetCurrent, AccountsPayableCurrent, PrepaidInsurance,
SalariesAndWages, OccupancyNet, etc.) exist in the broader rs-gaap
presentation library but are NOT in the Default Style's composed
Networks — they won't render until a richer Reporting Style is picked.

The demo resolves rs-gaap qnames → element IDs at runtime against the
library in the entity graph.
"""

# (coa_code, rs_gaap_qname)
MAPPINGS: list[tuple[str, str]] = [
  # Assets
  ("1000", "rs-gaap:CashCashEquivalentsAndShortTermInvestments"),  # Cash
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
  ("2000", "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent"),  # Accounts Payable
  (
    "2100",
    "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent",
  ),  # Accrued Liabilities
  (
    "2200",
    "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent",
  ),  # Payroll Taxes Payable
  # Equity
  ("3000", "rs-gaap:AdditionalPaidInCapital"),  # Owner's Equity
  ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),  # Retained Earnings
  # Revenue — services firm, no goods sold; everything is net service revenue
  ("4000", "rs-gaap:SalesRevenueNet"),  # Consulting Revenue
  ("4100", "rs-gaap:SalesRevenueNet"),  # Strategy Advisory Revenue
  ("4200", "rs-gaap:SalesRevenueNet"),  # Implementation Services Revenue
  # Operating expenses — Multi-step IS Network only admits SG&A as the
  # operating-expense bucket for non-COGS / non-D&A items. Everything
  # below rolls into SG&A. Depreciation gets its own line.
  ("5000", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Salaries & Wages
  ("5100", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Payroll Taxes
  ("5200", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Health Insurance
  ("6000", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Office Rent
  ("6100", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Software Subscriptions
  ("6200", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Cloud Hosting
  (
    "6300",
    "rs-gaap:SellingGeneralAndAdministrativeExpense",
  ),  # Professional Development
  ("6400", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Business Insurance
  ("6500", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Office Supplies
  ("6600", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Travel & Entertainment
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
