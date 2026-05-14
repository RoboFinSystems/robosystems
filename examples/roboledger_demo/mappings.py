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
- **PP&E (gross + accumulated)** → ``rs-gaap:PropertyPlantAndEquipmentNet``
  (the contra-balance accumulated-depreciation account nets naturally:
  Gross + (-Accum) = Net)
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
  ("1300", "rs-gaap:PropertyPlantAndEquipmentNet"),  # Computer Equipment
  ("1310", "rs-gaap:PropertyPlantAndEquipmentNet"),  # Office Furniture
  ("1350", "rs-gaap:PropertyPlantAndEquipmentNet"),  # Accumulated Depreciation (contra)
  # Liabilities
  ("2000", "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent"),  # Accounts Payable
  ("2100", "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent"),  # Accrued Liabilities
  ("2200", "rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent"),  # Payroll Taxes Payable
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
  ("6300", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Professional Development
  ("6400", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Business Insurance
  ("6500", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Office Supplies
  ("6600", "rs-gaap:SellingGeneralAndAdministrativeExpense"),  # Travel & Entertainment
  ("7000", "rs-gaap:DepreciationDepletionAndAmortization"),  # Depreciation Expense
]
