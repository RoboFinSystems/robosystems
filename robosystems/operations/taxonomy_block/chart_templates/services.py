"""Professional services — the ``services`` chart template.

Lifted verbatim from ``examples/roboledger_demo`` (Cascade Advisory
Group), which now imports it from here so there is one copy. A services
firm has no goods sold and no R&D: revenue is ASC 606 revenue from
contracts with customers, and every operating cost that is not
depreciation rolls to General & Administrative.
"""

from __future__ import annotations

from ._forms import form_aware

# (code, name, trait, sub_classification, balance_type, description)
ACCOUNTS: list[tuple[str, str, str, str, str, str | None]] = [
  # Assets
  ("1000", "Operating Checking", "asset", "cash_and_equivalents", "debit", None),
  ("1100", "Accounts Receivable", "asset", "accounts_receivable", "debit", None),
  ("1200", "Prepaid Insurance", "asset", "other_current_assets", "debit", None),
  ("1210", "Prepaid Software", "asset", "other_current_assets", "debit", None),
  ("1220", "Prepaid Cloud Hosting", "asset", "other_current_assets", "debit", None),
  ("1300", "Computer Equipment", "asset", "fixed_assets", "debit", None),
  ("1310", "Office Furniture", "asset", "fixed_assets", "debit", None),
  ("1350", "Accumulated Depreciation", "asset", "fixed_assets", "credit", None),
  # Liabilities
  ("2000", "Accounts Payable", "liability", "accounts_payable", "credit", None),
  (
    "2100",
    "Accrued Liabilities",
    "liability",
    "other_current_liabilities",
    "credit",
    None,
  ),
  (
    "2200",
    "Payroll Taxes Payable",
    "liability",
    "other_current_liabilities",
    "credit",
    None,
  ),
  # Equity
  ("3000", "Owner's Equity", "equity", "equity", "credit", None),
  ("3100", "Retained Earnings", "equity", "equity", "credit", None),
  # Revenue
  ("4000", "Consulting Revenue", "revenue", "operating_revenue", "credit", None),
  ("4100", "Strategy Advisory Revenue", "revenue", "operating_revenue", "credit", None),
  (
    "4200",
    "Implementation Services Revenue",
    "revenue",
    "operating_revenue",
    "credit",
    None,
  ),
  # Expenses
  ("5000", "Salaries & Wages", "expense", "operating_expense", "debit", None),
  ("5100", "Payroll Taxes", "expense", "operating_expense", "debit", None),
  ("5200", "Health Insurance", "expense", "operating_expense", "debit", None),
  ("6000", "Office Rent", "expense", "operating_expense", "debit", None),
  ("6100", "Software Subscriptions", "expense", "operating_expense", "debit", None),
  ("6200", "Cloud Hosting", "expense", "operating_expense", "debit", None),
  ("6300", "Professional Development", "expense", "operating_expense", "debit", None),
  ("6400", "Business Insurance", "expense", "operating_expense", "debit", None),
  ("6500", "Office Supplies", "expense", "operating_expense", "debit", None),
  ("6600", "Travel & Entertainment", "expense", "operating_expense", "debit", None),
  ("7000", "Depreciation Expense", "expense", "operating_expense", "debit", None),
]

# (coa_code, rs_gaap_qname) — corporation form; see ``mappings_for``.
MAPPINGS: list[tuple[str, str]] = [
  # Assets
  ("1000", "rs-gaap:CashAndCashEquivalentsAtCarryingValue"),
  ("1100", "rs-gaap:ReceivablesNetCurrent"),
  ("1200", "rs-gaap:PrepaidExpenseCurrent"),
  ("1210", "rs-gaap:PrepaidExpenseCurrent"),
  ("1220", "rs-gaap:PrepaidExpenseCurrent"),
  ("1300", "rs-gaap:PropertyPlantAndEquipmentGross"),
  ("1310", "rs-gaap:PropertyPlantAndEquipmentGross"),
  (
    "1350",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  ),
  # Liabilities
  ("2000", "rs-gaap:AccountsPayableCurrent"),
  ("2100", "rs-gaap:AccruedLiabilitiesCurrent"),
  ("2200", "rs-gaap:AccruedLiabilitiesCurrent"),
  # Equity
  ("3000", "rs-gaap:AdditionalPaidInCapital"),
  ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),
  # Revenue — services firm, all ASC 606 revenue from contracts with customers
  ("4000", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  ("4100", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  ("4200", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  # Operating expenses — all G&A for a back-office firm; depreciation on its own line
  ("5000", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("5100", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("5200", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6000", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6100", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6200", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6300", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6400", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6500", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6600", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("7000", "rs-gaap:DepreciationDepletionAndAmortization"),
]


def mappings_for(entity_type: str = "corporation") -> list[tuple[str, str]]:
  """CoA→rs-gaap mappings with equity rows tuned to the entity legal form."""
  return form_aware(MAPPINGS, entity_type)
