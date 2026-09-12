"""SaaS / subscription software — the ``saas`` chart template.

Lifted verbatim from ``examples/saas_startup_demo`` (Cadence Labs), which
now imports it from here so there is one copy. Every mapping target is a
leaf admitted by the Default Reporting Style's Networks (Classified BS /
Multi-step IS); a fact mapped outside them is dropped as out of structure.
"""

from __future__ import annotations

from ._forms import form_aware

# (code, name, trait, sub_classification, balance_type, description)
ACCOUNTS: list[tuple[str, str, str, str, str, str | None]] = [
  # Assets
  ("1000", "Cash", "asset", "cash_and_equivalents", "debit", None),
  ("1100", "Accounts Receivable", "asset", "accounts_receivable", "debit", None),
  ("1200", "Prepaid Software", "asset", "other_current_assets", "debit", None),
  ("1210", "Prepaid Insurance", "asset", "other_current_assets", "debit", None),
  ("1300", "Computer Equipment", "asset", "fixed_assets", "debit", None),
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
  ("2300", "Deferred Revenue", "liability", "deferred_revenue", "credit", None),
  # Equity
  ("3000", "Paid-in Capital", "equity", "equity", "credit", None),
  ("3100", "Accumulated Deficit", "equity", "equity", "credit", None),
  # Revenue
  ("4000", "Subscription Revenue", "revenue", "operating_revenue", "credit", None),
  ("4100", "Professional Services", "revenue", "operating_revenue", "credit", None),
  # Cost of revenue
  ("5000", "Cost of Revenue", "expense", "cost_of_goods_sold", "debit", None),
  # Operating expenses
  ("6000", "Research & Development", "expense", "operating_expense", "debit", None),
  ("6100", "Sales & Marketing", "expense", "operating_expense", "debit", None),
  ("6200", "General & Administrative", "expense", "operating_expense", "debit", None),
  ("6300", "Rent", "expense", "operating_expense", "debit", None),
  ("6400", "Software & Tools", "expense", "operating_expense", "debit", None),
  ("7000", "Depreciation Expense", "expense", "operating_expense", "debit", None),
]

# (coa_code, rs_gaap_qname) — corporation form; see ``mappings_for``.
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
  ("2300", "rs-gaap:DeferredRevenueCurrent"),
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


def mappings_for(entity_type: str = "corporation") -> list[tuple[str, str]]:
  """CoA→rs-gaap mappings with equity rows tuned to the entity legal form."""
  return form_aware(MAPPINGS, entity_type)
