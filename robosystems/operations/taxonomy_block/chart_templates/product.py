"""Product business — the ``product`` chart template.

Derived from ``examples/coffee_roaster_demo`` (Driftline Coffee Roasters):
same codes, traits, sub-classifications and mappings, with the
roaster-specific names generalized (``Inventory — Green Coffee`` →
``Inventory — Raw Materials``, ``Roastery Rent`` → ``Facility Rent`` …).
The demo keeps its own names; ``tests/operations/taxonomy_block/
test_chart_templates.py`` pins the structural equality so the two cannot
drift on anything but the label.

The leaves that make a product company's working-capital story renderable
under the Default Reporting Style, and that a services business never
needs: inventory → ``rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings``
(the Classified-BS inventory leaf), deferred subscription revenue →
``rs-gaap:DeferredRevenueCurrent``, COGS → ``rs-gaap:CostOfGoodsAndServicesSold``
(the Multi-step IS leaf behind ``GrossProfit``).
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
  ("1220", "Prepaid Fulfillment", "asset", "other_current_assets", "debit", None),
  ("1400", "Inventory — Raw Materials", "asset", "inventory", "debit", None),
  ("1410", "Inventory — Work in Process", "asset", "inventory", "debit", None),
  ("1420", "Inventory — Finished Goods", "asset", "inventory", "debit", None),
  ("1500", "Production Equipment", "asset", "fixed_assets", "debit", None),
  ("1550", "Accumulated Depreciation", "asset", "fixed_assets", "credit", None),
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
    "2300",
    "Deferred Subscription Revenue",
    "liability",
    "deferred_revenue",
    "credit",
    None,
  ),
  # Equity
  ("3000", "Paid-in Capital", "equity", "equity", "credit", None),
  ("3100", "Retained Earnings", "equity", "equity", "credit", None),
  # Revenue
  ("4000", "Subscription Revenue", "revenue", "operating_revenue", "credit", None),
  ("4100", "Wholesale Revenue", "revenue", "operating_revenue", "credit", None),
  ("4200", "Direct Sales Revenue", "revenue", "operating_revenue", "credit", None),
  # Cost of goods sold
  ("5000", "Cost of Goods Sold", "expense", "cost_of_goods_sold", "debit", None),
  # Operating expenses
  ("6000", "Salaries & Wages", "expense", "operating_expense", "debit", None),
  ("6100", "Facility Rent", "expense", "operating_expense", "debit", None),
  ("6200", "Fulfillment & Shipping", "expense", "operating_expense", "debit", None),
  ("6300", "Marketing & Advertising", "expense", "operating_expense", "debit", None),
  ("6400", "Software & Subscriptions", "expense", "operating_expense", "debit", None),
  ("6500", "Insurance", "expense", "operating_expense", "debit", None),
  ("6600", "Utilities & Supplies", "expense", "operating_expense", "debit", None),
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
  # All three inventory stages map to the same Classified-BS leaf.
  ("1400", "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"),
  ("1410", "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"),
  ("1420", "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"),
  ("1500", "rs-gaap:PropertyPlantAndEquipmentGross"),
  (
    "1550",
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  ),
  # Liabilities
  ("2000", "rs-gaap:AccountsPayableCurrent"),
  ("2100", "rs-gaap:AccruedLiabilitiesCurrent"),
  ("2300", "rs-gaap:DeferredRevenueCurrent"),
  # Equity
  ("3000", "rs-gaap:AdditionalPaidInCapital"),
  ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),
  # Revenue — all ASC 606 revenue from contracts with customers
  ("4000", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  ("4100", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  ("4200", "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"),
  # COGS — a clean gross margin line
  ("5000", "rs-gaap:CostOfGoodsAndServicesSold"),
  # Operating expenses — back-office costs are G&A; demand-gen is Selling & Marketing
  ("6000", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6100", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6200", "rs-gaap:SellingAndMarketingExpense"),
  ("6300", "rs-gaap:SellingAndMarketingExpense"),
  ("6400", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6500", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("6600", "rs-gaap:GeneralAndAdministrativeExpense"),
  ("7000", "rs-gaap:DepreciationDepletionAndAmortization"),
]


def mappings_for(entity_type: str = "corporation") -> list[tuple[str, str]]:
  """CoA→rs-gaap mappings with equity rows tuned to the entity legal form."""
  return form_aware(MAPPINGS, entity_type)
