"""CoA to US GAAP mapping definitions for Cascade Advisory Group LLC.

Maps each Chart of Accounts code to a deterministic GAAP element ID
from the US GAAP reporting taxonomy seed (robosystems/config/taxonomy/seed.py).
"""

# (coa_code, gaap_element_id)
MAPPINGS: list[tuple[str, str]] = [
  # Assets
  ("1000", "elem_gaap_cash"),
  ("1100", "elem_gaap_accounts_receivable"),
  ("1200", "elem_gaap_prepaid_expenses"),
  ("1210", "elem_gaap_prepaid_expenses"),
  ("1220", "elem_gaap_prepaid_expenses"),
  ("1300", "elem_gaap_ppe"),
  ("1310", "elem_gaap_ppe"),
  ("1350", "elem_gaap_ppe"),  # Accumulated Depreciation (contra) → PP&E net
  # Liabilities
  ("2000", "elem_gaap_accounts_payable"),
  ("2100", "elem_gaap_accrued_liabilities"),
  ("2200", "elem_gaap_accrued_liabilities"),
  # Equity
  ("3000", "elem_gaap_additional_paid_in_capital"),
  ("3100", "elem_gaap_retained_earnings"),
  # Revenue
  ("4000", "elem_gaap_revenues"),
  ("4100", "elem_gaap_revenues"),
  ("4200", "elem_gaap_revenues"),
  # Expenses
  ("5000", "elem_gaap_sga"),
  ("5100", "elem_gaap_sga"),
  ("5200", "elem_gaap_sga"),
  ("6000", "elem_gaap_sga"),
  ("6100", "elem_gaap_sga"),
  ("6200", "elem_gaap_sga"),
  ("6300", "elem_gaap_sga"),
  ("6400", "elem_gaap_sga"),
  ("6500", "elem_gaap_sga"),
  ("6600", "elem_gaap_sga"),
  ("7000", "elem_gaap_depreciation_amortization"),
]
