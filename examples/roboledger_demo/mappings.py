"""CoA to FAC mapping definitions for Cascade Advisory Group LLC.

CoA elements map to FAC (Fundamental Accounting Concepts) as the
primary reporting target. FAC is a small, stable set of high-level
statement concepts. Deterministic expansion to rs-gaap / us-gaap
happens via equivalence arcs on the FAC side.

The demo resolves FAC qnames → element IDs at runtime against the
library in the entity graph.
"""

# (coa_code, fac_qname)
MAPPINGS: list[tuple[str, str]] = [
  # Assets
  ("1000", "fac:CurrentAssets"),  # Cash
  ("1100", "fac:CurrentAssets"),  # Accounts Receivable
  ("1200", "fac:CurrentAssets"),  # Prepaid Expenses
  ("1210", "fac:CurrentAssets"),
  ("1220", "fac:CurrentAssets"),
  ("1300", "fac:FixedAssets"),  # PP&E
  ("1310", "fac:FixedAssets"),
  ("1350", "fac:FixedAssets"),  # Accumulated Depreciation (contra)
  # Liabilities
  ("2000", "fac:CurrentLiabilities"),  # Accounts Payable
  ("2100", "fac:CurrentLiabilities"),  # Accrued Liabilities
  ("2200", "fac:CurrentLiabilities"),
  # Equity
  ("3000", "fac:Equity"),  # Additional Paid-In Capital
  ("3100", "fac:Equity"),  # Retained Earnings
  # Revenue
  ("4000", "fac:Revenues"),
  ("4100", "fac:Revenues"),
  ("4200", "fac:Revenues"),
  # Expenses (SG&A rolls into CostsAndExpenses at the FAC level)
  ("5000", "fac:CostsAndExpenses"),
  ("5100", "fac:CostsAndExpenses"),
  ("5200", "fac:CostsAndExpenses"),
  ("6000", "fac:CostsAndExpenses"),
  ("6100", "fac:CostsAndExpenses"),
  ("6200", "fac:CostsAndExpenses"),
  ("6300", "fac:CostsAndExpenses"),
  ("6400", "fac:CostsAndExpenses"),
  ("6500", "fac:CostsAndExpenses"),
  ("6600", "fac:CostsAndExpenses"),
  ("7000", "fac:CostsAndExpenses"),  # Depreciation & Amortization
]
