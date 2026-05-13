"""CoA to FAC mapping definitions for Cascade Advisory Group LLC.

CoA elements map to FAC (Fundamental Accounting Concepts) — the small,
stable spine of high-level statement concepts that every rs-gaap
presentation Network anchors to. The renderer follows the calc DAG
from each FAC leaf into the rs-gaap detail when one of the Network's
calculations cites it.

The demo's Reporting Style is the Default Style (§3.2 Phase 1), so the
target Networks are rs-gaap Classified BS / Multi-step IS / Indirect
CF / Equity Roll Forward. Each line below maps to a concept that
appears in those Networks — otherwise the renderer drops the row as
"out of structure". In particular:

- Operating expenses target ``fac:OperatingExpenses`` (the Multi-step
  IS concept), not the Single-step's ``fac:CostsAndExpenses``. Without
  this, the IS would render empty above the bottom line and the
  NetIncome synthesis would have nothing to bridge.
- PP&E rolls into ``fac:NoncurrentAssets`` — BS-classified's parent
  for long-lived assets. The Classified BS Network has no ``FixedAssets``
  slot (that's a fac-presentation concept, not rs-gaap).

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
  ("1300", "fac:NoncurrentAssets"),  # PP&E — Classified BS parent
  ("1310", "fac:NoncurrentAssets"),
  ("1350", "fac:NoncurrentAssets"),  # Accumulated Depreciation (contra)
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
  # Operating expenses — rs-gaap Multi-step IS anchors on
  # fac:OperatingExpenses (the SG&A bucket); the calc DAG expands into
  # rs-gaap leaves via the calc-linkbase bridges. Cascade is a services
  # company, so nothing here is COGS-natured — everything sits under
  # OperatingExpenses.
  ("5000", "fac:OperatingExpenses"),
  ("5100", "fac:OperatingExpenses"),
  ("5200", "fac:OperatingExpenses"),
  ("6000", "fac:OperatingExpenses"),
  ("6100", "fac:OperatingExpenses"),
  ("6200", "fac:OperatingExpenses"),
  ("6300", "fac:OperatingExpenses"),
  ("6400", "fac:OperatingExpenses"),
  ("6500", "fac:OperatingExpenses"),
  ("6600", "fac:OperatingExpenses"),
  ("7000", "fac:OperatingExpenses"),  # Depreciation & Amortization
]
