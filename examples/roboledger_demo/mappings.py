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

# The mapping is the shipped chart template's — one copy, read from
# ``frameworks/chart-templates/services/v1/mappings/rs-gaap.jsonld``.
from robosystems.operations.taxonomy_block.chart_templates import CHART_TEMPLATES

_RS_GAAP = CHART_TEMPLATES["services"].mappings["rs-gaap"]
MAPPINGS = _RS_GAAP.arcs_for("corporation")
mappings_for = _RS_GAAP.arcs_for

__all__ = ["MAPPINGS", "mappings_for"]
