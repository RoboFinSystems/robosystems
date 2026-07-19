"""Driftline's authored disclosure notes — the report-engine proof.

The inventory-components note is authored **per-tenant** through the
TaxonomyBlock envelope (``reporting_extension``): three member concepts
parented under the rs-gaap balance-sheet inventory leaf, one
``regulatory_disclosure`` structure with ``concept_arrangement='roll_up'``,
and presentation + calculation arcs whose total **is** the library BS
leaf. Nothing is added to the framework source — this is the
extend-and-map path.

Rendering is fact-driven: the note joins the report render set because
its concepts receive mapped facts (each inventory account multi-maps to
its member concept AND the BS leaf), so the balance-sheet Inventory line
and the note total are literally the same fact — the cross-block tie-out
is definitional, and the note's internal footing (Σ members = total) is
validated by the auto-emitted RollUp rule.
"""

from __future__ import annotations

INVENTORY_TOTAL_QNAME = (
  "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"
)

INVENTORY_NOTE: dict = {
  "taxonomy_name": "Driftline Reporting Extension",
  "name": "Inventory Components",
  "description": (
    "Inventory by stage — green coffee (raw materials), roasting WIP, "
    "and bagged finished goods — footing to the balance-sheet "
    "Inventory line."
  ),
  "role_uri": (
    "https://driftlinecoffee.example.com/roles/disclosures/InventoryComponents"
  ),
  "total_qname": INVENTORY_TOTAL_QNAME,
  # (member qname, display name, CoA code multi-mapped to the member)
  "members": [
    {
      "qname": "driftline:InventoryRawMaterials",
      "name": "Raw Materials — Green Coffee",
      "coa_code": "1400",
    },
    {
      "qname": "driftline:InventoryWorkInProcess",
      "name": "Work in Process — Roasting",
      "coa_code": "1410",
    },
    {
      "qname": "driftline:InventoryFinishedGoods",
      "name": "Finished Goods — Bagged Coffee",
      "coa_code": "1420",
    },
  ],
  # Added AFTER creation via update-taxonomy-block — exercises the
  # update-path auto-rule refresh (the footing rule must re-derive to
  # include this member). Unmapped on purpose: no facts, so the note
  # still foots (a missing child sums as 0) while proving the refresh.
  "update_member": {
    "qname": "driftline:InventoryPackagingSupplies",
    "name": "Packaging Supplies — Bags & Labels",
  },
}

DISCLOSURE_NOTES: list[dict] = [INVENTORY_NOTE]

# ── Text-block notes — the narrative half of the disclosure surface ────────
#
# "Significant Accounting Policies" is the canonical first note of every
# report: pure narrative, no numeric grid. Each concept binds one of the
# uploaded policy documents (policies.py DOCUMENTS, matched by title) as a
# ``Nonnumeric`` text-block fact via bind-text-block — the Document→fact
# bridge. The document stays the editable source of truth; the report
# snapshots the bound text at generation time.

POLICIES_NOTE: dict = {
  "taxonomy_name": "Driftline Policy Notes",
  "name": "Significant Accounting Policies",
  "description": (
    "Significant accounting policies — inventory and COGS, revenue "
    "recognition, and depreciation — bound from the company's policy "
    "documents."
  ),
  "role_uri": (
    "https://driftlinecoffee.example.com/roles/disclosures/AccountingPolicies"
  ),
  "abstract_qname": "driftline:SignificantAccountingPoliciesAbstract",
  "abstract_name": "Significant Accounting Policies",
  # (concept qname, display name, uploaded document title to bind)
  "concepts": [
    {
      "qname": "driftline:InventoryPolicyTextBlock",
      "name": "Inventory and Cost of Goods Sold Policy",
      "document_title": "Inventory and COGS Policy",
    },
    {
      "qname": "driftline:RevenueRecognitionPolicyTextBlock",
      "name": "Revenue Recognition Policy",
      "document_title": "Revenue Recognition Policy",
    },
    {
      "qname": "driftline:DepreciationPolicyTextBlock",
      "name": "Depreciation and Prepaid Policy",
      "document_title": "Depreciation and Prepaid Policy",
    },
  ],
}

TEXT_BLOCK_NOTES: list[dict] = [POLICIES_NOTE]
