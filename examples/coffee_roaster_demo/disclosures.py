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
}

DISCLOSURE_NOTES: list[dict] = [INVENTORY_NOTE]
