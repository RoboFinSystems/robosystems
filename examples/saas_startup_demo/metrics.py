"""Cadence Labs' tenant-authored metric catalog — SaaS judgment metrics.

Gross Margin % is the canonical example of why tenant metrics exist as
the open-edges half of the metric surface: every SaaS company computes
it, and every SaaS company argues about the definition (what lands in
cost of revenue). The library's rs-metric catalog carries only
formula-canonical ratios; a house margin definition belongs here —
tenant-local, authored through the same ``reporting_extension``
envelope, computed by the same structure-agnostic ``compute-metrics``.
"""

from __future__ import annotations

CUSTOM_METRICS: list[dict] = [
  {
    "taxonomy_name": "Cadence Custom Metrics",
    "name": "Cadence Custom Metrics",
    "abstract_qname": "cadence:CustomMetricsAbstract",
    "description": (
      "Tenant-authored SaaS operating metrics — the house gross-margin "
      "definition."
    ),
    "metrics": [
      {
        "qname": "cadence:GrossMarginPercent",
        "name": "Gross Margin %",
        "description": (
          "Gross Margin % = (Revenue - Cost of Revenue) / Revenue"
        ),
        "period_type": "duration",
        "is_monetary": False,
        "balance_type": "credit",
        "expression": (
          "$GrossMarginPercent = "
          "(($Revenue - $CostOfRevenue) / $Revenue)"
        ),
        "variables": [
          {
            "variable_name": "GrossMarginPercent",
            "variable_qname": "cadence:GrossMarginPercent",
          },
          {
            "variable_name": "Revenue",
            "variable_qname": (
              "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
            ),
          },
          {
            "variable_name": "CostOfRevenue",
            "variable_qname": "rs-gaap:CostOfGoodsAndServicesSold",
          },
        ],
      },
    ],
  },
]
