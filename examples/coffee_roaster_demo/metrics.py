"""Driftline's tenant-authored metric catalog.

The library's ``Key Financial Metrics`` (rs-metric) is the curated core
catalog every tenant receives; this file is the tenant-local counterpart,
authored through the same ``reporting_extension`` TaxonomyBlock envelope
the disclosure notes use. Working Capital is a judgment measure — which
lines count as "current" is a house view — so it belongs to the tenant
rather than to the library.

``compute-metrics`` is structure-agnostic: the tenant catalog computes
through the identical operation as the seeded one, with operands bound
by qname to the report's persisted rs-gaap calc-DAG subtotals.
"""

from __future__ import annotations

CUSTOM_METRICS: list[dict] = [
  {
    "taxonomy_name": "Driftline Custom Metrics",
    "name": "Driftline Custom Metrics",
    "abstract_qname": "driftline:CustomMetricsAbstract",
    "description": (
      "Tenant-authored working-capital measures — the cash-squeeze arc, "
      "quantified."
    ),
    "metrics": [
      {
        "qname": "driftline:WorkingCapital",
        "name": "Working Capital",
        "description": "Working Capital = Current Assets - Current Liabilities",
        "period_type": "instant",
        "is_monetary": True,
        "balance_type": "debit",
        "expression": "$WorkingCapital = ($AssetsCurrent - $LiabilitiesCurrent)",
        "variables": [
          {
            "variable_name": "WorkingCapital",
            "variable_qname": "driftline:WorkingCapital",
          },
          {
            "variable_name": "AssetsCurrent",
            "variable_qname": "rs-gaap:AssetsCurrent",
          },
          {
            "variable_name": "LiabilitiesCurrent",
            "variable_qname": "rs-gaap:LiabilitiesCurrent",
          },
        ],
      },
    ],
  },
]
