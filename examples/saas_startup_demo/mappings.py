"""CoA → rs-gaap mappings for Cadence Labs, Inc.

Every target is a verified leaf of the Default Reporting Style's Networks
(Classified BS / Multi-step IS). The SaaS-shaped lines this episode leans on:

- **Deferred Revenue** → ``rs-gaap:DeferredRevenueCurrent`` (the annual-prepay
  liability at the heart of the runway reveal).
- **R&D** → ``rs-gaap:ResearchAndDevelopmentExpense`` and **S&M** →
  ``rs-gaap:SellingAndMarketingExpense`` (the burn, rendered as their own
  Multi-step IS lines below Gross Profit).
- **Cost of Revenue** → ``rs-gaap:CostOfGoodsAndServicesSold`` (hosting +
  support; drives the ~78% gross margin and the GrossProfit subtotal).
"""

# The mapping is the shipped chart template's — one copy, in core.
from robosystems.operations.taxonomy_block.chart_templates.saas import (
  MAPPINGS,
  mappings_for,
)

__all__ = ["MAPPINGS", "mappings_for"]
