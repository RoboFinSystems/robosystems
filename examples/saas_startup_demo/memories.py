"""Cadence Labs' seeded semantic memories — institutional knowledge.

Seeded through the MCP ``remember`` tool so the close co-pilot starts
with the context the founders carry: the annual-prepay billing motion,
the deferred-revenue float masking the burn, and the runway math the
Beat-4 reveal walks through.
"""

from __future__ import annotations

MEMORIES: list[dict] = [
  {
    "text": (
      "Cadence customers sign annual contracts billed up front. Cash "
      "collects on signing into Deferred Revenue (2300); each month's "
      "earn-out is DR 2300 / CR Subscription Revenue (4000). A growing "
      "deferred balance is service already sold and still owed — it is "
      "not free cash."
    ),
    "memory_type": "fact",
    "tags": ["deferred-revenue", "billing", "close"],
  },
  {
    "text": (
      "Runway must be computed net of deferred revenue: bank cash minus "
      "the deferred balance, divided by the monthly operating burn. The "
      "prepayment float flatters the raw bank balance, and it shrinks "
      "as growth slows."
    ),
    "memory_type": "fact",
    "tags": ["runway", "burn", "cash-flow"],
  },
  {
    "text": (
      "Onboarding professional services (4100) are recognized as "
      "delivered and run near break-even by design — they exist to land "
      "subscription logos, not to make margin. Watch that services "
      "revenue stays a small share of total revenue."
    ),
    "memory_type": "fact",
    "tags": ["professional-services", "revenue-mix"],
  },
  {
    "text": (
      "The two largest operating cash drains are engineering payroll "
      "(R&D, 6000) and paid acquisition (S&M, 6100 — LinkedIn). During "
      "close, reconcile AWS hosting (Cost of Revenue, 5000) against the "
      "Datadog usage report before accepting the month's gross margin."
    ),
    "memory_type": "fact",
    "tags": ["burn", "opex", "close"],
  },
]
