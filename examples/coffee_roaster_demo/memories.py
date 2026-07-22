"""Driftline's seeded semantic memories — institutional knowledge.

Seeded through the MCP ``remember`` tool (the same one an AI Operator
uses), so the close co-pilot starts with the context a returning
bookkeeper would carry: the cash-trap pattern, the margin target, and
the one wholesale account worth watching. ``recall`` surfaces these
during the close and the Beat-4 reveal.
"""

from __future__ import annotations

MEMORIES: list[dict] = [
  {
    "text": (
      "Driftline pre-buys green coffee from Andean Green Coffee Importers "
      "ahead of wholesale launches and the holiday season. The pre-buy "
      "builds Inventory — Green Coffee (1400) and drains cash months "
      "before the matching revenue and COGS land — during close, always "
      "compare the change in inventory to the change in cash."
    ),
    "memory_type": "fact",
    "tags": ["inventory", "cash-flow", "close"],
  },
  {
    "text": (
      "Target blended gross margin is roughly 55%. If COGS as a share of "
      "revenue moves materially off trend in a close, investigate roast "
      "loss and green-coffee pricing before accepting the number."
    ),
    "memory_type": "fact",
    "tags": ["margin", "cogs", "close"],
  },
  {
    "text": (
      "Summit Markets is the largest wholesale account and pays slowly — "
      "net-30 terms but balances routinely age past 60 days, and Summit "
      "is a concentrated share of total AR. Review the AR aging by "
      "customer every close and flag Summit's balance specifically."
    ),
    "memory_type": "fact",
    "tags": ["accounts-receivable", "summit-markets", "credit-risk"],
  },
  {
    "text": (
      "DTC subscribers prepay a quarter ahead into Deferred Subscription "
      "Revenue (2300). The monthly earn-out entry is DR 2300 / CR "
      "Subscription Revenue — DTC (4000); the remaining deferred balance "
      "must equal subscriptions billed but not yet shipped."
    ),
    "memory_type": "fact",
    "tags": ["deferred-revenue", "subscriptions", "close"],
  },
]
