"""
Canonical concept definition for XBRL semantic enrichment.

A CanonicalConcept maps a human-readable financial concept (e.g. "revenue")
to the various XBRL element names that represent it.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CanonicalConcept:
  """A canonical financial concept with its XBRL element mappings."""

  id: str  # "revenue"
  display_name: str  # "Revenue"
  category: str  # "income_statement" | "balance_sheet" | "cash_flow" | "per_share" | "structure"
  description: str  # Used for embedding: "Total revenue from operations"
  aliases: tuple[str, ...] = ()  # ("net revenue", "total revenue", "sales")
  expected_elements: tuple[str, ...] = ()  # ("us-gaap:Revenues", ...)
  period_type: str = "duration"  # "duration" | "instant"
  balance: str | None = None  # "debit" | "credit" | None
  is_monetary: bool = True
  embedding: list[float] | None = field(default=None, compare=False, hash=False)
