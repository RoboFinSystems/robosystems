"""Driftline Café, LLC — the roaster's one retail café, held as a wholly
owned subsidiary in the same graph. Its books are its own (a product chart,
its own calendar, its own close); the group's statements sum both companies
at the rs-gaap concepts. The multi-entity beat of the showcase."""

from __future__ import annotations

from examples._scenario.subsidiary import Subsidiary

SUBSIDIARY = Subsidiary(
  name="Driftline Café LLC",
  legal_name="Driftline Café, LLC",
  ticker="DCAFE",
  description="The roaster's retail café, a wholly owned subsidiary.",
  entity_type="llc",
  ownership_pct=100,
  template="product",
  opening_cash=2_500_000,
  monthly_revenue=2_140_000,
  revenue_memo="Café sales",
  expenses=(
    ("Café rent", 520_000),
    ("Barista wages", 880_000),
  ),
)
