"""The Driftline group — the roaster's cafés, held in the same graph.

Driftline Café is the roaster's own retail café, wholly owned. Driftline
Harbor Café is the second one, opened with an operating partner who holds
thirty percent — the outside owner whose share the consolidated statements
carry as noncontrolling interest. The roaster charges both a management fee,
lent the first café its fit-out money on a note, and rebilled a utilities
bill once; every one of those posts on both sides and is eliminated in the
Consolidation Adjustments entity. The multi-entity beat of the showcase, and
the target the consolidation engine is built against.

The fee and the distributions are kept small on purpose: the cafés send cash
up to the roaster, and the roaster's own story is that its cash falls while
its profit rises. Fictional throughout.
"""

from __future__ import annotations

from examples._scenario.group import Group, Member

GROUP = Group(
  members=(
    Member(
      name="Driftline Café LLC",
      legal_name="Driftline Café, LLC",
      ticker="DCAFE",
      description="The roaster's retail café, a wholly owned subsidiary.",
      entity_type="llc",
      template="product",
      ownership_pct=100,
      contributed_capital=2_500_000,
      monthly_revenue=2_140_000,
      revenue_memo="Café sales",
      expenses=(
        ("Café rent", 520_000),
        ("Barista wages", 880_000),
      ),
      management_fee_bps=300,
      note_from_parent=500_000,
      note_rate_bps=50,
      rebill=(2, 86_500),
      distribution=200_000,
    ),
    Member(
      name="Driftline Harbor Café LLC",
      legal_name="Driftline Harbor Café, LLC",
      ticker="DHARB",
      description=(
        "The second café, on the harbor, opened with an operating partner who "
        "holds thirty percent."
      ),
      entity_type="llc",
      template="product",
      ownership_pct=70,
      outside_owner="Pier & Tide Hospitality LLC",
      contributed_capital=2_000_000,
      monthly_revenue=1_960_000,
      revenue_memo="Harbor café sales",
      expenses=(
        ("Harbor café rent", 610_000),
        ("Barista wages", 790_000),
      ),
      management_fee_bps=300,
      distribution=150_000,
    ),
  ),
  consolidation_name="Consolidation Adjustments",
  consolidation_legal_name="Driftline Group — Consolidation Adjustments",
  consolidation_ticker="DCONS",
  founders_share=0.5,
  funding_memo="Capital contributed by the founders for the café group",
)
