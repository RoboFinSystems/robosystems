"""The Driftline group — the roaster's cafés, held in the same graph.

Driftline Café is the roaster's own retail café, wholly owned. Driftline
Harbor Café is the second one, opened with an operating partner who holds
thirty percent — the outside owner whose share the consolidated statements
carry as noncontrolling interest. The roaster charges both a management fee,
lent the first café its fit-out money on a note, and rebilled a utilities
bill once; every one of those posts on both sides and is eliminated in the
Consolidation Adjustments entity. Pier Market Kiosk is different: the
roaster holds forty percent and the market's operator runs it, so the group
never sums its books; it carries the roaster's share of the kiosk's net assets
as one investment and its share of the kiosk's income as one line, the
equity method. Driftline Northside Café is the one that was bought and
sold: a neighborhood café the roaster acquired from its founders a few
months in, paying more than its book value, bought a further tenth of
later, sold that tenth back to the founders at a price, and sold whole
before the year was out — the group lifecycle, with goodwill arriving, a
sale within the method that is no gain in the group, and a gain on the
final sale that is smaller in the group than on the roaster's own books. The multi-entity beat of the showcase, and the
target the consolidation engine is built against.

The fee and the distributions are kept small on purpose: the cafés send cash
up to the roaster, and the roaster's own story is that its cash falls while
its profit rises. Fictional throughout.
"""

from __future__ import annotations

from examples._scenario.group import Acquired, Group, Member

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
    Member(
      name="Pier Market Kiosk LLC",
      legal_name="Pier Market Kiosk, LLC",
      ticker="DKIOSK",
      description=(
        "A coffee kiosk in the pier market, run by the market's operator; the "
        "roaster holds forty percent."
      ),
      entity_type="llc",
      template="product",
      ownership_pct=40,
      outside_owner="Pier Market Operators LLC",
      contributed_capital=1_000_000,
      monthly_revenue=860_000,
      revenue_memo="Kiosk sales",
      expenses=(
        ("Kiosk stall fee", 240_000),
        ("Kiosk wages", 350_000),
      ),
      management_fee_bps=0,
      distribution=120_000,
    ),
    Member(
      name="Driftline Northside Café LLC",
      legal_name="Driftline Northside Café, LLC",
      ticker="DNORTH",
      description=(
        "A neighborhood café bought from its founders, who kept a quarter, "
        "sold a further tenth, bought it back, and were bought out alongside "
        "the roaster when the café was sold."
      ),
      entity_type="llc",
      template="product",
      ownership_pct=75,
      outside_owner="Northside Coffee Partners LLC",
      monthly_revenue=1_450_000,
      revenue_memo="Northside café sales",
      expenses=(
        ("Northside café rent", 380_000),
        ("Barista wages", 620_000),
      ),
      management_fee_bps=300,
      distribution=150_000,
      # Offsets into the demo's month windows: bought in the fourth month on
      # $12,000 of cash and $18,000 of equipment for $31,500 (goodwill
      # $9,000 over 75% of $30,000); a further 10% for $4,000 four months
      # on; that tenth sold back to the founders for $7,000 two months
      # later; sold whole the month after that for $62,000.
      acquired=Acquired(
        joins_at=3,
        opening_cash=1_200_000,
        opening_equipment=1_800_000,
        price=3_150_000,
        tops_up_at=7,
        top_up_pct=85,
        top_up_price=400_000,
        sells_back_at=9,
        sell_back_pct=75,
        sell_back_price=700_000,
        sold_at=10,
        sale_price=6_200_000,
        buyer="Ridgeline Hospitality Group LLC",
      ),
    ),
  ),
  consolidation_name="Consolidation Adjustments",
  consolidation_legal_name="Driftline Group — Consolidation Adjustments",
  consolidation_ticker="DCONS",
  founders_share=0.5,
  funding_memo="Capital contributed by the founders for the café group",
)
