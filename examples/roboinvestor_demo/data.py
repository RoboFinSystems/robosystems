"""Meridian Ventures Fund I — the synthetic venture portfolio.

The fund is an early-stage venture investor, which is the positioning
RoboInvestor actually models: ``Security`` is *ownership in a private
company* — preferred stock, SAFEs, LLC units, warrants — not a brokerage
position in a listed ticker. Every holding here is a private instrument
with terms, and none of them has a market price.

One holding is different in kind, and it is the whole point of the demo:
**Cadence Labs is another tenant on this platform.** The fund declares
that relationship with ``source_graph_id`` before any link exists (a
*pre-association*), and the link resolves when Cadence shares a published
report into the fund's graph. The other three holdings stay unlinked
forever, which is the ordinary case and the control group.

Dates are relative to today so the demo stays evergreen. Money is integer
cents everywhere, matching the storage contract.

Run ``uv run python -m examples.roboinvestor_demo.data`` for an offline
preview of the portfolio without the platform running.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

FUND_NAME = "Meridian Ventures Fund I, LP"
FUND_URI = "https://meridianventures.example.com"
FUND_TICKER = "MVF1"
PORTFOLIO_NAME = "Fund I — Core Positions"
PORTFOLIO_STRATEGY = "early_stage_venture"


def _today() -> date:
  return date.today()


def _months_ago(months: int) -> date:
  """Approximate month arithmetic — good enough for synthetic dates."""
  d = _today() - timedelta(days=months * 30)
  return d


@dataclass(frozen=True)
class SecuritySpec:
  """One private instrument the fund owns.

  ``links_to_issuer`` marks the holding whose issuer keeps its books on
  this platform. That security is created with ``source_graph_id`` set to
  the issuer's graph and no ``entity_id`` — the pre-association state the
  handshake resolves.
  """

  key: str
  name: str
  security_type: str
  security_subtype: str | None = None
  terms: dict = field(default_factory=dict)
  authorized_shares: int | None = None
  outstanding_shares: int | None = None
  links_to_issuer: bool = False


@dataclass(frozen=True)
class PositionSpec:
  """One lot held in the portfolio, in the fund's base currency."""

  security_key: str
  quantity: float
  quantity_type: str
  cost_basis: int
  acquisition_months_ago: int
  current_value: int | None = None
  valuation_source: str | None = None
  notes: str | None = None


# ── Securities ────────────────────────────────────────────────────────────
#
# Two of these carry `links_to_issuer=True` and both name the same issuer
# graph. That is deliberate: `_ensure_linked_entity` updates *every*
# security matching the source graph in one statement, so a single share
# must resolve both. One security would not have proven that.

SECURITIES: list[SecuritySpec] = [
  SecuritySpec(
    key="cadence_series_a",
    name="Cadence Labs, Inc. — Series A Preferred",
    security_type="preferred_stock",
    security_subtype="series_a",
    terms={
      "liquidation_preference": "1x_non_participating",
      "price_per_share_cents": 312,
      "pre_money_valuation_cents": 18_000_000_00,
      "board_seats": 1,
      "pro_rata_rights": True,
    },
    authorized_shares=4_000_000,
    outstanding_shares=3_205_128,
    links_to_issuer=True,
  ),
  SecuritySpec(
    key="cadence_warrant",
    name="Cadence Labs, Inc. — Bridge Warrant",
    security_type="warrant",
    security_subtype="bridge_2026",
    terms={
      "strike_price_cents": 1,
      "shares_underlying": 120_000,
      "expiration_years": 10,
      "coverage_pct": 15,
    },
    links_to_issuer=True,
  ),
  SecuritySpec(
    key="halyard_safe",
    name="Halyard Robotics, Inc. — SAFE (post-money)",
    security_type="safe",
    security_subtype="post_money_cap_no_discount",
    terms={
      "principal_cents": 750_000_00,
      "valuation_cap_cents": 12_000_000_00,
      "discount_pct": 0,
      "mfn": True,
    },
  ),
  SecuritySpec(
    key="thornbury_units",
    name="Thornbury Materials LLC — Class A Units",
    security_type="llc_unit",
    security_subtype="class_a",
    terms={
      "profit_share_pct": 8.5,
      "tax_distributions": True,
      "transfer_restriction": "right_of_first_refusal",
    },
    authorized_shares=2_000_000,
    outstanding_shares=1_400_000,
  ),
  SecuritySpec(
    key="aldergrove_seed",
    name="Alder Grove Bio, Inc. — Series Seed Preferred",
    security_type="preferred_stock",
    security_subtype="series_seed",
    terms={
      "liquidation_preference": "1x_non_participating",
      "price_per_share_cents": 89,
      "pre_money_valuation_cents": 6_000_000_00,
    },
    authorized_shares=1_500_000,
    outstanding_shares=1_123_595,
  ),
]


# ── Initial positions (the create-portfolio-block envelope) ───────────────

INITIAL_POSITIONS: list[PositionSpec] = [
  PositionSpec(
    security_key="cadence_series_a",
    quantity=3_205_128,
    quantity_type="shares",
    cost_basis=10_000_000_00,
    acquisition_months_ago=14,
    current_value=14_500_000_00,
    valuation_source="409a_valuation",
    notes="Led the Series A; 1 board seat, pro-rata reserved for Series B.",
  ),
  PositionSpec(
    security_key="halyard_safe",
    quantity=750_000_00,
    quantity_type="principal",
    cost_basis=750_000_00,
    acquisition_months_ago=9,
    current_value=750_000_00,
    valuation_source="cost",
    notes="Post-money SAFE, converts at the next priced round.",
  ),
  PositionSpec(
    security_key="aldergrove_seed",
    quantity=1_123_595,
    quantity_type="shares",
    cost_basis=1_000_000_00,
    acquisition_months_ago=22,
    current_value=1_000_000_00,
    valuation_source="cost",
    notes="Seed position — company later acquired for stock.",
  ),
]


# ── Deltas (the update-portfolio-block envelope) ──────────────────────────
#
# One of each kind, applied in a single atomic call:
#   add     — the bridge warrant taken alongside the Series A, and a new
#             LLC position from a later close
#   update  — a fresh mark on the Cadence position (a 409A refresh)
#   dispose — Alder Grove exits via acquisition

ADDED_POSITIONS: list[PositionSpec] = [
  PositionSpec(
    security_key="cadence_warrant",
    quantity=120_000,
    quantity_type="shares",
    cost_basis=0,
    acquisition_months_ago=6,
    current_value=54_000_00,
    valuation_source="black_scholes_internal",
    notes="Warrant coverage on the bridge note; nominal strike.",
  ),
  PositionSpec(
    security_key="thornbury_units",
    quantity=140_000,
    quantity_type="units",
    cost_basis=2_100_000_00,
    acquisition_months_ago=4,
    current_value=2_100_000_00,
    valuation_source="cost",
    notes="Growth-equity unit purchase; 8.5% profit share.",
  ),
]

# The re-mark applied to the Cadence Series A position: a 409A refresh
# after the company's annual report lands. Cents, like everything else.
CADENCE_REMARK_VALUE = 17_800_000_00
CADENCE_REMARK_SOURCE = "409a_valuation_refresh"

DISPOSED_REASON = "Acquired by Northwind Therapeutics — stock-for-stock merger"


def acquisition_date(spec: PositionSpec) -> date:
  return _months_ago(spec.acquisition_months_ago)


def inception_date() -> date:
  return _months_ago(26)


def valuation_date() -> date:
  """The as-of date every mark in this demo shares."""
  return _months_ago(1)


def security_by_key(key: str) -> SecuritySpec:
  for spec in SECURITIES:
    if spec.key == key:
      return spec
  raise KeyError(f"Unknown security key: {key}")


def issuer_linked_keys() -> list[str]:
  """Keys of the securities that pre-associate to the issuer graph."""
  return [s.key for s in SECURITIES if s.links_to_issuer]


def _preview() -> None:
  """Offline preview — the portfolio arc without the platform running."""
  print(f"\n{FUND_NAME}")
  print("=" * 68)
  print(f"  Portfolio:  {PORTFOLIO_NAME}")
  print(f"  Strategy:   {PORTFOLIO_STRATEGY}")
  print(f"  Inception:  {inception_date()}")

  print(f"\n  Securities ({len(SECURITIES)}):")
  for spec in SECURITIES:
    link = "  ← platform-native issuer" if spec.links_to_issuer else ""
    subtype = f"/{spec.security_subtype}" if spec.security_subtype else ""
    print(f"    {spec.name}")
    print(f"      {spec.security_type}{subtype}{link}")

  opening = sum(p.cost_basis for p in INITIAL_POSITIONS)
  print(f"\n  Opening block ({len(INITIAL_POSITIONS)} positions):")
  for pos in INITIAL_POSITIONS:
    spec = security_by_key(pos.security_key)
    print(
      f"    {spec.name[:44]:<44} "
      f"{pos.quantity:>12,.0f} {pos.quantity_type:<9} "
      f"${pos.cost_basis / 100:>14,.2f}"
    )
  print(f"    {'':<44} {'':>12} {'cost basis':<9} ${opening / 100:>14,.2f}")

  added = sum(p.cost_basis for p in ADDED_POSITIONS)
  print(f"\n  Deltas (one atomic update-portfolio-block call):")
  for pos in ADDED_POSITIONS:
    spec = security_by_key(pos.security_key)
    print(f"    add      {spec.name[:52]:<52} ${pos.cost_basis / 100:>14,.2f}")
  print(
    f"    update   {'Cadence Labs Series A — 409A refresh':<52} "
    f"${CADENCE_REMARK_VALUE / 100:>14,.2f}"
  )
  print(f"    dispose  {'Alder Grove Bio Series Seed':<52} {DISPOSED_REASON}")

  disposed = sum(
    p.cost_basis for p in INITIAL_POSITIONS if p.security_key == "aldergrove_seed"
  )
  print(f"\n  Ending cost basis: ${(opening + added - disposed) / 100:,.2f}")
  print(
    f"  Active positions:  "
    f"{len(INITIAL_POSITIONS) + len(ADDED_POSITIONS) - 1}\n"
  )


if __name__ == "__main__":
  _preview()
