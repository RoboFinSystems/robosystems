"""Cadence Labs — Season 1, Episode 2 synthetic company (B2B SaaS startup).

A seed-funded B2B SaaS business: high-margin recurring revenue growing fast,
but spending heavily on engineering (R&D) and sales (S&M), so it loses money
every month — it *burns*. Customers sign **annual** contracts and pay up
front, so a large deferred-revenue liability rides on the balance sheet and
the prepayment float softens the cash decline.

The reveal (Beat 4): the bank balance looks like comfortable runway, but a big
slice of that cash is **deferred revenue** — service already sold and owed.
Net it out and the real runway at the current operating burn is far shorter —
and the prepayment float that's been masking the burn shrinks as growth slows.

Same engine as the coffee roaster — this episode is just a different set of
flow parameters. Offline arc preview::

    uv run python -m examples.saas_startup_demo.data
"""

from __future__ import annotations

from datetime import date

from examples._scenario.drivers import (
  CapexItem,
  CollectionPolicy,
  ExpenseRhythm,
  PrepaidItem,
  RevenueStream,
  Scenario,
)
from examples._scenario.engine import (
  emit,
  get_demo_start_date,
  ramp,
  staggered_prepaid,
  validate,
)

MONTHS = 16

# ---------------------------------------------------------------------------
# Chart of Accounts — (code, name, trait, sub_classification, balance_type, description)
# ---------------------------------------------------------------------------

ACCOUNTS: list[tuple[str, str, str, str, str, str | None]] = [
  # Assets
  ("1000", "Cash", "asset", "cash_and_equivalents", "debit", None),
  ("1100", "Accounts Receivable", "asset", "accounts_receivable", "debit", None),
  ("1200", "Prepaid Software", "asset", "other_current_assets", "debit", None),
  ("1210", "Prepaid Insurance", "asset", "other_current_assets", "debit", None),
  ("1300", "Computer Equipment", "asset", "fixed_assets", "debit", None),
  ("1350", "Accumulated Depreciation", "asset", "fixed_assets", "credit", None),
  # Liabilities
  ("2000", "Accounts Payable", "liability", "accounts_payable", "credit", None),
  (
    "2100",
    "Accrued Liabilities",
    "liability",
    "other_current_liabilities",
    "credit",
    None,
  ),
  ("2300", "Deferred Revenue", "liability", "deferred_revenue", "credit", None),
  # Equity
  ("3000", "Paid-in Capital", "equity", "equity", "credit", None),
  ("3100", "Accumulated Deficit", "equity", "equity", "credit", None),
  # Revenue
  ("4000", "Subscription Revenue", "revenue", "operating_revenue", "credit", None),
  ("4100", "Professional Services", "revenue", "operating_revenue", "credit", None),
  # Cost of revenue
  ("5000", "Cost of Revenue", "expense", "cost_of_goods_sold", "debit", None),
  # Operating expenses
  ("6000", "Research & Development", "expense", "operating_expense", "debit", None),
  ("6100", "Sales & Marketing", "expense", "operating_expense", "debit", None),
  ("6200", "General & Administrative", "expense", "operating_expense", "debit", None),
  ("6300", "Rent", "expense", "operating_expense", "debit", None),
  ("6400", "Software & Tools", "expense", "operating_expense", "debit", None),
  ("7000", "Depreciation Expense", "expense", "operating_expense", "debit", None),
]

# B2B customer logos — annual contracts (prepaid) + onboarding services (net-30).
LOGOS = [
  "Northwind Trading",
  "Atlas Manufacturing",
  "Brightline Health",
  "Cobalt Logistics",
  "Summit Retail Group",
  "Vantage Financial",
  "Kestrel Media",
  "Onyx Robotics",
]

# ---------------------------------------------------------------------------
# The arc — recurring revenue up and to the right, but burning cash
# ---------------------------------------------------------------------------

# Subscriptions — annual contracts, paid up front (cohorts=12 → smooth monthly
# cash + a large deferred-revenue liability). Recognized MRR ramps as ARR grows.
SUBSCRIPTIONS = staggered_prepaid(
  "Subscriptions",
  customers=LOGOS,
  revenue_account="4000",
  monthly_recognized=ramp(55_000_00, 128_000_00, MONTHS),
  cohorts=12,
)

# Professional services / onboarding — net-30, builds a little AR.
SERVICES = RevenueStream(
  name="Professional Services",
  customers=LOGOS[:4],
  revenue_account="4100",
  monthly_recognized=ramp(4_000_00, 10_000_00, MONTHS),
  collection=CollectionPolicy(kind="net_terms", default_lag_days=30),
)

# Cost of revenue (hosting + customer support) — ~22% of revenue, ~78% margin.
COST_OF_REVENUE = ExpenseRhythm(
  "Cost of Revenue",
  "5000",
  ramp(12_000_00, 30_000_00, MONTHS),
  vendor="Amazon Web Services",
  day=2,
)

# The burn — engineering and go-to-market spend that swamps gross profit.
EXPENSES = [
  COST_OF_REVENUE,
  ExpenseRhythm(
    "Research & Development", "6000", ramp(40_000_00, 85_000_00, MONTHS), day=5
  ),
  ExpenseRhythm(
    "Sales & Marketing — Team", "6100", ramp(30_000_00, 62_000_00, MONTHS), day=5
  ),
  ExpenseRhythm(
    "Sales & Marketing — Ads",
    "6100",
    ramp(8_000_00, 18_000_00, MONTHS),
    vendor="LinkedIn",
    day=10,
  ),
  ExpenseRhythm(
    "General & Administrative", "6200", ramp(15_000_00, 26_000_00, MONTHS), day=5
  ),
  ExpenseRhythm("Rent", "6300", 9_000_00, vendor="Flatiron Workspaces", day=1),
  ExpenseRhythm(
    "Software & Tools",
    "6400",
    ramp(5_000_00, 9_000_00, MONTHS),
    vendor="Datadog",
    day=1,
  ),
]

# Capex — laptops/equipment owned at founding, plus an office build-out mid-year.
CAPEX = [
  CapexItem(
    "Computer Equipment",
    "1300",
    "1350",
    "7000",
    cost=80_000_00,
    life_months=36,
    purchase_offset=0,
    in_opening=True,
  ),
  CapexItem(
    "Office Build-out",
    "1300",
    "1350",
    "7000",
    cost=40_000_00,
    life_months=60,
    purchase_offset=6,
    vendor="Flatiron Workspaces",
  ),
]

# Prepaids — annual tooling and insurance, amortized straight-line.
PREPAIDS = [
  PrepaidItem(
    "Annual Tooling Prepay",
    "1200",
    "6400",
    cost=24_000_00,
    life_months=12,
    purchase_offset=0,
    vendor="Datadog",
  ),
  PrepaidItem(
    "Annual Tooling Prepay (renewal)",
    "1200",
    "6400",
    cost=24_000_00,
    life_months=12,
    purchase_offset=12,
    vendor="Datadog",
  ),
  PrepaidItem(
    "Business Insurance",
    "1210",
    "6200",
    cost=18_000_00,
    life_months=12,
    purchase_offset=1,
    vendor="Vouch Insurance",
  ),
  PrepaidItem(
    "Business Insurance (renewal)",
    "1210",
    "6200",
    cost=18_000_00,
    life_months=12,
    purchase_offset=13,
    vendor="Vouch Insurance",
  ),
]


def _opening(start: date) -> list:
  """Post-raise opening balance sheet: cash from the round, the equipment, an
  existing book of annual contracts (deferred revenue), paid-in capital, and
  the accumulated deficit already burned pre-window."""
  return [
    (
      start,
      "journal_entry",
      "Opening balances — post-seed-round equity & assets",
      None,
      [
        ("1000", 2_530_000_00, 0),  # Cash on hand after the raise
        ("1300", 80_000_00, 0),  # Computer equipment (gross)
        ("1350", 0, 25_000_00),  # Accumulated depreciation
        ("2300", 0, 450_000_00),  # Deferred revenue — existing annual contracts
        ("3000", 0, 2_835_000_00),  # Paid-in capital
        ("3100", 700_000_00, 0),  # Accumulated deficit (debit)
      ],
    ),
  ]


def build_scenario(start: date) -> Scenario:
  return Scenario(
    slug="saas_startup",
    company_name="Cadence Labs, Inc.",
    entity_type="corporation",
    ticker="CDNC",
    uri="https://cadencelabs.example.com",
    description="Seed-funded B2B SaaS — recurring revenue, annual prepay, burning cash",
    accounts=ACCOUNTS,
    cash_account="1000",
    opening=_opening(start),
    revenue=[*SUBSCRIPTIONS, SERVICES],
    expenses=EXPENSES,
    capex=CAPEX,
    prepaids=PREPAIDS,
    months=MONTHS,
    report_period=(date(2025, 1, 1), date(2025, 12, 31)),
  )


SCENARIO = build_scenario(get_demo_start_date(MONTHS))


def get_all_transactions(start: date | None = None) -> list:
  start = start or get_demo_start_date(MONTHS)
  return emit(build_scenario(start), start)


def validate_transactions(txns: list) -> bool:
  return validate(txns)


if __name__ == "__main__":
  from examples._scenario.engine import print_summary

  start = get_demo_start_date(MONTHS)
  txns = get_all_transactions(start)
  print(f"Cadence Labs, Inc. — {len(txns)} transactions over {MONTHS} months")
  print("All entries balance:", validate_transactions(txns))
  print_summary(SCENARIO, start)
