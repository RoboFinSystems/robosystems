"""Driftline Coffee Roasters — the Season 1 Episode 1 synthetic company.

A ~3-year-old DTC specialty roaster that is *profitable but cash-poor*: the
P&L glows while cash drains into green-coffee inventory and one slow-paying
wholesale account (Summit Markets). The arc is authored as flow **parameters**
(recognition curves, collection lags, inventory pre-buys), not 16 months of
hand-written entries — the engine derives the balanced transactions and the
working-capital squeeze emerges mechanically from the recognition-vs-cash gap.

Run the offline arc preview (no platform needed) to see the story bite::

    uv run python -m examples.coffee_roaster_demo.data

This module re-exports the trio ``main.py`` consumes — ``ACCOUNTS``,
``get_all_transactions``, ``validate_transactions`` — plus ``SCENARIO`` for
the company metadata / schedules the platform wiring reads.
"""

from __future__ import annotations

from datetime import date

from examples._scenario.drivers import (
  CapexItem,
  CollectionPolicy,
  ExpenseRhythm,
  InventoryCycle,
  PrepaidItem,
  RevenueStream,
  Scenario,
)
from examples._scenario.engine import (
  add,
  emit,
  get_demo_start_date,
  ramp,
  staggered_prepaid,
  step,
  trailing_year,
  validate,
  window,
)

MONTHS = 16

# ---------------------------------------------------------------------------
# Chart of Accounts — (code, name, trait, sub_classification, balance_type, description)
# ---------------------------------------------------------------------------

ACCOUNTS: list[tuple[str, str, str, str, str, str | None]] = [
  # Assets
  ("1000", "Operating Checking", "asset", "cash_and_equivalents", "debit", None),
  (
    "1100",
    "Accounts Receivable — Wholesale",
    "asset",
    "accounts_receivable",
    "debit",
    None,
  ),
  ("1200", "Prepaid Insurance", "asset", "other_current_assets", "debit", None),
  ("1210", "Prepaid Software", "asset", "other_current_assets", "debit", None),
  ("1220", "Prepaid Fulfillment", "asset", "other_current_assets", "debit", None),
  ("1400", "Inventory — Green Coffee", "asset", "inventory", "debit", None),
  ("1410", "Inventory — Roasting WIP", "asset", "inventory", "debit", None),
  ("1420", "Inventory — Finished Goods", "asset", "inventory", "debit", None),
  ("1500", "Roasting & Packaging Equipment", "asset", "fixed_assets", "debit", None),
  ("1550", "Accumulated Depreciation", "asset", "fixed_assets", "credit", None),
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
  (
    "2300",
    "Deferred Subscription Revenue",
    "liability",
    "deferred_revenue",
    "credit",
    None,
  ),
  # Equity
  ("3000", "Paid-in Capital", "equity", "equity", "credit", None),
  ("3100", "Retained Earnings", "equity", "equity", "credit", None),
  # Revenue
  (
    "4000",
    "Subscription Revenue — DTC",
    "revenue",
    "operating_revenue",
    "credit",
    None,
  ),
  ("4100", "Wholesale Revenue", "revenue", "operating_revenue", "credit", None),
  (
    "4200",
    "Single-Order Revenue — DTC",
    "revenue",
    "operating_revenue",
    "credit",
    None,
  ),
  # Cost of goods sold
  (
    "5000",
    "Cost of Goods Sold — Coffee",
    "expense",
    "cost_of_goods_sold",
    "debit",
    None,
  ),
  # Operating expenses
  ("6000", "Salaries & Wages", "expense", "operating_expense", "debit", None),
  ("6100", "Roastery Rent", "expense", "operating_expense", "debit", None),
  ("6200", "Fulfillment & Shipping", "expense", "operating_expense", "debit", None),
  ("6300", "Marketing & Advertising", "expense", "operating_expense", "debit", None),
  ("6400", "Software & Subscriptions", "expense", "operating_expense", "debit", None),
  ("6500", "Insurance", "expense", "operating_expense", "debit", None),
  ("6600", "Utilities & Supplies", "expense", "operating_expense", "debit", None),
  ("7000", "Depreciation Expense", "expense", "operating_expense", "debit", None),
]

# ---------------------------------------------------------------------------
# The arc — four storylines, authored as curves
# ---------------------------------------------------------------------------

# DTC subscriptions — steady, growing, PREPAID (cash-positive ballast). Members
# pay a quarter ahead, so a deferred-revenue liability rides on the balance
# sheet and cash leads recognition.
SUBSCRIPTIONS = staggered_prepaid(
  "DTC Subscriptions",
  customers=["Driftline Subscribers"],
  revenue_account="4000",
  monthly_recognized=ramp(35_000_00, 52_000_00, MONTHS),
  cohorts=3,  # members prepay a quarter ahead; 3 phased cohorts smooth the cash
)

# DTC single web orders — immediate cash (healthy, no AR).
SINGLE_ORDERS = RevenueStream(
  name="DTC Single Orders",
  customers=["Driftline Web Orders"],
  revenue_account="4200",
  monthly_recognized=ramp(10_000_00, 16_000_00, MONTHS),
  collection=CollectionPolicy(kind="immediate"),
)

# Wholesale — Summit Markets. Signs at month 7, ramps hard into the holiday,
# and then slips net-30 → net-90 in the squeeze (months 10-14). The recent,
# large, stretched invoices stay un-discharged at the window edge → the
# concentrated, aged AR that Beat 4 traces.
SUMMIT = RevenueStream(
  name="Wholesale — Summit Markets",
  customers=["Summit Markets"],
  revenue_account="4100",
  monthly_recognized=step(
    MONTHS, base=0, **{"7": 22_000_00, "10": 38_000_00, "13": 45_000_00}
  ),
  collection=CollectionPolicy(
    kind="net_terms",
    default_lag_days=30,
    lag_days_by_offset={10: 75, 11: 80, 12: 85, 13: 90, 14: 90, 15: 90},
  ),
)

# Wholesale — a pool of regional cafés/grocers on clean net-30. Steady, small,
# diversified — the counterpoint that makes Summit's concentration legible.
CAFES = RevenueStream(
  name="Wholesale — Regional Cafés",
  customers=["Pioneer Square Cafés", "Emerald City Grocers", "Cascadia Coffee Bars"],
  revenue_account="4100",
  monthly_recognized=ramp(16_000_00, 26_000_00, MONTHS),
  collection=CollectionPolicy(kind="net_terms", default_lag_days=30),
)

# Green coffee — bought ahead of demand (cash out the door first), drawn down
# to COGS as it sells. Baseline replenishment plus two pre-buys: stocking up
# for the Summit launch (months 7-9) and the holiday season (months 10-14).
# The cumulative purchase-minus-consumption gap is the inventory build.
GREEN_COFFEE = InventoryCycle(
  name="Green Coffee",
  vendor="Andean Green Coffee Importers",
  inventory_account="1400",
  cogs_account="5000",
  # Consumption (COGS) tracks the revenue steps so gross margin stays ~55%;
  # purchases sit just above consumption and add two pre-buys, so inventory
  # builds steadily and surges when stocking up for Summit + the holidays.
  monthly_purchase=add(
    step(MONTHS, base=30_000_00, **{"7": 44_000_00, "10": 52_000_00, "13": 62_000_00}),
    window(MONTHS, 6_000_00, 7, 9),  # pre-buy ahead of the Summit launch
    window(MONTHS, 8_000_00, 10, 13),  # holiday pre-buy
  ),
  monthly_consumption=step(
    MONTHS, base=28_000_00, **{"7": 42_000_00, "10": 50_000_00, "13": 60_000_00}
  ),
)

# Operating expenses — mostly cash, scaling with the business. They eat most of
# the gross profit, so the company stays only modestly profitable — which is
# exactly why the working-capital drain, not a P&L loss, is the story.
# Headcount and ad spend step up as the business scales (with the Summit
# launch and the holiday push), rather than ramping from day one — so the
# baseline months stay comfortably profitable instead of bleeding.
EXPENSES = [
  ExpenseRhythm(
    "Salaries & Wages",
    "6000",
    step(MONTHS, base=14_000_00, **{"7": 18_000_00, "10": 24_000_00, "13": 28_000_00}),
    day=5,
  ),
  ExpenseRhythm(
    "Roastery Rent", "6100", 6_500_00, vendor="Ballard Industrial Properties", day=1
  ),
  ExpenseRhythm(
    "Fulfillment & Shipping",
    "6200",
    ramp(3_500_00, 9_000_00, MONTHS),
    vendor="Shipwise Fulfillment",
    day=15,
  ),
  ExpenseRhythm(
    "Marketing & Advertising",
    "6300",
    step(MONTHS, base=3_000_00, **{"7": 7_000_00, "10": 12_000_00, "13": 13_000_00}),
    day=10,
    vendor="Meta Platforms",
  ),
  ExpenseRhythm("Software & Subscriptions", "6400", 1_500_00, vendor="Shopify", day=1),
  ExpenseRhythm(
    "Utilities & Supplies", "6600", 1_500_00, vendor="Pacific Power & Light", day=12
  ),
]

# Capex — the roasting line is already owned at founding (still depreciating);
# an automated packaging line is bought at month 8 (more cash out, mid-squeeze).
CAPEX = [
  CapexItem(
    "Roasting & Packaging Line",
    "1500",
    "1550",
    "7000",
    cost=90_000_00,
    life_months=84,
    purchase_offset=0,
    in_opening=True,
  ),
  CapexItem(
    "Automated Packaging Line",
    "1500",
    "1550",
    "7000",
    cost=24_000_00,
    life_months=60,
    purchase_offset=8,
    vendor="Probat Roasters",
  ),
]

# Prepaids — annual insurance (with a year-two renewal) and an annual platform
# prepay, straight-lined into expense.
PREPAIDS = [
  PrepaidItem(
    "Business Insurance",
    "1200",
    "6500",
    cost=12_000_00,
    life_months=12,
    purchase_offset=1,
    vendor="Nationwide Insurance",
  ),
  PrepaidItem(
    "Business Insurance (renewal)",
    "1200",
    "6500",
    cost=12_000_00,
    life_months=12,
    purchase_offset=13,
    vendor="Nationwide Insurance",
  ),
  PrepaidItem(
    "Annual Platform Prepay",
    "1210",
    "6400",
    cost=6_000_00,
    life_months=12,
    purchase_offset=2,
    vendor="Shopify",
  ),
]


def _opening(start: date) -> list:
  """Founding balance sheet: cash, on-hand inventory across the three stages
  (green coffee, roasting WIP, bagged finished goods — the breakdown the
  inventory-components disclosure note foots), the roasting line (net of
  accumulated depreciation), and paid-in capital + retained earnings."""
  return [
    (
      start,
      "journal_entry",
      "Opening balances — founding equity & assets",
      None,
      [
        ("1000", 60_000_00, 0),  # Operating cash
        ("1400", 8_000_00, 0),  # Green-coffee inventory on hand
        ("1410", 3_000_00, 0),  # Roasting WIP on the floor
        ("1420", 5_000_00, 0),  # Bagged finished goods
        ("1500", 90_000_00, 0),  # Roasting & packaging equipment (gross)
        ("1550", 0, 30_000_00),  # Accumulated depreciation
        ("3000", 0, 100_000_00),  # Paid-in capital
        ("3100", 0, 36_000_00),  # Retained earnings
      ],
    ),
  ]


def build_scenario(start: date) -> Scenario:
  """Assemble the Driftline scenario for a given evergreen start date."""
  return Scenario(
    slug="coffee_roaster",
    company_name="Driftline Coffee Roasters",
    entity_type="corporation",
    ticker="DRIFT",
    uri="https://driftlinecoffee.example.com",
    description="DTC + wholesale specialty coffee roaster — close-workflow showcase",
    accounts=ACCOUNTS,
    cash_account="1000",
    opening=_opening(start),
    revenue=[*SUBSCRIPTIONS, SINGLE_ORDERS, SUMMIT, CAFES],
    inventory=[GREEN_COFFEE],
    expenses=EXPENSES,
    capex=CAPEX,
    prepaids=PREPAIDS,
    months=MONTHS,
    # Trailing 12 months ending at the close target — captures the squeeze,
    # which lands in the most recent months of the rolling window.
    report_period=trailing_year(start, MONTHS),
  )


# Module-level scenario at the evergreen start — read by main.py for company
# metadata and schedule construction.
SCENARIO = build_scenario(get_demo_start_date(MONTHS))


# ---------------------------------------------------------------------------
# The trio main.py consumes
# ---------------------------------------------------------------------------


def get_all_transactions(start: date | None = None) -> list:
  """All balanced transaction tuples for the rolling window."""
  start = start or get_demo_start_date(MONTHS)
  return emit(build_scenario(start), start)


def validate_transactions(txns: list) -> bool:
  """Verify every entry balances (Σ debits == Σ credits)."""
  return validate(txns)


if __name__ == "__main__":
  from examples._scenario.engine import print_summary

  start = get_demo_start_date(MONTHS)
  txns = get_all_transactions(start)
  print(f"Driftline Coffee Roasters — {len(txns)} transactions over {MONTHS} months")
  print("All entries balance:", validate_transactions(txns))
  print_summary(SCENARIO, start)
