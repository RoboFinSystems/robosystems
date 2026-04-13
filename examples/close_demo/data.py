"""Synthetic chart of accounts and transactions for Cascade Advisory Group LLC.

Clean double-entry data — every entry has balanced DR/CR line items.
No CSV parsing, no QB General Ledger one-sided entries.

Dates are generated relative to "today" so the demo stays evergreen:
data spans a rolling 16-month window ending at the current month.
"""

from __future__ import annotations

from datetime import date

# ---------------------------------------------------------------------------
# Evergreen reference dates
# ---------------------------------------------------------------------------

DEMO_MONTHS = 16  # rolling window of synthetic data


def add_months(d: date, months: int) -> date:
  """Return the first day of the month that is `months` after d."""
  total = d.month - 1 + months
  year = d.year + total // 12
  month = total % 12 + 1
  return date(year, month, 1)


_MONTH_END_DAYS = {
  1: 31,
  2: 28,
  3: 31,
  4: 30,
  5: 31,
  6: 30,
  7: 31,
  8: 31,
  9: 30,
  10: 31,
  11: 30,
  12: 31,
}


def _last_day_of_month(year: int, month: int) -> int:
  """Last day of the month (non-leap; close enough for Feb in a demo)."""
  return _MONTH_END_DAYS[month]


def get_demo_start_date(today: date | None = None) -> date:
  """First day of the first month in the rolling demo window.

  The window ends at the current month so a user running the demo in April 2026
  sees data through April 2026, and a user running it in June 2027 sees data
  through June 2027.
  """
  today = today or date.today()
  current_month_start = date(today.year, today.month, 1)
  return add_months(current_month_start, -(DEMO_MONTHS - 1))


def get_month_date(start: date, offset: int, day: int = 1) -> date:
  """Return a date `offset` months after start on the given day-of-month,
  clamped to the last day of that month."""
  month_start = add_months(start, offset)
  end_day = _last_day_of_month(month_start.year, month_start.month)
  return date(month_start.year, month_start.month, min(day, end_day))


# ---------------------------------------------------------------------------
# Chart of Accounts
# ---------------------------------------------------------------------------

ACCOUNTS = [
  # code, name, classification, sub_classification, balance_type, parent_code
  # Assets
  ("1000", "Operating Checking", "asset", "cash_and_equivalents", "debit", None),
  ("1100", "Accounts Receivable", "asset", "accounts_receivable", "debit", None),
  ("1200", "Prepaid Insurance", "asset", "other_current_assets", "debit", None),
  ("1210", "Prepaid Software", "asset", "other_current_assets", "debit", None),
  ("1220", "Prepaid Cloud Hosting", "asset", "other_current_assets", "debit", None),
  ("1300", "Computer Equipment", "asset", "fixed_assets", "debit", None),
  ("1310", "Office Furniture", "asset", "fixed_assets", "debit", None),
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
  (
    "2200",
    "Payroll Taxes Payable",
    "liability",
    "other_current_liabilities",
    "credit",
    None,
  ),
  # Equity
  ("3000", "Owner's Equity", "equity", "equity", "credit", None),
  ("3100", "Retained Earnings", "equity", "equity", "credit", None),
  # Revenue
  ("4000", "Consulting Revenue", "revenue", "operating_revenue", "credit", None),
  ("4100", "Strategy Advisory Revenue", "revenue", "operating_revenue", "credit", None),
  (
    "4200",
    "Implementation Services Revenue",
    "revenue",
    "operating_revenue",
    "credit",
    None,
  ),
  # Expenses
  ("5000", "Salaries & Wages", "expense", "operating_expense", "debit", None),
  ("5100", "Payroll Taxes", "expense", "operating_expense", "debit", None),
  ("5200", "Health Insurance", "expense", "operating_expense", "debit", None),
  ("6000", "Office Rent", "expense", "operating_expense", "debit", None),
  ("6100", "Software Subscriptions", "expense", "operating_expense", "debit", None),
  ("6200", "Cloud Hosting", "expense", "operating_expense", "debit", None),
  ("6300", "Professional Development", "expense", "operating_expense", "debit", None),
  ("6400", "Business Insurance", "expense", "operating_expense", "debit", None),
  ("6500", "Office Supplies", "expense", "operating_expense", "debit", None),
  ("6600", "Travel & Entertainment", "expense", "operating_expense", "debit", None),
  ("7000", "Depreciation Expense", "expense", "operating_expense", "debit", None),
]


# ---------------------------------------------------------------------------
# Revenue growth curve (keyed by month offset 0..15)
# ---------------------------------------------------------------------------
# (client_name, revenue_account_code, amount_cents)

_REVENUE_BY_OFFSET: dict[int, list[tuple[str, str, int]]] = {
  # Year 1: ramping up
  0: [("TechCorp Inc", "4000", 6_000_00), ("Global Dynamics LLC", "4100", 3_000_00)],
  1: [
    ("TechCorp Inc", "4000", 7_000_00),
    ("Global Dynamics LLC", "4100", 3_500_00),
    ("Meridian Capital", "4200", 2_000_00),
  ],
  2: [
    ("TechCorp Inc", "4000", 7_500_00),
    ("Global Dynamics LLC", "4100", 4_000_00),
    ("Meridian Capital", "4200", 2_500_00),
  ],
  3: [
    ("TechCorp Inc", "4000", 8_000_00),
    ("Global Dynamics LLC", "4100", 4_500_00),
    ("Meridian Capital", "4200", 3_000_00),
  ],
  4: [
    ("TechCorp Inc", "4000", 8_500_00),
    ("Global Dynamics LLC", "4100", 5_000_00),
    ("Meridian Capital", "4200", 3_000_00),
  ],
  5: [
    ("TechCorp Inc", "4000", 9_000_00),
    ("Global Dynamics LLC", "4100", 5_000_00),
    ("Meridian Capital", "4200", 3_500_00),
  ],
  6: [
    ("TechCorp Inc", "4000", 8_000_00),
    ("Global Dynamics LLC", "4100", 4_500_00),
    ("Meridian Capital", "4200", 3_000_00),
  ],  # summer dip
  7: [
    ("TechCorp Inc", "4000", 7_500_00),
    ("Global Dynamics LLC", "4100", 4_000_00),
    ("Meridian Capital", "4200", 3_000_00),
  ],
  8: [
    ("TechCorp Inc", "4000", 9_500_00),
    ("Global Dynamics LLC", "4100", 5_500_00),
    ("Meridian Capital", "4200", 3_500_00),
  ],
  9: [
    ("TechCorp Inc", "4000", 10_000_00),
    ("Global Dynamics LLC", "4100", 6_000_00),
    ("Meridian Capital", "4200", 4_000_00),
  ],
  10: [
    ("TechCorp Inc", "4000", 10_500_00),
    ("Global Dynamics LLC", "4100", 6_500_00),
    ("Meridian Capital", "4200", 4_500_00),
  ],
  11: [
    ("TechCorp Inc", "4000", 9_000_00),
    ("Global Dynamics LLC", "4100", 5_000_00),
    ("Meridian Capital", "4200", 3_500_00),
  ],
  # Year 2: established
  12: [
    ("TechCorp Inc", "4000", 8_500_00),
    ("Global Dynamics LLC", "4100", 5_500_00),
    ("Meridian Capital", "4200", 4_000_00),
  ],
  13: [
    ("TechCorp Inc", "4000", 10_000_00),
    ("Global Dynamics LLC", "4100", 6_000_00),
    ("Meridian Capital", "4200", 4_500_00),
  ],
  14: [
    ("TechCorp Inc", "4000", 11_000_00),
    ("Global Dynamics LLC", "4100", 7_000_00),
    ("Meridian Capital", "4200", 4_000_00),
  ],
  15: [
    ("TechCorp Inc", "4000", 9_000_00),
    ("Global Dynamics LLC", "4100", 5_000_00),
    ("Meridian Capital", "4200", 5_000_00),
  ],
}

# Variable expense patterns by month offset (cents)
_SUPPLIES_BY_OFFSET = {
  0: 85_00,
  1: 120_00,
  2: 95_00,
  3: 110_00,
  4: 75_00,
  5: 130_00,
  6: 90_00,
  7: 100_00,
  8: 85_00,
  9: 115_00,
  10: 95_00,
  11: 140_00,
  12: 90_00,
  13: 105_00,
  14: 100_00,
  15: 120_00,
}
_TRAVEL_BY_OFFSET = {
  0: 250_00,
  1: 450_00,
  2: 350_00,
  3: 200_00,
  4: 500_00,
  5: 300_00,
  6: 150_00,
  7: 200_00,
  8: 600_00,
  9: 400_00,
  10: 350_00,
  11: 100_00,
  12: 300_00,
  13: 450_00,
  14: 400_00,
  15: 250_00,
}
_PROF_DEV_BY_OFFSET = {
  0: 0,
  1: 150_00,
  2: 300_00,
  3: 0,
  4: 200_00,
  5: 0,
  6: 250_00,
  7: 0,
  8: 0,
  9: 400_00,
  10: 0,
  11: 0,
  12: 150_00,
  13: 0,
  14: 350_00,
  15: 0,
}


# ---------------------------------------------------------------------------
# Transaction generators
# ---------------------------------------------------------------------------


def _opening_balances(start: date) -> list[tuple]:
  """Founding balance sheet on day 1 of month 0."""
  return [
    (
      start,
      "journal_entry",
      "Opening balances — founding contribution",
      None,
      [
        ("1000", 30_000_00, 0),  # Cash: $30,000
        ("1300", 4_800_00, 0),  # Computer equipment: $4,800
        ("3000", 0, 34_800_00),  # Owner's equity: $34,800
      ],
    ),
  ]


def _annual_events(start: date) -> list[tuple]:
  """One-time and annual purchases anchored to month offsets from start."""
  return [
    # Month 2: Office furniture purchased + insurance policy starts
    (
      get_month_date(start, 2, 5),
      "bill_payment",
      "Office furniture (standing desk + chair)",
      "Herman Miller",
      [("1310", 1_500_00, 0), ("1000", 0, 1_500_00)],
    ),
    (
      get_month_date(start, 2, 1),
      "bill_payment",
      "Annual business insurance policy",
      "Nationwide Insurance",
      [("1200", 1_200_00, 0), ("1000", 0, 1_200_00)],
    ),
    # Month 5: Software license annual purchase
    (
      get_month_date(start, 5, 1),
      "bill_payment",
      "Annual software license (Basecamp)",
      "Basecamp",
      [("1210", 300_00, 0), ("1000", 0, 300_00)],
    ),
    # Month 8: AWS Savings Plan annual commitment
    (
      get_month_date(start, 8, 1),
      "bill_payment",
      "AWS Savings Plan (1-year commitment)",
      "Amazon Web Services",
      [("1220", 600_00, 0), ("1000", 0, 600_00)],
    ),
    # Month 3: Owner's additional capital contribution
    (
      get_month_date(start, 3, 15),
      "journal_entry",
      "Owner additional capital contribution",
      None,
      [("1000", 15_000_00, 0), ("3000", 0, 15_000_00)],
    ),
    # Month 14: Insurance renewal (second year)
    (
      get_month_date(start, 14, 1),
      "bill_payment",
      "Annual business insurance renewal",
      "Nationwide Insurance",
      [("1200", 1_200_00, 0), ("1000", 0, 1_200_00)],
    ),
  ]


def _monthly_transactions(start: date, offset: int) -> list[tuple]:
  """Generate one month's transactions for the given month offset."""
  month_start = add_months(start, offset)
  year, m = month_start.year, month_start.month
  end_day = _last_day_of_month(year, m)
  is_first_month = offset == 0

  revenue = _REVENUE_BY_OFFSET.get(offset, [])
  supplies = _SUPPLIES_BY_OFFSET.get(offset, 100_00)
  travel = _TRAVEL_BY_OFFSET.get(offset, 200_00)
  prof_dev = _PROF_DEV_BY_OFFSET.get(offset, 0)

  txns: list[tuple] = []

  # 1st — Rent
  txns.append(
    (
      date(year, m, 1),
      "bill_payment",
      f"Office rent - {date(year, m, 1).strftime('%B %Y')}",
      "Cascade Business Center",
      [("6000", 2_500_00, 0), ("1000", 0, 2_500_00)],
    )
  )

  # 5th — Payroll
  txns.append(
    (
      date(year, m, 5),
      "journal_entry",
      f"Payroll - {date(year, m, 1).strftime('%B %Y')}",
      None,
      [
        ("5000", 8_000_00, 0),
        ("5100", 800_00, 0),
        ("5200", 700_00, 0),
        ("1000", 0, 8_700_00),
        ("2200", 0, 800_00),
      ],
    )
  )

  # 8th — Payroll tax payment (prior month's payable)
  if not is_first_month:
    txns.append(
      (
        date(year, m, 8),
        "bill_payment",
        "Payroll tax deposit - prior month",
        "IRS/State",
        [("2200", 800_00, 0), ("1000", 0, 800_00)],
      )
    )

  # Revenue invoices and payments
  for client_name, rev_account, amount in revenue:
    inv_day = 10 if rev_account == "4000" else (12 if rev_account == "4100" else 18)
    txns.append(
      (
        date(year, m, inv_day),
        "invoice",
        f"Consulting services - {client_name}",
        client_name,
        [("1100", amount, 0), (rev_account, 0, amount)],
      )
    )
    pay_day = min(inv_day + 8, end_day)
    txns.append(
      (
        date(year, m, pay_day),
        "payment",
        f"Payment received - {client_name}",
        client_name,
        [("1000", amount, 0), ("1100", 0, amount)],
      )
    )

  # Monthly software subscriptions
  txns.append(
    (
      date(year, m, 1),
      "bill_payment",
      "Monthly SaaS tools (Slack, Zoom, etc.)",
      "Various",
      [("6100", 450_00, 0), ("1000", 0, 450_00)],
    )
  )

  # Monthly cloud hosting
  txns.append(
    (
      date(year, m, 1),
      "bill_payment",
      "AWS monthly usage",
      "Amazon Web Services",
      [("6200", 180_00, 0), ("1000", 0, 180_00)],
    )
  )

  # Office supplies
  txns.append(
    (
      date(year, m, 15),
      "bill_payment",
      "Office supplies",
      "Amazon",
      [("6500", supplies, 0), ("1000", 0, supplies)],
    )
  )

  # Travel
  if travel > 0:
    txns.append(
      (
        date(year, m, min(20, end_day)),
        "bill_payment",
        "Travel & meals",
        "Various",
        [("6600", travel, 0), ("1000", 0, travel)],
      )
    )

  # Professional development
  if prof_dev > 0:
    txns.append(
      (
        date(year, m, min(22, end_day)),
        "bill_payment",
        "Professional development",
        "Various",
        [("6300", prof_dev, 0), ("1000", 0, prof_dev)],
      )
    )

  return txns


def get_all_transactions(start: date | None = None) -> list[tuple]:
  """Return all transactions for the rolling DEMO_MONTHS window starting at `start`
  (defaults to `get_demo_start_date()`).
  """
  start = start or get_demo_start_date()
  txns = _opening_balances(start)
  txns.extend(_annual_events(start))
  for offset in range(DEMO_MONTHS):
    txns.extend(_monthly_transactions(start, offset))
  txns.sort(key=lambda t: t[0])
  return txns


def validate_transactions(txns: list[tuple]) -> bool:
  """Verify every entry balances (sum debits == sum credits)."""
  ok = True
  for txn_date, _txn_type, desc, _merchant, lines in txns:
    total_dr = sum(dr for _, dr, _ in lines)
    total_cr = sum(cr for _, _, cr in lines)
    if total_dr != total_cr:
      print(f"  IMBALANCED: {txn_date} {desc}: DR={total_dr} CR={total_cr}")
      ok = False
  return ok
