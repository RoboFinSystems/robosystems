"""Scenario engine — lowers declarative flows to balanced transaction tuples.

The public surface an episode's ``data.py`` re-exports:

- ``emit(scenario, start)``        → sorted list of balanced ``Txn`` tuples
- ``validate(txns)``               → True iff every entry balances
- ``monthly_summary(scenario)``    → per-month P&L-vs-cash rows (articulation proof)
- ``print_summary(scenario)``      → renders that table to stdout

Plus date helpers (``get_demo_start_date``, ``add_months``, ``month_date``) and
curve builders (``flat``, ``ramp``, ``step``, ``window``) for authoring the
``dict[int, int]`` driver curves.
"""

from __future__ import annotations

from datetime import date, timedelta

from .drivers import (
  CapexItem,
  CollectionPolicy,
  ExpenseRhythm,
  InventoryCycle,
  PrepaidItem,
  RevenueStream,
  Scenario,
  Txn,
)

# ---------------------------------------------------------------------------
# Evergreen date helpers (rolling window ending at the current month)
# ---------------------------------------------------------------------------

_MONTH_END_DAYS = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
                   7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}  # fmt: skip


def add_months(d: date, months: int) -> date:
  """First day of the month ``months`` after ``d``."""
  total = d.month - 1 + months
  return date(d.year + total // 12, total % 12 + 1, 1)


def last_day_of_month(year: int, month: int) -> int:
  """Last day of the month (non-leap; close enough for a demo)."""
  return _MONTH_END_DAYS[month]


def month_date(start: date, offset: int, day: int = 1) -> date:
  """Date ``offset`` months after ``start`` on ``day`` (clamped to month end)."""
  ms = add_months(start, offset)
  return date(ms.year, ms.month, min(day, last_day_of_month(ms.year, ms.month)))


def get_demo_start_date(months: int, today: date | None = None) -> date:
  """First day of the first month in a rolling ``months``-long window ending
  at the current month, so the demo always covers recent history."""
  today = today or date.today()
  return add_months(date(today.year, today.month, 1), -(months - 1))


# ---------------------------------------------------------------------------
# Curve builders — author driver curves as intent, not 16 literals
# ---------------------------------------------------------------------------


def flat(value: int, months: int) -> dict[int, int]:
  """Constant ``value`` every month."""
  return dict.fromkeys(range(months), value)


def ramp(first: int, last: int, months: int) -> dict[int, int]:
  """Linear ramp from ``first`` (offset 0) to ``last`` (offset months-1)."""
  if months <= 1:
    return {0: first}
  step_amt = (last - first) / (months - 1)
  return {off: round(first + step_amt * off) for off in range(months)}


def step(months: int, base: int, **at: int) -> dict[int, int]:
  """Piecewise-constant curve. ``base`` until the first breakpoint, then each
  ``at={offset: value}`` holds from that offset onward.

  ``step(16, base=0, **{"7": 50_00, "10": 80_00})`` → 0 through 6, 50.00 at
  7-9, 80.00 from 10 on.
  """
  breaks = sorted((int(k), v) for k, v in at.items())
  out: dict[int, int] = {}
  for off in range(months):
    value = base
    for bpoint, bval in breaks:
      if off >= bpoint:
        value = bval
    out[off] = value
  return out


def window(months: int, value: int, lo: int, hi: int) -> dict[int, int]:
  """``value`` for offsets in [lo, hi], else 0 (a one-off bump, e.g. a pre-buy)."""
  return {off: (value if lo <= off <= hi else 0) for off in range(months)}


def add(*curves: dict[int, int]) -> dict[int, int]:
  """Sum several curves offset-by-offset."""
  out: dict[int, int] = {}
  for curve in curves:
    for off, value in curve.items():
      out[off] = out.get(off, 0) + value
  return out


def scale(curve: dict[int, int], num: int, den: int = 1) -> dict[int, int]:
  """Scale a curve by ``num/den`` (integer cents)."""
  return {off: value * num // den for off, value in curve.items()}


def staggered_prepaid(
  name: str,
  customers: list[str],
  revenue_account: str,
  monthly_recognized: dict[int, int],
  *,
  cohorts: int = 3,
  deferred_account: str = "2300",
  cash_account: str = "1000",
) -> list[RevenueStream]:
  """Split a prepaid subscription base into ``cohorts`` phase-shifted streams.

  Each cohort carries ``1/cohorts`` of the run-rate and bills every
  ``cohorts`` months on its own phase, so exactly one cohort bills each month.
  The result is smooth monthly cash (no quarterly lumps) plus a steady
  deferred-revenue liability — the realistic shape of a monthly+prepaid
  subscription book. Returns a list to splice into ``Scenario.revenue``.
  """
  return [
    RevenueStream(
      name=f"{name} (cohort {k + 1}/{cohorts})",
      customers=customers,
      revenue_account=revenue_account,
      monthly_recognized=scale(monthly_recognized, 1, cohorts),
      collection=CollectionPolicy(kind="prepaid", bill_every=cohorts, bill_phase=k),
      deferred_account=deferred_account,
      cash_account=cash_account,
    )
    for k in range(cohorts)
  ]


# ---------------------------------------------------------------------------
# Lowering: drivers → balanced transaction tuples
# ---------------------------------------------------------------------------


def _split(total: int, n: int) -> list[int]:
  """Split ``total`` cents into ``n`` parts (remainder onto the first)."""
  if n <= 0:
    return []
  base = total // n
  rem = total - base * n
  return [base + (rem if i == 0 else 0) for i in range(n)]


def _emit_accrual_revenue(
  stream: RevenueStream, total: int, off: int, start: date, window_end: date
) -> list[Txn]:
  """One offset of an immediate/net_terms stream: an invoice per customer,
  and a matching payment unless the collection lag falls outside the window
  (in which case the invoice stays open → AR aging)."""
  out: list[Txn] = []
  pol = stream.collection
  inv_date = month_date(start, off, stream.invoice_day)
  lag = (
    0
    if pol.kind == "immediate"
    else pol.lag_days_by_offset.get(off, pol.default_lag_days)
  )
  for cust, amt in zip(
    stream.customers, _split(total, len(stream.customers)), strict=True
  ):
    if amt <= 0:
      continue
    out.append(
      (
        inv_date,
        "invoice",
        f"{stream.name} invoice — {cust}",
        cust,
        [(stream.ar_account, amt, 0), (stream.revenue_account, 0, amt)],
      )
    )
    pay_date = inv_date + timedelta(days=lag)
    if pay_date <= window_end:
      out.append(
        (
          pay_date,
          "payment",
          f"Payment received — {cust}",
          cust,
          [(stream.cash_account, amt, 0), (stream.ar_account, 0, amt)],
        )
      )
  return out


def _billing_offsets(phase: int, every: int, months: int) -> list[int]:
  """Offsets where this cohort bills. Always starts at 0 (priming an
  in-progress cohort at founding so it recognizes from month 0), then bills on
  its phase cadence."""
  offs = [0]
  nxt = phase if phase > 0 else every
  while nxt < months:
    offs.append(nxt)
    nxt += every
  return offs


def _emit_prepaid_revenue(stream: RevenueStream, start: date, months: int) -> list[Txn]:
  """A prepaid (deferred-revenue) stream: each billing collects cash for the
  whole period it covers (DR cash / CR deferred), then recognizes the in-window
  months one at a time (DR deferred / CR revenue). The final billing commits a
  full period even past the data window — its out-of-window months stay in
  deferred (cash received, service not yet delivered), so the deferred balance
  is always ≥ 0 and never craters at the window edge."""
  out: list[Txn] = []
  pol = stream.collection
  cust = stream.customers[0] if stream.customers else None
  offs = _billing_offsets(pol.bill_phase, pol.bill_every, months)
  last_rate = stream.monthly_recognized.get(months - 1, 0)

  for idx, off in enumerate(offs):
    # The priming billing covers up to the first phase billing; a regular
    # billing covers a full cadence (the final one extends past the window).
    nxt = offs[idx + 1] if idx + 1 < len(offs) else off + pol.bill_every
    cash_period = range(off, nxt)
    cash = sum(
      stream.monthly_recognized.get(m, 0) if m < months else last_rate
      for m in cash_period
    )
    if cash <= 0:
      continue
    out.append(
      (
        month_date(start, off, 1),
        "payment",
        f"{stream.name} — prepaid billing",
        cust,
        [(stream.cash_account, cash, 0), (stream.deferred_account, 0, cash)],
      )
    )
    for m in cash_period:
      if m >= months:
        continue  # recognized beyond the window — stays in deferred
      amt = stream.monthly_recognized.get(m, 0)
      if amt > 0:
        out.append(
          (
            month_date(start, m, 28),
            "journal_entry",
            f"{stream.name} — revenue recognized",
            None,
            [(stream.deferred_account, amt, 0), (stream.revenue_account, 0, amt)],
          )
        )
  return out


def _emit_revenue(
  stream: RevenueStream, start: date, months: int, window_end: date
) -> list[Txn]:
  if stream.collection.kind == "prepaid":
    return _emit_prepaid_revenue(stream, start, months)
  out: list[Txn] = []
  for off in range(months):
    total = stream.monthly_recognized.get(off, 0)
    if total > 0:
      out += _emit_accrual_revenue(stream, total, off, start, window_end)
  return out


def _emit_inventory(cyc: InventoryCycle, start: date, months: int) -> list[Txn]:
  """Purchases are cash out (paid on delivery); consumption is COGS. The gap
  accumulates as the inventory asset."""
  out: list[Txn] = []
  for off in range(months):
    buy = cyc.monthly_purchase.get(off, 0)
    if buy > 0:
      out.append(
        (
          month_date(start, off, cyc.purchase_day),
          "bill_payment",
          f"{cyc.name} — purchase",
          cyc.vendor,
          [(cyc.inventory_account, buy, 0), (cyc.cash_account, 0, buy)],
        )
      )
    use = cyc.monthly_consumption.get(off, 0)
    if use > 0:
      out.append(
        (
          month_date(start, off, cyc.consume_day),
          "journal_entry",
          f"{cyc.name} — COGS recognized",
          None,
          [(cyc.cogs_account, use, 0), (cyc.inventory_account, 0, use)],
        )
      )
  return out


def _curve_value(curve: dict[int, int] | int, off: int) -> int:
  return curve if isinstance(curve, int) else curve.get(off, 0)


def _emit_expense(exp: ExpenseRhythm, start: date, months: int) -> list[Txn]:
  out: list[Txn] = []
  txn_type = "bill_payment" if exp.vendor else "journal_entry"
  for off in range(months):
    amt = _curve_value(exp.monthly, off)
    if amt <= 0:
      continue
    out.append(
      (
        month_date(start, off, exp.day),
        txn_type,
        exp.name,
        exp.vendor,
        [(exp.expense_account, amt, 0), (exp.cash_account, 0, amt)],
      )
    )
  return out


def _emit_capex(cx: CapexItem, start: date, months: int) -> list[Txn]:
  out: list[Txn] = []
  if not cx.in_opening and 0 <= cx.purchase_offset < months:
    out.append(
      (
        month_date(start, cx.purchase_offset, 5),
        "bill_payment" if cx.vendor else "journal_entry",
        f"{cx.name} — purchase",
        cx.vendor,
        [(cx.asset_account, cx.cost, 0), (cx.cash_account, 0, cx.cost)],
      )
    )
  monthly = cx.cost // cx.life_months
  first = 0 if cx.in_opening else cx.purchase_offset + 1
  for off in range(max(first, 0), months):
    d = month_date(start, off, 31)
    out.append(
      (
        d,
        "journal_entry",
        f"{cx.name} — depreciation",
        None,
        [(cx.dep_expense_account, monthly, 0), (cx.accum_dep_account, 0, monthly)],
      )
    )
  return out


def _emit_prepaid(pp: PrepaidItem, start: date, months: int) -> list[Txn]:
  out: list[Txn] = []
  if 0 <= pp.purchase_offset < months:
    out.append(
      (
        month_date(start, pp.purchase_offset, 3),
        "bill_payment" if pp.vendor else "journal_entry",
        f"{pp.name} — prepaid purchase",
        pp.vendor,
        [(pp.prepaid_account, pp.cost, 0), (pp.cash_account, 0, pp.cost)],
      )
    )
  monthly = pp.cost // pp.life_months
  last = min(pp.purchase_offset + pp.life_months, months)
  for off in range(max(pp.purchase_offset, 0), last):
    out.append(
      (
        month_date(start, off, 27),
        "journal_entry",
        f"{pp.name} — amortization",
        None,
        [(pp.expense_account, monthly, 0), (pp.prepaid_account, 0, monthly)],
      )
    )
  return out


def emit(scenario: Scenario, start: date | None = None) -> list[Txn]:
  """Lower every driver in ``scenario`` to a sorted list of balanced tuples."""
  start = start or get_demo_start_date(scenario.months)
  window_end = month_date(start, scenario.months - 1, 31)
  txns: list[Txn] = list(scenario.opening)
  for stream in scenario.revenue:
    txns += _emit_revenue(stream, start, scenario.months, window_end)
  for cyc in scenario.inventory:
    txns += _emit_inventory(cyc, start, scenario.months)
  for exp in scenario.expenses:
    txns += _emit_expense(exp, start, scenario.months)
  for cx in scenario.capex:
    txns += _emit_capex(cx, start, scenario.months)
  for pp in scenario.prepaids:
    txns += _emit_prepaid(pp, start, scenario.months)
  txns.sort(key=lambda t: t[0])
  return txns


# ---------------------------------------------------------------------------
# Validation + articulation summary
# ---------------------------------------------------------------------------


def validate(txns: list[Txn]) -> bool:
  """Every entry must balance (Σ debits == Σ credits)."""
  ok = True
  for txn_date, _type, desc, _agent, lines in txns:
    dr = sum(d for _, d, _ in lines)
    cr = sum(c for _, _, c in lines)
    if dr != cr:
      print(f"  IMBALANCED {txn_date} {desc}: DR={dr} CR={cr}")
      ok = False
  return ok


def _meta(scenario: Scenario) -> dict[str, tuple[str, str, str]]:
  """code → (trait, sub_classification, balance_type)."""
  return {a[0]: (a[2], a[3], a[4]) for a in scenario.accounts}


def _offset_of(d: date, start: date) -> int:
  return (d.year - start.year) * 12 + (d.month - start.month)


def monthly_summary(scenario: Scenario, start: date | None = None) -> list[dict]:
  """Per-month P&L (period activity) and end-of-month balances for the
  working-capital accounts. Articulation proof: assets - liabilities - equity
  nets to zero every month, while cash diverges from net income."""
  start = start or get_demo_start_date(scenario.months)
  meta = _meta(scenario)
  txns = emit(scenario, start)

  def codes(pred) -> set[str]:
    return {c for c, (trait, sub, _bt) in meta.items() if pred(trait, sub)}

  rev_codes = codes(lambda t, s: t == "revenue")
  cogs_codes = codes(lambda t, s: s == "cost_of_goods_sold")
  exp_codes = codes(lambda t, s: t == "expense") - cogs_codes
  cash_codes = codes(lambda t, s: s == "cash_and_equivalents")
  ar_codes = codes(lambda t, s: s == "accounts_receivable")
  inv_codes = codes(lambda t, s: s == "inventory")
  defrev_codes = codes(lambda t, s: s == "deferred_revenue")

  def signed(code: str, dr: int, cr: int) -> int:
    return (dr - cr) if meta[code][2] == "debit" else (cr - dr)

  rows: list[dict] = []
  balance: dict[str, int] = {}

  for off in range(scenario.months):
    period: dict[str, int] = {}
    for txn_date, _t, _desc, _a, lines in txns:
      if _offset_of(txn_date, start) != off:
        continue
      for code, dr, cr in lines:
        period[code] = period.get(code, 0) + signed(code, dr, cr)
        balance[code] = balance.get(code, 0) + signed(code, dr, cr)

    def total(group: set[str], src: dict[str, int]) -> int:
      return sum(src.get(c, 0) for c in group)

    revenue = total(rev_codes, period)
    cogs = total(cogs_codes, period)
    opex = total(exp_codes, period)
    rows.append(
      {
        "offset": off,
        "month": month_date(start, off).strftime("%Y-%m"),
        "revenue": revenue,
        "cogs": cogs,
        "gross_profit": revenue - cogs,
        "opex": opex,
        "net_income": revenue - cogs - opex,
        "cash": total(cash_codes, balance),
        "ar": total(ar_codes, balance),
        "inventory": total(inv_codes, balance),
        "deferred_rev": total(defrev_codes, balance),
      }
    )
  return rows


def print_summary(scenario: Scenario, start: date | None = None) -> None:
  """Render the monthly P&L-vs-cash arc — the 'does the story bite?' view."""
  rows = monthly_summary(scenario, start)

  def usd(cents: int) -> str:
    return f"{cents / 100:>11,.0f}"

  header = (
    f"  {'Month':>7} | {'Revenue':>11} {'COGS':>11} {'GrossPft':>11} "
    f"{'NetInc':>11} ‖ {'Cash':>11} {'AR':>11} {'Invntry':>11} {'DefRev':>11}"
  )
  print(f"\n{scenario.company_name} — monthly P&L vs. cash")
  print(header)
  print("  " + "-" * (len(header) - 2))
  cum_ni = 0
  for r in rows:
    cum_ni += r["net_income"]
    print(
      f"  {r['month']:>7} | {usd(r['revenue'])} {usd(r['cogs'])} "
      f"{usd(r['gross_profit'])} {usd(r['net_income'])} ‖ {usd(r['cash'])} "
      f"{usd(r['ar'])} {usd(r['inventory'])} {usd(r['deferred_rev'])}"
    )
  first, last = rows[0], rows[-1]
  print("  " + "-" * (len(header) - 2))
  print(
    f"  Arc: cumulative net income {usd(cum_ni)}  |  "
    f"cash {usd(first['cash'])} → {usd(last['cash'])}  "
    f"(Δ {usd(last['cash'] - first['cash'])})"
  )
  print(
    f"       AR {usd(first['ar'])} → {usd(last['ar'])}   "
    f"inventory {usd(first['inventory'])} → {usd(last['inventory'])}   "
    f"deferred {usd(first['deferred_rev'])} → {usd(last['deferred_rev'])}"
  )
