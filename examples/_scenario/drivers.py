"""Scenario vocabulary — the declarative knobs an episode authors.

Every driver is a *flow* with (up to) two clocks: when it hits the P&L
(recognition) and when it hits cash (settlement). The engine walks the
drivers month-by-month and emits the balanced transaction tuples; the gap
between the two clocks is what builds AR / inventory / deferred revenue on
the balance sheet. Nothing here authors a *balance* — balances are the
residual of the flows, which is what guarantees the books articulate.

All monetary amounts are integer **cents** (matching the demo harness).
Curves are ``dict[int, int]`` keyed by month offset (0-based) from the demo
start; missing offsets default to 0. See ``curves`` in ``engine`` for
helpers (``flat``, ``ramp``, ``step_up``) that build these dicts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Literal

# A chart-of-accounts row: (code, name, trait, sub_classification, balance_type,
# description). ``trait`` is the FASB metamodel trait (asset / liability /
# equity / revenue / expense); ``balance_type`` is "debit" | "credit". These
# are advisory — rendering is decided by the CoA→rs-gaap mapping, not the
# trait (classification is structural, the trait is a hint).
Account = tuple[str, str, str, str, str, str | None]

# The transaction tuple the demo harness consumes downstream:
# (date, txn_type, description, agent_name | None, [(code, debit_cents, credit_cents), ...])
# txn_type ∈ {invoice, payment, bill_payment, journal_entry} — main.py branches
# on it to emit the typed REA event vocabulary and infer the discharge chain.
Txn = tuple[date, str, str, "str | None", list[tuple[str, int, int]]]


@dataclass(frozen=True)
class CollectionPolicy:
  """How/when cash arrives relative to revenue recognition.

  - ``immediate``  — cash lands the same day revenue is recognized (no AR).
  - ``net_terms``  — invoice now, cash ``lag`` days later. If the lag pushes
    settlement past the demo window, no payment is emitted and the invoice
    stays open → that *un-discharged invoice_issued* is the AR-aging reveal.
    ``lag_days_by_offset`` lets the lag stretch over the arc (net-30 → net-90
    in the squeeze); ``default_lag_days`` covers unlisted offsets.
  - ``prepaid``    — cash arrives *before* recognition. At the start of each
    ``bill_every``-month billing period the customer pays for that whole
    period (crediting a deferred-revenue liability), and revenue is recognized
    one month at a time. The unrecognized remainder is the deferred-revenue
    balance (cash-positive working capital). ``bill_every=1`` ≈ pay-as-you-go;
    larger cadences carry a bigger deferred balance.
  """

  kind: Literal["immediate", "net_terms", "prepaid"]
  default_lag_days: int = 30
  lag_days_by_offset: dict[int, int] = field(default_factory=dict)
  bill_every: int = 3
  # Which month within each billing cycle this stream bills on (0-based).
  # Splitting a subscription base into ``bill_every`` phase-shifted cohorts
  # (phases 0..bill_every-1) smooths the prepaid cash into a steady monthly
  # stream while still carrying a real deferred-revenue balance — see
  # ``engine.staggered_prepaid``.
  bill_phase: int = 0


@dataclass(frozen=True)
class RevenueStream:
  """A revenue relationship: a recognition curve plus a collection policy.

  Model one *concentration unit* per stream — a single big customer
  (e.g. Summit Markets) is its own stream so its slow-paying AR is isolable;
  a pool of small accounts shares a stream and the monthly amount is split
  evenly into one invoice per customer.
  """

  name: str
  customers: list[str]
  revenue_account: str
  monthly_recognized: dict[int, int]
  collection: CollectionPolicy
  # AR control account (net_terms) — where the recognized-not-collected sits.
  ar_account: str = "1100"
  # Deferred-revenue liability (prepaid) — where collected-not-recognized sits.
  deferred_account: str = "2300"
  cash_account: str = "1000"
  invoice_day: int = 12


@dataclass(frozen=True)
class InventoryCycle:
  """Goods bought ahead of demand and drawn down to COGS as they sell.

  ``monthly_purchase`` is cash out the door (a vendor bill, paid on delivery);
  ``monthly_consumption`` is COGS recognized (DR COGS / CR inventory). Their
  cumulative gap is the inventory asset — buy ahead of consumption and
  inventory (and trapped cash) builds.
  """

  name: str
  vendor: str
  inventory_account: str
  cogs_account: str
  monthly_purchase: dict[int, int]
  monthly_consumption: dict[int, int]
  cash_account: str = "1000"
  purchase_day: int = 6
  consume_day: int = 28


@dataclass(frozen=True)
class ExpenseRhythm:
  """A recurring operating expense, settled in cash the same month.

  A ``vendor`` routes it through the bill_received/bill_paid split (a real
  counterparty); ``vendor=None`` (payroll, owner draws) posts a plain journal
  entry. ``monthly`` is a flat cents amount or a per-offset curve.
  """

  name: str
  expense_account: str
  monthly: dict[int, int] | int
  vendor: str | None = None
  cash_account: str = "1000"
  day: int = 1


@dataclass(frozen=True)
class CapexItem:
  """A depreciable asset: posted at purchase, then straight-lined monthly.

  Drives both the posted depreciation journal entries (so closed periods show
  D&A) and — in the platform wiring — the depreciation Schedule block the AI
  close acts on for the open period.
  """

  name: str
  asset_account: str
  accum_dep_account: str
  dep_expense_account: str
  cost: int
  life_months: int
  purchase_offset: int
  vendor: str | None = None
  cash_account: str = "1000"
  in_opening: bool = False  # already owned at founding (no purchase event)


@dataclass(frozen=True)
class PrepaidItem:
  """A prepaid expense: paid up front, amortized straight-line monthly."""

  name: str
  prepaid_account: str
  expense_account: str
  cost: int
  life_months: int
  purchase_offset: int
  vendor: str | None = None
  cash_account: str = "1000"


@dataclass(frozen=True)
class Scenario:
  """A complete synthetic company: identity + chart of accounts + flows."""

  slug: str  # episode id — drives demo slot, credential prefixes, tags, bundle names
  company_name: str
  entity_type: str
  ticker: str
  uri: str
  description: str
  accounts: list[Account]
  cash_account: str
  opening: list[Txn]
  revenue: list[RevenueStream] = field(default_factory=list)
  inventory: list[InventoryCycle] = field(default_factory=list)
  expenses: list[ExpenseRhythm] = field(default_factory=list)
  capex: list[CapexItem] = field(default_factory=list)
  prepaids: list[PrepaidItem] = field(default_factory=list)
  months: int = 16
  report_period: tuple[date, date] | None = None
