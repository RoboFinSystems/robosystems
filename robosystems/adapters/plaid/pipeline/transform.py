"""Transform — Plaid transactions into captured-event payloads.

An adapter emits *events*, never GL rows. Every posted Plaid transaction
lands ``captured`` in the inbox with a
Tier-0 suggestion from its personal-finance category; posting is a
classification made once. The event types are the bank-feed contract's:

- ``bank_transaction`` — money in or out against a third party. Fees the
  bank charges are ``bank_fee``.
- ``internal_transfer`` — one event per *pair* of legs between two of the
  Item's own accounts. Plaid does not pre-link the legs, so they are matched
  here: a transfer-shaped outflow and inflow of the same amount on different
  accounts within ``TRANSFER_WINDOW_DAYS``, closest dates first.
- ``external_transfer`` — a transfer-shaped line with no matching leg (money
  to an account the feed cannot see: an owner, another entity, a card at
  another bank). It carries no suggestion and is marked a transfer
  candidate, so a leg that posts on a later sync can still pair with it.
  A cash or check deposit and an ATM or teller withdrawal are not
  transfer-shaped: nothing the feed can see is their other side, and a
  customer's check is revenue. They are ordinary ``bank_transaction`` lines.

Pending transactions are deferred, as on every feed: an event is written once,
when the transaction posts. Plaid retires a pending id when it posts and
issues a new one, so deferring means that retirement never touches the inbox.

Plaid's amount is positive when money leaves the account; an event's amount
is positive when it arrives. ``cents`` makes the flip.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex, name_key
from robosystems.adapters.bank_feed.hints import HINTS, AccountHint
from robosystems.adapters.plaid.pipeline.tier0 import (
  GOVERNMENT_DONATIONS,
  GOVERNMENT_PRIMARY,
  category,
  hint_for_category,
)

SOURCE = "plaid"
# The account types a bank feed books: cash and cards. Loans and investment
# accounts are left out — their activity is the other leg of a payment the
# cash account already shows.
BOOKED_ACCOUNT_TYPES = frozenset({"depository", "credit"})
TRANSFER_PRIMARIES = frozenset({"TRANSFER_IN", "TRANSFER_OUT"})
CARD_PAYMENT = "LOAN_PAYMENTS_CREDIT_CARD_PAYMENT"
# Transfer-shaped by primary, but with no other leg any connected account can
# show: cash and check deposits, ATM and teller withdrawals.
NOT_A_TRANSFER_DETAILED = frozenset({"TRANSFER_IN_DEPOSIT", "TRANSFER_OUT_WITHDRAWAL"})
MOVEMENT_PRIMARIES = frozenset({"LOAN_PAYMENTS", "LOAN_DISBURSEMENTS"})
TREASURY_DETAILED = frozenset({"INCOME_INTEREST_EARNED", "INCOME_DIVIDENDS"})
INCOME_PRIMARY = "INCOME"
TRANSFER_WINDOW_DAYS = 3


def cents(amount: float | int | str | None) -> int:
  """Plaid dollars (money out positive) → event cents (money in positive)."""
  value = Decimal(str(amount or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
  return -int(value * 100)


def txn_external_id(transaction_id: str) -> str:
  return f"plaid_txn_{transaction_id}"


def pair_external_id(first: str, second: str) -> str:
  return f"plaid_xfer_{min(first, second)}"


# ── Accounts and counterparties ─────────────────────────────────────────────


def bank_accounts(
  accounts: list[dict[str, Any]], *, institution: str
) -> list[BankAccount]:
  """The Item's cash and card accounts, named the way the chart will show them."""
  booked: list[BankAccount] = []
  for acct in accounts:
    kind = str(acct.get("type") or "")
    if kind not in BOOKED_ACCOUNT_TYPES:
      continue
    subtype = str(acct.get("subtype") or kind)
    booked.append(
      BankAccount(
        account_id=str(acct["account_id"]),
        name=account_display_name(acct, institution),
        kind=subtype,
        trait="liability" if kind == "credit" else "asset",
        balance_type="credit" if kind == "credit" else "debit",
        institution=institution,
      )
    )
  return booked


def account_display_name(acct: dict[str, Any], institution: str) -> str:
  """``Chase Total Checking ••1234`` — the institution once, the mask always.

  An account name that already carries the institution's distinctive part
  (``Harborline Savings`` at ``Harborline Bank``) is not prefixed again.
  """
  name = str(acct.get("name") or acct.get("official_name") or acct.get("subtype") or "")
  name = name.strip() or "Account"
  core = _GENERIC_INSTITUTION_WORDS.sub("", institution).strip() or institution
  if core and not re.search(rf"\b{re.escape(core)}\b", name, re.IGNORECASE):
    name = f"{institution} {name}"
  mask = acct.get("mask")
  return f"{name} ••{mask}" if mask else name


_GENERIC_INSTITUTION_WORDS = re.compile(
  r"\s*\b(bank|credit union|federal|financial|fcu|n\.?a\.?|trust|company)\b.*$",
  re.IGNORECASE,
)


def is_transfer_candidate(txn: dict[str, Any]) -> bool:
  """A line whose other side may be on another of the Item's accounts."""
  primary, detailed, _confidence = category(txn)
  if detailed in NOT_A_TRANSFER_DETAILED:
    return False
  return primary in TRANSFER_PRIMARIES or detailed == CARD_PAYMENT


def counterparty_key(txn: dict[str, Any]) -> str | None:
  """A merchant's stable key: Plaid's entity id, else its normalized name.

  A line with no merchant and no counterparty (``INTRST PYMNT``) has no key,
  and no agent is made from a raw bank description.
  """
  parties = txn.get("counterparties") or []
  entity_id = txn.get("merchant_entity_id") or (
    parties[0].get("entity_id") if parties else None
  )
  if entity_id:
    return str(entity_id)
  name = counterparty_name(txn)
  return f"name:{name_key(name)}" if name else None


def counterparty_name(txn: dict[str, Any]) -> str | None:
  parties = txn.get("counterparties") or []
  return txn.get("merchant_name") or (parties[0].get("name") if parties else None)


def counterparties(
  transactions: list[dict[str, Any]], *, account_ids: set[str], source: str
) -> list[dict[str, Any]]:
  """One agent per distinct third party the booked accounts transact with."""
  by_key: dict[str, dict[str, Any]] = {}
  for txn in transactions:
    if txn.get("pending") or str(txn.get("account_id")) not in account_ids:
      continue
    primary, detailed, _confidence = category(txn)
    if is_transfer_candidate(txn) or primary in MOVEMENT_PRIMARIES:
      continue
    key = counterparty_key(txn)
    name = counterparty_name(txn)
    if key is None or not name:
      continue
    record = by_key.setdefault(
      key, {"name": name, "income": False, "count": 0, "government": False}
    )
    record["count"] += 1
    # Typed by what the bank says the money was, never by its direction: a
    # refund from a vendor is money in, and the vendor stays a vendor.
    if primary == INCOME_PRIMARY:
      record["income"] = True
    if primary == GOVERNMENT_PRIMARY and detailed != GOVERNMENT_DONATIONS:
      record["government"] = True
  agents: list[dict[str, Any]] = []
  for key, record in by_key.items():
    if record["government"]:
      agent_type = "government"
    else:
      agent_type = "customer" if record["income"] else "vendor"
    agents.append(
      {
        "agent_type": agent_type,
        "name": str(record["name"])[:200],
        "source": source,
        "external_id": key,
        "metadata": {"plaid_transactions": record["count"]},
      }
    )
  return agents


# ── Transfer legs ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Leg:
  """One side of a possible transfer — from a transaction, or from a captured
  single-leg event that is still waiting for its other side."""

  transaction_id: str
  account_id: str
  account_name: str | None
  element_id: str | None
  amount: int
  day: str
  description: str | None


def pair_legs(
  legs: list[Leg], *, window_days: int = TRANSFER_WINDOW_DAYS
) -> tuple[list[tuple[Leg, Leg]], list[Leg]]:
  """(``[(out_leg, in_leg)]``, unpaired) — each outflow takes the inflow of the
  same magnitude on another account whose date is closest, within the window."""
  outs = sorted((leg for leg in legs if leg.amount < 0), key=_leg_order)
  ins = sorted((leg for leg in legs if leg.amount > 0), key=_leg_order)
  used: set[str] = set()
  pairs: list[tuple[Leg, Leg]] = []
  for out_leg in outs:
    best: Leg | None = None
    best_gap: int | None = None
    for in_leg in ins:
      if in_leg.transaction_id in used or in_leg.account_id == out_leg.account_id:
        continue
      if in_leg.amount != -out_leg.amount:
        continue
      gap = abs(days_between(out_leg.day, in_leg.day))
      if gap > window_days:
        continue
      if best_gap is None or gap < best_gap:
        best, best_gap = in_leg, gap
    if best is not None:
      used.update({out_leg.transaction_id, best.transaction_id})
      pairs.append((out_leg, best))
  unpaired = [leg for leg in legs if leg.transaction_id not in used]
  return pairs, unpaired


def days_between(first: str, second: str) -> int:
  return (date.fromisoformat(second[:10]) - date.fromisoformat(first[:10])).days


def window(day: str, window_days: int = TRANSFER_WINDOW_DAYS) -> tuple[date, date]:
  center = date.fromisoformat(day[:10])
  return center - timedelta(days=window_days), center + timedelta(days=window_days)


def _leg_order(leg: Leg) -> tuple[str, str]:
  return leg.day, leg.transaction_id


def transfer_event(
  out_leg: Leg, in_leg: Leg, *, connection_id: str, item_id: str | None
) -> dict[str, Any]:
  """One ``internal_transfer`` for both legs — money arrives on the later
  date; each leg's own date rides along, for the leg that outlives the pair."""
  from_name = out_leg.account_name or "account"
  to_name = in_leg.account_name or "account"
  later = max(out_leg.day, in_leg.day)
  metadata = _prune(
    {
      "connection_id": connection_id,
      "item_id": item_id,
      "kind": "internal_transfer",
      "from_account_id": out_leg.account_id,
      "to_account_id": in_leg.account_id,
      "from_account_name": out_leg.account_name,
      "to_account_name": in_leg.account_name,
      "from_element_id": out_leg.element_id,
      "to_element_id": in_leg.element_id,
      "from_date": out_leg.day,
      "to_date": in_leg.day,
      "legs": [out_leg.transaction_id, in_leg.transaction_id],
      "bank_description": in_leg.description or out_leg.description,
      "classification_source": "transfer",
    }
  )
  return _prune(
    {
      "event_type": "internal_transfer",
      "event_category": "treasury",
      "event_class": "economic",
      "event_action": "move",
      "resource_type": "money",
      "occurred_at": f"{later}T00:00:00Z",
      "source": SOURCE,
      "external_id": pair_external_id(out_leg.transaction_id, in_leg.transaction_id),
      "amount": abs(in_leg.amount),
      "currency": "USD",
      "description": f"Transfer {from_name} to {to_name}"[:200],
      "resource_element_id": in_leg.element_id,
      "metadata": metadata,
      "apply_handlers": False,
    }
  )


# ── Events ───────────────────────────────────────────────────────────────────


@dataclass
class TransformResult:
  events: list[dict[str, Any]]
  skipped: Counter[str] = field(default_factory=Counter)
  classification: Counter[str] = field(default_factory=Counter)
  resolved: Counter[str] = field(default_factory=Counter)


def transform(
  transactions: list[dict[str, Any]],
  *,
  accounts: list[BankAccount],
  connection_id: str,
  item_id: str | None,
  account_elements: dict[str, str],
  chart: ChartIndex | None = None,
  agent_ids: dict[str, str] | None = None,
  since: date | None = None,
  exclude: frozenset[str] = frozenset(),
  unpairable: frozenset[str] = frozenset(),
) -> TransformResult:
  """Posted transactions → event payloads.

  ``exclude`` names transactions already inside a captured transfer pair (they
  emit nothing); ``unpairable`` names ones already captured on their own (they
  emit their single-leg payload, for the hint refresh, and never pair).
  """
  chart = chart or ChartIndex()
  agent_ids = agent_ids or {}
  by_id = {account.account_id: account for account in accounts}
  result = TransformResult(events=[])

  live: list[dict[str, Any]] = []
  for txn in transactions:
    if txn.get("pending"):
      result.skipped["pending"] += 1
      continue
    if str(txn.get("account_id")) not in by_id:
      result.skipped["excluded_account"] += 1
      continue
    if since is not None and str(txn.get("date") or "") < since.isoformat():
      result.skipped["before_start"] += 1
      continue
    if str(txn["transaction_id"]) in exclude:
      result.skipped["already_paired"] += 1
      continue
    live.append(txn)

  candidates = [
    leg_from_transaction(txn, by_id, account_elements)
    for txn in live
    if is_transfer_candidate(txn) and str(txn["transaction_id"]) not in unpairable
  ]
  pairs, _unpaired = pair_legs(candidates)
  paired: set[str] = set()
  for out_leg, in_leg in pairs:
    paired.update({out_leg.transaction_id, in_leg.transaction_id})
    result.events.append(
      transfer_event(out_leg, in_leg, connection_id=connection_id, item_id=item_id)
    )
    result.classification["transfer"] += 1

  for txn in live:
    if str(txn["transaction_id"]) in paired:
      continue
    event, classification = bank_event(
      txn,
      by_id,
      account_elements,
      chart,
      agent_ids,
      connection_id=connection_id,
      item_id=item_id,
    )
    result.classification[classification] += 1
    if event["metadata"].get("suggested_account_name"):
      result.resolved[
        "resolved" if event["metadata"].get("suggested_element_id") else "hint_only"
      ] += 1
    result.events.append(event)

  result.events.sort(key=lambda event: str(event["occurred_at"]))
  return result


def leg_from_transaction(
  txn: dict[str, Any],
  accounts: dict[str, BankAccount],
  account_elements: dict[str, str],
) -> Leg:
  account_id = str(txn.get("account_id"))
  account = accounts.get(account_id)
  return Leg(
    transaction_id=str(txn["transaction_id"]),
    account_id=account_id,
    account_name=account.name if account else None,
    element_id=account_elements.get(account_id),
    amount=cents(txn.get("amount")),
    day=str(txn.get("date") or "")[:10],
    description=txn.get("original_description") or txn.get("name"),
  )


def bank_event(
  txn: dict[str, Any],
  accounts: dict[str, BankAccount],
  account_elements: dict[str, str],
  chart: ChartIndex,
  agent_ids: dict[str, str],
  *,
  connection_id: str,
  item_id: str | None,
) -> tuple[dict[str, Any], str]:
  """One single-leg event, and how its suggestion was reached."""
  amount = cents(txn.get("amount"))
  primary, detailed, confidence = category(txn)
  suggested, known = hint_for_category(primary, detailed)
  classification = "plaid_category" if known else "none"
  transfer_candidate = is_transfer_candidate(txn)

  if transfer_candidate:
    event_type, event_category = "external_transfer", "treasury"
    suggested, classification = None, "transfer"
  elif primary == "BANK_FEES":
    event_type, event_category = "bank_fee", "treasury"
    suggested = suggested or HINTS["BankFees"]
  elif primary in MOVEMENT_PRIMARIES or detailed in TREASURY_DETAILED:
    event_type, event_category = "bank_transaction", "treasury"
  else:
    # The category names the line's nature, never its direction: a refund
    # from a vendor is money in on a purchase, and a rule keyed on sales
    # must not see it. A deposit or withdrawal the bank could not attribute
    # is a movement of the company's own cash until someone says otherwise.
    event_type = "bank_transaction"
    if primary == INCOME_PRIMARY:
      event_category = "sales"
    elif primary in TRANSFER_PRIMARIES:
      event_category = "treasury"
    else:
      event_category = "purchase"

  agent_key = (
    None
    if transfer_candidate or primary in MOVEMENT_PRIMARIES
    else counterparty_key(txn)
  )
  account_id = str(txn.get("account_id"))
  account = accounts.get(account_id)
  parties = txn.get("counterparties") or []
  party = parties[0] if parties else {}
  merchant = counterparty_name(txn)
  bank_description = txn.get("original_description") or txn.get("name")
  hint: AccountHint | None = suggested
  metadata = _prune(
    {
      "connection_id": connection_id,
      "item_id": item_id,
      "transaction_id": txn.get("transaction_id"),
      "status": "posted",
      "posted_date": txn.get("date"),
      "authorized_date": txn.get("authorized_date"),
      "account_id": account_id,
      "account_name": account.name if account else None,
      "counterparty_name": merchant,
      "counterparty_external_id": agent_key,
      "counterparty_type": party.get("type"),
      "merchant_name": txn.get("merchant_name"),
      "merchant_entity_id": txn.get("merchant_entity_id"),
      "website": txn.get("website") or party.get("website"),
      "logo_url": txn.get("logo_url") or party.get("logo_url"),
      "bank_description": bank_description,
      "payment_channel": txn.get("payment_channel"),
      "transaction_code": txn.get("transaction_code"),
      "check_number": txn.get("check_number"),
      "plaid_category_primary": primary,
      "plaid_category_detailed": detailed,
      "plaid_category_confidence": confidence,
      "transfer_candidate": True if transfer_candidate else None,
      "suggested_account_key": hint.key if hint else None,
      "suggested_account_name": hint.name if hint else None,
      "suggested_element_id": chart.resolve_hint(hint) if hint else None,
      "classification_source": classification,
    }
  )
  event = {
    "event_type": event_type,
    "event_category": event_category,
    "event_class": "economic",
    "event_action": "transfer",
    "resource_type": "money",
    "occurred_at": f"{str(txn.get('date'))[:10]}T00:00:00Z",
    "source": SOURCE,
    "external_id": txn_external_id(str(txn["transaction_id"])),
    "amount": amount,
    "currency": txn.get("iso_currency_code")
    or txn.get("unofficial_currency_code")
    or "USD",
    "description": str(merchant or bank_description or "Bank transaction")[:200],
    "agent_id": agent_ids.get(agent_key) if agent_key else None,
    "resource_element_id": account_elements.get(account_id),
    "metadata": metadata,
    "apply_handlers": False,
  }
  return _prune(event), classification


def _prune(payload: dict[str, Any]) -> dict[str, Any]:
  return {key: value for key, value in payload.items() if value is not None}
