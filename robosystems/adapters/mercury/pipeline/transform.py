"""Transform — Mercury transactions into captured-event payloads.

The adapter thesis (``ref/adapters.md``): an adapter emits *events*
("Stripe paid us $95.14 on the 13th"), never GL rows. Every event lands
``captured`` in the inbox with a Tier-0 suggestion attached; posting is a
classification the operator (or Claude over MCP) makes once and remembers.

Three event types come out of a bank feed:

- ``bank_transaction`` — money in or out against a third party (card spend,
  ACH, wires, payouts, interest, cashback). Fees Mercury bills directly are
  ``bank_fee``.
- ``internal_transfer`` — one event per *pair* of legs between the org's own
  Mercury accounts (checking ↔ savings, the IO card autopay), so a movement
  never double-counts.
- ``external_transfer`` — a transfer to or from a bank account outside
  Mercury. Ownership is unknowable from the feed (owner draw? another
  entity? a loan?), so these carry no suggestion.

The bank leg of every event is the chart account linked to the Mercury
account (``accounts.link_bank_accounts``); the suggestion resolves against
the same chart by name or code. Nothing here creates an account.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from robosystems.adapters.mercury.pipeline.tier0 import (
  HINTS,
  AccountHint,
  hint_for_custom_category,
  hint_for_gl_code,
  hint_for_mercury_category,
  parse_gl_code_name,
)

# Failed / cancelled / blocked / reversed never become events. Pending ones
# are deferred: an event is written once, when the transaction posts, so the
# captured payload is the settled one.
SKIP_STATUSES = frozenset({"failed", "cancelled", "blocked", "reversed", "pending"})
TRANSFER_KINDS = frozenset({"internalTransfer", "treasuryTransfer"})
TREASURY_KINDS = frozenset({"treasury"})
FEE_KINDS = frozenset(
  {
    "wireFee",
    "cardInternationalTransactionFee",
    "personalBankingSubscriptionFee",
    "billingEngineSubscriptionFee",
  }
)
GOVERNMENT_CATEGORIES = frozenset({"Taxes", "GovernmentServices"})
# The checking-account side of an IO card autopay names the card this way.
CREDIT_CARD_LABEL = "Mercury Credit"


@dataclass(frozen=True)
class BankAccount:
  """One account the feed exposes — a chart account is linked to each."""

  mercury_id: str
  name: str
  kind: str
  trait: str
  balance_type: str
  legal_business_name: str | None = None

  @property
  def is_credit(self) -> bool:
    return self.kind == "credit"


@dataclass
class ChartIndex:
  """Resolve a suggestion against the graph's chart, by name then by code."""

  by_name: dict[str, str] = field(default_factory=dict)
  by_code: dict[str, str] = field(default_factory=dict)

  def resolve(self, *candidates: str | None) -> str | None:
    for candidate in candidates:
      if not candidate:
        continue
      element_id = self.by_name.get(name_key(candidate)) or self.by_code.get(
        candidate.strip()
      )
      if element_id:
        return element_id
    return None

  def resolve_gl_code(self, gl_code_name: str | None) -> str | None:
    """A ``<code> - <name>`` label resolves by its code, then by its name."""
    if not gl_code_name:
      return None
    code, name = parse_gl_code_name(gl_code_name)
    if code and code in self.by_code:
      return self.by_code[code]
    return self.resolve(name, gl_code_name)


@dataclass
class TransformResult:
  events: list[dict[str, Any]]
  skipped: Counter[str] = field(default_factory=Counter)
  classification: Counter[str] = field(default_factory=Counter)
  resolved: Counter[str] = field(default_factory=Counter)


def cents(amount: float | int | str) -> int:
  return round(float(amount) * 100)


def name_key(value: str) -> str:
  """``Mercury Checking ••5424`` and ``Mercury Checking 5424`` resolve alike."""
  return re.sub(r"[^a-z0-9]", "", value.lower())


# ── Accounts and counterparties ─────────────────────────────────────────────


def bank_accounts(
  raw: dict[str, Any], *, include_treasury: bool = True
) -> list[BankAccount]:
  accounts: list[BankAccount] = []
  for acct in raw.get("accounts") or []:
    kind = str(acct.get("kind") or acct.get("type") or "checking")
    if kind in TREASURY_KINDS and not include_treasury:
      continue
    name = str(acct.get("name") or f"Mercury {kind.title()}")
    accounts.append(
      BankAccount(
        mercury_id=str(acct["id"]),
        name=name,
        kind=kind,
        trait="asset",
        balance_type="debit",
        legal_business_name=acct.get("legalBusinessName"),
      )
    )
  for index, acct in enumerate(raw.get("credit_accounts") or [], start=1):
    name = str(
      acct.get("name")
      or ("Mercury Credit Card" if index == 1 else f"Mercury Credit Card {index}")
    )
    accounts.append(
      BankAccount(
        mercury_id=str(acct["id"]),
        name=name,
        kind="credit",
        trait="liability",
        balance_type="credit",
        legal_business_name=acct.get("legalBusinessName"),
      )
    )
  return accounts


def own_counterparty_names(accounts: list[BankAccount]) -> set[str]:
  names = {account.name for account in accounts}
  if any(account.is_credit for account in accounts):
    names.add(CREDIT_CARD_LABEL)
  return names


def counterparty_key(txn: dict[str, Any]) -> str | None:
  name = txn.get("counterpartyName")
  if not name:
    return None
  return txn.get("counterpartyId") or f"name:{name_key(str(name))}"


def counterparties(
  raw: dict[str, Any], own_names: set[str], source: str
) -> list[dict[str, Any]]:
  """One agent per distinct third party the feed transacts with."""
  by_key: dict[str, dict[str, Any]] = {}
  for txn in raw.get("transactions") or []:
    if txn.get("status") in SKIP_STATUSES or txn.get("kind") in TRANSFER_KINDS:
      continue
    name = txn.get("counterpartyName")
    key = counterparty_key(txn)
    if not name or key is None or name in own_names:
      continue
    record = by_key.setdefault(
      key, {"name": name, "net": 0, "count": 0, "government": False}
    )
    record["net"] += cents(txn.get("amount") or 0)
    record["count"] += 1
    if txn.get("mercuryCategory") in GOVERNMENT_CATEGORIES:
      record["government"] = True
  agents: list[dict[str, Any]] = []
  for key, record in by_key.items():
    if record["government"]:
      agent_type = "government"
    elif str(record["name"]).startswith("Mercury"):
      agent_type = "other"
    else:
      agent_type = "customer" if record["net"] > 0 else "vendor"
    agents.append(
      {
        "agent_type": agent_type,
        "name": str(record["name"])[:200],
        "source": source,
        "external_id": key,
        "metadata": {"mercury_transactions": record["count"]},
      }
    )
  return agents


# ── Events ───────────────────────────────────────────────────────────────────


def transform(
  raw: dict[str, Any],
  *,
  source: str,
  connection_id: str,
  account_elements: dict[str, str],
  chart: ChartIndex | None = None,
  agent_ids: dict[str, str] | None = None,
  include_treasury: bool = True,
) -> TransformResult:
  """``account_elements`` maps a Mercury account id to the chart element
  linked to it; ``agent_ids`` maps a counterparty key to its agent id."""
  chart = chart or ChartIndex()
  agent_ids = agent_ids or {}
  accounts = {
    account.mercury_id: account
    for account in bank_accounts(raw, include_treasury=include_treasury)
  }
  own_names = own_counterparty_names(list(accounts.values()))
  result = TransformResult(events=[])

  live: list[dict[str, Any]] = []
  for txn in raw.get("transactions") or []:
    status = str(txn.get("status") or "unknown")
    if status in SKIP_STATUSES:
      result.skipped[status] += 1
      continue
    if str(txn.get("accountId")) not in accounts:
      # An excluded treasury account, or one the feed did not expose.
      result.skipped["excluded_account"] += 1
      continue
    live.append(txn)

  # Pair the two legs of a movement between the org's own accounts.
  buckets: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
  for txn in live:
    if txn.get("kind") in TRANSFER_KINDS or txn.get("counterpartyName") in own_names:
      buckets[(_day(txn), abs(cents(txn.get("amount") or 0)))].append(txn)
  paired: set[str] = set()
  for group in buckets.values():
    debits = [t for t in group if cents(t.get("amount") or 0) < 0]
    credits = [t for t in group if cents(t.get("amount") or 0) > 0]
    for source_leg, target_leg in zip(debits, credits, strict=False):
      paired.update({source_leg["id"], target_leg["id"]})
      result.events.append(
        _transfer_event(
          source_leg, target_leg, accounts, account_elements, source, connection_id
        )
      )
      result.classification["transfer"] += 1

  for txn in live:
    if txn["id"] in paired:
      continue
    event, classification = _bank_event(
      txn,
      accounts,
      own_names,
      account_elements,
      chart,
      agent_ids,
      source,
      connection_id,
    )
    result.classification[classification] += 1
    if event["metadata"].get("suggested_account_name"):
      result.resolved[
        "resolved" if event["metadata"].get("suggested_element_id") else "hint_only"
      ] += 1
    result.events.append(event)

  result.events.sort(key=lambda event: str(event["occurred_at"]))
  return result


def _day(txn: dict[str, Any]) -> str:
  return str(txn.get("postedAt") or txn.get("createdAt") or "")[:10]


def _when(txn: dict[str, Any]) -> str:
  return str(txn.get("postedAt") or txn.get("createdAt"))


def _suggest(txn: dict[str, Any]) -> tuple[AccountHint | None, str]:
  allocations = txn.get("glAllocations") or []
  if allocations and allocations[0].get("glCodeName"):
    return hint_for_gl_code(str(allocations[0]["glCodeName"])), "gl_allocation"
  custom_hint, known = hint_for_custom_category(
    (txn.get("categoryData") or {}).get("name")
  )
  if known:
    return custom_hint, "custom_category"
  bucket_hint = hint_for_mercury_category(txn.get("mercuryCategory"))
  if bucket_hint is not None:
    return bucket_hint, "mercury_category"
  return None, "none"


def _transfer_event(
  source_leg: dict[str, Any],
  target_leg: dict[str, Any],
  accounts: dict[str, BankAccount],
  account_elements: dict[str, str],
  source: str,
  connection_id: str,
) -> dict[str, Any]:
  from_account = accounts.get(str(source_leg.get("accountId")))
  to_account = accounts.get(str(target_leg.get("accountId")))
  from_name = from_account.name if from_account else "external"
  to_name = to_account.name if to_account else "external"
  from_element = account_elements.get(str(source_leg.get("accountId")))
  to_element = account_elements.get(str(target_leg.get("accountId")))
  metadata = _prune(
    {
      "connection_id": connection_id,
      "kind": "internal_transfer",
      "status": target_leg.get("status"),
      "from_account_id": source_leg.get("accountId"),
      "to_account_id": target_leg.get("accountId"),
      "from_account_name": from_account.name if from_account else None,
      "to_account_name": to_account.name if to_account else None,
      "from_element_id": from_element,
      "to_element_id": to_element,
      "legs": [source_leg["id"], target_leg["id"]],
      "bank_description": target_leg.get("bankDescription")
      or source_leg.get("bankDescription"),
      "classification_source": "transfer",
      "dashboard_link": target_leg.get("dashboardLink"),
    }
  )
  return _prune(
    {
      "event_type": "internal_transfer",
      "event_category": "treasury",
      "event_class": "economic",
      "event_action": "move",
      "resource_type": "money",
      "occurred_at": _when(target_leg),
      "source": source,
      "external_id": f"mercury_xfer_{min(source_leg['id'], target_leg['id'])}",
      "external_url": target_leg.get("dashboardLink"),
      "amount": abs(cents(target_leg.get("amount") or 0)),
      "currency": "USD",
      "description": f"Transfer {from_name} to {to_name}"[:200],
      "resource_element_id": to_element,
      "metadata": metadata,
      "apply_handlers": False,
    }
  )


def _bank_event(
  txn: dict[str, Any],
  accounts: dict[str, BankAccount],
  own_names: set[str],
  account_elements: dict[str, str],
  chart: ChartIndex,
  agent_ids: dict[str, str],
  source: str,
  connection_id: str,
) -> tuple[dict[str, Any], str]:
  amount = cents(txn.get("amount") or 0)
  kind = str(txn.get("kind") or "other")
  name = str(txn.get("counterpartyName") or txn.get("bankDescription") or "Unknown")
  account = accounts.get(str(txn.get("accountId")))
  suggested, classification = _suggest(txn)

  if kind == "externalTransfer":
    event_type, category = "external_transfer", "treasury"
    suggested, classification = None, "transfer"
  elif kind in FEE_KINDS or (kind == "other" and txn.get("feeId")):
    event_type, category = "bank_fee", "treasury"
    suggested = suggested or HINTS["BankFees"]
  elif kind == "interestPayment":
    event_type, category = "bank_transaction", "treasury"
    suggested = suggested or HINTS["InterestIncome"]
  elif name in own_names or (name.startswith("Mercury") and amount > 0):
    event_type, category = "bank_transaction", "treasury"
  else:
    event_type = "bank_transaction"
    category = "sales" if amount > 0 else "purchase"

  agent_key = (
    None if name in own_names or kind in TRANSFER_KINDS else counterparty_key(txn)
  )
  allocations = txn.get("glAllocations") or []
  merchant = txn.get("merchant") or {}
  gl_code_name = allocations[0].get("glCodeName") if allocations else None
  suggested_element_id = (
    (chart.resolve_gl_code(gl_code_name) or chart.resolve(suggested.name))
    if suggested
    else None
  )
  metadata = _prune(
    {
      "connection_id": connection_id,
      "kind": kind,
      "status": txn.get("status"),
      "posted_at": txn.get("postedAt"),
      "created_at": txn.get("createdAt"),
      "account_id": txn.get("accountId"),
      "account_name": account.name if account else None,
      "counterparty_id": txn.get("counterpartyId"),
      "counterparty_name": txn.get("counterpartyName"),
      "counterparty_external_id": agent_key,
      "bank_description": txn.get("bankDescription"),
      "external_memo": txn.get("externalMemo"),
      "note": txn.get("note"),
      "mercury_category": txn.get("mercuryCategory"),
      "custom_category": (txn.get("categoryData") or {}).get("name"),
      "gl_allocations": [
        {
          "gl_code_name": allocation.get("glCodeName"),
          "amount": allocation.get("amount"),
          "description": allocation.get("description"),
        }
        for allocation in allocations
      ]
      or None,
      "split": len(allocations) > 1 or None,
      "merchant_category": merchant.get("category"),
      "merchant_category_code": merchant.get("categoryCode"),
      "card_id": txn.get("cardId"),
      "suggested_account_key": suggested.key if suggested else None,
      "suggested_account_name": suggested.name if suggested else None,
      "suggested_element_id": suggested_element_id,
      "classification_source": classification,
      "dashboard_link": txn.get("dashboardLink"),
    }
  )
  description = name
  if txn.get("bankDescription") and txn.get("bankDescription") != name:
    description = f"{name}: {txn['bankDescription']}"
  if kind in ("creditCardTransaction", "debitCardTransaction"):
    description = f"{name} (card)"
  event = {
    "event_type": event_type,
    "event_category": category,
    "event_class": "economic",
    "event_action": "transfer",
    "resource_type": "money",
    "occurred_at": _when(txn),
    "source": source,
    "external_id": f"mercury_txn_{txn['id']}",
    "external_url": txn.get("dashboardLink"),
    "amount": amount,
    "currency": "USD",
    "description": description[:200],
    "agent_id": agent_ids.get(agent_key) if agent_key else None,
    "resource_element_id": account_elements.get(str(txn.get("accountId"))),
    "metadata": metadata,
    "apply_handlers": False,
  }
  return _prune(event), classification


def _prune(payload: dict[str, Any]) -> dict[str, Any]:
  return {key: value for key, value in payload.items() if value is not None}
