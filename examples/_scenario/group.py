"""A group beside the episode's company — the multi-entity beat.

The episode's company is the group parent. The roster puts its subsidiaries
in the same graph, each with its own books, chart of accounts and fiscal
calendar, and one more entity whose only entries are the consolidating ones:
the parent's investment against each subsidiary's contributed capital, the
outside owner's share as noncontrolling interest, the intercompany balances
and flows, and the parent's share of an equity-method member's income. Every
intercompany event is posted on both sides, so the
group creates no cash from nothing, and the consolidated statement is the
sum of every entity's at the rs-gaap concepts.

The consolidating entries are the engine's: each month, once every side of
it is in the books, ``propose-consolidation-entries`` drafts them in the
consolidation entity, and its close posts them. Nothing here computes one,
so the read-back below checks the engine against the scenario's own figures.

A member can also be bought rather than formed (``Member.acquired``): its
books open on the balance sheet it brings, the parent pays its founders for
a share, ``record-acquisition`` carries the excess as goodwill, a later
``record-ownership-change`` the price of buying more, and when the whole of
it is sold ``left_on`` and ``record-deconsolidation`` take it out of the
group with the gain or loss on its leaving — the group lifecycle, end to
end, with the read-back checking each month against ASC 805 and 810.

The entity-scoped operations are posted directly: the published client's
facades predate ``entity_id``.
"""

from __future__ import annotations

import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import httpx

_FORM_EQUITY = {
  "llc": "rs-gaap:MembersEquity",
  "partnership": "rs-gaap:PartnersCapital",
  "corporation": "rs-gaap:RetainedEarningsAccumulatedDeficit",
}

# A distribution every third month, split by ownership; the read-back counts
# the same months.
_DISTRIBUTION_EVERY = 3

# Accounts a member's template chart lacks and the group needs: (code, name,
# trait, sub_classification, balance, rs-gaap target). The distributions row
# maps to the form's equity concept.
_MEMBER_ACCOUNTS: tuple[tuple[str, str, str, str, str, str | None], ...] = (
  (
    "2150",
    "Due to Affiliates",
    "liability",
    "accounts_payable",
    "credit",
    "rs-gaap:DueToAffiliateCurrent",
  ),
  (
    "2600",
    "Note Payable — Parent",
    "liability",
    "long_term_debt",
    "credit",
    "rs-gaap:LongTermDebtAndCapitalLeaseObligations",
  ),
  ("3200", "Member Distributions", "equity", "equity", "debit", None),
  (
    "6700",
    "Management Fees — Parent",
    "expense",
    "operating_expense",
    "debit",
    "rs-gaap:GeneralAndAdministrativeExpense",
  ),
  (
    "6800",
    "Interest Expense — Parent",
    "expense",
    "non_operating_expense",
    "debit",
    "rs-gaap:InterestExpense",
  ),
)

# The parent's side of the same events: added to its chart when missing.
_PARENT_ACCOUNTS: tuple[tuple[str, str, str, str, str, str | None], ...] = (
  (
    "1150",
    "Due from Affiliates",
    "asset",
    "accounts_receivable",
    "debit",
    "rs-gaap:DueFromAffiliateCurrent",
  ),
  (
    "1600",
    "Investment in Subsidiaries",
    "asset",
    "other_noncurrent_assets",
    "debit",
    "rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures",
  ),
  (
    "1650",
    "Notes Receivable — Affiliates",
    "asset",
    "other_noncurrent_assets",
    "debit",
    "rs-gaap:LongTermInvestmentsAndReceivablesNet",
  ),
  (
    "4300",
    "Management Fee Revenue",
    "revenue",
    "operating_revenue",
    "credit",
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
  ),
  (
    "4400",
    "Interest Income — Affiliates",
    "revenue",
    "non_operating_revenue",
    "credit",
    "rs-gaap:InvestmentIncomeInterest",
  ),
  (
    "1700",
    "Investment in Associates",
    "asset",
    "other_noncurrent_assets",
    "debit",
    "rs-gaap:EquityMethodInvestments",
  ),
  (
    "4600",
    "Gain on Sale of Subsidiary",
    "revenue",
    "non_operating_revenue",
    "credit",
    "rs-gaap:GainOrLossOnSaleOfStockInSubsidiary",
  ),
)

# The consolidation template's equity-method rows and the group-equity rows
# a deconsolidation credits under the parent's form, added to a
# consolidation chart made before the template carried them.
_CONSOLIDATION_ACCOUNTS: tuple[tuple[str, str, str, str, str, str | None], ...] = (
  (
    "1700",
    "Investment in Associates — Equity-Method Adjustment",
    "asset",
    "other_noncurrent_assets",
    "debit",
    "rs-gaap:EquityMethodInvestments",
  ),
  (
    "3110",
    "Retained Earnings — Group (corporate parent)",
    "equity",
    "equity",
    "credit",
    "rs-gaap:RetainedEarningsAccumulatedDeficit",
  ),
  (
    "3120",
    "Partners' Capital — Group (partnership parent)",
    "equity",
    "equity",
    "credit",
    "rs-gaap:PartnersCapital",
  ),
  (
    "3130",
    "Members' Equity — Group (LLC parent)",
    "equity",
    "equity",
    "credit",
    "rs-gaap:MembersEquity",
  ),
  (
    "4500",
    "Share of Associates' Income",
    "revenue",
    "non_operating_revenue",
    "credit",
    "rs-gaap:IncomeLossFromEquityMethodInvestments",
  ),
)
_TOLERANCE = 0.05


@dataclass(frozen=True)
class ParentAccounts:
  """The parent's chart codes the group posts to."""

  cash: str = "1000"
  due_from_affiliates: str = "1150"
  investment: str = "1600"
  note_receivable: str = "1650"
  associate_investment: str = "1700"
  paid_in_capital: str = "3000"
  fee_revenue: str = "4300"
  interest_income: str = "4400"
  sale_gain: str = "4600"


@dataclass(frozen=True)
class Acquired:
  """A member bought from its founders rather than formed by the group, by
  window offset. It joins on the first day of window ``joins_at``, its books
  opening the day before on the cash and equipment it brings against its
  founders' capital; the parent pays ``price`` for the member's
  ``ownership_pct`` and the founders (its ``outside_owner``) keep the rest.
  From window ``tops_up_at`` the parent holds ``top_up_pct``, having paid
  the founders ``top_up_price`` for the difference; from window
  ``sells_back_at`` it holds ``sell_back_pct``, the founders having bought
  the difference back for ``sell_back_price`` — a sale within the method,
  the parent keeping control. Window ``sold_at`` is its last: the parent
  sells the whole of it to ``buyer`` for ``sale_price`` on the first day of
  the next, the founders' share going with it."""

  joins_at: int
  opening_cash: int
  opening_equipment: int
  price: int
  tops_up_at: int | None = None
  top_up_pct: float | None = None
  top_up_price: int = 0
  sells_back_at: int | None = None
  sell_back_pct: float | None = None
  sell_back_price: int = 0
  sold_at: int | None = None
  sale_price: int = 0
  buyer: str | None = None


@dataclass(frozen=True)
class Member:
  """A subsidiary, with the shape of its months. All amounts are integer cents.

  ``template`` is one of the shipped chart templates; the accounts the group
  needs beyond it (due to affiliates, the note, distributions, the fee and
  interest expense) are added to the chart. The months post to the
  ``product`` template's codes (1000 cash, 1500 equipment, 3000 contributed
  capital, 4000 revenue, 6600 utilities for the rebill), so another template
  has to carry the same ones. ``ownership_pct`` is the parent's share;
  ``outside_owner`` holds the rest and contributes it at formation — or, for
  a member ``acquired``, sold the parent its share. Under half, the member is
  on the equity method: never summed, its income picked up at the parent's
  share.
  """

  name: str
  legal_name: str
  ticker: str
  description: str
  entity_type: str = "llc"
  template: str = "product"
  ownership_pct: float = 100.0
  outside_owner: str | None = None
  contributed_capital: int = 2_500_000
  monthly_revenue: int = 1_800_000
  revenue_memo: str = "Monthly sales"
  # (memo, monthly cents) per expense line; each takes the next expense account.
  expenses: tuple[tuple[str, int], ...] = (
    ("Rent", 450_000),
    ("Wages", 720_000),
  )
  # The parent's monthly management fee, in basis points of revenue, settled
  # on the 10th of the following month with everything else the member owes.
  management_fee_bps: int = 500
  # A note from the parent at formation, interest accrued monthly at the rate.
  note_from_parent: int = 0
  note_rate_bps: int = 50
  # One bill the parent pays on the member's behalf and rebills: (month, cents).
  rebill: tuple[int, int] | None = None
  # A distribution every third month, split by ownership.
  distribution: int = 0
  # Bought mid-life, and perhaps sold, rather than formed by the group.
  acquired: Acquired | None = None

  @property
  def parent_share(self) -> float:
    return self.ownership_pct / 100.0

  @property
  def outside_share(self) -> float:
    return 1.0 - self.parent_share

  @property
  def method(self) -> str:
    """The treatment the platform derives from the share: in full from half,
    the equity method from a fifth, at cost below."""
    if self.ownership_pct >= 50:
      return "full"
    return "equity" if self.ownership_pct >= 20 else "cost"

  @property
  def joins_at(self) -> int:
    """The offset of the first window the member is in the group."""
    return self.acquired.joins_at if self.acquired else 0

  @property
  def sold_at(self) -> int | None:
    """The offset of the member's last window in the group, if it is sold."""
    return self.acquired.sold_at if self.acquired else None

  def is_member_at(self, index: int) -> bool:
    return index >= self.joins_at and (self.sold_at is None or index <= self.sold_at)

  def share_at(self, index: int) -> float:
    """The parent's share in window ``index``: the one it joined with, the
    one a top-up took it to from that window on, or the one a sale back to
    the founders left it with after that."""
    a = self.acquired
    if a and a.sells_back_at is not None and a.sell_back_pct is not None:
      if index >= a.sells_back_at:
        return a.sell_back_pct / 100.0
    if a and a.tops_up_at is not None and a.top_up_pct is not None:
      if index >= a.tops_up_at:
        return a.top_up_pct / 100.0
    return self.parent_share

  def outside_share_at(self, index: int) -> float:
    return 1.0 - self.share_at(index)

  def windows(self, windows: list[tuple[date, date]]) -> list[tuple[date, date]]:
    """The windows the member's own books cover, the last one left open: all
    of them from the one it joined in, or through the one after its sale."""
    if self.sold_at is None:
      return windows[self.joins_at :]
    return windows[self.joins_at : self.sold_at + 2]


@dataclass(frozen=True)
class Group:
  members: tuple[Member, ...]
  consolidation_name: str = "Consolidation Adjustments"
  consolidation_legal_name: str = "Consolidation Adjustments"
  consolidation_ticker: str = "CONSOL"
  parent: ParentAccounts = field(default_factory=ParentAccounts)
  # The share of the group's formation (investments and notes) the founders
  # put into the parent for it; the parent funds the rest from its own cash.
  founders_share: float = 1.0
  funding_memo: str = "Capital contributed by the founders for the group"


@dataclass
class GroupCheck:
  name: str
  passed: bool
  detail: str


@dataclass
class GroupResult:
  member_ids: dict[str, str]
  consolidation_id: str
  months_closed: int
  parent_cash: float | None
  member_cash: dict[str, float | None]
  consolidated_cash: float | None
  noncontrolling_interest: float | None
  checks: list[GroupCheck]

  @property
  def passed(self) -> bool:
    return all(c.passed for c in self.checks)


_RATE_LIMIT_RETRIES = 3


class _Api:
  """The REST operations and the GraphQL read, with the demo's credentials."""

  def __init__(self, graph_id: str, config: dict[str, str]) -> None:
    self.graph_id = graph_id
    self.base_url = config["base_url"].rstrip("/")
    self.http = httpx.Client(
      headers={"X-API-Key": config["token"], "Content-Type": "application/json"},
      timeout=180.0,
    )
    # ``{qname: element id}`` for the graph's rs-gaap library copy, read once.
    self.rs_gaap_ids: dict[str, str] | None = None

  def close(self) -> None:
    self.http.close()

  def operation(self, name: str, body: dict[str, Any], *, ok: tuple[int, ...] = ()):
    """POST one roboledger operation; returns the envelope's ``result``.
    Statuses in ``ok`` (a 409 for something already done) return None.
    A 429 waits out the rate-limit window and retries."""
    url = f"{self.base_url}/extensions/roboledger/{self.graph_id}/operations/{name}"
    response = self.http.post(url, json=body)
    for _ in range(_RATE_LIMIT_RETRIES):
      if response.status_code != 429:
        break
      time.sleep(int(response.headers.get("Retry-After", "60")))
      response = self.http.post(url, json=body)
    if response.status_code in ok:
      return None
    if response.status_code >= 400:
      raise RuntimeError(
        f"{name} failed ({response.status_code}): {response.text[:600]}"
      )
    envelope = response.json()
    if envelope.get("status") not in ("completed", None) or envelope.get("error"):
      raise RuntimeError(f"{name} did not complete: {envelope}")
    return envelope.get("result")

  def graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict:
    response = self.http.post(
      f"{self.base_url}/extensions/{self.graph_id}/graphql",
      json={"query": query, "variables": variables or {}},
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
      raise RuntimeError(f"GraphQL failed: {payload['errors']}")
    return payload["data"]


def _period(d: date) -> str:
  return f"{d.year:04d}-{d.month:02d}"


def _share(cents: int, share: float) -> int:
  return round(cents * share)


def _chart_namespace(ticker: str) -> str:
  """The qname prefix ``initialize-chart-of-accounts`` mints for a sibling's
  chart (``chart_namespace``): the ticker, lower-cased and squeezed."""
  slug = re.sub(r"[^a-z0-9]", "", ticker.lower())[:12]
  return f"coa-{slug}"


class _Books:
  """One entity's accounts by code, and its postings."""

  def __init__(self, api: _Api, entity_id: str, name: str) -> None:
    self.api = api
    self.entity_id = entity_id
    self.name = name
    self._accounts: dict[str, dict] | None = None

  def refresh(self) -> None:
    self._accounts = None

  def _by_code(self) -> dict[str, dict]:
    if self._accounts is None:
      rows = self.api.graphql(
        "query($e: String!) { accounts(entityId: $e, limit: 500) "
        "{ accounts { id code name } } }",
        {"e": self.entity_id},
      )["accounts"]["accounts"]
      self._accounts = {row["code"]: row for row in rows}
    return self._accounts

  def account(self, code: str) -> dict:
    try:
      return self._by_code()[code]
    except KeyError:
      raise RuntimeError(f"{self.name} has no account {code}") from None

  def any_account(self) -> dict:
    """One of the entity's accounts, whichever comes first."""
    accounts = self._by_code()
    if not accounts:
      raise RuntimeError(f"{self.name} has no accounts")
    return next(iter(accounts.values()))

  def has_account(self, code: str) -> bool:
    try:
      self.account(code)
    except RuntimeError:
      return False
    return True

  def expense_accounts(self) -> list[dict]:
    rows = self.api.graphql(
      'query($e: String!) { accounts(entityId: $e, classification: "expense", '
      "limit: 200) { accounts { id code name } } }",
      {"e": self.entity_id},
    )["accounts"]["accounts"]
    return sorted(rows, key=lambda r: r["code"])

  def post(
    self,
    posting_date: date,
    memo: str,
    lines: list[tuple[str, int, int, str | None]],
    *,
    agent_id: str | None = None,
    adjustment: tuple[str, str] | None = None,
  ) -> None:
    """A posted journal entry; ``lines`` are (code, debit, credit, flow_qname).
    ``adjustment`` is a consolidating entry's (kind, entity it adjusts)."""
    items = []
    for code, debit, credit, flow in lines:
      if not debit and not credit:
        continue
      item: dict[str, Any] = {
        "element_id": self.account(code)["id"],
        "debit_amount": debit,
        "credit_amount": credit,
      }
      if flow:
        item["flow_qname"] = flow
      items.append(item)
    if not items:
      return
    debits = sum(i["debit_amount"] for i in items)
    credits = sum(i["credit_amount"] for i in items)
    if debits != credits:
      raise RuntimeError(
        f"{self.name}: {memo!r} does not balance ({debits} / {credits})"
      )
    body: dict[str, Any] = {
      "entity_id": self.entity_id,
      "event_type": "journal_entry_recorded",
      "event_category": "adjustment",
      "event_class": "economic",
      "source": "manual",
      "occurred_at": f"{posting_date.isoformat()}T00:00:00+00:00",
      "apply_handlers": True,
      "amount": debits,
      "currency": "USD",
      "description": memo,
      "metadata": {
        "posting_date": posting_date.isoformat(),
        "memo": memo,
        "type": "standard",
        "status": "posted",
        "line_items": items,
      },
    }
    if adjustment:
      body["metadata"]["adjustment_kind"] = adjustment[0]
      body["metadata"]["adjusts_entity_id"] = adjustment[1]
    if agent_id:
      body["agent_id"] = agent_id
    self.api.operation("create-event-block", body)

  def has_postings(self) -> bool:
    total = self.api.graphql(
      "query($e: String!) { transactions(entityId: $e, limit: 1) "
      "{ pagination { total } } }",
      {"e": self.entity_id},
    )["transactions"]["pagination"]["total"]
    return bool(total)

  def closed_through(self) -> str | None:
    status = self.api.graphql(
      "query($e: String!) { fiscalCalendar(entityId: $e) { closedThrough } }",
      {"e": self.entity_id},
    )["fiscalCalendar"]
    return (status or {}).get("closedThrough")


def _statement(
  api: _Api,
  statement_type: str,
  start: date,
  end: date,
  *,
  entity_id: str | None = None,
  consolidated: bool = False,
) -> dict:
  body: dict[str, Any] = {
    "statement_type": statement_type,
    "period_start": start.isoformat(),
    "period_end": end.isoformat(),
  }
  if entity_id:
    body["entity_id"] = entity_id
  if consolidated:
    body["consolidated"] = True
  return api.operation("live-financial-statement", body)


def _row(statement: dict, qname: str, end: date) -> float | None:
  """The value of one concept's row for the column ending on ``end``; None
  when the statement has no such row (an all-zero row is dropped)."""
  periods = statement.get("periods") or []
  column = next(
    (i for i, p in enumerate(periods) if p.get("end") == end.isoformat()), 0
  )
  for fact in statement.get("facts") or []:
    if fact.get("qname") == qname:
      values = fact.get("values") or []
      return values[column] if column < len(values) else None
  return None


def _value(statement: dict, qname: str, end: date) -> float:
  return _row(statement, qname, end) or 0.0


def _ensure_entity(api: _Api, name: str, body: dict[str, Any]) -> tuple[str, bool]:
  own = api.graphql("{ entities { id name source } }")["entities"]
  existing = next(
    (e for e in own if e["name"] == name and e["source"] != "linked"), None
  )
  if existing:
    return existing["id"], True
  created = api.operation("create-entity", body)
  assert created is not None
  return created["id"], False


def _ensure_chart(
  api: _Api, entity_id: str, template: str, entity_type: str
) -> str | None:
  """Initialize the entity's chart from ``template``; returns the chart's
  taxonomy id, or None when it already had one."""
  chart = api.operation(
    "initialize-chart-of-accounts",
    {"template": template, "entity_id": entity_id, "entity_type": entity_type},
    ok=(409,),
  )
  return chart.get("taxonomy_id") if chart else None


def _rs_gaap_ids(api: _Api) -> dict[str, str]:
  """``{qname: element id}`` for the graph's rs-gaap library copy, once."""
  if api.rs_gaap_ids is not None:
    return api.rs_gaap_ids
  ids: dict[str, str] = {}
  offset = 0
  while True:
    page = api.graphql(
      'query($o: Int!) { elements(source: "rs-gaap", limit: 1000, offset: $o) '
      "{ elements { id qname } } }",
      {"o": offset},
    )["elements"]["elements"]
    ids.update({e["qname"]: e["id"] for e in page if e.get("qname")})
    if len(page) < 1000:
      break
    offset += 1000
  api._rs_gaap_ids = ids  # type: ignore[attr-defined]
  return ids


def _mapping_structure(
  api: _Api, books: _Books, taxonomy_id: str | None
) -> tuple[str, str]:
  """The entity's CoA→rs-gaap mapping structure and its chart's id. The
  chart's id names it directly; without one (a re-run), the structure whose
  taxonomy holds one of the entity's accounts."""
  candidates = api.graphql(
    'query($t: String) { structures(taxonomyId: $t, blockType: "coa_mapping") '
    "{ structures { id taxonomyId } } }",
    {"t": taxonomy_id},
  )["structures"]["structures"]
  if taxonomy_id is None:
    anchor_id = books.any_account()["id"]
    for candidate in candidates:
      ids = {
        e["id"]
        for e in api.graphql(
          "query($t: String!) { elements(taxonomyId: $t, limit: 500) "
          "{ elements { id } } }",
          {"t": candidate["taxonomyId"]},
        )["elements"]["elements"]
      }
      if anchor_id in ids:
        return candidate["id"], candidate["taxonomyId"]
    raise RuntimeError(f"{books.name}: no mapping structure holds its chart")
  if not candidates:
    raise RuntimeError(f"{books.name}: the chart has no mapping structure")
  return candidates[0]["id"], taxonomy_id


def _extend_chart(
  api: _Api,
  books: _Books,
  taxonomy_id: str | None,
  rows: tuple[tuple[str, str, str, str, str, str | None], ...],
  *,
  namespace: str,
  equity_concept: str,
) -> int:
  """Add the group's accounts to an entity's chart when missing, each mapped
  to its rs-gaap concept the way a person would: the accounts through
  ``update-taxonomy-block``, the arcs one by one through
  ``create-mapping-association`` on the chart's mapping structure. Resolves
  every target before it writes anything, and maps whatever is still unmapped
  afterwards, so a run that died between the two steps heals on the next,
  and a chart reused from an earlier run takes the rows it lacks."""
  targets = _rs_gaap_ids(api)
  wanted = {code: target or equity_concept for code, *_rest, target in rows}
  unknown = sorted(q for q in wanted.values() if q not in targets)
  if unknown:
    raise RuntimeError(
      f"{books.name}: not in this graph's rs-gaap library: {', '.join(unknown)}"
    )

  # Unmapped is measured against the mapping structure, not the entity.
  mapping_id, chart_id = _mapping_structure(api, books, taxonomy_id)
  missing = [row for row in rows if not books.has_account(row[0])]
  if missing:
    api.operation(
      "update-taxonomy-block",
      {
        "taxonomy_id": chart_id,
        "elements_to_add": [
          {
            "qname": f"{namespace}:{code}",
            "name": name,
            "trait": trait,
            "balance_type": balance,
            "code": code,
            "sub_classification": sub_classification,
          }
          for code, name, trait, sub_classification, balance, _target in missing
        ],
      },
    )
    books.refresh()

  unmapped = {
    e.get("code")
    for e in api.graphql(
      "query($m: String!) { unmappedElements(mappingId: $m) { code } }",
      {"m": mapping_id},
    )["unmappedElements"]
  }
  to_map = [code for code in wanted if code in unmapped]
  if to_map:
    for code in to_map:
      api.operation(
        "create-mapping-association",
        {
          "mapping_id": mapping_id,
          "from_element_id": books.account(code)["id"],
          "to_element_id": targets[wanted[code]],
        },
        ok=(409,),
      )
  return len(missing)


def _ensure_calendar(api: _Api, entity_id: str, ticker: str, windows) -> None:
  api.operation(
    "initialize",
    {
      "entity_id": entity_id,
      "closed_through": None,
      "earliest_data_period": _period(windows[0][0]),
      "note": f"{ticker} initialization",
    },
    ok=(409,),
  )
  api.operation(
    "set-close-target",
    {
      "entity_id": entity_id,
      "period": _period(windows[-1][1]),
      "note": "initialization",
    },
  )


def _ensure_agent(
  api: _Api, agents: dict[str, str], name: str, agent_type: str, external_id: str | None
) -> str:
  if name in agents:
    return agents[name]
  created = api.operation(
    "create-agent",
    {
      "agent_type": agent_type,
      "name": name,
      "source": "native",
      **({"external_id": external_id} if external_id else {}),
    },
  )
  assert created is not None
  agents[name] = created["id"]
  return created["id"]


def _close_history(books: _Books, windows, label: str) -> int:
  closed_through = books.closed_through()
  closed = 0
  for _start, end in windows[:-1]:
    period = _period(end)
    if closed_through and period <= closed_through:
      continue
    books.api.operation(
      "close-period",
      {
        "entity_id": books.entity_id,
        "period": period,
        "note": "historical close (provisioning)",
      },
    )
    closed += 1
  print(
    f"  Closed:       {label} {closed} months (target {_period(windows[-1][1])} stays open)"
  )
  return closed


def load_group(
  graph_id: str,
  group: Group,
  *,
  config: dict[str, str],
  windows: list[tuple[date, date]],
  parent_taxonomy_id: str | None = None,
  close_history: bool = True,
  close_parent_history: Callable[[], None] | None = None,
) -> GroupResult:
  """Put the group in the graph beside the parent and read it back.

  ``windows`` are the parent's month windows; the last one is the open close
  target and stays open here too. Must run before the parent's history
  closes: the parent's side of every intercompany event posts in those
  months. ``close_parent_history`` closes it, between the members' closes and
  the group's: the consolidation entity's close is the group close, which
  waits for every entity the month consolidates. Re-runs reuse the entities, charts and calendars, and post nothing
  twice: a group with any posting already in its books is left as it is.
  """
  api = _Api(graph_id, config)
  try:
    return _load(
      api, group, windows, parent_taxonomy_id, close_history, close_parent_history
    )
  finally:
    api.close()


def _check_roster(group: Group, windows: list[tuple[date, date]]) -> None:
  """What the loader models, before it writes anything."""
  # The consolidation entity presents its own form's cash flow rows, and its
  # eliminations tag the rows the members' lines took, so a member of another
  # form would need a consolidation entity on that form.
  for member in group.members:
    if member.method == "cost" or (
      member.method == "equity"
      and (member.note_from_parent or member.management_fee_bps or member.rebill)
    ):
      raise RuntimeError(
        f"{member.name}: an equity-method member takes no note, fee or rebill, "
        "and a member at cost is not modelled"
      )
    a = member.acquired
    if a is None:
      continue
    # The read-back month is the last closed one; a sale lands its
    # deconsolidation in the month after, which has to close before it.
    last_closed = len(windows) - 2
    if member.method != "full" or member.note_from_parent or member.rebill:
      raise RuntimeError(
        f"{member.name}: an acquired member is bought in full, and takes no "
        "note or rebill"
      )
    if member.ownership_pct < 100 and not member.outside_owner:
      raise RuntimeError(f"{member.name}: name the founders who keep the rest")
    if not 0 < a.joins_at <= last_closed:
      raise RuntimeError(f"{member.name}: joins_at must fall inside the closed months")
    if a.tops_up_at is not None:
      if a.top_up_pct is None or a.top_up_pct <= member.ownership_pct:
        raise RuntimeError(f"{member.name}: a top-up raises the parent's share")
      if (
        not a.joins_at
        < a.tops_up_at
        <= (a.sold_at if a.sold_at is not None else last_closed)
      ):
        raise RuntimeError(f"{member.name}: tops_up_at must fall within its membership")
    if a.sells_back_at is not None:
      held = a.top_up_pct if a.tops_up_at is not None else member.ownership_pct
      if a.sell_back_pct is None or not 50 <= a.sell_back_pct < (held or 0):
        raise RuntimeError(
          f"{member.name}: a sale back lowers the parent's share, keeping half"
        )
      after = a.tops_up_at if a.tops_up_at is not None else a.joins_at
      if (
        not after
        < a.sells_back_at
        <= (a.sold_at if a.sold_at is not None else last_closed)
      ):
        raise RuntimeError(
          f"{member.name}: sells_back_at must follow the top-up within its membership"
        )
    if a.sold_at is not None:
      if not a.joins_at <= a.sold_at < last_closed or not a.buyer:
        raise RuntimeError(
          f"{member.name}: sold_at leaves a closed month after it, and names the buyer"
        )
  other_forms = sorted({m.entity_type for m in group.members} - {"llc"})
  if other_forms:
    raise RuntimeError(
      "the consolidation entity is an LLC; a member of another form "
      f"({', '.join(other_forms)}) needs one on its own form"
    )


def _load(
  api: _Api,
  group: Group,
  windows: list[tuple[date, date]],
  parent_taxonomy_id: str | None,
  close_history: bool,
  close_parent_history: Callable[[], None] | None,
) -> GroupResult:
  parent_entity = next(
    e
    for e in api.graphql("{ entities { id name source isParent } }")["entities"]
    if e.get("isParent") and e["source"] != "linked"
  )
  parent = _Books(api, parent_entity["id"], parent_entity["name"])
  added = _extend_chart(
    api,
    parent,
    parent_taxonomy_id,
    _PARENT_ACCOUNTS,
    namespace="coa",
    equity_concept=_FORM_EQUITY["corporation"],
  )
  if added:
    print(f"  Parent:       {parent.name} chart + {added} group accounts")

  # 1. The entities, their charts and calendars; reused on a re-run.
  _check_roster(group, windows)
  members: dict[str, _Books] = {}
  reused_members: set[str] = set()
  for member in group.members:
    body: dict[str, Any] = {
      "name": member.name,
      "legal_name": member.legal_name,
      "ticker": member.ticker,
      "entity_type": member.entity_type,
      "ownership_pct": member.ownership_pct,
    }
    if member.acquired:
      body["joined_on"] = windows[member.joins_at][0].isoformat()
    entity_id, reused = _ensure_entity(api, member.name, body)
    if reused:
      reused_members.add(member.ticker)
    books = _Books(api, entity_id, member.name)
    members[member.ticker] = books
    taxonomy_id = _ensure_chart(api, entity_id, member.template, member.entity_type)
    added = _extend_chart(
      api,
      books,
      taxonomy_id,
      _MEMBER_ACCOUNTS,
      namespace=_chart_namespace(member.ticker),
      equity_concept=_FORM_EQUITY[member.entity_type],
    )
    # An acquired member's books start the month it joins.
    _ensure_calendar(api, entity_id, member.ticker, member.windows(windows))
    lifecycle = ""
    if member.acquired:
      a = member.acquired
      lifecycle = f", acquired {_period(windows[a.joins_at][0])}"
      if a.tops_up_at is not None:
        lifecycle += f", {a.top_up_pct:g}% from {_period(windows[a.tops_up_at][0])}"
      if a.sells_back_at is not None:
        lifecycle += (
          f", {a.sell_back_pct:g}% from {_period(windows[a.sells_back_at][0])}"
        )
      if a.sold_at is not None:
        lifecycle += f", sold after {_period(windows[a.sold_at][1])}"
    print(
      f"  {member.ticker:<8} {entity_id} ({member.ownership_pct:g}% owned"
      + (f", {member.outside_owner} holds the rest" if member.outside_owner else "")
      + lifecycle
      + (", reused" if reused else "")
      + f"; {member.template} chart"
      + (f" + {added} group accounts" if added else "")
      + ")"
    )
  consolidation_id, reused = _ensure_entity(
    api,
    group.consolidation_name,
    {
      "name": group.consolidation_name,
      "legal_name": group.consolidation_legal_name,
      "ticker": group.consolidation_ticker,
      "entity_type": "llc",
      "entity_role": "consolidation",
    },
  )
  if reused:
    # A reset keeps entity rows; one made before the role existed is typed here.
    api.operation(
      "update-entity",
      {"entity_id": consolidation_id, "entity_role": "consolidation"},
    )
  consolidation = _Books(api, consolidation_id, group.consolidation_name)
  _extend_chart(
    api,
    consolidation,
    _ensure_chart(api, consolidation_id, "consolidation", "llc"),
    _CONSOLIDATION_ACCOUNTS,
    namespace=_chart_namespace(group.consolidation_ticker),
    equity_concept=_FORM_EQUITY["llc"],
  )
  _ensure_calendar(api, consolidation_id, group.consolidation_ticker, windows)
  print(
    f"  {group.consolidation_ticker:<8} {consolidation_id} (the consolidation entity"
    + (", reused" if reused else "")
    + "; consolidation chart)"
  )

  # 2. The counterparties: each member and the parent as a `self` agent, so an
  # intercompany event names the entity on its other side; outside owners.
  agents = {
    a["name"]: a["id"]
    for a in api.graphql("{ agents(limit: 500, isActive: null) { id name } }")["agents"]
  }
  agent_of: dict[str, str] = {
    parent.entity_id: _ensure_agent(api, agents, parent.name, "self", parent.entity_id)
  }
  for member in group.members:
    books = members[member.ticker]
    agent_of[books.entity_id] = _ensure_agent(
      api, agents, member.name, "self", books.entity_id
    )
    if member.outside_owner:
      agent_of[member.outside_owner] = _ensure_agent(
        api, agents, member.outside_owner, "owner", None
      )
    if member.acquired and member.acquired.buyer:
      agent_of[member.acquired.buyer] = _ensure_agent(
        api, agents, member.acquired.buyer, "owner", None
      )

  # 3. The months, on every side at once — unless the group is already in.
  # A whole group is reused; a partial one — a run that died mid-way — is not
  # patched up, since the parent's side would post twice.
  first_start, first_end = windows[0]
  parent_invested = _value(
    _statement(api, "balance_sheet", first_start, first_end),
    "rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures",
    first_end,
  )
  present = {m.name: members[m.ticker].has_postings() for m in group.members}
  present[group.consolidation_name] = consolidation.has_postings()
  present[parent.name] = bool(parent_invested)
  if all(present.values()):
    print("  Activity:     already in the books; nothing posted")
  elif any(present.values()):
    half = ", ".join(name for name, there in present.items() if there)
    raise RuntimeError(
      f"a partial group is in the books ({half} only); reset the demo and run "
      "again rather than posting on top of it"
    )
  else:
    # A reset keeps entity rows as the last run left them — a share bought
    # up, a member marked as left — so a reused member starts over from the
    # roster before anything posts.
    for member in group.members:
      if member.ticker in reused_members:
        api.operation(
          "update-entity",
          {
            "entity_id": members[member.ticker].entity_id,
            "ownership_pct": member.ownership_pct,
            "joined_on": windows[member.joins_at][0].isoformat()
            if member.acquired
            else None,
            "left_on": None,
          },
        )
    _post_group(api, group, parent, members, consolidation, agent_of, windows)

  # 4. Close every member's history, then the parent's, then the group's (the
  # consolidation entity's close, which waits for them), month by month; the
  # close target stays open.
  months_closed = 0
  if close_history:
    for member in group.members:
      # A member sold closed its months as it left; its windows are closed.
      months_closed += _close_history(
        members[member.ticker], member.windows(windows), member.ticker
      )
    if close_parent_history is not None:
      close_parent_history()
    months_closed += _close_history(consolidation, windows, group.consolidation_ticker)

  # 5. Read the group back on the last closed month and check the
  # consolidation.
  last_start, last_end = windows[-2]
  return _read_back(
    api,
    group,
    parent,
    members,
    consolidation,
    last_start,
    last_end,
    months_closed,
    windows=windows,
  )


def _post_group(
  api: _Api,
  group: Group,
  parent: _Books,
  members: dict[str, _Books],
  consolidation: _Books,
  agent_of: dict[str, str],
  windows: list[tuple[date, date]],
) -> None:
  pa = group.parent
  demo_start = windows[0][0]
  last_index = len(windows) - 1
  parent_agent = agent_of[parent.entity_id]

  # What each member owes the parent at the month end, in cents.
  due: dict[str, int] = {m.ticker: 0 for m in group.members}
  # What the parent has taken back of each member's capital in distributions,
  # and what a sale back to the founders released: its investment's carrying
  # amount is what it paid less these.
  received: dict[str, int] = {m.ticker: 0 for m in group.members}
  released: dict[str, int] = {m.ticker: 0 for m in group.members}

  # Formation. The founders fund the parent for the group, the parent forms
  # each member, and the outside owner puts in its share directly. A member
  # bought mid-life is not formed here; it arrives in its month.
  funding = sum(
    _share(m.contributed_capital, m.parent_share) + m.note_from_parent
    for m in group.members
    if not m.acquired
  )
  raised = _share(funding, group.founders_share)
  count = 0
  if raised:
    parent.post(
      demo_start,
      group.funding_memo,
      [(pa.cash, raised, 0, None), (pa.paid_in_capital, 0, raised, None)],
    )
    count = 1
  for member in group.members:
    if member.acquired:
      continue
    books = members[member.ticker]
    member_agent = agent_of[books.entity_id]
    parent_part = _share(member.contributed_capital, member.parent_share)
    outside_part = member.contributed_capital - parent_part
    investment = pa.investment if member.method == "full" else pa.associate_investment
    parent.post(
      demo_start,
      f"Investment in {member.name} at formation",
      [(investment, parent_part, 0, None), (pa.cash, 0, parent_part, None)],
      agent_id=member_agent,
    )
    books.post(
      demo_start,
      f"Capital contributed by {parent.name}",
      [("1000", parent_part, 0, None), ("3000", 0, parent_part, None)],
      agent_id=parent_agent,
    )
    count += 2
    if outside_part and member.outside_owner:
      books.post(
        demo_start,
        f"Capital contributed by {member.outside_owner}",
        [("1000", outside_part, 0, None), ("3000", 0, outside_part, None)],
        agent_id=agent_of[member.outside_owner],
      )
      count += 1
    if member.note_from_parent:
      parent.post(
        demo_start,
        f"Note to {member.name}",
        [
          (pa.note_receivable, member.note_from_parent, 0, None),
          (pa.cash, 0, member.note_from_parent, None),
        ],
        agent_id=member_agent,
      )
      books.post(
        demo_start,
        f"Note from {parent.name}",
        [
          ("1000", member.note_from_parent, 0, "rs-gaap:ProceedsFromRelatedPartyDebt"),
          ("2600", 0, member.note_from_parent, None),
        ],
        agent_id=parent_agent,
      )
      count += 2

  for index, (start, end) in enumerate(windows):
    mid = start.replace(day=15)
    settle_day = start.replace(day=10)
    for member in group.members:
      books = members[member.ticker]
      member_agent = agent_of[books.entity_id]
      lifecycle = member.acquired
      if lifecycle and index == lifecycle.joins_at:
        count += _acquire(api, group, member, books, parent, agent_of, start)
      if lifecycle and lifecycle.sold_at is not None and index == lifecycle.sold_at + 1:
        count += _sell(
          api,
          group,
          member,
          books,
          parent,
          agent_of,
          start,
          received[member.ticker] + released[member.ticker],
        )

      # Last month's charges settle on the 10th — a member sold pays its last.
      if index and due[member.ticker]:
        owed = due[member.ticker]
        books.post(
          settle_day,
          f"Settlement of amounts due to {parent.name}",
          [("2150", owed, 0, None), ("1000", 0, owed, None)],
          agent_id=parent_agent,
        )
        parent.post(
          settle_day,
          f"Settlement received from {member.name}",
          [(pa.cash, owed, 0, None), (pa.due_from_affiliates, 0, owed, None)],
          agent_id=member_agent,
        )
        due[member.ticker] = 0
        count += 2
      if not member.is_member_at(index):
        continue
      if lifecycle and index == lifecycle.tops_up_at:
        count += _top_up(api, group, member, books, parent, agent_of, start)
      if lifecycle and index == lifecycle.sells_back_at:
        released[member.ticker] = _sell_back(
          api, group, member, books, parent, agent_of, start, received[member.ticker]
        )
        count += 2
      expense_accounts = books.expense_accounts()
      if len(expense_accounts) < len(member.expenses):
        raise RuntimeError(
          f"{member.name}'s chart has {len(expense_accounts)} expense accounts"
        )

      # The member's own month.
      books.post(
        mid,
        member.revenue_memo,
        [
          ("1000", member.monthly_revenue, 0, None),
          ("4000", 0, member.monthly_revenue, None),
        ],
      )
      count += 1
      for (memo, cents), account in zip(
        member.expenses, expense_accounts, strict=False
      ):
        books.post(
          end, memo, [(account["code"], cents, 0, None), ("1000", 0, cents, None)]
        )
        count += 1

      # What passes between the member and the parent this month.
      fee = _share(member.monthly_revenue, member.management_fee_bps / 10_000)
      interest = _share(member.note_from_parent, member.note_rate_bps / 10_000)
      rebill = member.rebill[1] if member.rebill and member.rebill[0] == index else 0
      if fee:
        parent.post(
          end,
          f"Management fee — {member.name}",
          [(pa.due_from_affiliates, fee, 0, None), (pa.fee_revenue, 0, fee, None)],
          agent_id=member_agent,
        )
        books.post(
          end,
          f"Management fee — {parent.name}",
          [("6700", fee, 0, None), ("2150", 0, fee, None)],
          agent_id=parent_agent,
        )
        count += 2
      if interest:
        parent.post(
          end,
          f"Interest on the note — {member.name}",
          [
            (pa.due_from_affiliates, interest, 0, None),
            (pa.interest_income, 0, interest, None),
          ],
          agent_id=member_agent,
        )
        books.post(
          end,
          f"Interest on the note — {parent.name}",
          [("6800", interest, 0, None), ("2150", 0, interest, None)],
          agent_id=parent_agent,
        )
        count += 2
      if rebill:
        parent.post(
          end,
          f"Utilities paid for {member.name}, rebilled",
          [(pa.due_from_affiliates, rebill, 0, None), (pa.cash, 0, rebill, None)],
          agent_id=member_agent,
        )
        books.post(
          end,
          f"Utilities paid by {parent.name}, rebilled",
          [("6600", rebill, 0, None), ("2150", 0, rebill, None)],
          agent_id=parent_agent,
        )
        count += 2
      due[member.ticker] = fee + interest + rebill

      # A distribution every third month, split by ownership.
      if (
        member.distribution
        and (index + 1) % _DISTRIBUTION_EVERY == 0
        and index < last_index
      ):
        parent_part = _share(member.distribution, member.share_at(index))
        received[member.ticker] += parent_part
        books.post(
          end,
          "Distribution to members",
          [
            ("3200", member.distribution, 0, None),
            ("1000", 0, member.distribution, None),
          ],
        )
        investment = (
          pa.investment if member.method == "full" else pa.associate_investment
        )
        parent.post(
          end,
          f"Distribution from {member.name}",
          [(pa.cash, parent_part, 0, None), (investment, 0, parent_part, None)],
          agent_id=member_agent,
        )
        count += 2

    # The month's consolidating entries, drafted by the engine now that every
    # side of it is in the books; the consolidation entity's close posts them.
    proposed = api.operation("propose-consolidation-entries", {"period": _period(end)})
    assert proposed is not None
    count += len(proposed["entries"])
    for residual in proposed["residuals"]:
      print(f"  ⚠️  {_period(end)} residual: {residual['explanation']}")
    for item in proposed["reconciling_items"]:
      print(f"  ⚠️  {_period(end)} reconciling item: {item['description']}")
    for note in proposed.get("notes") or []:
      print(f"  ℹ️  {_period(end)}: {note}")

    # A member sold leaves after this month: its books close through it
    # first, as the exit guards require, and the next month reads it out.
    for member in group.members:
      if member.sold_at == index:
        _leave(api, member, members[member.ticker], windows, index)

  print(f"  Activity:     {count} entries over {len(windows)} months, on every side")


def _acquire(
  api: _Api,
  group: Group,
  member: Member,
  books: _Books,
  parent: _Books,
  agent_of: dict[str, str],
  joined_on: date,
) -> int:
  """The member arrives: its books open on the balance sheet it brings, the
  parent pays its founders for its share (the purchase names the seller, so
  no line names the member), and the acquisition is recorded against the
  equity it brought — the excess is goodwill."""
  a = member.acquired
  assert a is not None and member.outside_owner
  pa = group.parent
  api.operation(
    "record-opening-balance",
    {
      "entity_id": books.entity_id,
      "balances": [
        {"element_id": books.account("1000")["id"], "debit_amount": a.opening_cash},
        {
          "element_id": books.account("1500")["id"],
          "debit_amount": a.opening_equipment,
        },
        {
          "element_id": books.account("3000")["id"],
          "credit_amount": a.opening_cash + a.opening_equipment,
        },
      ],
      "memo": f"{member.name}'s balance sheet at acquisition",
    },
  )
  # The parent's cash line carries the business-acquisition row, as a
  # controller tags it: the group's read nets the cash that came with the
  # member against it, so the consolidated statement shows one row, net of
  # cash acquired, and nothing on the investment row.
  parent.post(
    joined_on,
    f"Purchase of {member.ownership_pct:g}% of {member.name} from {member.outside_owner}",
    [
      (pa.investment, a.price, 0, None),
      (pa.cash, 0, a.price, "rs-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired"),
    ],
    agent_id=agent_of[member.outside_owner],
  )
  recorded = api.operation(
    "record-acquisition",
    {
      "entity_id": books.entity_id,
      "consideration": a.price,
      "basis": (
        f"Purchase agreement with {member.outside_owner}, closed "
        f"{joined_on.isoformat()}: {member.ownership_pct:g}% of {member.name} "
        "at the balance sheet it brought, the excess over book as goodwill."
      ),
    },
  )
  assert recorded is not None
  print(
    f"  Acquired:     {member.ticker} on {joined_on} for {_money(a.price / 100)}; "
    f"goodwill {_money(recorded['goodwill'] / 100)}"
  )
  return 3


def _top_up(
  api: _Api,
  group: Group,
  member: Member,
  books: _Books,
  parent: _Books,
  agent_of: dict[str, str],
  start: date,
) -> int:
  """The parent buys more of the member from its founders, staying in
  control: the price above the book value bought adjusts the parent's
  equity, never a gain (ASC 810)."""
  a = member.acquired
  assert a is not None and a.top_up_pct is not None and member.outside_owner
  pa = group.parent
  parent.post(
    start,
    f"Purchase of a further {a.top_up_pct - member.ownership_pct:g}% of "
    f"{member.name} from {member.outside_owner}",
    [(pa.investment, a.top_up_price, 0, None), (pa.cash, 0, a.top_up_price, None)],
    agent_id=agent_of[member.outside_owner],
  )
  recorded = api.operation(
    "record-ownership-change",
    {
      "entity_id": books.entity_id,
      "period": _period(start),
      "ownership_pct": a.top_up_pct,
      "consideration": a.top_up_price,
      "basis": (
        f"Purchase agreement with {member.outside_owner}, {start.isoformat()}: "
        f"the roaster's share of {member.name} to {a.top_up_pct:g}%."
      ),
    },
  )
  assert recorded is not None
  print(
    f"  Bought more:  {member.ticker} to {a.top_up_pct:g}% from {_period(start)} "
    f"for {_money(a.top_up_price / 100)}; the parent's equity takes "
    f"{_money(recorded['equity_adjustment'] / 100)}"
  )
  return 2


def _sell_back(
  api: _Api,
  group: Group,
  member: Member,
  books: _Books,
  parent: _Books,
  agent_of: dict[str, str],
  start: date,
  received: int,
) -> int:
  """The founders buy some of their café back; the parent keeps control. The
  parent's books take the proceeds against the carrying amount of the shares
  sold — its cost, pro rata — and its own gain; the group reverses that gain
  (ASC 810: an equity transaction) and the proceeds become a financing
  inflow from the outside owners. Returns what the parent released."""
  a = member.acquired
  assert a is not None and a.sell_back_pct is not None and member.outside_owner
  pa = group.parent
  held = (
    a.top_up_pct
    if a.tops_up_at is not None and a.top_up_pct is not None
    else member.ownership_pct
  )
  carrying = a.price + a.top_up_price - received
  released = round(carrying * (held - a.sell_back_pct) / held)
  gain = a.sell_back_price - released
  parent.post(
    start,
    f"Sale of {held - a.sell_back_pct:g}% of {member.name} to {member.outside_owner}",
    [
      (
        pa.cash,
        a.sell_back_price,
        0,
        "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates",
      ),
      (pa.investment, 0, released, None),
      (pa.sale_gain, max(-gain, 0), max(gain, 0), None),
    ],
    agent_id=agent_of[member.outside_owner],
  )
  recorded = api.operation(
    "record-ownership-change",
    {
      "entity_id": books.entity_id,
      "period": _period(start),
      "ownership_pct": a.sell_back_pct,
      "consideration": a.sell_back_price,
      "basis": (
        f"Sale agreement with {member.outside_owner}, {start.isoformat()}: "
        f"the roaster's share of {member.name} to {a.sell_back_pct:g}%."
      ),
    },
  )
  assert recorded is not None
  print(
    f"  Sold back:    {member.ticker} to {a.sell_back_pct:g}% from {_period(start)} "
    f"for {_money(a.sell_back_price / 100)}; the parent's gain "
    f"{_money(recorded['gain_reversed'] / 100)} reversed, its equity takes "
    f"{_money(recorded['equity_adjustment'] / 100)}"
  )
  return released


def _leave(
  api: _Api,
  member: Member,
  books: _Books,
  windows: list[tuple[date, date]],
  index: int,
) -> None:
  """The member's last month is over: its books close through it, then the
  entity records the day it left — the order the exit guards require."""
  _close_history(books, windows[member.joins_at : index + 2], member.ticker)
  left_on = windows[index][1]
  api.operation(
    "update-entity", {"entity_id": books.entity_id, "left_on": left_on.isoformat()}
  )
  print(f"  Left:         {member.ticker} after {_period(left_on)}")


def _sell(
  api: _Api,
  group: Group,
  member: Member,
  books: _Books,
  parent: _Books,
  agent_of: dict[str, str],
  start: date,
  received: int,
) -> int:
  """The first day after the member's last month: the parent's books take
  the proceeds against the investment's carrying amount, the difference its
  own gain, and the deconsolidation adjusts that gain to the group's — the
  price less the carrying amount of what left (ASC 810)."""
  a = member.acquired
  assert a is not None and a.buyer
  pa = group.parent
  carrying = a.price + a.top_up_price - received
  gain = a.sale_price - carrying
  parent.post(
    start,
    f"Sale of {member.name} to {a.buyer}",
    [
      (
        pa.cash,
        a.sale_price,
        0,
        "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates",
      ),
      (pa.investment, 0, carrying, None),
      (pa.sale_gain, max(-gain, 0), max(gain, 0), None),
    ],
    agent_id=agent_of[a.buyer],
  )
  recorded = api.operation(
    "record-deconsolidation",
    {
      "entity_id": books.entity_id,
      "consideration": a.sale_price,
      "basis": (
        f"Sale agreement with {a.buyer}, closed {start.isoformat()}: the whole "
        f"of {member.name}, the roaster's share and its founders' together."
      ),
    },
  )
  assert recorded is not None
  print(
    f"  Sold:         {member.ticker} on {start} for {_money(a.sale_price / 100)}; "
    f"the parent's gain {_money(gain / 100)}, the group's adjustment "
    f"{_money(recorded['gain_loss'] / 100)}"
  )
  return 2


def _read_back(
  api: _Api,
  group: Group,
  parent: _Books,
  members: dict[str, _Books],
  consolidation: _Books,
  start: date,
  end: date,
  months_closed: int,
  *,
  windows: list[tuple[date, date]],
) -> GroupResult:
  cash = "rs-gaap:CashAndCashEquivalentsAtCarryingValue"
  checks: list[GroupCheck] = []

  parent_bs = _statement(api, "balance_sheet", start, end)
  member_bs = {
    t: _statement(api, "balance_sheet", start, end, entity_id=b.entity_id)
    for t, b in members.items()
  }
  member_is = {
    t: _statement(api, "income_statement", start, end, entity_id=b.entity_id)
    for t, b in members.items()
  }
  consolidated = {
    kind: _statement(api, kind, start, end, consolidated=True)
    for kind in (
      "balance_sheet",
      "income_statement",
      "cash_flow_statement",
      "equity_statement",
    )
  }
  bs, is_ = consolidated["balance_sheet"], consolidated["income_statement"]

  def check(name: str, actual: float, expected: float) -> None:
    ok = abs(actual - expected) <= _TOLERANCE
    checks.append(GroupCheck(name, ok, f"{actual:,.2f} vs {expected:,.2f}"))

  for qname, label in (
    (
      "rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures",
      "investment in subsidiaries",
    ),
    ("rs-gaap:DueFromAffiliateCurrent", "due from affiliates"),
    ("rs-gaap:DueToAffiliateCurrent", "due to affiliates"),
    ("rs-gaap:LongTermInvestmentsAndReceivablesNet", "note receivable"),
    ("rs-gaap:LongTermDebtAndCapitalLeaseObligations", "note payable"),
  ):
    check(f"consolidated {label} eliminated", _value(bs, qname, end), 0.0)

  last = len(windows) - 2
  nci_expected = 0.0
  nci_income_expected = 0.0
  for member in group.members:
    outside = member.outside_share_at(last)
    if member.method != "full" or not member.is_member_at(last) or not outside:
      continue
    mbs = member_bs[member.ticker]
    net_assets = _value(mbs, "rs-gaap:Assets", end) - _value(
      mbs, "rs-gaap:Liabilities", end
    )
    nci_expected += outside * net_assets
    nci_income_expected += outside * _value(
      member_is[member.ticker], "rs-gaap:NetIncomeLoss", end
    )
  nci = _value(bs, "rs-gaap:MinorityInterest", end)
  check("noncontrolling interest = outside share of net assets", nci, nci_expected)
  check(
    "NCI share of income = outside share of the month's net income",
    _value(is_, "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest", end),
    nci_income_expected,
  )
  for kind, statement in consolidated.items():
    validation = statement.get("validation") or {}
    checks.append(
      GroupCheck(
        f"consolidated {kind.replace('_', ' ')} validates",
        bool(validation.get("passed")),
        "; ".join(validation.get("failures") or []) or "passed",
      )
    )

  # The consolidated cash flow over the whole run, month by month — a column
  # spanning a member's joining or leaving is not stated, so the months are
  # summed: what the group paid itself is gone, and the outside owners' flows
  # stand on their own rows.
  cf_all = _cash_flows_summed(api, windows[:-1])
  for qname, label in (
    (
      "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates",
      "the investment in subsidiaries",
    ),
    ("rs-gaap:PaymentsToAcquireInvestments", "the note to a subsidiary"),
    ("rs-gaap:ProceedsFromRelatedPartyDebt", "the note from the parent"),
  ):
    check(f"consolidated cash flow nets {label}", _value(cf_all, qname, end), 0.0)
  # The returns of the investment net out too; what remains on the
  # divestiture row is a member sold — its price, net of the cash that left.
  sold = sum(
    m.acquired.sale_price / 100
    - _value(
      _statement(
        api,
        "balance_sheet",
        *windows[m.acquired.sold_at],
        entity_id=members[m.ticker].entity_id,
      ),
      cash,
      windows[m.acquired.sold_at][1],
    )
    for m in group.members
    if m.acquired and m.acquired.sold_at is not None
  )
  check(
    "consolidated cash flow nets the returns of that investment, bar a member sold",
    _value(
      cf_all,
      "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates",
      end,
    ),
    sold,
  )
  # An acquired member's founders contributed before it joined; only what
  # they were paid out while it was in the group is the group's flow.
  outside_in = sum(
    m.contributed_capital - _share(m.contributed_capital, m.parent_share)
    for m in group.members
    if m.method == "full" and m.outside_share and not m.acquired
  )
  paid_offsets = [
    i for i in range(len(windows) - 1) if (i + 1) % _DISTRIBUTION_EVERY == 0
  ]
  paid_months = len(paid_offsets)
  outside_out = sum(
    m.distribution - _share(m.distribution, m.share_at(i))
    for m in group.members
    if m.method == "full" and m.distribution
    for i in paid_offsets
    if m.is_member_at(i) and m.outside_share_at(i)
  )
  # Beside the contributions, what the founders paid for shares bought back:
  # proceeds from the outside owners, not a divestiture.
  sold_back = sum(m.acquired.sell_back_price for m in group.members if m.acquired)
  check(
    "outside owners' contributions and purchases on their own row",
    _value(cf_all, "rs-gaap:ProceedsFromMinorityShareholders", end),
    (outside_in + sold_back) / 100,
  )
  # Beside the distributions, the price of every share bought from an
  # outside owner: a payment to them, not an investment.
  bought = sum(m.acquired.top_up_price for m in group.members if m.acquired)
  check(
    "distributions to and purchases from outside owners on their own row",
    -_value(cf_all, "rs-gaap:PaymentsToMinorityShareholders", end),
    (outside_out + bought) / 100,
  )

  associates = [m for m in group.members if m.method == "equity"]
  if associates:
    _check_associates(
      api,
      associates,
      members,
      member_bs,
      member_is,
      consolidated,
      cf_all,
      end,
      paid_months,
      check,
      checks,
    )
  for member in group.members:
    if member.acquired:
      _check_lifecycle(api, group, member, members, windows, check, checks)

  result = GroupResult(
    member_ids={m.name: members[m.ticker].entity_id for m in group.members},
    consolidation_id=consolidation.entity_id,
    months_closed=months_closed,
    parent_cash=_row(parent_bs, cash, end),
    member_cash={m.name: _row(member_bs[m.ticker], cash, end) for m in group.members},
    consolidated_cash=_row(bs, cash, end),
    noncontrolling_interest=_row(bs, "rs-gaap:MinorityInterest", end),
    checks=checks,
  )
  print(
    f"  Cash {_period(end)}: parent {_money(result.parent_cash)}, "
    + ", ".join(
      f"{m.ticker} {_money(result.member_cash[m.name])}" for m in group.members
    )
    + f", consolidated {_money(result.consolidated_cash)}; NCI {_money(result.noncontrolling_interest)}"
  )
  for c in checks:
    print(f"  {'✅' if c.passed else '❌'} {c.name}: {c.detail}")
  return result


def _check_associates(
  api: _Api,
  associates: list[Member],
  members: dict[str, _Books],
  member_bs: dict[str, dict],
  member_is: dict[str, dict],
  consolidated: dict[str, dict],
  cf_all: dict,
  end: date,
  paid_months: int,
  check,
  checks: list[GroupCheck],
) -> None:
  """An equity-method member is never summed: the group carries one asset at
  the parent's share of its net assets and one income line at the parent's
  share of its income, and the investment's cash moves on its own rows."""
  bs, is_ = consolidated["balance_sheet"], consolidated["income_statement"]
  net_assets = sum(
    m.parent_share
    * (
      _value(member_bs[m.ticker], "rs-gaap:Assets", end)
      - _value(member_bs[m.ticker], "rs-gaap:Liabilities", end)
    )
    for m in associates
  )
  check(
    "equity-method investment = the parent's share of net assets",
    _value(bs, "rs-gaap:EquityMethodInvestments", end),
    net_assets,
  )
  check(
    "equity-method income = the parent's share of the month's income",
    _value(is_, "rs-gaap:IncomeLossFromEquityMethodInvestments", end),
    sum(
      m.parent_share * _value(member_is[m.ticker], "rs-gaap:NetIncomeLoss", end)
      for m in associates
    ),
  )
  summed = set(bs.get("combined_entity_ids") or [])
  inside = [m.name for m in associates if members[m.ticker].entity_id in summed]
  checks.append(
    GroupCheck(
      "equity-method members are not summed",
      not inside,
      ", ".join(inside) + " summed" if inside else "none summed",
    )
  )
  check(
    "the investment in associates on its own row",
    -_value(cf_all, "rs-gaap:PaymentsToAcquireEquityMethodInvestments", end),
    sum(_share(m.contributed_capital, m.parent_share) for m in associates) / 100,
  )
  check(
    "distributions from associates on their own row",
    _value(cf_all, "rs-gaap:ProceedsFromSaleOfEquityMethodInvestments", end),
    sum(_share(m.distribution, m.parent_share) * paid_months for m in associates) / 100,
  )

  # The worksheet lays an associate beside the sum; skipped on a server that
  # predates it.
  worksheet = api.operation(
    "consolidation-worksheet",
    {
      "statement_type": "balance_sheet",
      "period_start": end.replace(day=1).isoformat(),
      "period_end": end.isoformat(),
    },
    ok=(404,),
  )
  if worksheet is None:
    return
  kinds = {c.get("entity_id"): c.get("kind") for c in worksheet["columns"]}
  misplaced = [
    m.name for m in associates if kinds.get(members[m.ticker].entity_id) != "not_summed"
  ]
  checks.append(
    GroupCheck(
      "the worksheet shows equity-method members beside the sum",
      not misplaced,
      ", ".join(misplaced) + " misplaced" if misplaced else "beside the sum",
    )
  )


def _cash_flows_summed(api: _Api, windows: list[tuple[date, date]]) -> dict:
  """The consolidated cash flow rows summed over ``windows``, in the shape
  ``_row`` reads, with the last window's end as the column."""
  totals: dict[str, float] = {}
  for start, end in windows:
    statement = _statement(api, "cash_flow_statement", start, end, consolidated=True)
    for fact in statement.get("facts") or []:
      if fact.get("period_role"):
        continue
      value = _row(statement, fact["qname"], end)
      if value:
        totals[fact["qname"]] = totals.get(fact["qname"], 0.0) + value
  end = windows[-1][1]
  return {
    "periods": [{"start": windows[0][0].isoformat(), "end": end.isoformat()}],
    "facts": [{"qname": q, "values": [v]} for q, v in totals.items()],
  }


def _net_assets(api: _Api, books: _Books, start: date, end: date) -> float:
  """A member's net assets at ``end``, from its own balance sheet."""
  mbs = _statement(api, "balance_sheet", start, end, entity_id=books.entity_id)
  return _value(mbs, "rs-gaap:Assets", end) - _value(mbs, "rs-gaap:Liabilities", end)


def _nci_expected(
  api: _Api,
  group: Group,
  members: dict[str, _Books],
  windows: list[tuple[date, date]],
  index: int,
) -> float:
  """The outside owners' share of every full member's net assets at the end
  of window ``index``, at the share each holds that month."""
  start, end = windows[index]
  return sum(
    m.outside_share_at(index) * _net_assets(api, members[m.ticker], start, end)
    for m in group.members
    if m.method == "full" and m.is_member_at(index) and m.outside_share_at(index)
  )


def _outside_distributions(group: Group, index: int) -> int:
  """What the outside owners of the full members are paid in window
  ``index``, in cents."""
  if (index + 1) % _DISTRIBUTION_EVERY:
    return 0
  return sum(
    m.distribution - _share(m.distribution, m.share_at(index))
    for m in group.members
    if m.method == "full" and m.distribution and m.is_member_at(index)
  )


def _validates(
  checks: list[GroupCheck], label: str, statements: dict[str, dict]
) -> None:
  for kind, statement in statements.items():
    validation = statement.get("validation") or {}
    checks.append(
      GroupCheck(
        f"{label}: consolidated {kind.replace('_', ' ')} validates",
        bool(validation.get("passed")),
        "; ".join(validation.get("failures") or []) or "passed",
      )
    )


def _check_lifecycle(
  api: _Api,
  group: Group,
  member: Member,
  members: dict[str, _Books],
  windows: list[tuple[date, date]],
  check,
  checks: list[GroupCheck],
) -> None:
  """An acquired member's months against ASC 805 and 810. Joining: goodwill
  is the price over the parent's share of the equity it brought, and the
  group's cash flow shows the price net of the cash that came with it.
  Buying more: no gain or loss, the outside owners' share smaller, the price
  a financing flow. Sold: the group's gain is the price less the carrying
  amount of what left — the parent's own gain less the results the group
  had already taken — its cash leaves on the divestiture row, nothing is
  held for it after, and the entity shows as former."""
  a = member.acquired
  assert a is not None
  books = members[member.ticker]
  t = member.ticker
  goodwill_q = "rs-gaap:Goodwill"
  gain_q = "rs-gaap:GainOrLossOnSaleOfStockInSubsidiary"
  nci_q = "rs-gaap:MinorityInterest"
  investment_flow = "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates"
  goodwill = a.price - _share(a.opening_cash + a.opening_equipment, member.parent_share)

  js, je = windows[a.joins_at]
  bs = _statement(api, "balance_sheet", js, je, consolidated=True)
  cf = _statement(api, "cash_flow_statement", js, je, consolidated=True)
  check(
    f"{t} joins: goodwill = the price over the share of book",
    _value(bs, goodwill_q, je),
    goodwill / 100,
  )
  check(
    f"{t} joins: NCI = the founders' share of net assets",
    _value(bs, nci_q, je),
    _nci_expected(api, group, members, windows, a.joins_at),
  )
  check(
    f"{t} joins: the price net of the cash acquired on the acquisition row",
    _value(cf, "rs-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired", je),
    -(a.price - a.opening_cash) / 100,
  )
  check(
    f"{t} joins: nothing of it on the investment row",
    _value(cf, investment_flow, je),
    0.0,
  )
  _validates(checks, f"{t} joins", {"balance_sheet": bs, "cash_flow_statement": cf})

  if a.tops_up_at is not None:
    ts, te = windows[a.tops_up_at]
    bs = _statement(api, "balance_sheet", ts, te, consolidated=True)
    is_ = _statement(api, "income_statement", ts, te, consolidated=True)
    cf = _statement(api, "cash_flow_statement", ts, te, consolidated=True)
    check(f"{t} top-up: no gain or loss in the group", _value(is_, gain_q, te), 0.0)
    check(
      f"{t} top-up: NCI = the founders' smaller share of net assets",
      _value(bs, nci_q, te),
      _nci_expected(api, group, members, windows, a.tops_up_at),
    )
    check(
      f"{t} top-up: the price paid to its founders is a financing flow",
      -_value(cf, "rs-gaap:PaymentsToMinorityShareholders", te),
      (a.top_up_price + _outside_distributions(group, a.tops_up_at)) / 100,
    )
    check(
      f"{t} top-up: nothing of it on the investment row",
      _value(cf, investment_flow, te),
      0.0,
    )
    _validates(
      checks,
      f"{t} top-up",
      {"balance_sheet": bs, "income_statement": is_, "cash_flow_statement": cf},
    )

  if a.sells_back_at is not None:
    ss, se = windows[a.sells_back_at]
    bs = _statement(api, "balance_sheet", ss, se, consolidated=True)
    is_ = _statement(api, "income_statement", ss, se, consolidated=True)
    cf = _statement(api, "cash_flow_statement", ss, se, consolidated=True)
    check(f"{t} sold back: no gain or loss in the group", _value(is_, gain_q, se), 0.0)
    check(
      f"{t} sold back: NCI = the founders' larger share of net assets",
      _value(bs, nci_q, se),
      _nci_expected(api, group, members, windows, a.sells_back_at),
    )
    check(
      f"{t} sold back: the proceeds are a financing inflow from its founders",
      _value(cf, "rs-gaap:ProceedsFromMinorityShareholders", se),
      a.sell_back_price / 100,
    )
    check(
      f"{t} sold back: nothing of it on the divestiture row",
      _value(
        cf, "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates", se
      ),
      0.0,
    )
    _validates(
      checks,
      f"{t} sold back",
      {"balance_sheet": bs, "income_statement": is_, "cash_flow_statement": cf},
    )

  if a.sold_at is None:
    return
  ms, me = windows[a.sold_at]
  xs, xe = windows[a.sold_at + 1]
  net_assets = _net_assets(api, books, ms, me)
  own_bs = _statement(api, "balance_sheet", ms, me, entity_id=books.entity_id)
  cash_left = _value(own_bs, "rs-gaap:CashAndCashEquivalentsAtCarryingValue", me)
  consolidated = {
    kind: _statement(api, kind, xs, xe, consolidated=True)
    for kind in (
      "balance_sheet",
      "income_statement",
      "cash_flow_statement",
      "equity_statement",
    )
  }
  bs, is_, cf = (
    consolidated["balance_sheet"],
    consolidated["income_statement"],
    consolidated["cash_flow_statement"],
  )
  check(
    f"{t} sold: the group's gain = the price less the carrying amount of what left",
    _value(is_, gain_q, xe),
    a.sale_price / 100 - member.share_at(a.sold_at) * net_assets - goodwill / 100,
  )
  check(f"{t} sold: no goodwill held for it", _value(bs, goodwill_q, xe), 0.0)
  check(
    f"{t} sold: NCI = the remaining members' outside share",
    _value(bs, nci_q, xe),
    _nci_expected(api, group, members, windows, a.sold_at + 1),
  )
  check(
    f"{t} sold: the price net of the cash that left on the divestiture row",
    _value(
      cf, "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates", xe
    ),
    a.sale_price / 100 - cash_left,
  )
  _validates(checks, f"{t} sold", consolidated)
  # The worksheet: a leaving column on the cash flow for the month its cash
  # goes, none on the balance sheet it is no longer in.
  for statement_type, present in (
    ("cash_flow_statement", True),
    ("balance_sheet", False),
  ):
    worksheet = api.operation(
      "consolidation-worksheet",
      {
        "statement_type": statement_type,
        "period_start": xs.isoformat(),
        "period_end": xe.isoformat(),
      },
      ok=(404,),
    )
    if worksheet is None:
      break
    ids = {c.get("entity_id") for c in worksheet["columns"]}
    checks.append(
      GroupCheck(
        f"{t} sold: {'a leaving column on' if present else 'no column on'} the "
        f"{statement_type.replace('_', ' ')} worksheet",
        (books.entity_id in ids) == present,
        "a column" if books.entity_id in ids else "no column",
      )
    )
  rows = api.graphql("{ entities { id isFormer } }")["entities"]
  former = next((e.get("isFormer") for e in rows if e["id"] == books.entity_id), None)
  checks.append(
    GroupCheck(
      f"{t} sold: shown as a former member", former is True, f"isFormer {former}"
    )
  )


def _money(value: float | None) -> str:
  return "—" if value is None else f"${value:,.2f}"


if __name__ == "__main__":  # pragma: no cover - manual smoke
  print("Import this module from an episode; see coffee_roaster_demo/group.py.")
  sys.exit(1)
