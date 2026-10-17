"""A group beside the episode's company — the multi-entity beat.

The episode's company is the group parent. The roster puts its subsidiaries
in the same graph, each with its own books, chart of accounts and fiscal
calendar, and one more entity whose only entries are the consolidating ones:
the parent's investment against each subsidiary's contributed capital, the
outside owner's share as noncontrolling interest, and the intercompany
balances and flows. Every intercompany event is posted on both sides, so the
group creates no cash from nothing, and the consolidated statement is the
sum of every entity's at the rs-gaap concepts.

The consolidation entity keeps a running ledger, so a cumulative adjustment
(the eliminated investment, the eliminated equity, noncontrolling interest)
is posted as the difference between its target balance at the month end and
what the entity already holds, while period activity (an intercompany fee,
the outside owners' share of the month's income) posts as it is. That is the
rule the consolidation engine will follow (``consolidation.md`` §4.3, D8), so
the entries here are its acceptance test: it must propose exactly these.

The entity-scoped operations are posted directly: the published client's
facades predate ``entity_id``.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import httpx

_FORM_EQUITY = {
  "llc": "rs-gaap:MembersEquity",
  "partnership": "rs-gaap:PartnersCapital",
  "corporation": "rs-gaap:RetainedEarningsAccumulatedDeficit",
}

# The consolidation template's row for a subsidiary's eliminated equity, by
# the subsidiary's legal form.
_ELIMINATED_EQUITY_CODE = {
  "llc": "3020",
  "partnership": "3030",
  "corporation": "3010",
}

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
)
_TOLERANCE = 0.05


@dataclass(frozen=True)
class ParentAccounts:
  """The parent's chart codes the group posts to."""

  cash: str = "1000"
  due_from_affiliates: str = "1150"
  investment: str = "1600"
  note_receivable: str = "1650"
  paid_in_capital: str = "3000"
  fee_revenue: str = "4300"
  interest_income: str = "4400"


@dataclass(frozen=True)
class Member:
  """A subsidiary, with the shape of its months. All amounts are integer cents.

  ``template`` is one of the shipped chart templates; the accounts the group
  needs beyond it (due to affiliates, the note, distributions, the fee and
  interest expense) are added to the chart. The months post to the
  ``product`` template's codes (1000 cash, 3000 contributed capital, 4000
  revenue, 6600 utilities for the rebill), so another template has to carry
  the same ones. ``ownership_pct`` is the parent's share; ``outside_owner``
  holds the rest and contributes it at formation.
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

  @property
  def parent_share(self) -> float:
    return self.ownership_pct / 100.0

  @property
  def outside_share(self) -> float:
    return 1.0 - self.parent_share


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
    Statuses in ``ok`` (a 409 for something already done) return None."""
    response = self.http.post(
      f"{self.base_url}/extensions/roboledger/{self.graph_id}/operations/{name}",
      json=body,
    )
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

  def account(self, code: str) -> dict:
    if self._accounts is None:
      rows = self.api.graphql(
        "query($e: String!) { accounts(entityId: $e, limit: 500) "
        "{ accounts { id code name } } }",
        {"e": self.entity_id},
      )["accounts"]["accounts"]
      self._accounts = {row["code"]: row for row in rows}
    try:
      return self._accounts[code]
    except KeyError:
      raise RuntimeError(f"{self.name} has no account {code}") from None

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
  ) -> None:
    """A posted journal entry; ``lines`` are (code, debit, credit, flow_qname)."""
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


class _Adjustments:
  """The consolidation entity's cumulative balances, posted as deltas."""

  def __init__(self, books: _Books) -> None:
    self.books = books
    # Debit-positive balances held per (code, kind); the kind keeps the
    # capital eliminations apart from the income allocations that share the
    # noncontrolling-interest row.
    self.held: dict[tuple[str, str], int] = {}

  def bring_to(
    self,
    posting_date: date,
    memo: str,
    targets: dict[tuple[str, str], int],
  ) -> None:
    lines: list[tuple[str, int, int, str | None]] = []
    for key, target in targets.items():
      delta = target - self.held.get(key, 0)
      if not delta:
        continue
      code = key[0]
      lines.append((code, delta, 0, None) if delta > 0 else (code, 0, -delta, None))
      self.held[key] = target
    self.books.post(posting_date, memo, lines)

  def activity(
    self,
    posting_date: date,
    memo: str,
    lines: list[tuple[str, int, int, str | None]],
    *,
    agent_id: str | None = None,
  ) -> None:
    self.books.post(posting_date, memo, lines, agent_id=agent_id)


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


def _cents(statement: dict, qname: str, end: date) -> int:
  return round(_value(statement, qname, end) * 100)


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


def _mapping_structure(api: _Api, books: _Books, taxonomy_id: str | None) -> str:
  """The entity's CoA→rs-gaap mapping structure. The chart's id names it
  directly; without one (a re-run), the structure whose taxonomy holds the
  entity's cash account."""
  candidates = api.graphql(
    'query($t: String) { structures(taxonomyId: $t, blockType: "coa_mapping") '
    "{ structures { id taxonomyId } } }",
    {"t": taxonomy_id},
  )["structures"]["structures"]
  if taxonomy_id is None:
    cash_id = books.account("1000")["id"]
    for candidate in candidates:
      ids = {
        e["id"]
        for e in api.graphql(
          "query($t: String!) { elements(taxonomyId: $t, limit: 500) "
          "{ elements { id } } }",
          {"t": candidate["taxonomyId"]},
        )["elements"]["elements"]
      }
      if cash_id in ids:
        return candidate["id"]
    raise RuntimeError(f"{books.name}: no mapping structure holds its chart")
  if not candidates:
    raise RuntimeError(f"{books.name}: the chart has no mapping structure")
  return candidates[0]["id"]


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
  afterwards, so a run that died between the two steps heals on the next."""
  targets = _rs_gaap_ids(api)
  wanted = {code: target or equity_concept for code, *_rest, target in rows}
  unknown = sorted(q for q in wanted.values() if q not in targets)
  if unknown:
    raise RuntimeError(
      f"{books.name}: not in this graph's rs-gaap library: {', '.join(unknown)}"
    )

  missing = [row for row in rows if not books.has_account(row[0])]
  if missing:
    if taxonomy_id is None:
      raise RuntimeError(
        f"{books.name}: the chart exists without the group's accounts and the "
        "chart id is unknown; reset the demo and run again"
      )
    api.operation(
      "update-taxonomy-block",
      {
        "taxonomy_id": taxonomy_id,
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

  # Unmapped is measured against the mapping structure, not the entity.
  mapping_id = _mapping_structure(api, books, taxonomy_id)
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
) -> GroupResult:
  """Put the group in the graph beside the parent and read it back.

  ``windows`` are the parent's month windows; the last one is the open close
  target and stays open here too. Must run before the parent's history
  closes: the parent's side of every intercompany event posts in those
  months. Re-runs reuse the entities, charts and calendars, and post nothing
  twice: a group with any posting already in its books is left as it is.
  """
  api = _Api(graph_id, config)
  try:
    return _load(api, group, windows, parent_taxonomy_id, close_history)
  finally:
    api.close()


def _load(
  api: _Api,
  group: Group,
  windows: list[tuple[date, date]],
  parent_taxonomy_id: str | None,
  close_history: bool,
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
  members: dict[str, _Books] = {}
  for member in group.members:
    entity_id, reused = _ensure_entity(
      api,
      member.name,
      {
        "name": member.name,
        "legal_name": member.legal_name,
        "ticker": member.ticker,
        "entity_type": member.entity_type,
        "ownership_pct": member.ownership_pct,
      },
    )
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
    _ensure_calendar(api, entity_id, member.ticker, windows)
    print(
      f"  {member.ticker:<8} {entity_id} ({member.ownership_pct:g}% owned"
      + (f", {member.outside_owner} holds the rest" if member.outside_owner else "")
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
  _ensure_chart(api, consolidation_id, "consolidation", "llc")
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
    _post_group(api, group, parent, members, consolidation, agent_of, windows)

  # 4. Close every member's history and the consolidation entity's, month by
  # month; the close target stays open.
  months_closed = 0
  if close_history:
    for member in group.members:
      months_closed += _close_history(members[member.ticker], windows, member.ticker)
    months_closed += _close_history(consolidation, windows, group.consolidation_ticker)

  # 5. Read the group back on the last closed month and check the
  # consolidation.
  last_start, last_end = windows[-2]
  return _read_back(
    api, group, parent, members, consolidation, last_start, last_end, months_closed
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
  adjustments = _Adjustments(consolidation)
  demo_start = windows[0][0]
  last_index = len(windows) - 1
  parent_agent = agent_of[parent.entity_id]

  # Running state per member, in cents.
  due: dict[str, int] = {m.ticker: 0 for m in group.members}
  distributions: dict[str, int] = {m.ticker: 0 for m in group.members}
  nci_income: dict[str, int] = {m.ticker: 0 for m in group.members}

  # Formation. The founders fund the parent for the group, the parent forms
  # each member, and the outside owner puts in its share directly.
  funding = sum(
    _share(m.contributed_capital, m.parent_share) + m.note_from_parent
    for m in group.members
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
    books = members[member.ticker]
    member_agent = agent_of[books.entity_id]
    parent_part = _share(member.contributed_capital, member.parent_share)
    outside_part = member.contributed_capital - parent_part
    parent.post(
      demo_start,
      f"Investment in {member.name} at formation",
      [(pa.investment, parent_part, 0, None), (pa.cash, 0, parent_part, None)],
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
      expense_accounts = books.expense_accounts()
      if len(expense_accounts) < len(member.expenses):
        raise RuntimeError(
          f"{member.name}'s chart has {len(expense_accounts)} expense accounts"
        )

      # Last month's charges settle on the 10th.
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
      if member.distribution and (index + 1) % 3 == 0 and index < last_index:
        parent_part = _share(member.distribution, member.parent_share)
        books.post(
          end,
          "Distribution to members",
          [
            ("3200", member.distribution, 0, None),
            ("1000", 0, member.distribution, None),
          ],
        )
        parent.post(
          end,
          f"Distribution from {member.name}",
          [(pa.cash, parent_part, 0, None), (pa.investment, 0, parent_part, None)],
          agent_id=member_agent,
        )
        distributions[member.ticker] += member.distribution
        count += 2

      # The consolidating entries for the month: period activity as it is.
      if fee or interest:
        adjustments.activity(
          end,
          f"Eliminate intercompany charges — {member.name}",
          [
            ("4000", fee, 0, None),
            ("5000", 0, fee, None),
            ("4100", interest, 0, None),
            ("5100", 0, interest, None),
          ],
          agent_id=member_agent,
        )
        count += 1
      if member.outside_share:
        income = _cents(
          _statement(api, "income_statement", start, end, entity_id=books.entity_id),
          "rs-gaap:NetIncomeLoss",
          end,
        )
        share = _share(income, member.outside_share)
        if share:
          lines = (
            [("5900", share, 0, None), ("3200", 0, share, None)]
            if share > 0
            else [("3200", -share, 0, None), ("5900", 0, -share, None)]
          )
          adjustments.activity(
            end,
            f"Share of {member.name}'s income attributable to {member.outside_owner}",
            lines,
          )
          nci_income[member.ticker] += share
          count += 1

    # The cumulative adjustments, brought to their month-end targets: the
    # parent's investment against each member's contributed capital net of
    # distributions, the outside owners' share of that capital, and the
    # intercompany balances.
    targets: dict[tuple[str, str], int] = {}
    for member in group.members:
      capital = member.contributed_capital - distributions[member.ticker]
      parent_part = _share(capital, member.parent_share)
      eq = _ELIMINATED_EQUITY_CODE[member.entity_type]
      targets[(eq, "capital")] = targets.get((eq, "capital"), 0) + capital
      targets[("1600", "capital")] = targets.get(("1600", "capital"), 0) - parent_part
      targets[("3200", "capital")] = targets.get(("3200", "capital"), 0) - (
        capital - parent_part
      )
      targets[("2150", "balance")] = (
        targets.get(("2150", "balance"), 0) + due[member.ticker]
      )
      targets[("1150", "balance")] = (
        targets.get(("1150", "balance"), 0) - due[member.ticker]
      )
      targets[("2600", "balance")] = (
        targets.get(("2600", "balance"), 0) + member.note_from_parent
      )
      targets[("1650", "balance")] = (
        targets.get(("1650", "balance"), 0) - member.note_from_parent
      )
    adjustments.bring_to(end, f"Consolidating entries at {_period(end)}", targets)
    count += 1

  print(f"  Activity:     {count} entries over {len(windows)} months, on every side")


def _read_back(
  api: _Api,
  group: Group,
  parent: _Books,
  members: dict[str, _Books],
  consolidation: _Books,
  start: date,
  end: date,
  months_closed: int,
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
    for kind in ("balance_sheet", "income_statement", "cash_flow_statement")
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

  nci_expected = 0.0
  nci_income_expected = 0.0
  for member in group.members:
    if not member.outside_share:
      continue
    mbs = member_bs[member.ticker]
    net_assets = _value(mbs, "rs-gaap:Assets", end) - _value(
      mbs, "rs-gaap:Liabilities", end
    )
    nci_expected += member.outside_share * net_assets
    nci_income_expected += member.outside_share * _value(
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


def _money(value: float | None) -> str:
  return "—" if value is None else f"${value:,.2f}"


if __name__ == "__main__":  # pragma: no cover - manual smoke
  print("Import this module from an episode; see coffee_roaster_demo/group.py.")
  sys.exit(1)
