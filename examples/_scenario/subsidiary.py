"""A subsidiary beside the episode's company — the multi-entity beat.

The episode's company is the group parent. A subsidiary is a second entity
in the same graph with its own books, chart of accounts and fiscal calendar;
the group's statement is the sum of both at the rs-gaap concepts. This
loader creates the entity, gives it a template chart and a calendar on the
group's cadence, posts a few months of simple activity in its books, closes
them, and reads the parent's combined balance sheet back.

The entity-scoped operations are posted directly: the published client's
facades predate ``entity_id``.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx


@dataclass(frozen=True)
class Subsidiary:
  """A wholly or partly owned subsidiary, with the shape of its months.

  All amounts are integer cents. ``template`` is one of the shipped chart
  templates (``saas`` / ``services`` / ``product``); accounts are picked by
  classification, so any of them works.
  """

  name: str
  legal_name: str
  ticker: str
  description: str
  entity_type: str = "llc"
  ownership_pct: float = 100.0
  template: str = "services"
  opening_cash: int = 2_500_000
  monthly_revenue: int = 1_800_000
  revenue_memo: str = "Monthly sales"
  # (memo, monthly cents) per expense line; each takes the next expense account.
  expenses: tuple[tuple[str, int], ...] = (
    ("Rent", 450_000),
    ("Wages", 720_000),
  )


@dataclass(frozen=True)
class SubsidiaryResult:
  entity_id: str
  name: str
  months_closed: int
  parent_cash: float | None
  subsidiary_cash: float | None
  combined_cash: float | None
  combined_entity_ids: list[str]


class _Api:
  """The REST operations and the GraphQL read, with the demo's credentials."""

  def __init__(self, graph_id: str, config: dict[str, str]) -> None:
    self.graph_id = graph_id
    self.base_url = config["base_url"].rstrip("/")
    self.http = httpx.Client(
      headers={"X-API-Key": config["token"], "Content-Type": "application/json"},
      timeout=120.0,
    )

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
        f"{name} failed ({response.status_code}): {response.text[:400]}"
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


def _accounts(api: _Api, entity_id: str, classification: str) -> list[dict]:
  data = api.graphql(
    "query($e: String!, $c: String!) { accounts(entityId: $e, classification: $c, "
    "limit: 200) { accounts { id code name } } }",
    {"e": entity_id, "c": classification},
  )
  return data["accounts"]["accounts"]


def _journal(
  api: _Api,
  entity_id: str,
  posting_date: date,
  memo: str,
  debit_id: str,
  credit_id: str,
  cents: int,
) -> None:
  api.operation(
    "create-event-block",
    {
      "entity_id": entity_id,
      "event_type": "journal_entry_recorded",
      "event_category": "adjustment",
      "event_class": "economic",
      "source": "manual",
      "occurred_at": f"{posting_date.isoformat()}T00:00:00+00:00",
      "apply_handlers": True,
      "amount": cents,
      "currency": "USD",
      "description": memo,
      "metadata": {
        "posting_date": posting_date.isoformat(),
        "memo": memo,
        "type": "standard",
        "status": "posted",
        "line_items": [
          {"element_id": debit_id, "debit_amount": cents, "credit_amount": 0},
          {"element_id": credit_id, "debit_amount": 0, "credit_amount": cents},
        ],
      },
    },
  )


def _cash(facts: list[dict], periods: list[dict], period_end: date) -> float | None:
  """The cash line of a rendered balance sheet, for the column ending on
  ``period_end``."""
  column = next(
    (i for i, p in enumerate(periods) if p.get("end") == period_end.isoformat()), 0
  )
  for fact in facts:
    if fact.get("qname", "").endswith("CashAndCashEquivalentsAtCarryingValue"):
      values = fact.get("values") or []
      return values[column] if column < len(values) else None
  return None


def load_subsidiary(
  graph_id: str,
  sub: Subsidiary,
  *,
  config: dict[str, str],
  windows: list[tuple[date, date]],
  close_history: bool = True,
) -> SubsidiaryResult:
  """Put ``sub`` in the graph beside the parent and read the group back.

  ``windows`` are the parent's month windows; the last one is the open
  close target and stays open here too. Re-runs reuse the entity, its
  chart and its calendar, and post nothing twice.
  """
  api = _Api(graph_id, config)
  demo_start, _ = windows[0]
  close_target = _period(windows[-1][1])

  # 1. The entity, reused on a re-run.
  own = api.graphql("{ entities { id name source } }")["entities"]
  existing = next(
    (e for e in own if e["name"] == sub.name and e["source"] != "linked"), None
  )
  if existing:
    entity_id = existing["id"]
    print(f"  Entity:       {entity_id} (reused)")
  else:
    created = api.operation(
      "create-entity",
      {
        "name": sub.name,
        "legal_name": sub.legal_name,
        "ticker": sub.ticker,
        "entity_type": sub.entity_type,
        "ownership_pct": sub.ownership_pct,
      },
    )
    assert created is not None
    entity_id = created["id"]
    print(f"  Entity:       {entity_id} ({sub.ownership_pct:g}% owned)")

  # 2. Its own chart, from a template; 409 once it has one.
  chart = api.operation(
    "initialize-chart-of-accounts",
    {"template": sub.template, "entity_id": entity_id},
    ok=(409,),
  )
  print(
    f"  Chart:        {sub.template} template"
    + (f", {chart['elements_created']} accounts" if chart else " (already there)")
  )

  cash = next(
    (a for a in _accounts(api, entity_id, "asset") if a["code"] == "1000"), None
  )
  cash = cash or _accounts(api, entity_id, "asset")[0]
  equity = _accounts(api, entity_id, "equity")[0]
  revenue = _accounts(api, entity_id, "revenue")[0]
  expenses = _accounts(api, entity_id, "expense")
  if len(expenses) < len(sub.expenses):
    raise RuntimeError(f"{sub.template} template has {len(expenses)} expense accounts")

  # 3. Its own calendar, on the group's cadence; 409 once initialized.
  api.operation(
    "initialize",
    {
      "entity_id": entity_id,
      "closed_through": None,
      "earliest_data_period": _period(demo_start),
      "note": f"{sub.ticker} initialization",
    },
    ok=(409,),
  )
  api.operation(
    "set-close-target",
    {
      "entity_id": entity_id,
      "period": _period(windows[-2][1]),
      "note": "initialization",
    },
  )

  # 4. Its months, posted in its books on its accounts — unless already there.
  posted = api.graphql(
    "query($e: String!) { transactions(entityId: $e, limit: 1) { pagination { total } } }",
    {"e": entity_id},
  )["transactions"]["pagination"]["total"]
  if posted:
    print(f"  Activity:     {posted} transactions already in its books (reused)")
  else:
    _journal(
      api,
      entity_id,
      demo_start,
      "Capital contributed by the parent",
      cash["id"],
      equity["id"],
      sub.opening_cash,
    )
    count = 1
    for period_start, period_end in windows:
      mid = period_start.replace(day=15)
      _journal(
        api,
        entity_id,
        mid,
        sub.revenue_memo,
        cash["id"],
        revenue["id"],
        sub.monthly_revenue,
      )
      count += 1
      for (memo, cents), account in zip(sub.expenses, expenses, strict=False):
        _journal(api, entity_id, period_end, memo, account["id"], cash["id"], cents)
        count += 1
    print(f"  Activity:     {count} entries over {len(windows)} months")

  # 5. Close its history, month by month; the close target stays open.
  months_closed = 0
  if close_history:
    status = api.graphql(
      "query($e: String!) { fiscalCalendar(entityId: $e) { closedThrough } }",
      {"e": entity_id},
    )["fiscalCalendar"]
    closed_through = (status or {}).get("closedThrough")
    for _period_start, period_end in windows[:-1]:
      period = _period(period_end)
      if closed_through and period <= closed_through:
        continue
      api.operation(
        "close-period",
        {
          "entity_id": entity_id,
          "period": period,
          "note": "historical close (provisioning)",
        },
      )
      months_closed += 1
    print(f"  Closed:       {months_closed} months (target {close_target} stays open)")

  # 6. The group, read back three ways on the last closed month.
  last_start, last_end = windows[-2]

  def balance_sheet(**extra) -> dict:
    return api.operation(
      "live-financial-statement",
      {
        "statement_type": "balance_sheet",
        "period_start": last_start.isoformat(),
        "period_end": last_end.isoformat(),
        **extra,
      },
    )

  parent_bs = balance_sheet()
  sub_bs = balance_sheet(entity_id=entity_id)
  combined_bs = balance_sheet(consolidated=True)
  result = SubsidiaryResult(
    entity_id=entity_id,
    name=sub.name,
    months_closed=months_closed,
    parent_cash=_cash(parent_bs["facts"], parent_bs["periods"], last_end),
    subsidiary_cash=_cash(sub_bs["facts"], sub_bs["periods"], last_end),
    combined_cash=_cash(combined_bs["facts"], combined_bs["periods"], last_end),
    combined_entity_ids=list(combined_bs.get("combined_entity_ids") or []),
  )
  print(
    f"  Cash {_period(last_end)}: parent {_money(result.parent_cash)}, "
    f"{sub.ticker} {_money(result.subsidiary_cash)}, "
    f"combined {_money(result.combined_cash)}"
  )
  return result


def _money(value: float | None) -> str:
  return "—" if value is None else f"${value:,.2f}"


if __name__ == "__main__":  # pragma: no cover - manual smoke
  print("Import this module from an episode; see coffee_roaster_demo/subsidiary.py.")
  sys.exit(1)
