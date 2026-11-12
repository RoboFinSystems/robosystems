"""Run the reporting acceptance checks against one graph. Read-only.

just bench-reporting coffee_roaster           # a demo slot in .local/config.json
just bench-reporting kg1a… --months 6         # any graph id, last six closed months
just bench-reporting coffee_roaster --json out.json
"""

from __future__ import annotations

import argparse
import calendar
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import httpx

from .checks import Check, Checker
from .expected import Ledger, combine, fold, less

_ROOT = Path(__file__).resolve().parents[2]
_CONFIG = _ROOT / ".local" / "config.json"
_STATEMENTS = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)
_RETRIES = 3
CONSOLIDATED = "Consolidated"


class Api:
  def __init__(self, base_url: str, graph_id: str, api_key: str) -> None:
    self.base_url = base_url.rstrip("/")
    self.graph_id = graph_id
    self.http = httpx.Client(
      headers={"X-API-Key": api_key, "Content-Type": "application/json"},
      timeout=180.0,
    )

  def _post(self, url: str, body: dict[str, Any]) -> httpx.Response:
    response = self.http.post(url, json=body)
    for _ in range(_RETRIES):
      if response.status_code != 429:
        break
      time.sleep(int(response.headers.get("Retry-After", "60")))
      response = self.http.post(url, json=body)
    return response

  def graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict:
    response = self._post(
      f"{self.base_url}/extensions/{self.graph_id}/graphql",
      {"query": query, "variables": variables or {}},
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("errors"):
      raise RuntimeError(f"GraphQL failed: {payload['errors']}")
    return payload["data"]

  def statement(
    self, kind: str, start: date, end: date, *, entity_id: str | None
  ) -> dict:
    body: dict[str, Any] = {
      "statement_type": kind,
      "period_start": start.isoformat(),
      "period_end": end.isoformat(),
    }
    if entity_id:
      body["entity_id"] = entity_id
    else:
      body["consolidated"] = True
    response = self._post(
      f"{self.base_url}/extensions/roboledger/{self.graph_id}"
      "/operations/live-financial-statement",
      body,
    )
    if response.status_code >= 400:
      raise RuntimeError(
        f"{kind} failed ({response.status_code}): {response.text[:300]}"
      )
    return response.json()["result"]


@dataclass
class Entity:
  id: str
  name: str
  is_parent: bool
  method: str
  role: str
  joined_on: date | None
  left_on: date | None
  closed_through: str | None
  concepts_by_code: dict[str, list[str]]

  def summed_at(self, end: date) -> bool:
    """Whether the consolidated balance sheet at ``end`` sums this entity. A
    member is still in the group on the day it leaves."""
    if not (self.is_parent or self.role == "consolidation" or self.method == "full"):
      return False
    if self.joined_on and self.joined_on > end:
      return False
    return not (self.left_on and self.left_on < end)

  def summed_during(self, start: date, end: date) -> bool:
    """Whether the consolidated activity of the month includes this entity's."""
    if not (self.is_parent or self.role == "consolidation" or self.method == "full"):
      return False
    if self.joined_on and self.joined_on > end:
      return False
    return not (self.left_on and self.left_on < start)


def _day(value: str | None) -> date | None:
  return date.fromisoformat(value) if value else None


def _month_end(month: str) -> date:
  year, mon = (int(part) for part in month.split("-"))
  return date(year, mon, calendar.monthrange(year, mon)[1])


def _previous(month: str) -> str:
  year, mon = (int(part) for part in month.split("-"))
  return f"{year - 1}-12" if mon == 1 else f"{year}-{mon - 1:02d}"


def load_entities(api: Api) -> list[Entity]:
  data = api.graphql(
    "{ entities { id name isParent consolidationMethod entityRole joinedOn leftOn } "
    "mappings { structures { id entityId blockType } } }"
  )
  mapping_ids = {
    s["entityId"]: s["id"]
    for s in data["mappings"]["structures"]
    if s.get("blockType") == "coa_mapping"
  }
  entities = []
  for row in data["entities"]:
    concepts_by_code: dict[str, list[str]] = {}
    mapping_id = mapping_ids.get(row["id"])
    if mapping_id is None and row["isParent"]:
      mapping_id = mapping_ids.get(None)
    if mapping_id:
      detail = api.graphql(
        "query($m: String!) { mapping(mappingId: $m) { associations "
        "{ fromElementQname toElementQname } } }",
        {"m": mapping_id},
      )["mapping"]
      for association in (detail or {}).get("associations") or []:
        code = (association.get("fromElementQname") or "").split(":", 1)[-1]
        if code and association.get("toElementQname"):
          concepts_by_code.setdefault(code, []).append(association["toElementQname"])
    calendar_state = api.graphql(
      "query($e: String!) { fiscalCalendar(entityId: $e) { closedThrough } }",
      {"e": row["id"]},
    )["fiscalCalendar"]
    entities.append(
      Entity(
        id=row["id"],
        name=row["name"],
        is_parent=bool(row["isParent"]),
        method=row.get("consolidationMethod") or "none",
        role=row.get("entityRole") or "operating",
        joined_on=_day(row.get("joinedOn")),
        left_on=_day(row.get("leftOn")),
        closed_through=(calendar_state or {}).get("closedThrough"),
        concepts_by_code=concepts_by_code,
      )
    )
  return sorted(
    entities, key=lambda e: (not e.is_parent, e.role == "consolidation", e.name)
  )


def cumulative(api: Api, entity: Entity, end: date) -> Ledger:
  rows = api.graphql(
    "query($e: String!, $d: Date!) { trialBalance(entityId: $e, endDate: $d) "
    "{ rows { accountCode trait totalDebits totalCredits } } }",
    {"e": entity.id, "d": end.isoformat()},
  )["trialBalance"]["rows"]
  return fold(rows, entity.concepts_by_code, entity.id)


def months_to_check(entities: list[Entity], limit: int) -> list[str]:
  parent = next((e for e in entities if e.is_parent), entities[0])
  if not parent.closed_through:
    return []
  months = [parent.closed_through]
  while len(months) < limit:
    months.append(_previous(months[-1]))
  return list(reversed(months))


def run(api: Api, limit: int, only: str | None) -> list[Check]:
  entities = load_entities(api)
  months = months_to_check(entities, limit)
  if not months:
    raise SystemExit("the group parent has no closed month to check")
  boundaries = [_previous(months[0]), *months]
  ledgers: dict[str, dict[str, Ledger]] = {
    e.id: {m: cumulative(api, e, _month_end(m)) for m in boundaries} for e in entities
  }
  grouped = len(entities) > 1
  checks: list[Check] = []

  for index, month in enumerate(months):
    prior = boundaries[index]
    end = _month_end(month)
    start = end.replace(day=1)
    scopes: list[tuple[str, str | None, Ledger, Ledger]] = []
    for entity in entities:
      at_end, before = ledgers[entity.id][month], ledgers[entity.id][prior]
      active = any(abs(line.net) > 0.005 for line in at_end.lines.values())
      closed = entity.closed_through and month <= entity.closed_through
      if active and closed:
        scopes.append((entity.name, entity.id, before, at_end))
    if grouped:
      at_end = combine(ledgers[e.id][month] for e in entities if e.summed_at(end))
      before = combine(
        ledgers[e.id][prior] for e in entities if e.summed_at(_month_end(prior))
      )
      activity = combine(
        less(ledgers[e.id][month], ledgers[e.id][prior])
        for e in entities
        if e.summed_during(start, end)
      )
      scopes.append((CONSOLIDATED, None, before, at_end))
    else:
      activity = None

    for name, entity_id, before, at_end in scopes:
      if only and only.lower() not in name.lower():
        continue
      checker = Checker(name, month)
      period = activity if entity_id is None and activity else less(at_end, before)
      statements: dict[str, dict | str] = {}
      for kind in _STATEMENTS:
        try:
          statements[kind] = api.statement(kind, start, end, entity_id=entity_id)
        except (RuntimeError, httpx.HTTPError) as error:
          statements[kind] = str(error)
      checker.statements(statements, before, at_end, period)
      checks.extend(checker.checks)
      failed = sum(not c.passed for c in checker.checks)
      print(
        f"  {month}  {name:<34} {len(checker.checks) - failed:>3} ok"
        + (f"  {failed} FAILED" if failed else ""),
        flush=True,
      )
  return checks


def print_grid(checks: list[Check]) -> None:
  months = sorted({c.month for c in checks})
  scopes = list(dict.fromkeys(c.scope for c in checks))
  failed: dict[tuple[str, str, str], int] = {}
  seen: set[tuple[str, str, str]] = set()
  for c in checks:
    key = (c.scope, c.statement, c.month)
    seen.add(key)
    if not c.passed:
      failed[key] = failed.get(key, 0) + 1
  print("\n" + " " * 52 + " ".join(m[2:] for m in months))
  for scope in scopes:
    for statement in ("ledger", *_STATEMENTS):
      cells = []
      for month in months:
        key = (scope, statement, month)
        cells.append(
          "  -  "
          if key not in seen
          else f" x{failed[key]:<3}"
          if key in failed
          else "  ok "
        )
      print(f"{scope[:30]:<31}{statement[:19]:<20} " + " ".join(cells))
  failures = [c for c in checks if not c.passed]
  print(f"\n{len(checks) - len(failures)} of {len(checks)} checks passed")
  kinds: dict[tuple[str, str], list[Check]] = {}
  for c in failures:
    kinds.setdefault((c.statement, c.name), []).append(c)
  for (statement, name), group in sorted(kinds.items()):
    first = group[0]
    print(f"\nFAILED {statement}: {name} ({len(group)} scope-months)")
    print(f"  first: {first.scope} {first.month}: {first.detail}")


def _target(args: argparse.Namespace) -> Api:
  config = json.loads(_CONFIG.read_text()) if _CONFIG.exists() else {}
  slot = (config.get("graphs") or {}).get(args.graph)
  graph_id = (slot.get("graph_id") if isinstance(slot, dict) else slot) or args.graph
  base_url = args.base_url or config.get("base_url") or "http://localhost:8000"
  api_key = os.environ.get("ROBOSYSTEMS_API_KEY") or config.get("api_key")
  if not api_key:
    raise SystemExit("no API key: run `just demo-user` or set ROBOSYSTEMS_API_KEY")
  return Api(base_url, graph_id, api_key)


def main() -> None:
  parser = argparse.ArgumentParser(prog="benchmarks.reporting", description=__doc__)
  parser.add_argument("graph", help="a graph id, or a demo slot in .local/config.json")
  parser.add_argument("--months", type=int, default=12, help="closed months to check")
  parser.add_argument("--scope", help="only scopes whose name contains this")
  parser.add_argument("--base-url", help="API to read (default: the local stack)")
  parser.add_argument("--json", type=Path, help="write every check to this file")
  args = parser.parse_args()
  api = _target(args)
  print(f"Reporting acceptance: {api.graph_id} at {api.base_url}")
  checks = run(api, args.months, args.scope)
  if not checks:
    raise SystemExit("no checks ran: no scope has books for these months")
  print_grid(checks)
  if args.json:
    args.json.write_text(json.dumps([c.as_dict() for c in checks], indent=2))
  sys.exit(0 if all(c.passed for c in checks) else 1)


if __name__ == "__main__":
  main()
