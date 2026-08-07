#!/usr/bin/env python3
"""Author rollforward IBs for each World Online BS leaf with activity.

A rollforward explains a balance-sheet account's movement by decomposing it
into the business events that produced it. This reads ``GeneralLedger.csv``,
groups its lines by balance-sheet concept, derives the unique business-event
set for each, and creates one ``block_type='rollforward'`` Information Block
per leaf, with one attribution filter per distinct event.

Because the opening balances ingest as ordinary tagged transactions rather
than as an injected starting figure, the rollforward period can span the
opening date itself. The account then starts from a true zero and the
decomposition is complete:

    0 (genesis) + OpeningBalance + Σ(flow events) = ending balance

leaving a residual of 0 — which is the check that no posting went untagged.
The filter engine matches on ``LineItem.flow_element_id``, so every event
used as a filter target must resolve to a loaded Element; the load step adds
``mini:OpeningBalance`` as an extension concept for exactly that reason,
since it would otherwise be dropped here as unresolvable.

Prerequisites: the load and ingest steps have run against this graph.

Run it (the orchestrator runs this as step 6):
    uv run python -m examples.seattle_method_world_online.author_rollforwards <graph_id>
    uv run python -m examples.seattle_method_world_online.author_rollforwards <graph_id> --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import OrderedDict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "local" / "datasets" / "seattle_method_world_online"
GL_PATH = DATA_DIR / "GeneralLedger.csv"

# The balance-sheet leaves with activity in this dataset. It has no accrued
# expenses, unlike the 14-JE lemonade stand. Retained earnings is also absent
# by design: the Report architecture derives it from net income, so a manual
# rollforward over it would double-count.
BS_LEAVES: tuple[str, ...] = (
  "mini:CashAndCashEquivalents",
  "mini:Receivables",
  "mini:Inventories",
  "mini:PropertyPlantAndEquipment",
  "mini:AccountsPayable",
  "mini:LongtermDebt",
  "mini:PaidInCapital",
)


def _get_ledger_client():
  from robosystems_client.clients.ledger_client import LedgerClient

  config_path = REPO_ROOT / ".local" / "config.json"
  if not config_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  with config_path.open() as f:
    cfg = json.load(f)
  return LedgerClient(
    {
      "base_url": cfg.get("base_url", "http://localhost:8000"),
      "token": cfg["api_key"],
    }
  )


def _find_mini_taxonomy_id(client, graph_id: str) -> str | None:
  taxonomies = client.list_taxonomies(graph_id) or []
  for tax in taxonomies:
    if "mini" in (tax.get("name") or "").lower():
      return tax.get("id")
  return None


def _build_known_mini_qname_set(client, graph_id: str) -> set[str]:
  """List every mini Element qname on the graph (by taxonomy_id)."""
  taxonomy_id = _find_mini_taxonomy_id(client, graph_id)
  if not taxonomy_id:
    raise SystemExit("No mini CoA taxonomy found on this graph. Did the load step run?")
  known: set[str] = set()
  offset = 0
  page_size = 1000
  while True:
    page = client.list_elements(
      graph_id, taxonomy_id=taxonomy_id, limit=page_size, offset=offset
    )
    items = (page or {}).get("elements", [])
    if not items:
      break
    for e in items:
      if e.get("qname"):
        known.add(e["qname"])
    if len(items) < page_size:
      break
    offset += page_size
  return known


def collect_events_per_bs_leaf(
  gl_path: Path = GL_PATH,
) -> OrderedDict[str, list[str]]:
  """Return ``{bs_qname: [unique_business_event_qnames_in_first-seen order]}``.

  For each GL line whose line-item concept is a balance-sheet leaf, records
  its business-event tag. First-seen order is preserved, so the filters read
  in the order events appear in the ledger — opening balance first.
  """
  events: OrderedDict[str, list[str]] = OrderedDict((bs, []) for bs in BS_LEAVES)
  seen: dict[str, set[str]] = {bs: set() for bs in BS_LEAVES}
  with gl_path.open(encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
      account = (row.get("userDefined2") or "").strip()
      ev = (row.get("tags") or "").strip()
      if account in seen and ev and ev not in seen[account]:
        events[account].append(ev)
        seen[account].add(ev)
  return events


def _build_rollforward_arm(bs_qname: str, event_qnames: list[str]):
  """Build a typed ``CreateRollforwardArm`` for one rollforward IB."""
  from robosystems_client.models.attribution_filter import AttributionFilter
  from robosystems_client.models.create_rollforward_arm import CreateRollforwardArm
  from robosystems_client.models.create_rollforward_request import (
    CreateRollforwardRequest,
  )
  from robosystems_client.models.line_item_metadata_predicate import (
    LineItemMetadataPredicate,
  )

  short = bs_qname.removeprefix("mini:")
  filters = [
    AttributionFilter(
      target_qname=ev,
      predicate=LineItemMetadataPredicate(
        field="transaction_description_code",
        values=[ev],
      ),
    )
    for ev in event_qnames
  ]
  payload = CreateRollforwardRequest(
    name=f"{short} Rollforward (World Online)",
    bs_source_qname=bs_qname,
    attribution_filters=filters,
  )
  return CreateRollforwardArm(block_type="rollforward", payload=payload)


def author_rollforwards(
  graph_id: str, gl_path: Path = GL_PATH, dry_run: bool = False
) -> tuple[int, list[str]]:
  """Create one rollforward IB per BS leaf with activity.

  Returns ``(created_count, warnings)``.
  """
  events_per_leaf = collect_events_per_bs_leaf(gl_path)

  active = {bs: evs for bs, evs in events_per_leaf.items() if evs}
  print(
    f"Derived business-event sets from {gl_path.name} — "
    f"{len(active)}/{len(BS_LEAVES)} BS leaves have activity:"
  )
  for bs in BS_LEAVES:
    evs = events_per_leaf.get(bs, [])
    print(f"  {bs.removeprefix('mini:'):<32} {len(evs)} filter(s)")
    for ev in evs:
      print(f"    → {ev}")

  if dry_run:
    print(f"\nDry run — would create {len(active)} rollforward IB(s).")
    return len(active), []

  # Filter targets against the loaded mini Elements: a target present in the
  # GL but not on the graph would crash the create handler.
  # mini:OpeningBalance is loaded as an extension concept, so it survives.
  client = _get_ledger_client()
  known_qnames = _build_known_mini_qname_set(client, graph_id)

  warnings: list[str] = []
  filtered: dict[str, list[str]] = {}
  for bs_qname, evs in active.items():
    valid = [e for e in evs if e in known_qnames]
    for dropped in (e for e in evs if e not in known_qnames):
      msg = (
        f"phantom business event {dropped!r} on {bs_qname}: in GL but not a "
        f"loaded mini Element — dropping from filter list"
      )
      warnings.append(msg)
      print(f"  ⚠️  {msg}")
    if valid:
      filtered[bs_qname] = valid

  created = 0
  for bs_qname, evs in filtered.items():
    body = _build_rollforward_arm(bs_qname, evs)
    try:
      envelope = client.create_information_block(graph_id, body)
      created += 1
      print(f"  ✓ {bs_qname:<40} → IB {envelope.id} ({len(evs)} filters)")
    except Exception as exc:
      warnings.append(f"{bs_qname}: {exc}")
      print(f"  ✗ {bs_qname}: {exc}")

  return created, warnings


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Author rollforward IBs for the World Online BS leaves.",
  )
  parser.add_argument("graph_id", help="Target graph id.")
  parser.add_argument(
    "--gl", type=Path, default=GL_PATH, help="GeneralLedger.csv path."
  )
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print the filter sets that would be created; do not call the API.",
  )
  args = parser.parse_args()

  created, warnings = author_rollforwards(args.graph_id, args.gl, dry_run=args.dry_run)

  if warnings:
    print(f"\n{len(warnings)} warning(s):")
    for w in warnings:
      print(f"  ⚠️  {w}")

  action = "Would create" if args.dry_run else "Created"
  print(f"\n{action} {created} rollforward IB(s).")


if __name__ == "__main__":
  main()
