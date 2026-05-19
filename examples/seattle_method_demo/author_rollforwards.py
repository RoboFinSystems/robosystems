#!/usr/bin/env python3
"""Author rollforward IBs for every BS leaf with activity.

Reads ``fixtures/transactions.csv``, groups LineItems by BS-leaf
account, derives the unique ``TransactionDescriptionCode`` set per
account, and creates one ``block_type='rollforward'`` IB per BS leaf
via ``create-information-block``. Each filter targets the
corresponding mini flow concept.

Pre-condition: ``load_taxonomy.py``, ``seed_mappings.py``, and
``ingest_transactions.py`` have all run against this graph.

Why auto-derive instead of hardcoding the §4.4 table:

- Filter values stay in lockstep with the actual data. If Charlie's
  CSV gains/loses a TDC, the rollforwards re-derive correctly on
  the next run.
- Future cross-taxonomy test cases (us-gaap, IFRS) reuse the same
  pattern by pointing at a different CSV.
- The §4.4 table in ``cross-taxonomy-projection.md`` becomes the
  *expected* output, which the dry-run prints for review.

The eight BS leaves (instant period_type, monetary, balance-bearing)
in Charlie's dataset are listed in ``BS_LEAVES`` — hardcoded because
the loader currently doesn't expose period_type via the LedgerClient
list_elements endpoint. When that surfaces, this can become a
dynamic query.

Usage:
    uv run python -m examples.seattle_method_demo.author_rollforwards <graph_id>
    uv run python -m examples.seattle_method_demo.author_rollforwards <graph_id> --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

CSV_PATH = Path("examples/seattle_method_demo/fixtures/transactions.csv")

# The eight balance-sheet leaves in mini that have activity in Charlie's
# 14-JE dataset. Each gets its own rollforward IB.
BS_LEAVES: tuple[str, ...] = (
  "mini:CashAndCashEquivalents",
  "mini:Receivables",
  "mini:Inventories",
  "mini:PropertyPlantAndEquipment",
  "mini:AccountsPayable",
  "mini:AccruedExpenses",
  "mini:LongtermDebt",
  "mini:PaidInCapital",
)


def _get_ledger_client():
  from robosystems_client.clients.ledger_client import LedgerClient

  config_path = Path(".local/config.json")
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


def _post_operation_raw(graph_id: str, op_name: str, payload: dict) -> dict:
  """POST directly to the extensions operation endpoint via httpx.

  Bypasses the typed SDK because ``rollforward`` was added to the
  ``CreateInformationBlockRequest`` discriminated union AFTER the SDK
  was last regenerated — the SDK still only knows ``CreateLegacyArm``
  and ``CreateScheduleArm`` as valid body types. The operation
  endpoint itself accepts JSON dicts and validates against the
  current server-side Pydantic model, so we can author rollforward
  IBs without an SDK regenerate.

  TODO: drop this helper and call
  ``client.create_information_block()`` once the SDK is regenerated
  to include ``CreateRollforwardArm``.
  """
  import httpx

  config_path = Path(".local/config.json")
  with config_path.open() as f:
    cfg = json.load(f)
  base_url = cfg.get("base_url", "http://localhost:8000")
  api_key = cfg["api_key"]

  url = f"{base_url}/extensions/roboledger/{graph_id}/operations/{op_name}"
  response = httpx.post(
    url,
    json=payload,
    headers={"X-API-Key": api_key, "Content-Type": "application/json"},
    timeout=30.0,
  )
  if response.status_code >= 400:
    raise RuntimeError(
      f"POST {op_name} → {response.status_code}: {response.text[:500]}"
    )
  return response.json()


def _find_mini_taxonomy_id(client, graph_id: str) -> str | None:
  """Find the mini CoA taxonomy_id on the graph by name."""
  taxonomies = client.list_taxonomies(graph_id) or []
  for tax in taxonomies:
    if "mini" in (tax.get("name") or "").lower():
      return tax.get("id")
  return None


def _build_known_mini_qname_set(client, graph_id: str) -> set[str]:
  """List every mini Element qname on the graph (by taxonomy_id).

  Used to pre-filter rollforward target qnames — phantom TDCs that
  appear in the source CSV but not in mini.xsd would crash the
  create handler. Pre-filtering drops them with a warning instead.

  Mini elements get ``source='native'`` (CHECK constraint), so we
  scope by ``taxonomy_id`` instead of ``source``.
  """
  taxonomy_id = _find_mini_taxonomy_id(client, graph_id)
  if not taxonomy_id:
    raise SystemExit(
      "No mini CoA taxonomy found on this graph. Did load_taxonomy.py run?"
    )
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


def _collect_tdcs_per_bs_leaf(csv_path: Path) -> dict[str, list[str]]:
  """Return ``{bs_qname: [unique_tdc_qnames_in_csv_order]}``.

  Walks the CSV and records each (account, TDC) pair. Insertion order
  is preserved so the rollforward filters appear in the same order
  they'd be listed in a hand-authored §4.4 table — which is the order
  they first show up in Charlie's transaction stream.
  """
  tdcs_per_account: dict[str, list[str]] = defaultdict(list)
  seen: dict[str, set[str]] = defaultdict(set)
  with csv_path.open(encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
      account = row["GeneralLedgerAccountCode"]
      tdc = row["TransactionDescriptionCode"]
      if account in BS_LEAVES and tdc and tdc not in seen[account]:
        tdcs_per_account[account].append(tdc)
        seen[account].add(tdc)
  return dict(tdcs_per_account)


def _build_rollforward_payload(
  bs_qname: str, tdcs: list[str]
) -> dict:
  """Build the ``create-information-block`` payload for one rollforward.

  No ``default_change_tag_qname`` — Charlie's TDCs cover every line,
  so the residual should be zero. If a residual appears at render
  time, ``validation_mode='residual_as_default'`` (the default) will
  surface it as a synthetic ``#residual`` fact, which becomes the
  signal of an untagged JE — a useful audit artifact.
  """
  short = bs_qname.removeprefix("mini:")
  return {
    "block_type": "rollforward",
    "payload": {
      "name": f"{short} Rollforward (Seattle Method Test Case 1)",
      "bs_source_qname": bs_qname,
      "default_change_tag_qname": None,
      "attribution_filters": [
        {
          "target_qname": tdc,
          "predicate": {
            "kind": "line_item_metadata_field",
            "field": "transaction_description_code",
            "values": [tdc],
          },
        }
        for tdc in tdcs
      ],
      "validation_mode": "residual_as_default",
    },
  }


def author_rollforwards(
  graph_id: str, csv_path: Path = CSV_PATH, dry_run: bool = False
) -> tuple[int, list[str]]:
  """Walk the BS leaves and create one rollforward IB each.

  Returns ``(created_count, warnings)``.
  """
  tdcs_per_account = _collect_tdcs_per_bs_leaf(csv_path)

  print(
    f"Derived filter sets from {csv_path.name} — "
    f"{len(tdcs_per_account)}/{len(BS_LEAVES)} BS leaves have activity:"
  )
  for bs in BS_LEAVES:
    tdcs = tdcs_per_account.get(bs, [])
    short = bs.removeprefix("mini:")
    print(f"  {short:<35} {len(tdcs)} filter(s)")
    for tdc in tdcs:
      print(f"    → {tdc}")

  if dry_run:
    print(f"\nDry run — would create {len(tdcs_per_account)} rollforward IB(s).")
    return len(tdcs_per_account), []

  # Pre-filter: drop target qnames that don't resolve to a loaded
  # mini Element on this graph. Charlie's CSV contains at least one
  # phantom TDC (``mini:PaymentOfInterest`` — present in his data
  # but absent from mini.xsd); the create handler would crash on it,
  # so we drop with a warning and let the reconciliation report's
  # "Their data quality" section consume the finding.
  client = _get_ledger_client()
  known_qnames = _build_known_mini_qname_set(client, graph_id)

  warnings: list[str] = []
  filtered_per_account: dict[str, list[str]] = {}
  for bs_qname, tdcs in tdcs_per_account.items():
    valid = [t for t in tdcs if t in known_qnames]
    dropped = [t for t in tdcs if t not in known_qnames]
    for d in dropped:
      msg = (
        f"phantom TDC {d!r} on {bs_qname}: in CSV but not in loaded "
        f"mini taxonomy — dropping from filter list"
      )
      warnings.append(msg)
      print(f"  ⚠️  {msg}")
    if valid:
      filtered_per_account[bs_qname] = valid

  # Bypass the typed SDK — see _post_operation_raw docstring.
  created = 0
  for bs_qname, tdcs in filtered_per_account.items():
    payload = _build_rollforward_payload(bs_qname, tdcs)
    try:
      response = _post_operation_raw(
        graph_id, "create-information-block", payload
      )
      # OperationEnvelope shape: {result: {...envelope...}, operation_id, status}
      envelope = response.get("result") or response
      ib_id = envelope.get("id", "?")
      created += 1
      print(f"  ✓ {bs_qname:<40} → IB {ib_id} ({len(tdcs)} filters)")
    except Exception as exc:  # noqa: BLE001
      warnings.append(f"{bs_qname}: {exc}")
      print(f"  ✗ {bs_qname}: {exc}")

  return created, warnings


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Author rollforward IBs for the 8 BS leaves with activity.",
  )
  parser.add_argument("graph_id", help="Target graph id.")
  parser.add_argument(
    "--csv",
    type=Path,
    default=CSV_PATH,
    help="Path to the transactions CSV.",
  )
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print the filter sets that would be created; do not call the API.",
  )
  args = parser.parse_args()

  created, warnings = author_rollforwards(args.graph_id, args.csv, dry_run=args.dry_run)

  if warnings:
    print(f"\n{len(warnings)} IB creation(s) failed:")
    for w in warnings:
      print(f"  ⚠️  {w}")

  action = "Would create" if args.dry_run else "Created"
  print(f"\n{action} {created} rollforward IB(s).")


if __name__ == "__main__":
  main()
