#!/usr/bin/env python3
"""Author rollforward Information Blocks for every BS leaf with activity.

A rollforward decomposes a balance-sheet account's movement over a period
into the business events that caused it: opening balance, plus each tagged
flow, equals the closing balance. That is the analytical payoff of tagging
every line with its flow concept at ingest — the balance sheet stops being a
set of opaque totals and becomes an explained one.

This reads the journal CSV, groups its lines by balance-sheet leaf, derives
the unique ``TransactionDescriptionCode`` set for each, and creates one
``block_type='rollforward'`` Information Block per leaf. Every distinct flow
concept becomes one attribution filter.

The filter sets are derived from the data rather than hardcoded, so they stay
in lockstep with it: if the source gains or loses a flow concept, the next run
re-derives correctly, and the same script works against a different dataset by
pointing ``--csv`` elsewhere. ``--dry-run`` prints the filter table it would
create, which doubles as a review of the tagging.

``BS_LEAVES`` is listed explicitly because ``list_elements`` does not expose
period_type, so the balance-sheet leaves cannot yet be selected by query.

Prerequisites: ``load_taxonomy.py``, ``seed_mappings.py`` and
``ingest_transactions.py`` have all run against this graph. Standalone runs
must pass ``--csv`` — the orchestrator supplies the pulled
``local/datasets/seattle_method/GeneralJournal.csv``.

Run it (the orchestrator runs this as step 6):
    uv run python -m examples.seattle_method_demo.author_rollforwards <graph_id> --csv <path>
    uv run python -m examples.seattle_method_demo.author_rollforwards <graph_id> --csv <path> --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

# Resolved relative to this file so the script works from any working
# directory. Override with ``--csv`` when running outside the orchestrator.
CSV_PATH = (
  Path(__file__).resolve().parent / "fixtures" / "transactions.csv"
)

# The mini balance-sheet leaves with activity in the 14-JE dataset. Each gets
# its own rollforward IB.
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


def _find_mini_taxonomy_id(client, graph_id: str) -> str | None:
  """Find the mini CoA taxonomy_id on the graph by name."""
  taxonomies = client.list_taxonomies(graph_id) or []
  for tax in taxonomies:
    # ariadne-generated GraphQL models: attribute access, not mapping `.get()`.
    if "mini" in (tax.name or "").lower():
      return tax.id
  return None


def _build_known_mini_qname_set(client, graph_id: str) -> set[str]:
  """List every mini Element qname on the graph (by taxonomy_id).

  Used to pre-filter rollforward targets: a flow concept present in the CSV
  but absent from mini.xsd would crash the create handler, so it is dropped
  with a warning instead.

  Mini elements carry ``source='native'`` (a CHECK-constraint enum), so the
  lookup scopes by ``taxonomy_id``.
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
    items = page.elements if page else []
    if not items:
      break
    for e in items:
      if e.qname:
        known.add(e.qname)
    if len(items) < page_size:
      break
    offset += page_size
  return known


def _collect_tdcs_per_bs_leaf(csv_path: Path) -> dict[str, list[str]]:
  """Return ``{bs_qname: [unique_tdc_qnames_in_csv_order]}``.

  First-seen order is preserved, so the rollforward filters read in the order
  the flows first appear in the transaction stream.
  """
  tdcs_per_account: dict[str, list[str]] = defaultdict(list)
  seen: dict[str, set[str]] = defaultdict(set)
  with csv_path.open(encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
      account = row["GeneralLedgerAccountCode"]
      raw_tdc = row["TransactionDescriptionCode"]
      # Normalize exactly as ingest does, so a filter targets the same value
      # that was stamped on the LineItem. Skip this and an un-normalized
      # target silently matches nothing, and the rollforward residual
      # swallows the attribution instead.
      from examples.seattle_method_demo.ingest_transactions import (
        _KNOWN_TDC_ALIASES,
      )

      tdc = _KNOWN_TDC_ALIASES.get(raw_tdc, raw_tdc)
      if account in BS_LEAVES and tdc and tdc not in seen[account]:
        tdcs_per_account[account].append(tdc)
        seen[account].add(tdc)
  return dict(tdcs_per_account)


def _build_rollforward_arm(bs_qname: str, tdcs: list[str]):
  """Build a typed ``CreateRollforwardArm`` for one rollforward IB.

  No ``default_change_tag_qname`` is set: every line in this dataset carries
  a flow concept, so the rollforward should foot exactly and the residual
  should be zero. If one does appear, the default
  ``validation_mode='residual_as_default'`` surfaces it as a synthetic
  ``#residual`` fact — which is the signal that a journal entry went
  untagged, and a useful audit artifact in its own right.
  """
  # Lazy imports — the SDK module is heavy and a dry run does not need it.
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
      target_qname=tdc,
      predicate=LineItemMetadataPredicate(
        field="transaction_description_code",
        values=[tdc],
      ),
    )
    for tdc in tdcs
  ]
  payload = CreateRollforwardRequest(
    name=f"{short} Rollforward (Seattle Method Test Case 1)",
    bs_source_qname=bs_qname,
    attribution_filters=filters,
  )
  return CreateRollforwardArm(
    block_type="rollforward",
    payload=payload,
  )


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

  # Drop target qnames that don't resolve to a loaded mini Element. The
  # source CSV carries flow concepts that are absent from mini.xsd; the
  # create handler would crash on those, so they are dropped with a warning
  # and the reconciliation report picks them up as a data-quality finding.
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

  created = 0
  for bs_qname, tdcs in filtered_per_account.items():
    body = _build_rollforward_arm(bs_qname, tdcs)
    try:
      envelope = client.create_information_block(graph_id, body)
      created += 1
      print(f"  ✓ {bs_qname:<40} → IB {envelope.id} ({len(tdcs)} filters)")
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
