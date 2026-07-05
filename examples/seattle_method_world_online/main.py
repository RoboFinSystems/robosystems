#!/usr/bin/env python3
"""The World Online — Seattle Method cross-taxonomy demo (orchestrator).

End-to-end driver for Charlie Hoffman's *The World Online* dataset —
22,288 GL lines across 3,389 journal entries, tagged against the MINI
2026 reporting framework. The scaled-up sibling of
``examples.seattle_method_demo`` (the 14-JE lemonade stand): same
methodology (line-item + business-event tagging → rollforwards →
statements), at a realistic mid-size-company scale, with a real chart of
accounts and opening balances.

Shared with the lemonade-stand demo (imported, not copied):
``load_taxonomy`` (mini parser/loader) and ``seed_mappings``
(mini→rs-gaap). New here: the GL ingest, the rollforward authoring, and
the reconciliation against ``SummaryOfTransactions.csv``.

Usage:
    uv run python -m examples.seattle_method_world_online.main                  # new graph + all steps
    uv run python -m examples.seattle_method_world_online.main --graph <id>     # existing graph
    uv run python -m examples.seattle_method_world_online.main --step ingest --graph <id>
    uv run python -m examples.seattle_method_world_online.main --limit 50        # smoke-test ingest subset
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from examples.seattle_method_demo import load_taxonomy as load_mod
from examples.seattle_method_demo import seed_mappings as seed_mod

from . import author_rollforwards as auth_rf_mod
from . import ingest_gl as ingest_mod

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_DIR = REPO_ROOT / "examples" / "seattle_method_world_online"
PULL_TAXONOMY_SCRIPT = DEMO_DIR / "pull_mini_2026.sh"
PULL_DATA_SCRIPT = DEMO_DIR / "pull_world_online.sh"
TAXONOMY_DIR = REPO_ROOT / "local" / "taxonomies" / "mini-2026"

BASE_URL = "http://localhost:8000"
CREDENTIALS_FILE = Path(".local/config.json")

DEMO_NAME = "world_online_test"
GRAPH_DISPLAY_NAME = "The World Online (Seattle Method MINI 2026)"
ENTITY_NAME = "The World Online (Charlie Hoffman demo)"

# mini:OpeningBalance is NOT in the MINI 2026 taxonomy (Charlie flags
# this in the dataset README). The 98 BBF lines tag it, and for those
# LineItems to resolve a flow_element_id — and thus attribute in the
# rollforward and reconcile against Charlie's OpeningBalance pivot row —
# it must exist as a loaded Element. We add it as a small extension
# concept on top of the pulled mini CoA. The trait is cosmetic here: the
# rollforward sums debit-positive cents regardless of the flow concept's
# own trait, and OpeningBalance is intentionally left unmapped to rs-gaap
# (it's an opening position, not a cash-flow movement).
OPENING_BALANCE_PAYLOAD: dict = {
  "qname": "mini:OpeningBalance",
  "name": "Opening Balance",
  "trait": "equity",
  "balance_type": "credit",
  "element_type": "concept",
  # The CoA validator couples trait↔period_type: an equity trait requires
  # period_type='instant'. Semantically apt — opening balance is a
  # position (the genesis net position), not a duration flow. period_type
  # is irrelevant to the rollforward, which matches on flow_element_id.
  "period_type": "instant",
  "is_monetary": True,
  "description": (
    "Extension concept — opening balance brought forward. NOT part of "
    "MINI 2026; added so opening-balance LineItems resolve flow_element_id "
    "and attribute in the rollforward decomposition."
  ),
  "code": "mini:OpeningBalance",
  "sub_classification": None,
  "parent_ref": None,
  "metadata": {
    "source_taxonomy": "seattle-method-mini-2026-extension",
    "concept_type": "extension",
  },
}


# ── Steps ────────────────────────────────────────────────────────────────


def step_pull() -> None:
  """Step 1 — pull MINI 2026 taxonomy + the World Online dataset."""
  print("─" * 70)
  print("Step 1 — pull MINI 2026 taxonomy + World Online GL/CoA/Summary")
  print("─" * 70)
  for label, script in (
    ("MINI 2026 taxonomy", PULL_TAXONOMY_SCRIPT),
    ("World Online dataset", PULL_DATA_SCRIPT),
  ):
    print(f"  → {label}")
    result = subprocess.run(["bash", str(script)], cwd=str(REPO_ROOT), check=False)
    if result.returncode != 0:
      raise SystemExit(f"{script.name} exited with code {result.returncode}")


def step_provision_graph() -> str:
  """Step 2 — create (or reuse) a dedicated test graph."""
  print("─" * 70)
  print("Step 2 — provision test graph")
  print("─" * 70)

  if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

  from examples._common.config import get_graph_id, save_graph_id
  from examples.credentials.utils import (
    CredentialContext,
    ensure_user_credentials,
  )

  context = CredentialContext(
    base_url=BASE_URL,
    credentials_path=CREDENTIALS_FILE,
    force=False,
    default_name_prefix="World Online Demo",
    default_email_prefix="world_online_demo",
    api_key_prefix="World Online Demo Key",
    display_title="The World Online — Cross-Taxonomy Demo Setup",
  )
  credentials = ensure_user_credentials(context)
  api_key = credentials["api_key"]

  existing = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)
  if existing:
    print(f"  Reusing existing graph: {existing}")
    return existing

  from robosystems_client.api.graphs.create_graph import (
    sync_detailed as api_create_graph,
  )
  from robosystems_client.api.operations.get_operation_status import (
    sync_detailed as api_get_operation_status,
  )
  from robosystems_client.client import AuthenticatedClient
  from robosystems_client.models import CreateGraphRequest, GraphMetadata

  client = AuthenticatedClient(
    base_url=BASE_URL, token=api_key, prefix="", auth_header_name="X-API-Key"
  )

  metadata = GraphMetadata(
    graph_name=GRAPH_DISPLAY_NAME,
    description=(
      "Cross-taxonomy projection at scale — Charlie Hoffman's 'The World "
      "Online' dataset (22,288 GL lines) tagged against MINI 2026, ingested "
      "as Events, rolled forward and reconciled. See "
      "examples/seattle_method_world_online/README.md."
    ),
    schema_extensions=["roboledger"],
  )
  request = CreateGraphRequest(
    metadata=metadata,
    initial_entity={
      "name": ENTITY_NAME,
      "uri": "https://github.com/seattlemethod/prototypes/tree/main/the-world-online-demo-data",
      "entity_type": "corporation",
    },
    tags=["demo", "seattle-method", "world-online", "mini-2026"],
  )
  print(f"  Creating graph: {GRAPH_DISPLAY_NAME}")
  response = api_create_graph(client=client, body=request)
  if response.status_code >= 400:
    body = response.content.decode() if response.content else "(no body)"
    raise SystemExit(f"Failed to create graph: HTTP {response.status_code}\n  {body}")
  if not response.parsed:
    raise SystemExit(
      f"Failed to create graph: empty response (HTTP {response.status_code})"
    )

  parsed = response.parsed
  graph_id = getattr(parsed, "graph_id", None)
  operation_id = getattr(parsed, "operation_id", None)
  if isinstance(parsed, dict):
    graph_id = parsed.get("graph_id")
    operation_id = parsed.get("operation_id")

  if not graph_id and operation_id:
    print(f"  Queued (operation: {operation_id}), waiting…")
    for _ in range(30):
      time.sleep(2)
      status_resp = api_get_operation_status(operation_id=operation_id, client=client)
      if not status_resp.parsed:
        continue
      status_data = status_resp.parsed
      if isinstance(status_data, dict):
        status = status_data.get("status")
        result = status_data.get("result", {})
      elif hasattr(status_data, "additional_properties"):
        props = status_data.additional_properties
        status = props.get("status")
        result = props.get("result", {})
      else:
        status = getattr(status_data, "status", None)
        result = getattr(status_data, "result", {})
      if status == "completed":
        graph_id = result.get("graph_id") if isinstance(result, dict) else None
        break
      if status == "failed":
        error = result.get("error") if isinstance(result, dict) else "unknown"
        raise SystemExit(f"Graph creation failed: {error}")

  if not graph_id:
    raise SystemExit("Timed out waiting for graph creation")

  save_graph_id(
    CREDENTIALS_FILE, DEMO_NAME, graph_id, time.strftime("%Y-%m-%d %H:%M:%S")
  )
  print(f"  ✓ Provisioned graph {graph_id}")
  return graph_id


def step_load_taxonomy(graph_id: str, dry_run: bool = False) -> None:
  """Step 3 — load MINI 2026 as a CoA + the OpeningBalance extension."""
  print("─" * 70)
  print(f"Step 3 — load MINI 2026 taxonomy → graph {graph_id}")
  print("─" * 70)
  if not TAXONOMY_DIR.exists():
    raise SystemExit(
      f"{TAXONOMY_DIR} missing — run step 'pull' first or re-run end-to-end."
    )
  parsed = load_mod.parse_all(TAXONOMY_DIR)
  payloads = load_mod.build_element_payloads(parsed)
  payloads.append(OPENING_BALANCE_PAYLOAD)
  print(
    f"  Parsed {len(parsed.concepts)} concepts → {len(payloads)} element "
    f"payloads (incl. the mini:OpeningBalance extension)"
  )
  if dry_run:
    print("  (dry-run — no upload)")
    return
  taxonomy_id = load_mod.upload_taxonomy(graph_id, payloads)
  print(f"  ✓ Created taxonomy {taxonomy_id}")


def step_seed_mappings(graph_id: str, dry_run: bool = False) -> None:
  """Step 4 — seed mini→rs-gaap derivation associations (shared table)."""
  print("─" * 70)
  print(f"Step 4 — seed mini→rs-gaap mappings → graph {graph_id}")
  print("─" * 70)
  created, warnings = seed_mod.seed_mappings(graph_id, dry_run=dry_run)
  for w in warnings:
    print(f"  ⚠️  {w}")
  action = "Would create" if dry_run else "Created"
  print(f"  {action} {created} association(s)")


def step_ingest(graph_id: str, dry_run: bool = False, limit: int | None = None) -> None:
  """Step 5 — ingest the GL (~3,389 entries) via create-event-block."""
  print("─" * 70)
  print(f"Step 5 — ingest World Online GL → graph {graph_id}")
  print("─" * 70)
  created, warnings = ingest_mod.ingest(graph_id, dry_run=dry_run, limit=limit)
  if warnings:
    print(f"  {len(warnings)} warning(s) (see ingest output above)")
  action = "Would create" if dry_run else "Created"
  print(f"  {action} {created} event(s)")


def step_author_rollforwards(graph_id: str, dry_run: bool = False) -> None:
  """Step 6 — author rollforward IBs (one per BS leaf with activity)."""
  print("─" * 70)
  print(f"Step 6 — author rollforward IBs → graph {graph_id}")
  print("─" * 70)
  created, warnings = auth_rf_mod.author_rollforwards(graph_id, dry_run=dry_run)
  for w in warnings:
    print(f"  ⚠️  {w}")
  action = "Would create" if dry_run else "Created"
  print(f"  {action} {created} rollforward IB(s)")


def step_reconcile(graph_id: str, dry_run: bool = False) -> None:
  """Step 7 — reconcile the graph pivot against SummaryOfTransactions.csv."""
  print("─" * 70)
  print(f"Step 7 — reconcile vs SummaryOfTransactions.csv → graph {graph_id}")
  print("─" * 70)
  if dry_run:
    print("  (dry-run — skipping reconcile)")
    return
  result = subprocess.run(
    [
      "uv",
      "run",
      "python",
      "-m",
      "examples.seattle_method_world_online.reconcile",
      graph_id,
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"reconcile exited with code {result.returncode}")


def step_create_report(graph_id: str, dry_run: bool = False) -> None:
  """Step 8 — materialize the rs-gaap 4-statement Report + render markdown."""
  print("─" * 70)
  print(f"Step 8 — create-report (rs-gaap 4-statement) → graph {graph_id}")
  print("─" * 70)
  if dry_run:
    print("  (dry-run — skipping create-report)")
    return
  result = subprocess.run(
    [
      "uv",
      "run",
      "python",
      "-m",
      "examples.seattle_method_world_online.create_report",
      graph_id,
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"create_report exited with code {result.returncode}")


def step_trial_balance(graph_id: str, dry_run: bool = False) -> None:
  """Step 9 — render the trial balance (Charlie's objective item 9)."""
  print("─" * 70)
  print(f"Step 9 — trial balance → graph {graph_id}")
  print("─" * 70)
  if dry_run:
    print("  (dry-run — skipping trial balance)")
    return
  result = subprocess.run(
    [
      "uv",
      "run",
      "python",
      "-m",
      "examples.seattle_method_world_online.trial_balance",
      graph_id,
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"trial_balance exited with code {result.returncode}")


def step_download_bundles(graph_id: str, dry_run: bool = False) -> None:
  """Step 10 — Render the latest filed Report's aligned artifact set.

  Delegates to the shared ``render_report_artifacts`` (via
  ``download_bundles.py``): pulls the flat JSON-LD, the native holon
  (``.holon.jsonld`` — the viewer's input), and the XBRL 2.1 zip from the
  report endpoint, then validates them container-free (SHACL over the JSON-LD,
  Arelle over the XBRL 2.1) and writes the DataBook with the verdicts inlined.
  Subprocess invocation for the same isolation reason as the other
  rendering / download steps.
  """
  print("─" * 70)
  print(
    f"Step 10 — Render bundle artifacts (download + validate + DataBook) → "
    f"graph {graph_id}"
  )
  print("─" * 70)
  if dry_run:
    print("  (dry-run — skipping download-bundles)")
    return
  result = subprocess.run(
    [
      "uv",
      "run",
      "python",
      "-m",
      "examples.seattle_method_world_online.download_bundles",
      graph_id,
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"download_bundles exited with code {result.returncode}")


def step_statement_reconcile(graph_id: str, dry_run: bool = False) -> None:
  """Step 11 — statement-level reconcile vs Charlie's reference instance.xml.

  Reads our four-statement anchors from the v2 bundle (``output/...jsonld``,
  written by step 10) and diffs them against Charlie's published reference
  report. Complements the GL-pivot ``reconcile`` (step 7) at the rendered-
  statement level.
  """
  print("─" * 70)
  print(f"Step 11 — statement-level reconcile vs reference instance → {graph_id}")
  print("─" * 70)
  if dry_run:
    print("  (dry-run — skipping statement reconcile)")
    return
  result = subprocess.run(
    [
      "uv",
      "run",
      "python",
      "-m",
      "examples.seattle_method_world_online.statement_reconcile",
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"statement_reconcile exited with code {result.returncode}")


# ── Step registry ──────────────────────────────────────────────────────────

STEPS = {
  "pull": ("Pull MINI 2026 taxonomy + World Online dataset", step_pull),
  "provision": ("Provision a dedicated test graph", step_provision_graph),
  "load": ("Load MINI 2026 as a CoA + OpeningBalance extension", step_load_taxonomy),
  "seed-mappings": ("Seed mini→rs-gaap derivation associations", step_seed_mappings),
  "ingest": ("Ingest the GL (~3,389 entries) as Events", step_ingest),
  "author-rollforwards": (
    "Author rollforward IBs for every BS leaf with activity",
    step_author_rollforwards,
  ),
  "reconcile": ("Reconcile vs SummaryOfTransactions.csv", step_reconcile),
  "create-report": ("Materialize the rs-gaap 4-statement Report", step_create_report),
  "trial-balance": ("Render the trial balance", step_trial_balance),
  "download-bundles": (
    "Render the aligned artifact set (download + validate + DataBook)",
    step_download_bundles,
  ),
  "statement-reconcile": (
    "Reconcile rendered-statement anchors vs Charlie's reference instance",
    step_statement_reconcile,
  ),
}


def main() -> None:
  parser = argparse.ArgumentParser(
    description="The World Online cross-taxonomy demo orchestrator.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="Steps:\n" + "\n".join(f"  {k:<22} {v[0]}" for k, v in STEPS.items()),
  )
  parser.add_argument(
    "--graph", metavar="GRAPH_ID", help="Run against an existing graph."
  )
  parser.add_argument("--step", choices=list(STEPS.keys()), help="Run a single step.")
  parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Cap the ingest to the first N entries (smoke test).",
  )
  parser.add_argument(
    "--dry-run", action="store_true", help="Validate + report; no API writes."
  )
  args = parser.parse_args()

  # Single-step mode.
  if args.step:
    fn = STEPS[args.step][1]
    if args.step == "pull":
      fn()
    elif args.step == "provision":
      graph_id = fn()
      print(f"\nProvisioned graph: {graph_id}")
      print(f"To continue: --graph {graph_id} --step <next>")
    else:
      if not args.graph:
        raise SystemExit(
          f"--step {args.step} requires --graph <id> "
          "(or run end-to-end without --step to provision one)."
        )
      if args.step == "ingest":
        fn(args.graph, dry_run=args.dry_run, limit=args.limit)
      else:
        fn(args.graph, dry_run=args.dry_run)
    return

  # Full end-to-end run.
  print("The World Online — Cross-Taxonomy Demo — full run\n")

  step_pull()
  print()

  if args.graph:
    graph_id = args.graph
    print(f"Step 2 — using existing graph {graph_id}\n")
  else:
    graph_id = step_provision_graph()
    print()

  step_load_taxonomy(graph_id, dry_run=args.dry_run)
  print()
  step_seed_mappings(graph_id, dry_run=args.dry_run)
  print()
  step_ingest(graph_id, dry_run=args.dry_run, limit=args.limit)
  print()
  step_author_rollforwards(graph_id, dry_run=args.dry_run)
  print()
  step_reconcile(graph_id, dry_run=args.dry_run)
  print()
  step_create_report(graph_id, dry_run=args.dry_run)
  print()
  step_trial_balance(graph_id, dry_run=args.dry_run)
  print()
  step_download_bundles(graph_id, dry_run=args.dry_run)
  print()
  step_statement_reconcile(graph_id, dry_run=args.dry_run)
  print()

  print("─" * 70)
  print(f"✓ End-to-end demo run complete against graph {graph_id}")
  print("─" * 70)
  print("\nArtifacts in examples/seattle_method_world_online/output/:")
  print("  - world-online-reconciliation.md   (mini pivot vs SummaryOfTransactions)")
  print("  - world-online-four-statements.md  (rs-gaap 4-statement Report)")
  print("  - world-online-trial-balance.md    (trial balance)")
  print("  - world-online.jsonld              (flat JSON-LD bundle)")
  print("  - world-online.holon.jsonld        (native holon — viewer input)")
  print("  - world-online.zip                 (XBRL 2.1 report package)")
  print("  - world-online-shacl-validation.md (SHACL conformance)")
  print("  - world-online-xbrl-validation.md  (Arelle conformance)")
  print("  - world-online.databook.md         (DataBook)")


if __name__ == "__main__":
  main()
