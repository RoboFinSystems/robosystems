#!/usr/bin/env python3
"""The World Online — Seattle Method cross-taxonomy demo (orchestrator).

The same methodology as ``examples.seattle_method_demo``, at realistic scale.
Where the lemonade stand has 14 journal entries, Charlie Hoffman's *The World
Online* dataset has 22,288 general-ledger lines across 3,389 entries, tagged
against the MINI 2026 reporting framework — with a real 239-account chart of
accounts and genuine opening balances. Every line is tagged with both its
line-item concept and the business event behind it, which is what lets a
balance sheet be decomposed into the events that produced it and then
re-rendered in a second vocabulary.

The mini parser/loader and the mini→rs-gaap mapping table are imported from
the lemonade-stand demo rather than copied. What is specific here: the GL
ingest, the rollforward authoring, the trial balance, and two reconciliations
— one against the published transaction pivot, one against the published
reference statements.

Prerequisites:
    just start        # local stack (API, PostgreSQL, Valkey, LadybugDB)
    just demo-user    # writes credentials to .local/config.json

Run it:
    just demo-world-online                          # new graph + every step
    just demo-world-online --graph <id>             # against an existing graph
    just demo-world-online --step ingest --graph <id>
    just demo-world-online --limit 50               # ingest a subset, as a smoke test

The full ingest posts ~3,389 events one at a time and takes a while; ``--limit``
is the fast path when checking the pipeline rather than the numbers. Artifacts
land in ``examples/seattle_method_world_online/output/``.
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

# mini:OpeningBalance is not part of MINI 2026, but the dataset's
# beginning-balance-forward lines tag it. Those line items need a resolvable
# flow concept to attribute in the rollforward and to reconcile against the
# published OpeningBalance pivot row, so it is added as a small extension
# concept on top of the pulled mini CoA — the ordinary way a tenant extends a
# base framework. It is deliberately left unmapped to rs-gaap: an opening
# balance is a position carried forward, not a cash-flow movement, so it has
# no counterpart on a cash flow statement.
OPENING_BALANCE_PAYLOAD: dict = {
  "qname": "mini:OpeningBalance",
  "name": "Opening Balance",
  "trait": "equity",
  "balance_type": "credit",
  "element_type": "concept",
  # The CoA validator couples trait to period_type: an equity trait requires
  # period_type='instant'. That fits — an opening balance is the genesis net
  # position, not a duration flow — and the rollforward matches on
  # flow_element_id regardless.
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
  created, warnings, failures = ingest_mod.ingest(
    graph_id, dry_run=dry_run, limit=limit
  )
  if warnings:
    print(f"  {len(warnings)} warning(s) (see ingest output above)")
  action = "Would create" if dry_run else "Created"
  print(f"  {action} {created} event(s)")

  # Stop the run rather than reconcile against a ledger with holes in it. Every
  # downstream step — rollforwards, reconciliation, the four statements — reads
  # these events, so continuing produces totals that look authoritative and are
  # quietly short by whatever failed to post.
  if failures:
    print(f"\n  ❌ {len(failures)} entr(y/ies) failed to post:")
    for f in failures[:25]:
      print(f"    ✗ {f}")
    if len(failures) > 25:
      print(f"    … and {len(failures) - 25} more")
    raise SystemExit(
      "  Ingest incomplete — the ledger is missing entries, so the "
      "reconciliation below would be meaningless."
    )


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


def step_reconcile(
  graph_id: str, dry_run: bool = False, no_diff: bool = False
) -> None:
  """Step 7 — reconcile the graph pivot against SummaryOfTransactions.csv."""
  print("─" * 70)
  print(f"Step 7 — reconcile vs SummaryOfTransactions.csv → graph {graph_id}")
  print("─" * 70)
  if dry_run:
    print("  (dry-run — skipping reconcile)")
    return
  cmd = [
    "uv",
    "run",
    "python",
    "-m",
    "examples.seattle_method_world_online.reconcile",
    graph_id,
  ]
  if no_diff:
    cmd.append("--no-diff")
  result = subprocess.run(cmd, cwd=str(REPO_ROOT), check=False)
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
  """Step 9 — render the trial balance from the ingested ledger."""
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

  Pulls the flat JSON-LD, the native holon (``.holon.jsonld`` — the viewer's
  input) and the XBRL 2.1 zip from the report endpoint, validates them
  container-free (SHACL over the JSON-LD, Arelle over the XBRL 2.1), and
  writes the DataBook with both verdicts inlined. Step 11 reads the JSON-LD
  this step writes, so it must run first.
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


def step_statement_reconcile(
  graph_id: str, dry_run: bool = False, no_fetch: bool = False
) -> None:
  """Step 11 — statement-level reconcile vs the published reference instance.

  Reads the four-statement anchor totals out of the JSON-LD bundle step 10
  wrote and diffs them against Charlie Hoffman's published reference report.
  Step 7 checks the ingestion against his transaction pivot; this checks the
  rendered statements against his statements.
  """
  print("─" * 70)
  print(f"Step 11 — statement-level reconcile vs reference instance → {graph_id}")
  print("─" * 70)
  if dry_run:
    print("  (dry-run — skipping statement reconcile)")
    return
  cmd = [
    "uv",
    "run",
    "python",
    "-m",
    "examples.seattle_method_world_online.statement_reconcile",
  ]
  if no_fetch:
    cmd.append("--no-fetch")
  result = subprocess.run(cmd, cwd=str(REPO_ROOT), check=False)
  if result.returncode != 0:
    raise SystemExit(f"statement_reconcile exited with code {result.returncode}")


def _resolve_step_graph_id(cli_graph: str | None) -> str:
  """Resolve the target graph for a single-step run.

  An explicit ``--graph`` wins; otherwise fall back to the
  ``world_online_test`` slot the provision step cached, so re-running one
  step never requires remembering the id.
  """
  if cli_graph:
    return cli_graph

  from examples._common.config import require_cached_graph_id, require_config

  return require_cached_graph_id(require_config(), DEMO_NAME, "just demo-world-online")


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
  parser.add_argument(
    "--no-diff",
    action="store_true",
    help="reconcile step only: print the graph pivot; skip the comparison.",
  )
  parser.add_argument(
    "--no-fetch",
    action="store_true",
    help="statement-reconcile step only: use the cached reference instance.",
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
      graph_id = _resolve_step_graph_id(args.graph)
      if args.step == "ingest":
        fn(graph_id, dry_run=args.dry_run, limit=args.limit)
      elif args.step == "reconcile":
        fn(graph_id, dry_run=args.dry_run, no_diff=args.no_diff)
      elif args.step == "statement-reconcile":
        fn(graph_id, dry_run=args.dry_run, no_fetch=args.no_fetch)
      else:
        fn(graph_id, dry_run=args.dry_run)
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
