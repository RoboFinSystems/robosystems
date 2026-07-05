#!/usr/bin/env python3
"""Seattle Method Cross-Taxonomy Demo — orchestrator.

End-to-end driver for Test Case 1 (mini / Charlie Hoffman 14-JE
lemonade-stand dataset). Provisions a dedicated test graph, pulls
the mini taxonomy artifacts, loads them as a CoA, seeds the
mini→rs-gaap mappings, ingests the 14 JEs, and authors the 8
rollforward IBs.

Mirrors the structure of ``examples.roboledger_demo.main`` — one
``LedgerClient``-driven orchestrator with discrete steps that can be
re-run individually via ``--step``.

Reconciliation (the rendered comparison against
``luca.pacioli.ai/luca/view/0f24fd35…``) lives in
``reconcile.py`` and is invoked separately; see the README's
"What Gets Created" section for the full step list.

Usage:
    uv run python -m examples.seattle_method_demo.main                       # Create new graph + run every step
    uv run python -m examples.seattle_method_demo.main --graph <id>          # Run against an existing graph
    uv run python -m examples.seattle_method_demo.main --step pull           # Run a single step
    uv run python -m examples.seattle_method_demo.main --dry-run             # Validate + report; no API calls
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from . import author_rollforwards as auth_rf_mod
from . import ingest_transactions as ingest_mod
from . import load_taxonomy as load_mod
from . import seed_mappings as seed_mod

REPO_ROOT = Path(__file__).resolve().parents[2]
PULL_TAXONOMY_SCRIPT = REPO_ROOT / "examples" / "seattle_method_demo" / "pull_mini.sh"
PULL_JOURNAL_SCRIPT = (
  REPO_ROOT / "examples" / "seattle_method_demo" / "pull_general_journal.sh"
)
PULL_REPORT_SCRIPT = (
  REPO_ROOT / "examples" / "seattle_method_demo" / "pull_expected_report.sh"
)
TAXONOMY_DIR = REPO_ROOT / "local" / "taxonomies" / "mini"
CSV_PATH = REPO_ROOT / "local" / "datasets" / "seattle_method" / "GeneralJournal.csv"

BASE_URL = "http://localhost:8000"
CREDENTIALS_FILE = Path(".local/config.json")

DEMO_NAME = "seattle_method_test"
GRAPH_DISPLAY_NAME = "Seattle Method Cross-Taxonomy Test"
ENTITY_NAME = "Lemonade Stand (Charlie Hoffman Test Case 1)"


# ── Client helpers ───────────────────────────────────────────────────────


def _client_config() -> dict[str, str]:
  config_path = Path(".local/config.json")
  if not config_path.exists():
    raise SystemExit(
      "Missing .local/config.json — run `just demo-user` first to create credentials."
    )
  with config_path.open() as f:
    cfg = json.load(f)
  return {
    "base_url": cfg.get("base_url", "http://localhost:8000"),
    "token": cfg["api_key"],
  }


def _get_ledger_client():
  from robosystems_client.clients.ledger_client import LedgerClient

  return LedgerClient(_client_config())


def _get_graph_client():
  from robosystems_client.clients.graph_client import GraphClient

  return GraphClient(_client_config())


# ── Steps ────────────────────────────────────────────────────────────────


def step_pull() -> None:
  """Step 1 — Pull Charlie's upstream artifacts.

  Fetches three artifacts from Charlie's published sources so the
  demo ingests AND validates against his canonical files rather than
  committed copies:

  - **mini taxonomy** (xbrlsite.azurewebsites.net) → local/taxonomies/mini/
  - **GeneralJournal.csv** (github.com/seattlemethod/prototypes) →
    local/datasets/seattle_method/
  - **Record-to-Report instance package** (xbrlsite.com/seattlemethod/
    platinum-testcases/record-to-report/) → local/datasets/seattle_method/report/
    — the XBRL ``instance.xml`` Charlie publishes is the reconciliation
    reference (strictly stronger than the earlier hand-derived CSV)

  All destinations are gitignored under ``local/`` and idempotent
  (re-runs overwrite). Aborts on any fetch failure.
  """
  print("─" * 70)
  print("Step 1 — pull mini taxonomy + GeneralJournal.csv + instance.xml from Charlie")
  print("─" * 70)
  for label, script in (
    ("mini taxonomy", PULL_TAXONOMY_SCRIPT),
    ("GeneralJournal.csv", PULL_JOURNAL_SCRIPT),
    ("Record-to-Report instance.xml", PULL_REPORT_SCRIPT),
  ):
    print(f"  → {label}")
    result = subprocess.run(["bash", str(script)], cwd=str(REPO_ROOT), check=False)
    if result.returncode != 0:
      raise SystemExit(f"{script.name} exited with code {result.returncode}")


def step_provision_graph() -> str:
  """Step 2 — Create a dedicated test graph for this run.

  Returns the new graph_id. Reuses the credentials flow from
  ``examples.roboledger_demo.main.create_demo_graph`` — same
  ``CredentialContext`` / ``ensure_user_credentials`` /
  ``save_graph_id`` plumbing — but stores under its own slot
  (``seattle_method_test``) so it doesn't collide with the
  RoboLedger demo's graph. Idempotent: re-runs reuse the same graph
  unless that slot is cleared in ``.local/config.json``.
  """
  print("─" * 70)
  print("Step 2 — provision test graph")
  print("─" * 70)

  # Re-use roboledger_demo's credential helpers — generic enough to
  # handle any per-demo slot.
  project_root = REPO_ROOT
  if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

  from examples._common.config import get_graph_id, save_graph_id
  from examples.credentials.utils import (
    CredentialContext,
    ensure_user_credentials,
  )

  context = CredentialContext(
    base_url=BASE_URL,
    credentials_path=CREDENTIALS_FILE,
    force=False,
    default_name_prefix="Seattle Method Demo",
    default_email_prefix="seattle_method_demo",
    api_key_prefix="Seattle Method Demo Key",
    display_title="Seattle Method Cross-Taxonomy Demo Setup",
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
    base_url=BASE_URL,
    token=api_key,
    prefix="",
    auth_header_name="X-API-Key",
  )

  metadata = GraphMetadata(
    graph_name=GRAPH_DISPLAY_NAME,
    description=(
      "Cross-taxonomy projection test — Charlie Hoffman's mini "
      "reporting framework loaded as a CoA, his 14-JE lemonade-stand "
      "dataset ingested, rollforward IBs authored. See "
      "examples/seattle_method_demo/README.md."
    ),
    schema_extensions=["roboledger"],
  )
  request = CreateGraphRequest(
    metadata=metadata,
    initial_entity={
      "name": ENTITY_NAME,
      "uri": "https://luca.pacioli.ai/luca/view/0f24fd35e961e167a727b663c75a4c5ec9fb7eb86730d6292f46e6e180fc2018980cd52e/index",
      "entity_type": "corporation",
    },
    tags=["demo", "seattle-method", "cross-taxonomy-test", "mini"],
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
    print(f"  Queued (operation: {operation_id}), waiting...")
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
  """Step 3 — Load mini.xsd + linkbases as a CoA TaxonomyBlock."""
  print("─" * 70)
  print(f"Step 3 — load mini taxonomy → graph {graph_id}")
  print("─" * 70)
  if not TAXONOMY_DIR.exists():
    raise SystemExit(
      f"{TAXONOMY_DIR} missing — run step 'pull' first or re-run end-to-end."
    )
  parsed = load_mod.parse_all(TAXONOMY_DIR)
  payloads = load_mod.build_element_payloads(parsed)
  print(
    f"  Parsed {len(parsed.concepts)} concepts, built {len(payloads)} element payloads"
  )
  if dry_run:
    print("  (dry-run — no upload)")
    return
  taxonomy_id = load_mod.upload_taxonomy(graph_id, payloads)
  print(f"  ✓ Created taxonomy {taxonomy_id}")


def step_seed_mappings(graph_id: str, dry_run: bool = False) -> None:
  """Step 4 — Seed mini→rs-gaap derivation associations."""
  print("─" * 70)
  print(f"Step 4 — seed mini→rs-gaap mappings → graph {graph_id}")
  print("─" * 70)
  created, warnings = seed_mod.seed_mappings(graph_id, dry_run=dry_run)
  for w in warnings:
    print(f"  ⚠️  {w}")
  action = "Would create" if dry_run else "Created"
  print(f"  {action} {created} association(s)")


def step_ingest_transactions(graph_id: str, dry_run: bool = False) -> None:
  """Step 5 — Ingest the 14 JEs via create-event-block."""
  print("─" * 70)
  print(f"Step 5 — ingest 14 JEs → graph {graph_id}")
  print("─" * 70)
  created, warnings = ingest_mod.ingest(graph_id, CSV_PATH, dry_run=dry_run)
  for w in warnings:
    print(f"  ⚠️  {w}")
  action = "Would create" if dry_run else "Created"
  print(f"  {action} {created} event(s)")


def step_author_rollforwards(graph_id: str, dry_run: bool = False) -> None:
  """Step 6 — Author 8 rollforward IBs (one per BS leaf with activity)."""
  print("─" * 70)
  print(f"Step 6 — author rollforward IBs → graph {graph_id}")
  print("─" * 70)
  created, warnings = auth_rf_mod.author_rollforwards(
    graph_id, CSV_PATH, dry_run=dry_run
  )
  for w in warnings:
    print(f"  ⚠️  {w}")
  action = "Would create" if dry_run else "Created"
  print(f"  {action} {created} rollforward IB(s)")


def step_reconcile(graph_id: str, dry_run: bool = False) -> None:
  """Step 7 — Reconcile against Charlie Hoffman's published mini facts.

  Produces ``output/seattle-method-case-1.md`` — the source-vocabulary
  cross-check (17/17 exact match against `luca.pacioli.ai`). Subprocess
  invocation to keep reconcile.py's argparse + GraphQL plumbing isolated.
  """
  print("─" * 70)
  print(f"Step 7 — reconcile mini facts → graph {graph_id}")
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
      "examples.seattle_method_demo.reconcile",
      graph_id,
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"reconcile exited with code {result.returncode}")


def step_download_bundles(graph_id: str, dry_run: bool = False) -> None:
  """Step 9 — Render the latest filed Report's aligned artifact set.

  Delegates to the shared ``render_report_artifacts`` (via
  ``download_bundles.py``): pulls the flat JSON-LD, the native holon
  (``.holon.jsonld`` — the viewer's input), and the XBRL 2.1 zip from the
  report endpoint, then validates them container-free (SHACL over the
  JSON-LD, Arelle over the XBRL 2.1) and writes the DataBook with the verdicts
  inlined. Pairs with ``reconcile.py``'s value-parity check (Charlie's data →
  our DB → export → SHACL + Arelle say it's well-formed). Subprocess
  invocation for the same isolation reason as the other download/render steps.
  """
  print("─" * 70)
  print(
    f"Step 9 — Render bundle artifacts (download + validate + DataBook) → "
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
      "examples.seattle_method_demo.download_bundles",
      graph_id,
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"download_bundles exited with code {result.returncode}")


def step_create_report(graph_id: str, dry_run: bool = False) -> None:
  """Step 8 — Materialize the 4-IB rs-gaap Report + render markdown.

  Produces ``output/seattle-method-case-1-four-statements.md`` — the
  rs-gaap projection of the same 14 JEs, materialized through the
  Report architecture (create-report + reportPackage / statement
  GraphQL queries). Subprocess invocation for the same isolation reason
  as step_reconcile.
  """
  print("─" * 70)
  print(f"Step 8 — create-report 4-IB rs-gaap Report → graph {graph_id}")
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
      "examples.seattle_method_demo.create_report",
      graph_id,
    ],
    cwd=str(REPO_ROOT),
    check=False,
  )
  if result.returncode != 0:
    raise SystemExit(f"create_report exited with code {result.returncode}")


# ── Step registry ────────────────────────────────────────────────────────

STEPS = {
  "pull": ("Pull mini taxonomy artifacts", step_pull),
  "provision": ("Provision a dedicated test graph", step_provision_graph),
  "load": ("Load mini as a CoA TaxonomyBlock", step_load_taxonomy),
  "seed-mappings": ("Seed mini→rs-gaap derivation associations", step_seed_mappings),
  "ingest": ("Ingest the 14 JEs as Events", step_ingest_transactions),
  "author-rollforwards": (
    "Author rollforward IBs for every BS leaf with activity",
    step_author_rollforwards,
  ),
  "reconcile": (
    "Reconcile against Charlie Hoffman's published mini facts",
    step_reconcile,
  ),
  "create-report": (
    "Materialize the 4-IB rs-gaap Report + render markdown",
    step_create_report,
  ),
  "download-bundles": (
    "Render the aligned artifact set (download + validate + DataBook)",
    step_download_bundles,
  ),
}


# ── Main ─────────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Seattle Method Cross-Taxonomy Demo orchestrator.",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="Steps:\n" + "\n".join(f"  {k:<22} {v[0]}" for k, v in STEPS.items()),
  )
  parser.add_argument(
    "--graph",
    metavar="GRAPH_ID",
    help="Run against an existing graph instead of provisioning a new one.",
  )
  parser.add_argument(
    "--step",
    choices=list(STEPS.keys()),
    help="Run a single step only.",
  )
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Validate + report; do not write to the API.",
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
      fn(args.graph, dry_run=args.dry_run)
    return

  # Full end-to-end run.
  print("Seattle Method Cross-Taxonomy Demo — full run")
  print()

  # Step 1 — pull
  step_pull()
  print()

  # Step 2 — provision (or reuse)
  if args.graph:
    graph_id = args.graph
    print(f"Step 2 — using existing graph {graph_id}")
    print()
  else:
    graph_id = step_provision_graph()
    print()

  # Step 3-8 — sequential
  step_load_taxonomy(graph_id, dry_run=args.dry_run)
  print()
  step_seed_mappings(graph_id, dry_run=args.dry_run)
  print()
  step_ingest_transactions(graph_id, dry_run=args.dry_run)
  print()
  step_author_rollforwards(graph_id, dry_run=args.dry_run)
  print()
  step_reconcile(graph_id, dry_run=args.dry_run)
  print()
  step_create_report(graph_id, dry_run=args.dry_run)
  print()
  step_download_bundles(graph_id, dry_run=args.dry_run)
  print()

  print("─" * 70)
  print(f"✓ End-to-end demo run complete against graph {graph_id}")
  print("─" * 70)
  print()
  print("Artifacts in examples/seattle_method_demo/output/:")
  print("  - seattle-method-case-1.md                   (mini reconciliation)")
  print("  - seattle-method-case-1-four-statements.md   (rs-gaap 4-statement Report)")
  print("  - seattle-method-case-1.jsonld               (flat JSON-LD bundle)")
  print("  - seattle-method-case-1.holon.jsonld         (native holon — viewer input)")
  print("  - seattle-method-case-1.zip                  (XBRL 2.1 report package)")
  print("  - seattle-method-case-1-shacl-validation.md  (SHACL conformance)")
  print("  - seattle-method-case-1-xbrl-validation.md   (Arelle conformance)")
  print("  - seattle-method-case-1.databook.md          (DataBook)")


if __name__ == "__main__":
  main()
