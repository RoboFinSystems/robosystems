#!/usr/bin/env python3
"""XBRL 2.1 validation — Step 9 of the Seattle Method demo.

Downloads the latest filed Report's XBRL zip via the published Python
SDK (``LedgerClient.download_report_bundle``), extracts it, and runs
Arelle's XBRL validator against the ``instance.xml`` entry point.
Arelle is the de-facto XBRL processor — the same library that
validates SEC EDGAR filings, that ingests Charlie Hoffman's reference
data into our pipeline (``robosystems/adapters/sec/client/arelle.py``).
Passing Arelle's validation is the concrete claim that our output is
**valid XBRL 2.1**, parseable by every standards-compliant consumer.

This pairs with ``reconcile.py`` (ingest-side cross-check vs. Charlie's
facts) to close the round-trip loop:

    Charlie's data → our DB        (step 7 reconcile — value parity)
                  → our XBRL → Arelle says valid  (step 9 here — shape parity)

The validator is intentionally not a fact-by-fact diff against
Charlie's reference instance: cross-taxonomy (rs-gaap vs. mini)
concept-name divergence makes most facts incomparable, so a diff would
mostly report taxonomy differences instead of bugs. Arelle's
spec-conformance check is the actually-defensible claim.

Writes markdown to ``output/seattle-method-case-1-xbrl-validation.md``.

Usage:
    uv run python -m examples.seattle_method_demo.xbrl_validate <graph_id>
    uv run python -m examples.seattle_method_demo.xbrl_validate  # uses cached
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

from robosystems_client.clients import LedgerClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPO_ROOT / "examples" / "seattle_method_demo"
DEFAULT_REPORT_PATH = DEMO_ROOT / "output" / "seattle-method-case-1-xbrl-validation.md"
EXTENSIONS_DB_URL = os.environ.get("EXTENSIONS_DATABASE_URL")


# ── Dataclasses ────────────────────────────────────────────────────────


@dataclass
class ValidationOutcome:
  """Result of running Arelle against our XBRL zip."""

  valid: bool
  errors: list[str]
  load_errors: list[str]
  bundle_size_bytes: int
  files_in_zip: list[str]
  instance_facts: int


# ── Config + report lookup (mirrors xbrl_diff.py pre-pivot) ────────────


def _load_config() -> dict:
  cfg_path = REPO_ROOT / ".local" / "config.json"
  if not cfg_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  return json.loads(cfg_path.read_text())


def _resolve_graph_id(cfg: dict, cli_graph: str | None) -> str:
  if cli_graph:
    return cli_graph
  cached = cfg.get("graphs", {}).get("seattle_method_test")
  if not cached:
    raise SystemExit(
      "No seattle_method_test graph cached in .local/config.json. "
      "Pass a graph_id or run `just demo-seattle-method` first."
    )
  return cached["graph_id"]


def _latest_report_id(graph_id: str) -> str:
  """Look up the most-recent Report id for this graph via a direct DB
  query (same boundary ``reconcile.py`` crosses; revisited if/when a
  GraphQL "latest report for graph" surface lands)."""
  if not EXTENSIONS_DB_URL:
    raise SystemExit(
      "EXTENSIONS_DATABASE_URL not set. The justfile sets it from "
      ".env.local; if invoking outside the justfile, export it yourself."
    )
  engine = create_engine(EXTENSIONS_DB_URL)
  SessionLocal = sessionmaker(bind=engine, autoflush=False)
  with SessionLocal() as session:
    row = (
      session.execute(
        text(
          f"SET search_path TO {graph_id}; "
          "SELECT id FROM reports "
          "WHERE bundle_url IS NOT NULL "
          "ORDER BY created_at DESC LIMIT 1"
        )
      )
      .mappings()
      .first()
    )
  if row is None:
    raise SystemExit(
      f"No filed Report with a stamped bundle found for graph {graph_id}. "
      "Run `just demo-seattle-method-create-report` first."
    )
  return str(row["id"])


# ── Arelle validation ──────────────────────────────────────────────────


def _validate_with_arelle(extracted_dir: Path) -> tuple[list[str], list[str], int]:
  """Run Arelle against the extracted ``instance.xml`` and return
  ``(load_errors, validation_errors, fact_count)``.

  Imports are deferred so the script imports quickly when the validator
  isn't the bottleneck (Arelle's package initialisation is heavy). The
  controller loads the instance + every referenced linkbase + the
  ``report.xsd`` schema; load errors surface taxonomy-discovery issues
  (broken schemaRefs, missing linkbase namespaces). The validator then
  runs the XBRL 2.1 conformance checks proper.
  """
  from robosystems.adapters.sec.client.arelle import ArelleClient

  client = ArelleClient()
  instance_path = extracted_dir / "instance.xml"
  model = client.controller(str(instance_path))

  load_errors = [str(e) for e in (model.errors or [])]
  # Snapshot fact count before the validation pass so we can report what
  # Arelle actually saw — useful when the zip parses but the validator
  # finds zero facts (something is off with the schemaRef resolution).
  fact_count = len(model.facts) if hasattr(model, "facts") else 0

  validation_result = client.validate(model)
  validation_errors = [
    e for e in validation_result.get("errors", []) if e not in load_errors
  ]
  return load_errors, validation_errors, fact_count


# ── Markdown output ────────────────────────────────────────────────────


def _render_markdown(
  graph_id: str,
  report_id: str,
  outcome: ValidationOutcome,
) -> str:
  status_line = "✅ **Valid XBRL 2.1**" if outcome.valid else "❌ **Validation failed**"
  lines = [
    "# Seattle Method — XBRL 2.1 validation",
    "",
    f"Graph: `{graph_id}`",
    f"Report: `{report_id}`",
    "",
    f"## Result: {status_line}",
    "",
    "## Summary",
    "",
    f"- **Bundle size**: {outcome.bundle_size_bytes:,} bytes",
    f"- **Files in zip**: {len(outcome.files_in_zip)} (`{', '.join(outcome.files_in_zip)}`)",
    f"- **Facts loaded by Arelle**: {outcome.instance_facts}",
    f"- **Load errors**: {len(outcome.load_errors)}",
    f"- **Validation errors**: {len(outcome.errors)}",
    "",
    "## Notes",
    "",
    (
      "Arelle is the de-facto XBRL processor (also used by SEC EDGAR for "
      + "filing validation). Zero load errors + zero validation errors is "
      + "the structural correctness claim: our output is **valid XBRL 2.1**, "
      + "consumable by any standards-compliant XBRL processor, with no "
      + "vendor-specific shape requirements."
    ),
    "",
    (
      "This complements ``reconcile.py`` (which validates **values** — "
      + "our DB facts match Charlie Hoffman's published reference at the "
      + "value level across the 17 mini concepts that round-trip through "
      + "our mini→rs-gaap mapping). Together they close the loop:"
    ),
    "",
    "- **Values** match Charlie's reference (reconcile.py)",
    "- **Shape** matches the XBRL 2.1 spec (this script)",
    "",
  ]

  if outcome.load_errors:
    lines += ["## Load errors", ""]
    for err in outcome.load_errors:
      lines.append(f"- `{err}`")
    lines.append("")

  if outcome.errors:
    lines += ["## Validation errors", ""]
    for err in outcome.errors:
      lines.append(f"- `{err}`")
    lines.append("")

  if not outcome.load_errors and not outcome.errors:
    lines += [
      "## Errors",
      "",
      (
        "_None._ Arelle reported no load errors and no XBRL 2.1 validation "
        + "errors against the emitted instance + schema + linkbases."
      ),
      "",
    ]

  return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(
    description=("Validate our XBRL emit for the Seattle Method demo via Arelle.")
  )
  parser.add_argument(
    "graph_id",
    nargs="?",
    default=None,
    help="Target graph id. Defaults to the cached seattle_method_test entry.",
  )
  parser.add_argument(
    "--report-id",
    default=None,
    help="Report id to validate. Defaults to the most recent filed Report.",
  )
  parser.add_argument(
    "--out",
    type=Path,
    default=DEFAULT_REPORT_PATH,
    help=f"Output markdown path (default: {DEFAULT_REPORT_PATH.relative_to(REPO_ROOT)}).",
  )
  args = parser.parse_args()

  cfg = _load_config()
  base_url = cfg.get("base_url", "http://localhost:8000")
  api_key = cfg["api_key"]
  graph_id = _resolve_graph_id(cfg, args.graph_id)
  report_id = args.report_id or _latest_report_id(graph_id)

  print(f"Validating XBRL emit for graph={graph_id}, report={report_id}")

  # Pull our XBRL via the published SDK.
  client = LedgerClient(
    {"base_url": base_url, "token": api_key, "headers": {}, "timeout": 60}
  )
  bundle = client.download_report_bundle(graph_id, report_id, format="xbrl-2.1")
  print(f"  Downloaded our XBRL: {len(bundle.content):,} bytes")

  with tempfile.TemporaryDirectory(prefix="xbrl-validate-") as tmp:
    extracted = Path(tmp)
    with zipfile.ZipFile(io.BytesIO(bundle.content)) as zf:
      files_in_zip = sorted(zf.namelist())
      zf.extractall(extracted)
    print(f"  Extracted to: {extracted}")
    print("  Running Arelle validation...")

    load_errors, validation_errors, fact_count = _validate_with_arelle(extracted)

  outcome = ValidationOutcome(
    valid=not (load_errors or validation_errors),
    errors=validation_errors,
    load_errors=load_errors,
    bundle_size_bytes=len(bundle.content),
    files_in_zip=files_in_zip,
    instance_facts=fact_count,
  )

  markdown = _render_markdown(graph_id, report_id, outcome)
  args.out.parent.mkdir(parents=True, exist_ok=True)
  args.out.write_text(markdown)
  print(f"\nWrote {args.out.relative_to(REPO_ROOT)}")

  print()
  print(f"  Facts loaded: {outcome.instance_facts}")
  print(f"  Load errors: {len(outcome.load_errors)}")
  print(f"  Validation errors: {len(outcome.errors)}")
  if outcome.valid:
    print("\n  ✅ Valid XBRL 2.1")
    sys.exit(0)
  else:
    print("\n  ❌ Validation failed — see report for details")
    sys.exit(1)


if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
