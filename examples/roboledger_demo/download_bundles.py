#!/usr/bin/env python3
"""Download the latest Report's bundle artifacts for the RoboLedger demo.

Pulls both serialization flavors via the published Python SDK
(``LedgerClient.download_report_bundle``) and writes them to the demo's
``output/`` folder:

- ``roboledger-demo.jsonld`` — the canonical JSON-LD bundle (the
  artifact stamped on publish in S3 at ``g{generation}.jsonld``)
- ``roboledger-demo.zip`` — the XBRL 2.1 report package (flat zip
  containing ``instance.xml`` + ``report.xsd`` + the three linkbases),
  rebuilt on-demand by the backend at download time

Same shape as the Seattle Method ``download_bundles.py`` scripts; the
three are intentionally parallel so each demo is self-contained.

Importable as ``download_bundles_for_report(client, graph_id, report_id,
out_dir)`` so ``main.py`` can reuse it in-process at the end of
``generate_fy2025_report`` without a subprocess hop.

Usage:
    uv run python -m examples.roboledger_demo.download_bundles <graph_id>
    uv run python -m examples.roboledger_demo.download_bundles  # uses cached
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from robosystems_client.clients import LedgerClient

from examples._common.config import require_cached_graph_id, require_config
from examples._common.sdk import latest_report_id, make_ledger_client

DEMO_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = DEMO_ROOT / "output"
JSONLD_PATH = OUTPUT_DIR / "roboledger-demo.jsonld"
XBRL_PATH = OUTPUT_DIR / "roboledger-demo.zip"

GRAPH_SLOT = "roboledger_demo"
DEMO_RECIPE = "just demo-roboledger"


def download_bundles_for_report(
  client: LedgerClient,
  graph_id: str,
  report_id: str,
  out_dir: Path = OUTPUT_DIR,
) -> None:
  """Download both bundle flavors for a known ``(graph_id, report_id)``.

  Used by ``main.py`` at the tail of ``generate_fy2025_report`` where the
  ids are already in scope. Errors propagate — failed downloads are
  loud, not swallowed.
  """
  out_dir.mkdir(parents=True, exist_ok=True)
  jsonld = client.download_report_bundle(
    graph_id, report_id, format="jsonld", to=out_dir / JSONLD_PATH.name
  )
  xbrl = client.download_report_bundle(
    graph_id, report_id, format="xbrl-2.1", to=out_dir / XBRL_PATH.name
  )
  print(f"  JSON-LD:      {jsonld.path} ({len(jsonld.content):,} bytes)")
  print(f"  XBRL 2.1:     {xbrl.path} ({len(xbrl.content):,} bytes)")

  # Validate the just-received artifacts on the host — container-free:
  # JSON-LD → SHACL (ontology conformance), XBRL zip → Arelle (XBRL 2.1).
  from examples._common.databook import write_databook
  from examples._common.validate import validate_arelle, validate_shacl

  shacl_md = out_dir / "roboledger-demo-shacl-validation.md"
  xbrl_md = out_dir / "roboledger-demo-xbrl-validation.md"
  conforms = validate_shacl(out_dir / JSONLD_PATH.name, shacl_md, "RoboLedger")
  valid = validate_arelle(out_dir / XBRL_PATH.name, xbrl_md, "RoboLedger")
  print(f"  SHACL:        {shacl_md} ({'conforms' if conforms else 'VIOLATIONS'})")
  print(f"  Arelle:       {xbrl_md} ({'valid' if valid else 'INVALID'})")

  # DataBook: one self-describing markdown file — the report as a collection of
  # Information Blocks (each a table + an addressable turtle slice), with the
  # SHACL/Arelle verdicts inlined as evidence (Charlie Hoffman's serialization).
  databook_md = out_dir / "roboledger-demo.databook.md"
  write_databook(
    out_dir / JSONLD_PATH.name,
    databook_md,
    "RoboLedger Demo",
    shacl_md=shacl_md,
    xbrl_md=xbrl_md,
  )
  print(f"  DataBook:     {databook_md}")


def main() -> None:
  parser = argparse.ArgumentParser(
    description=(
      "Download the JSON-LD + XBRL bundle artifacts for the RoboLedger "
      "demo's latest Report."
    )
  )
  parser.add_argument(
    "graph_id",
    nargs="?",
    default=None,
    help=f"Target graph id. Defaults to the cached {GRAPH_SLOT} entry.",
  )
  parser.add_argument(
    "--report-id",
    default=None,
    help="Report id to download. Defaults to the most recent Report.",
  )
  args = parser.parse_args()

  cfg = require_config()
  graph_id = args.graph_id or require_cached_graph_id(cfg, GRAPH_SLOT, DEMO_RECIPE)
  client = make_ledger_client(cfg)
  report_id = args.report_id or latest_report_id(client, graph_id)

  print(f"Downloading bundle artifacts for graph={graph_id}, report={report_id}")
  download_bundles_for_report(client, graph_id, report_id)


if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
