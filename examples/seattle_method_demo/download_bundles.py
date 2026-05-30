#!/usr/bin/env python3
"""Download the latest filed Report's bundle artifacts — Step 9 of the
Seattle Method demo.

Pulls both serialization flavors via the published Python SDK
(``LedgerClient.download_report_bundle``) and writes them to the demo's
``output/`` folder:

- ``seattle-method-case-1.jsonld`` — the canonical JSON-LD bundle (the
  artifact stamped on publish in S3 at ``g{generation}.jsonld``)
- ``seattle-method-case-1.zip`` — the XBRL 2.1 report package (flat zip
  containing ``instance.xml`` + ``report.xsd`` + the three linkbases),
  rebuilt on-demand by the backend at download time

The two files are the *same Report content* projected into different
formats by the two encoder families — JSON-LD for modern programmatic
consumers, XBRL 2.1 for filing-grade interop. Charlie Hoffman's
Seattle Method test grades on the XBRL emit; JSON-LD is the headline
modern format.

Pairs with the `validate` step (which checks the downloaded artifacts through
Arelle) — that step proves the **shape** is valid; this step writes the
artifacts to disk so a reviewer can inspect them directly.

Usage:
    uv run python -m examples.seattle_method_demo.download_bundles <graph_id>
    uv run python -m examples.seattle_method_demo.download_bundles  # uses cached
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from examples._common.config import REPO_ROOT, require_cached_graph_id, require_config
from examples._common.sdk import latest_report_id, make_ledger_client

DEMO_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = DEMO_ROOT / "output"
JSONLD_PATH = OUTPUT_DIR / "seattle-method-case-1.jsonld"
XBRL_PATH = OUTPUT_DIR / "seattle-method-case-1.zip"

GRAPH_SLOT = "seattle_method_test"
DEMO_RECIPE = "just demo-seattle-method"


def main() -> None:
  parser = argparse.ArgumentParser(
    description=(
      "Download the JSON-LD + XBRL bundle artifacts for the Seattle "
      "Method demo's latest filed Report."
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
    help="Report id to download. Defaults to the most recent filed Report.",
  )
  args = parser.parse_args()

  cfg = require_config()
  graph_id = args.graph_id or require_cached_graph_id(cfg, GRAPH_SLOT, DEMO_RECIPE)
  client = make_ledger_client(cfg)
  report_id = args.report_id or latest_report_id(client, graph_id)

  print(f"Downloading bundle artifacts for graph={graph_id}, report={report_id}")
  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

  jsonld = client.download_report_bundle(
    graph_id, report_id, format="jsonld", to=JSONLD_PATH
  )
  print(
    f"  JSON-LD: {JSONLD_PATH.relative_to(REPO_ROOT)} ({len(jsonld.content):,} bytes)"
  )

  xbrl = client.download_report_bundle(
    graph_id, report_id, format="xbrl-2.1", to=XBRL_PATH
  )
  print(f"  XBRL 2.1: {XBRL_PATH.relative_to(REPO_ROOT)} ({len(xbrl.content):,} bytes)")


if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
