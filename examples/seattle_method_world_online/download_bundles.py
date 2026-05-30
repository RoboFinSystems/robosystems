#!/usr/bin/env python3
"""Download the latest filed Report's bundle artifacts — Step 10 of
the World Online demo.

Pulls both serialization flavors via the published Python SDK
(``LedgerClient.download_report_bundle``) and writes them to the
demo's ``output/`` folder:

- ``world-online.jsonld`` — the canonical JSON-LD bundle (stamped on
  publish in S3 at ``g{generation}.jsonld``)
- ``world-online.zip`` — the XBRL 2.1 report package (flat zip with
  ``instance.xml`` + ``report.xsd`` + the three linkbases), rebuilt
  on-demand by the backend at download time

Same shape as ``examples/seattle_method_demo/download_bundles.py``;
the two are intentionally parallel so each demo is self-contained.

Usage:
    uv run python -m examples.seattle_method_world_online.download_bundles <graph_id>
    uv run python -m examples.seattle_method_world_online.download_bundles  # uses cached
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from examples._common.config import REPO_ROOT, require_cached_graph_id, require_config
from examples._common.sdk import latest_report_id, make_ledger_client

DEMO_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = DEMO_ROOT / "output"
JSONLD_PATH = OUTPUT_DIR / "world-online.jsonld"
XBRL_PATH = OUTPUT_DIR / "world-online.zip"

GRAPH_SLOT = "world_online_test"
DEMO_RECIPE = "just demo-world-online"


def main() -> None:
  parser = argparse.ArgumentParser(
    description=(
      "Download the JSON-LD + XBRL bundle artifacts for the World "
      "Online demo's latest filed Report."
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
