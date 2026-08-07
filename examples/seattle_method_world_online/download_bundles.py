#!/usr/bin/env python3
"""Export the latest filed Report in every serialization — step 10 of the
World Online demo.

One Report, four artifacts, pulled from the report endpoint via the published
SDK: the flat JSON-LD bundle, the native holon (``.holon.jsonld``, the holon
viewer's input), the XBRL 2.1 report package, and the DataBook. Each
serialization is then validated container-free — SHACL over the JSON-LD,
Arelle over the XBRL 2.1 — and both verdicts are inlined into the DataBook.

Where ``reconcile.py`` and ``statement_reconcile.py`` check the *numbers*
against Charlie Hoffman's reference, this step proves the exported *shape*.
It must run before ``statement_reconcile.py``, which reads its JSON-LD bundle.

Prerequisites: ``just demo-world-online`` has run against the graph, so a
filed Report exists.

Run it:
    uv run python -m examples.seattle_method_world_online.download_bundles <graph_id>
    uv run python -m examples.seattle_method_world_online.download_bundles  # cached slot

Writes ``output/world-online.{jsonld,holon.jsonld,zip}`` plus the two
validation reports and the DataBook.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from examples._common.artifacts import render_report_artifacts
from examples._common.config import require_cached_graph_id, require_config
from examples._common.sdk import latest_report_id, make_ledger_client

DEMO_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = DEMO_ROOT / "output"
STEM = "world-online"
LABEL = "World Online"

GRAPH_SLOT = "world_online_test"
DEMO_RECIPE = "just demo-world-online"


def main() -> None:
  parser = argparse.ArgumentParser(
    description=(
      "Download the aligned bundle artifact set for the World Online "
      "demo's latest filed Report."
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
  render_report_artifacts(client, graph_id, report_id, OUTPUT_DIR, STEM, LABEL)


if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    sys.exit(130)
