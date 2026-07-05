#!/usr/bin/env python3
"""Download the latest Report's aligned artifact set for the RoboLedger demo.

Thin wrapper over the shared ``render_report_artifacts``
(``examples/_common/artifacts.py``) — the single definition of what a report
demo emits: the flat JSON-LD bundle, the native holon (``.holon.jsonld``), the
XBRL 2.1 zip, the SHACL + Arelle verdicts, and the DataBook. All flavors are
pulled from the report endpoint via the published SDK.

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

from examples._common.artifacts import render_report_artifacts
from examples._common.config import require_cached_graph_id, require_config
from examples._common.sdk import latest_report_id, make_ledger_client

DEMO_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = DEMO_ROOT / "output"
STEM = "roboledger-demo"
LABEL = "RoboLedger"

GRAPH_SLOT = "roboledger_demo"
DEMO_RECIPE = "just demo-roboledger"


def download_bundles_for_report(
  client: LedgerClient,
  graph_id: str,
  report_id: str,
  out_dir: Path = OUTPUT_DIR,
) -> None:
  """Render the aligned artifact set for a known ``(graph_id, report_id)``.

  Used by ``main.py`` at the tail of ``generate_fy2025_report`` where the ids
  are already in scope.
  """
  render_report_artifacts(client, graph_id, report_id, out_dir, STEM, LABEL)


def main() -> None:
  parser = argparse.ArgumentParser(
    description=(
      "Download the aligned bundle artifact set for the RoboLedger demo's "
      "latest Report."
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
