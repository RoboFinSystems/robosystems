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
formats by the two encoder families described in
``local/docs/ref/serialization.md`` §3 — JSON-LD for modern programmatic
consumers, XBRL 2.1 for filing-grade interop. Charlie Hoffman's
Seattle Method test grades on the XBRL emit; JSON-LD is the headline
modern format.

Pairs with ``xbrl_validate.py`` (which validates the XBRL emit through
Arelle) — that step proves the **shape** is valid; this step writes the
artifacts to disk so a reviewer can inspect them directly.

Usage:
    uv run python -m examples.seattle_method_demo.download_bundles <graph_id>
    uv run python -m examples.seattle_method_demo.download_bundles  # uses cached
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from robosystems_client.clients import LedgerClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_ROOT = REPO_ROOT / "examples" / "seattle_method_demo"
OUTPUT_DIR = DEMO_ROOT / "output"
JSONLD_PATH = OUTPUT_DIR / "seattle-method-case-1.jsonld"
XBRL_PATH = OUTPUT_DIR / "seattle-method-case-1.zip"
EXTENSIONS_DB_URL = os.environ.get("EXTENSIONS_DATABASE_URL")


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
  """Look up the most-recent filed Report id for this graph by querying
  the extensions DB directly. Same boundary ``reconcile.py`` and
  ``xbrl_validate.py`` cross."""
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
    help="Target graph id. Defaults to the cached seattle_method_test entry.",
  )
  parser.add_argument(
    "--report-id",
    default=None,
    help="Report id to download. Defaults to the most recent filed Report.",
  )
  args = parser.parse_args()

  cfg = _load_config()
  base_url = cfg.get("base_url", "http://localhost:8000")
  api_key = cfg["api_key"]
  graph_id = _resolve_graph_id(cfg, args.graph_id)
  report_id = args.report_id or _latest_report_id(graph_id)

  print(f"Downloading bundle artifacts for graph={graph_id}, report={report_id}")

  OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
  client = LedgerClient(
    {"base_url": base_url, "token": api_key, "headers": {}, "timeout": 60}
  )

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
