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
See ``local/docs/ref/serialization.md`` for the design.

Usage:
    uv run python -m examples.seattle_method_world_online.download_bundles <graph_id>
    uv run python -m examples.seattle_method_world_online.download_bundles  # uses cached
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
DEMO_ROOT = REPO_ROOT / "examples" / "seattle_method_world_online"
OUTPUT_DIR = DEMO_ROOT / "output"
JSONLD_PATH = OUTPUT_DIR / "world-online.jsonld"
XBRL_PATH = OUTPUT_DIR / "world-online.zip"
EXTENSIONS_DB_URL = os.environ.get("EXTENSIONS_DATABASE_URL")


def _load_config() -> dict:
  cfg_path = REPO_ROOT / ".local" / "config.json"
  if not cfg_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  return json.loads(cfg_path.read_text())


def _resolve_graph_id(cfg: dict, cli_graph: str | None) -> str:
  if cli_graph:
    return cli_graph
  cached = cfg.get("graphs", {}).get("world_online_test")
  if not cached:
    raise SystemExit(
      "No world_online_test graph cached in .local/config.json. "
      "Pass a graph_id or run `just demo-world-online` first."
    )
  return cached["graph_id"]


def _latest_report_id(graph_id: str) -> str:
  """Look up the most-recent filed Report id for this graph by querying
  the extensions DB directly. Same boundary ``reconcile.py`` crosses."""
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
      "Run `just demo-world-online-create-report` first."
    )
  return str(row["id"])


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
    help="Target graph id. Defaults to the cached world_online_test entry.",
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
