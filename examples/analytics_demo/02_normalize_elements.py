#!/usr/bin/env python3
"""
Stage 2: Element Normalization

Standalone script that clusters equivalent XBRL elements using
graph embedding and community detection algorithms.

Usage:
    uv run examples/analytics_demo/02_normalize_elements.py sec
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console

from examples.analytics_demo.main import STAGING_DIR, run_normalization
from robosystems.adapters.sec.analytics import ArcExtractor

console = Console()


def main() -> None:
  parser = argparse.ArgumentParser(description="Element Normalization Demo")
  parser.add_argument(
    "graph_id",
    help="Graph ID (resolves to ./data/staging/<graph_id>.duckdb)",
  )
  args = parser.parse_args()

  db_path = STAGING_DIR / f"{args.graph_id}.duckdb"
  if not db_path.exists():
    console.print(f"[red]Database not found: {db_path}[/red]")
    sys.exit(1)

  extractor = ArcExtractor(db_path)
  run_normalization(extractor)


if __name__ == "__main__":
  main()
