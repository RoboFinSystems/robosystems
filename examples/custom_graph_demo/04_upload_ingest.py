#!/usr/bin/env python3
"""
Custom Graph Demo - Upload & Materialize

Uploads the generated parquet files for the generic custom graph demo and
materializes them into the selected graph.

Workflow:
  1. Upload node tables (Company, Project, Person)
  2. Upload relationship tables (PERSON_WORKS_FOR_COMPANY, PERSON_WORKS_ON_PROJECT, COMPANY_SPONSORS_PROJECT)
  3. Trigger materialization
  4. Run quick verification queries
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable

from robosystems_client.extensions import (
  RoboSystemsExtensions,
  RoboSystemsExtensionConfig,
  MaterializationOptions,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id

DEMO_DIR = Path(__file__).parent
DEFAULT_CREDENTIALS_FILE = Path(__file__).resolve().parents[1] / "credentials" / "config.json"
DATA_DIR = DEMO_DIR / "data"
NODES_DIR = DATA_DIR / "nodes"
RELATIONSHIPS_DIR = DATA_DIR / "relationships"
DEMO_NAME = "custom_graph_demo"

EXPECTED_NODE_TABLES = ["Company", "Project", "Person"]
EXPECTED_REL_TABLES = [
  "PERSON_WORKS_FOR_COMPANY",
  "PERSON_WORKS_ON_PROJECT",
  "COMPANY_SPONSORS_PROJECT",
]


def load_credentials(credentials_path: Path) -> dict:
  """Load saved credentials for the demo user."""
  if not credentials_path.exists():
    print(f"\n❌ No credentials found at {credentials_path}")
    print("   Run: uv run 01_setup_credentials.py first")
    sys.exit(1)

  with credentials_path.open() as fh:
    return json.load(fh)


def _list_parquet_files(directory: Path, expected: Iterable[str]) -> list[Path]:
  """Return parquet files in a directory, verifying expected tables exist."""
  files = sorted(directory.glob("*.parquet"))
  present = {file.stem for file in files}
  missing = [name for name in expected if name not in present]

  if missing:
    raise FileNotFoundError(
      f"Missing expected tables in {directory}: {', '.join(sorted(missing))}"
    )

  return files


def upload_tables(
  extensions: RoboSystemsExtensions,
  graph_id: str,
  files: list[Path],
  phase_name: str,
) -> None:
  """Upload a list of parquet files to the given graph."""
  print(f"\n{'=' * 70}")
  print(f"📤 {phase_name}")
  print("=" * 70)

  for file_path in files:
    table_name = file_path.stem
    file_size = file_path.stat().st_size
    print(f"\n📋 Uploading table: {table_name}")
    print(f"   File: {file_path.name}")
    print(f"   Size: {file_size:,} bytes")

    try:
      result = extensions.files.upload(
        graph_id,
        table_name,
        str(file_path),
      )
      if result.success:
        print(
          f"   ✅ Uploaded {file_path.name} "
          f"({result.file_size:,} bytes, {result.row_count} rows)"
        )
      else:
        print(f"   ❌ Upload failed: {result.error}")
        raise RuntimeError(f"Upload failed: {result.error}")
    except Exception as exc:  # noqa: BLE001
      print(f"   ❌ Upload failed: {exc}")
      raise


def materialize_graph_data(extensions: RoboSystemsExtensions, graph_id: str) -> None:
  """Materialize all uploaded tables into the graph."""
  print(f"\n{'=' * 70}")
  print("🔄 Materializing graph from DuckDB")
  print("=" * 70)

  try:
    result = extensions.materialization.materialize(
      graph_id,
      MaterializationOptions(ignore_errors=True, rebuild=False),
    )
    if result.success:
      print(f"\n✅ Materialization complete: {result.message}")
      print(f"   Tables: {len(result.tables_materialized)}")
      print(f"   Total rows: {result.total_rows:,}")
      print(f"   Execution time: {result.execution_time_ms:.2f}ms")
    else:
      print(f"\n❌ Materialization failed: {result.error}")
      raise RuntimeError(f"Materialization failed: {result.error}")
  except Exception as exc:  # noqa: BLE001
    print(f"\n❌ Materialization failed: {exc}")
    raise


def run_post_ingest_checks(extensions: RoboSystemsExtensions, graph_id: str) -> None:
  """Run a small set of sanity-check queries after materialization."""
  print(f"\n{'=' * 70}")
  print("🔍 Verification queries")
  print("=" * 70)

  print("\n📊 Node counts")
  for label in EXPECTED_NODE_TABLES:
    query = f"MATCH (n:{label}) RETURN count(n) AS count"
    try:
      result = extensions.query.query(graph_id, query)
      count = 0
      if getattr(result, "data", None):
        first_row = result.data[0]
        count = first_row.get("count") or next(iter(first_row.values()), 0)
      print(f"   • {{'label': '{label}', 'count': {count}}}")
    except Exception as exc:  # noqa: BLE001
      print(f"   ❌ Query failed for label '{label}': {exc}")

  print("\n📊 Relationship counts")
  for rel in EXPECTED_REL_TABLES:
    query = f"MATCH ()-[r:{rel}]->() RETURN count(r) AS count"
    try:
      result = extensions.query.query(graph_id, query)
      count = 0
      if getattr(result, "data", None):
        first_row = result.data[0]
        count = first_row.get("count") or next(iter(first_row.values()), 0)
      print(f"   • {{'type': '{rel}', 'count': {count}}}")
    except Exception as exc:  # noqa: BLE001
      print(f"   ❌ Query failed for relationship '{rel}': {exc}")


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Upload and ingest data for the custom generic graph demo"
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument(
    "--credentials-file",
    default=str(DEFAULT_CREDENTIALS_FILE),
    help="Path to credentials file (default: credentials/config.json)",
  )
  args = parser.parse_args()
  credentials_path = Path(args.credentials_file).expanduser()

  try:
    credentials = load_credentials(credentials_path)
    api_key = credentials.get("api_key")
    graph_id = get_graph_id(credentials_path, DEMO_NAME)

    if not api_key or not graph_id:
      print("\n❌ Missing API key or graph_id in credentials")
      print("   Run steps 01 and 02 first")
      sys.exit(1)

    if not NODES_DIR.exists() or not RELATIONSHIPS_DIR.exists():
      print("\n❌ Generated data not found.")
      print("   Run: uv run 03_generate_data.py")
      sys.exit(1)
  except Exception as exc:  # noqa: BLE001
    print(f"\n❌ Setup failed: {exc}")
    sys.exit(1)

  node_files = _list_parquet_files(NODES_DIR, EXPECTED_NODE_TABLES)
  relationship_files = _list_parquet_files(RELATIONSHIPS_DIR, EXPECTED_REL_TABLES)

  print("\n" + "=" * 70)
  print("📊 Custom Graph Demo - Upload & Ingest")
  print("=" * 70)
  print(f"Graph ID: {graph_id}")
  print(f"Base URL: {args.base_url}")
  print(f"Credentials: {credentials_path}")
  print("=" * 70)

  config = RoboSystemsExtensionConfig(
    base_url=args.base_url,
    headers={"X-API-Key": api_key},
    s3_endpoint_url="http://localhost:4566",  # LocalStack S3 endpoint
  )
  extensions = RoboSystemsExtensions(config)

  try:
    upload_tables(extensions, graph_id, node_files, "Phase 1 - Node Tables")
    upload_tables(
      extensions, graph_id, relationship_files, "Phase 2 - Relationship Tables"
    )
    materialize_graph_data(extensions, graph_id)
    run_post_ingest_checks(extensions, graph_id)
  except Exception:
    print("\n❌ Upload & ingest process failed.")
    sys.exit(1)

  print("\n✅ Upload & ingest complete!")


if __name__ == "__main__":
  main()
