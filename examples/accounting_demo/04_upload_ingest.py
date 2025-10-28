#!/usr/bin/env python3
"""
Accounting Demo Upload & Ingest

This script uploads and ingests accounting data:
1. Upload ALL parquet files (nodes + relationships)
2. Ingest everything ONCE

Usage:
    uv run 04_upload_ingest.py
"""

import argparse
import json
import sys
from pathlib import Path

from robosystems_client.extensions import (
  RoboSystemsExtensions,
  RoboSystemsExtensionConfig,
  UploadOptions,
  IngestOptions,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id

CREDENTIALS_FILE = Path(__file__).resolve().parents[1] / "credentials" / "config.json"
DATA_DIR = Path(__file__).parent / "data"
NODES_DIR = DATA_DIR / "nodes"
RELATIONSHIPS_DIR = DATA_DIR / "relationships"
DEMO_NAME = "accounting_demo"


def load_credentials():
  """Load saved credentials."""
  if not CREDENTIALS_FILE.exists():
    print(f"\n❌ No credentials found at {CREDENTIALS_FILE}")
    print("   Run: uv run 01_setup_credentials.py first")
    sys.exit(1)

  with open(CREDENTIALS_FILE) as f:
    return json.load(f)


def upload_directory(
  extensions,
  graph_id,
  directory,
  phase_name,
  skip_tables: set[str] | None = None,
):
  """Upload parquet files from a directory."""
  parquet_files = sorted(directory.glob("*.parquet"))

  if not parquet_files:
    print(f"\n⚠️  No parquet files found in {directory}")
    return 0

  print(f"\n{'=' * 70}")
  print(f"📤 {phase_name}: Uploading Data")
  print("=" * 70)

  success_count = 0
  skip_tables = skip_tables or set()

  for file_path in parquet_files:
    table_name = file_path.stem
    file_size = file_path.stat().st_size

    if table_name in skip_tables:
      print(
        f"\n⏭️  Skipping table: {table_name} (already managed during graph creation)"
      )
      continue

    print(f"\n📋 Uploading table: {table_name}")
    print(f"   File: {file_path.name}")
    print(f"   Size: {file_size:,} bytes")

    try:
      result = extensions.tables.upload_parquet_file(
        graph_id, table_name, str(file_path), UploadOptions(fix_localstack_url=True)
      )

      print(
        f"   ✅ Uploaded {file_path.name} ({result.file_size:,} bytes, {result.row_count} rows)"
      )
      success_count += 1

    except Exception as e:
      print(f"   ❌ Upload failed: {e}")

  return success_count


def ingest_data(extensions, graph_id):
  """Ingest uploaded data into graph."""
  print(f"\n{'=' * 70}")
  print("🔄 Ingesting Data into Graph")
  print("=" * 70)
  print("\n⚠️  This may take a few moments...")

  try:
    result = extensions.tables.ingest_all_tables(
      graph_id, IngestOptions(ignore_errors=True, rebuild=False)
    )

    print("\n✅ Ingestion Complete!")
    if hasattr(result, "message"):
      print(f"   {result.message}")

  except Exception as e:
    print(f"\n❌ Ingestion failed: {e}")
    sys.exit(1)


def run_verification_queries(extensions, graph_id):
  """Run simple verification queries."""
  print(f"\n{'=' * 70}")
  print("🔍 Verification Queries")
  print("=" * 70)

  queries = {
    "Total nodes": "MATCH (n) RETURN count(n) AS total_nodes",
    "Total relationships": "MATCH ()-[r]->() RETURN count(r) AS total_rels",
    "Node types": "MATCH (n) RETURN labels(n)[0] AS type, count(n) AS count ORDER BY count DESC",
  }

  for description, query in queries.items():
    print(f"\n📊 {description}:")
    print(f"   {query}")

    try:
      result = extensions.query.query(graph_id, query)
      if hasattr(result, "data") and result.data:
        print("   ✅ Results:")
        for i, record in enumerate(result.data, 1):
          print(f"      {i}. {record}")
      else:
        print("   ⚠️  No results")

    except Exception as e:
      print(f"   ❌ Query failed: {e}")


def main():
  parser = argparse.ArgumentParser(description="Upload and ingest accounting data")
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )

  args = parser.parse_args()

  try:
    credentials = load_credentials()
    api_key = credentials.get("api_key")
    graph_id = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)

    if not api_key or not graph_id:
      print("\n❌ Missing API key or graph_id in credentials")
      print(
        "   Run: uv run 01_setup_credentials.py and uv run 02_create_graph.py first"
      )
      sys.exit(1)

    print("\n" + "=" * 70)
    print("📊 Accounting Demo - Upload & Ingest")
    print("=" * 70)
    print(f"Graph ID: {graph_id}")

    config = RoboSystemsExtensionConfig(
      base_url=args.base_url,
      headers={"X-API-Key": api_key},
    )
    extensions = RoboSystemsExtensions(config)

    nodes_uploaded = upload_directory(
      extensions,
      graph_id,
      NODES_DIR,
      "Phase 1 - Nodes",
    )
    rels_uploaded = upload_directory(
      extensions,
      graph_id,
      RELATIONSHIPS_DIR,
      "Phase 2 - Relationships",
    )

    total_uploaded = nodes_uploaded + rels_uploaded

    if total_uploaded > 0:
      print(f"\n{'=' * 70}")
      print(
        f"✅ Upload Complete: {total_uploaded} files ({nodes_uploaded} nodes, {rels_uploaded} relationships)"
      )
      print("=" * 70)

      ingest_data(extensions, graph_id)
      run_verification_queries(extensions, graph_id)

    print("\n" + "=" * 70)
    print("✅ Upload & Ingest Complete!")
    print("=" * 70)
    print(f"\nGraph ID: {graph_id}")
    print("\n💡 Next step: uv run 05_query_graph.py")
    print("=" * 70 + "\n")

  except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()
