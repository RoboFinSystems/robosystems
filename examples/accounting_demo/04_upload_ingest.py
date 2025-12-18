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
import time
from pathlib import Path

from robosystems_client.extensions import (
  RoboSystemsExtensions,
  RoboSystemsExtensionConfig,
  MaterializationOptions,
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
      result = extensions.files.upload(
        graph_id, table_name, str(file_path)
      )

      if result.success:
        print(
          f"   ✅ Uploaded {file_path.name} ({result.file_size:,} bytes, {result.row_count} rows)"
        )
        success_count += 1
      else:
        print(f"   ❌ Upload failed: {result.error}")

    except Exception as e:
      print(f"   ❌ Upload failed: {e}")

  return success_count


def wait_for_staging(extensions, graph_id, timeout_seconds=120):
  """Wait for all uploaded files to be staged in DuckDB."""
  print(f"\n{'=' * 70}")
  print("⏳ Waiting for DuckDB Staging to Complete")
  print("=" * 70)

  start_time = time.time()
  poll_interval = 2

  while True:
    elapsed = time.time() - start_time
    if elapsed > timeout_seconds:
      print(f"\n⚠️  Staging timeout after {timeout_seconds}s - some files may not be staged")
      return False

    # Get all files for the graph
    files = extensions.files.list(graph_id)
    if not files:
      print("   No files found")
      return True

    # Check status of each file
    pending_count = 0
    staged_count = 0
    failed_count = 0

    for f in files:
      # Get detailed file info with layers
      file_info = extensions.files.get(graph_id, f.file_id)
      if file_info and file_info.layers:
        # layers is a Pydantic model with duckdb attribute
        duckdb_layer = getattr(file_info.layers, "duckdb", None)
        if duckdb_layer:
          duckdb_status = getattr(duckdb_layer, "status", "pending")
        else:
          duckdb_status = "pending"
        if duckdb_status == "staged":
          staged_count += 1
        elif duckdb_status == "failed":
          failed_count += 1
        else:
          pending_count += 1
      else:
        pending_count += 1

    total = staged_count + pending_count + failed_count
    print(f"   Staging progress: {staged_count}/{total} staged, {pending_count} pending, {failed_count} failed ({elapsed:.0f}s)")

    if pending_count == 0:
      if failed_count > 0:
        print(f"\n⚠️  {failed_count} files failed to stage")
      else:
        print(f"\n✅ All {staged_count} files staged successfully!")
      return failed_count == 0

    time.sleep(poll_interval)


def materialize_graph(extensions, graph_id):
  """Materialize uploaded data into graph."""
  print(f"\n{'=' * 70}")
  print("🔄 Materializing Graph from DuckDB")
  print("=" * 70)
  print("\n⚠️  This may take a few moments...")

  try:
    result = extensions.materialization.materialize(
      graph_id, MaterializationOptions(ignore_errors=True, rebuild=False)
    )

    if result.success:
      print("\n✅ Materialization Complete!")
      print(f"   {result.message}")
      print(f"   Tables: {len(result.tables_materialized)}")
      print(f"   Total rows: {result.total_rows:,}")
      print(f"   Execution time: {result.execution_time_ms:.2f}ms")
    else:
      print(f"\n❌ Materialization failed: {result.error}")
      sys.exit(1)

  except Exception as e:
    print(f"\n❌ Materialization failed: {e}")
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
      s3_endpoint_url="http://localhost:4566",  # LocalStack S3 endpoint
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

      # Wait for DuckDB staging to complete before materializing
      # Note: With Dagster, staging is async and can be slow (each file = 1 job)
      staging_ok = wait_for_staging(extensions, graph_id, timeout_seconds=600)
      if not staging_ok:
        print("\n⚠️  Continuing with materialization despite staging issues...")

      materialize_graph(extensions, graph_id)
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
