#!/usr/bin/env python3
"""
Create Accounting Graph

This script creates a new graph for the accounting demo. It uses saved
credentials from step 01.

Usage:
    uv run 02_create_graph.py                           # Auto-generate graph name
    uv run 02_create_graph.py --name "Acme Consulting"
    uv run 02_create_graph.py --reuse                   # Reuse existing graph

After running, graph_id is saved to credentials/config.json
"""

import argparse
import json
import sys
import time
from pathlib import Path

from robosystems_client.extensions import (
  RoboSystemsExtensions,
  RoboSystemsExtensionConfig,
  GraphMetadata,
  InitialEntityData,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id, save_graph_id

CREDENTIALS_DIR = Path(__file__).resolve().parents[1] / "credentials"
DEFAULT_CREDENTIALS_FILE = CREDENTIALS_DIR / "config.json"
DEMO_NAME = "element_mapping_demo"


def load_credentials(credentials_path: Path):
  """Load saved credentials."""
  if not credentials_path.exists():
    print(f"\n❌ No credentials found at {credentials_path}")
    print("   Run: uv run 01_setup_credentials.py first")
    sys.exit(1)

  with open(credentials_path) as f:
    return json.load(f)


def save_graph_data(graph_id: str, credentials_path: Path):
  """Save graph_id to credentials file."""
  graph_created_at = time.strftime("%Y-%m-%d %H:%M:%S")
  save_graph_id(credentials_path, DEMO_NAME, graph_id, graph_created_at)
  print(f"\n💾 Graph ID saved to: {credentials_path}")


def create_accounting_graph(
  base_url: str,
  api_key: str,
  graph_name: str = None,
  reuse: bool = False,
  credentials_path: Path = DEFAULT_CREDENTIALS_FILE,
):
  """Create a new graph for accounting demo."""

  credentials = load_credentials(credentials_path)

  existing_graph_id = get_graph_id(credentials_path, DEMO_NAME)
  if reuse and existing_graph_id:
    credentials_data = load_credentials(credentials_path)
    demo_graph_data = credentials_data.get("graphs", {}).get(DEMO_NAME, {})
    print("\n✅ Reusing existing graph")
    print(f"   Graph ID: {existing_graph_id}")
    print(f"   Created: {demo_graph_data.get('graph_created_at', 'unknown')}")
    return existing_graph_id

  timestamp = int(time.time())
  graph_name = graph_name or f"accounting_demo_{timestamp}"

  print("\n" + "=" * 70)
  print("📊 Accounting Demo - Graph Creation")
  print("=" * 70)

  config = RoboSystemsExtensionConfig(
    base_url=base_url,
    headers={"X-API-Key": api_key},
  )
  extensions = RoboSystemsExtensions(config)

  metadata = GraphMetadata(
    graph_name=graph_name,
    description="Accounting demo with chart of accounts and transactions using RoboLedger schema",
    schema_extensions=["roboledger"],
    tags=["accounting", "demo"],
  )

  initial_entity = InitialEntityData(
    name="Acme Consulting LLC",
    uri=f"https://accounting.example.com/{graph_name}",
    category="Professional Services",
    sic="8742",
    sic_description="Management Consulting Services",
  )

  try:
    graph_id = extensions.graphs.create_graph_and_wait(
      metadata=metadata,
      initial_entity=initial_entity,
      create_entity=False,
      timeout=60,
      on_progress=lambda msg: print(f"   {msg}"),
    )
  except Exception as e:
    print(f"\n❌ Graph creation failed: {e}")
    sys.exit(1)

  save_graph_data(graph_id, credentials_path)

  print("\n" + "=" * 70)
  print("✅ Graph Created Successfully!")
  print("=" * 70)
  print(f"\nGraph ID: {graph_id}")
  print(f"Graph Name: {graph_name}")
  print("\n💡 Next steps:")
  print("   1. Generate data: uv run 03_generate_data.py")
  print("   2. Upload & ingest: uv run 04_upload_ingest.py")
  print("   3. Run queries: uv run 05_query_graph.py")
  print("=" * 70 + "\n")

  return graph_id


def main():
  parser = argparse.ArgumentParser(description="Create accounting graph")
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument(
    "--name",
    help="Graph name (auto-generated if not provided)",
  )
  parser.add_argument(
    "--reuse",
    action="store_true",
    help="Reuse existing graph if available",
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

    if not api_key:
      print("\n❌ No API key found in credentials")
      print("   Run: uv run 01_setup_credentials.py first")
      sys.exit(1)

    create_accounting_graph(
      base_url=args.base_url,
      api_key=api_key,
      graph_name=args.name,
      reuse=args.reuse,
      credentials_path=credentials_path,
    )
  except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()
