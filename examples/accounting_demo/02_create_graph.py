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


CREDENTIALS_DIR = Path(__file__).parent / "credentials"
CREDENTIALS_FILE = CREDENTIALS_DIR / "config.json"


def load_credentials():
    """Load saved credentials."""
    if not CREDENTIALS_FILE.exists():
        print(f"\n❌ No credentials found at {CREDENTIALS_FILE}")
        print("   Run: uv run 01_setup_credentials.py first")
        sys.exit(1)

    with open(CREDENTIALS_FILE) as f:
        return json.load(f)


def save_graph_id(credentials: dict, graph_id: str):
    """Save graph_id to credentials file."""
    credentials["graph_id"] = graph_id
    credentials["graph_created_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(CREDENTIALS_FILE, "w") as f:
        json.dump(credentials, f, indent=2)
    print(f"\n💾 Graph ID saved to: {CREDENTIALS_FILE}")


def create_accounting_graph(
    base_url: str,
    api_key: str,
    graph_name: str = None,
    reuse: bool = False,
):
    """Create a new graph for accounting demo."""

    credentials = load_credentials()

    if reuse and credentials.get("graph_id"):
        print("\n✅ Reusing existing graph")
        print(f"   Graph ID: {credentials['graph_id']}")
        print(f"   Created: {credentials.get('graph_created_at', 'unknown')}")
        return credentials["graph_id"]

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
            timeout=60,
            on_progress=lambda msg: print(f"   {msg}")
        )
    except Exception as e:
        print(f"\n❌ Graph creation failed: {e}")
        sys.exit(1)

    save_graph_id(credentials, graph_id)

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

    args = parser.parse_args()

    try:
        credentials = load_credentials()
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
        )
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
