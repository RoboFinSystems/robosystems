#!/usr/bin/env python3
"""
SEC Repository Demo - Setup Script

This script sets up access to the SEC shared repository:
1. Creates or reuses demo user credentials
2. Loads SEC data for a specific ticker and year
3. Grants repository access to the user
4. Updates credentials config to include SEC as a graph
5. Runs example queries (unless --skip-queries is set)

Usage:
    uv run main.py                              # Load NVDA 2025, run queries
    uv run main.py --ticker AAPL --year 2024    # Load specific ticker and year
    uv run main.py --skip-queries               # Skip running example queries
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import (
    ensure_user_credentials,
    CredentialContext,
)

DEMO_DIR = Path(__file__).parent
CREDENTIALS_FILE = Path(__file__).resolve().parents[1] / "credentials" / "config.json"


def run_just_command(command: str):
    """Run a just command and handle errors."""
    print(f"\n{'=' * 70}")
    print(f"Running: just {command}")
    print(f"{'=' * 70}\n")

    result = subprocess.run(["just"] + command.split(), cwd=PROJECT_ROOT)

    if result.returncode != 0:
        print(f"\n❌ Command 'just {command}' failed with exit code {result.returncode}")
        sys.exit(result.returncode)


def load_or_create_credentials(base_url: str) -> dict:
    """Load existing credentials or create new user if none exist."""
    if not CREDENTIALS_FILE.exists():
        print("\n⚠️  No credentials found. Creating new demo user...")
        context = CredentialContext(
            base_url=base_url,
            credentials_path=CREDENTIALS_FILE,
            default_name_prefix="SEC Demo User",
            default_email_prefix="sec_demo",
            api_key_prefix="SEC Demo API Key",
            display_title="SEC Repository Demo - User Setup",
        )
        return ensure_user_credentials(context)

    try:
        with CREDENTIALS_FILE.open() as f:
            credentials = json.load(f)
            print("\n✅ Using existing credentials")
            user_id = credentials.get("user_id") or credentials.get("user", {}).get("id")
            if user_id:
                print(f"   User ID: {user_id}")
            print(f"   User: {credentials.get('user', {}).get('name')}")
            return credentials
    except Exception as e:
        print(f"❌ Failed to load credentials: {e}")
        sys.exit(1)


def update_credentials(data: dict):
    """Update credentials file with new graph info."""
    try:
        with CREDENTIALS_FILE.open("w") as f:
            json.dump(data, f, indent=2)
        print(f"💾 Updated credentials file: {CREDENTIALS_FILE}")
    except Exception as e:
        print(f"❌ Failed to save credentials: {e}")
        sys.exit(1)




def main():
    parser = argparse.ArgumentParser(
        description="Setup SEC repository demo with data loading and access"
    )
    parser.add_argument(
        "--ticker", default="NVDA", help="Stock ticker to load (default: NVDA)"
    )
    parser.add_argument(
        "--year", default="2025", help="Year to load data for (default: 2025)"
    )
    parser.add_argument(
        "--skip-queries",
        action="store_true",
        help="Skip running example queries after setup",
    )
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="API base URL (default: http://localhost:8000)",
    )

    args = parser.parse_args()

    print("\n📊 SEC Repository Demo Setup")
    print("=" * 70)
    print(f"Ticker: {args.ticker}")
    print(f"Year: {args.year}")
    print("=" * 70)

    # Load or create credentials
    credentials = load_or_create_credentials(args.base_url)

    # Get user_id
    user_id = credentials.get("user_id") or credentials.get("user", {}).get("id")
    if not user_id:
        print("❌ No user_id found in credentials")
        sys.exit(1)

    print(f"User ID: {user_id}\n")

    # Step 1: Load SEC data
    print("📥 Step 1: Loading SEC data...")
    run_just_command(f"sec-load {args.ticker} {args.year}")

    # Brief pause to let graph settle
    time.sleep(2)

    # Step 2: Grant repository access
    print("\n🔑 Step 2: Granting SEC repository access...")
    run_just_command(f"repo-grant-access {user_id} sec read")

    # Step 3: Update config.json to add sec graph
    print("\n💾 Step 3: Updating credentials config...")

    # Add sec to graphs section
    if "graphs" not in credentials:
        credentials["graphs"] = {}

    credentials["graphs"]["sec"] = {
        "graph_id": "sec",
        "graph_created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "repository_type": "shared",
        "description": "SEC Public Filings Repository",
    }

    update_credentials(credentials)

    print("\n" + "=" * 70)
    print("✅ SEC Repository Setup Complete!")
    print("=" * 70)
    print(f"\nYou can now query SEC data:")
    print(
        f"  just graph-query sec \"MATCH (e:Entity {{ticker: '{args.ticker}'}}) RETURN e.name, e.ticker LIMIT 5\""
    )
    print(f"\nOr explore with preset queries:")
    print(f"  uv run examples/sec_demo/query_examples.py --list")
    print(f"  uv run examples/sec_demo/query_examples.py --preset entities")
    print(f"  uv run examples/sec_demo/query_examples.py --all")
    print(
        f"\nOr via API with the credentials in {CREDENTIALS_FILE.relative_to(PROJECT_ROOT)}"
    )
    print("=" * 70 + "\n")

    if not args.skip_queries:
        print("\n" + "=" * 70)
        print("📊 Running Example Queries")
        print("=" * 70 + "\n")
        result = subprocess.run(
            ["uv", "run", "examples/sec_demo/query_examples.py", "--all"],
            cwd=PROJECT_ROOT,
        )
        if result.returncode != 0:
            print(f"\n⚠️  Example queries failed with exit code {result.returncode}")
            print("   You can still query the SEC data manually using the commands above")
        else:
            print("\n" + "=" * 70)
            print("✅ Example Queries Complete!")
            print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
