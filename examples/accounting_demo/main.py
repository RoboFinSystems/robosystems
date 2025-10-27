#!/usr/bin/env python3
"""
Accounting Demo - Main Orchestration Script

This script runs the complete accounting demo workflow:
1. Setup credentials (user account and API key)
2. Create graph database
3. Generate accounting data (parquet files)
4. Upload and ingest data
5. Run verification queries

Usage:
    uv run main.py                        # Reuse existing user & graph
    uv run main.py --new-user             # Create new user
    uv run main.py --new-graph            # Create new graph (same user)
    uv run main.py --new-user --new-graph # Create everything new
    uv run main.py --regenerate-data      # Regenerate parquet files
"""

import argparse
import subprocess
import sys
from pathlib import Path


DEMO_DIR = Path(__file__).parent


def run_script(script_name: str, args: list[str] | None = None):
  """Run a demo script and handle errors."""
  script_path = DEMO_DIR / script_name
  cmd = ["uv", "run", str(script_path)]

  if args:
    cmd.extend(args)

  print(f"\n{'=' * 70}")
  print(f"Running: {script_name}")
  print(f"{'=' * 70}\n")

  result = subprocess.run(cmd, cwd=DEMO_DIR.parent.parent)

  if result.returncode != 0:
    print(f"\n❌ Script {script_name} failed with exit code {result.returncode}")
    sys.exit(result.returncode)


def main():
  parser = argparse.ArgumentParser(
    description="Run the complete accounting demo workflow"
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument(
    "--flags",
    default="",
    help="Comma-separated flags: new-user,new-graph,regenerate-data,skip-queries",
  )
  parser.add_argument(
    "--new-user",
    action="store_true",
    help="Create new user credentials (default: reuse existing)",
  )
  parser.add_argument(
    "--new-graph",
    action="store_true",
    help="Create new graph (default: reuse existing if available)",
  )
  parser.add_argument(
    "--regenerate-data",
    action="store_true",
    help="Force regenerate data files",
  )
  parser.add_argument(
    "--skip-queries",
    action="store_true",
    help="Skip running verification queries at the end",
  )

  args = parser.parse_args()

  if args.flags:
    for flag in args.flags.split(","):
      flag = flag.strip()
      if flag == "new-user":
        args.new_user = True
      elif flag == "new-graph":
        args.new_graph = True
      elif flag == "regenerate-data":
        args.regenerate_data = True
      elif flag == "skip-queries":
        args.skip_queries = True
      elif flag:
        print(
          f"⚠️  Warning: Unknown flag '{flag}' (valid: new-user, new-graph, regenerate-data, skip-queries)"
        )
        sys.exit(1)

  print("\n" + "=" * 70)
  print("📊 Accounting Demo - Complete Workflow")
  print("=" * 70)
  print(f"Base URL: {args.base_url}")
  print(f"Create new user: {args.new_user}")
  print(f"Create new graph: {args.new_graph}")
  print(f"Regenerate data: {args.regenerate_data}")
  print("=" * 70)

  step1_args = ["--base-url", args.base_url]
  if args.new_user:
    step1_args.append("--force")
  run_script("01_setup_credentials.py", step1_args)

  step2_args = ["--base-url", args.base_url]
  if not args.new_graph:
    step2_args.append("--reuse")
  run_script("02_create_graph.py", step2_args)

  step3_args = []
  if args.regenerate_data:
    step3_args.append("--regenerate")
  run_script("03_generate_data.py", step3_args)

  step4_args = ["--base-url", args.base_url]
  run_script("04_upload_ingest.py", step4_args)

  if not args.skip_queries:
    step5_args = ["--all", "--base-url", args.base_url]
    run_script("05_query_graph.py", step5_args)

  print("\n" + "=" * 70)
  print("✅ Accounting Demo - Complete!")
  print("=" * 70)
  print("\n💡 Next steps:")
  print("   - Run custom queries: uv run 05_query_graph.py")
  print("   - Interactive mode: uv run 05_query_graph.py")
  print("   - Create another graph: uv run main.py --new-graph")
  print("=" * 70 + "\n")


if __name__ == "__main__":
  main()
