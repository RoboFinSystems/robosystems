#!/usr/bin/env python3
"""
Custom Graph Demo - Main Orchestration Script

This script runs the complete custom graph demo workflow:
1. Setup credentials (user account and API key)
2. Create graph database
3. Generate custom graph data (parquet files)
4. Upload and ingest data
5. Run verification queries

Usage:
    uv run main.py                        # Reuse existing user & graph, regenerate data automatically
    uv run main.py --new-user             # Create new user + graph, regenerate data
    uv run main.py --new-graph            # Create new graph for existing user, regenerate data
    uv run main.py --skip-queries         # Skip verification queries after ingestion
    uv run main.py --flags new-user,new-graph  # Legacy comma-separated flags (no spaces)
"""

import argparse
import subprocess
import sys
from pathlib import Path


DEMO_DIR = Path(__file__).parent
DEFAULT_CREDENTIALS_FILE = Path(__file__).resolve().parents[1] / "credentials" / "config.json"


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
    description="Run the complete custom graph demo workflow"
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument(
    "--flags",
    default="",
    help="Comma-separated flags: new-user,new-graph,skip-queries (legacy compatibility)",
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
    "--skip-queries",
    action="store_true",
    help="Skip running verification queries at the end",
  )
  parser.add_argument(
    "--credentials-file",
    default=str(DEFAULT_CREDENTIALS_FILE),
    help="Path to credentials file shared across demo scripts",
  )

  args = parser.parse_args()
  credentials_path = Path(args.credentials_file).expanduser()

  if args.flags:
    for flag in args.flags.split(","):
      flag = flag.strip()
      if not flag:
        continue
      if flag == "new-user":
        args.new_user = True
      elif flag == "new-graph":
        args.new_graph = True
      elif flag == "skip-queries":
        args.skip_queries = True
      else:
        print(
          f"⚠️  Warning: Unknown flag '{flag}' "
          "(valid options: new-user,new-graph,skip-queries)"
        )
        sys.exit(1)

  # Creating a new user always implies provisioning a fresh graph.
  if args.new_user:
    args.new_graph = True

  step1_args = ["--base-url", args.base_url]
  if args.new_user:
    step1_args.append("--force")
    args.new_graph = True

  step1_args.extend(["--credentials-file", str(credentials_path)])
  run_script("01_setup_credentials.py", step1_args)

  print("\n" + "=" * 70)
  print("📊 Custom Graph Demo - Complete Workflow")
  print("=" * 70)
  print(f"Base URL: {args.base_url}")
  print(f"Create new user: {args.new_user}")
  print(f"Create new graph: {args.new_graph}")
  print("Regenerate data: True (always)")
  print("=" * 70)

  step2_args = ["--base-url", args.base_url, "--credentials-file", str(credentials_path)]
  if not args.new_graph:
    step2_args.append("--reuse")
  run_script("02_create_graph.py", step2_args)

  # Regenerate data every run to align parquet identifiers with the current graph.
  step3_args = ["--regenerate", "--credentials-file", str(credentials_path)]
  run_script("03_generate_data.py", step3_args)

  step4_args = ["--base-url", args.base_url, "--credentials-file", str(credentials_path)]
  run_script("04_upload_ingest.py", step4_args)

  if not args.skip_queries:
    step5_args = [
      "--all",
      "--base-url",
      args.base_url,
      "--credentials-file",
      str(credentials_path),
    ]
    run_script("05_query_graph.py", step5_args)

  print("\n" + "=" * 70)
  print("✅ Custom Graph Demo - Complete!")
  print("=" * 70)
  print("\n💡 Next steps:")
  print("   - Run custom queries: uv run 05_query_graph.py")
  print("   - Interactive mode: uv run 05_query_graph.py")
  print("   - Create another graph: uv run main.py --new-graph")
  print("=" * 70 + "\n")


if __name__ == "__main__":
  main()
