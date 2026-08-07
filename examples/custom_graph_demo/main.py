#!/usr/bin/env python3
"""
Custom Graph Demo — orchestrator.

Builds a domain-neutral graph (people, companies, projects) from the custom
schema in `schema.json`, showing how to model an arbitrary domain and load it
through the production bulk-ingest path. Each step is a standalone script this
module runs in order:

1. `setup_credentials.py` — user account and API key
2. `create_graph.py` — graph with the custom schema
3. `generate_data.py` — synthetic node/relationship Parquet files
4. `upload_ingest.py` — upload to S3, stage, validate, materialize
5. `query_graph.py` — verification queries (org charts, collaborations)
6. `upload_documents.py` — index the markdown files in `documents/`
7. `memory_subgraph.py` — a `knowledge` subgraph with structured + semantic memory

Data is regenerated on every run so the Parquet identifiers line up with the
current graph, which is why step 3 follows step 2.

Prerequisites:
    just start        # Docker stack (API, PostgreSQL, Valkey, LadybugDB)

Usage:
    just demo-custom-graph                  # reuse the existing user and graph
    just demo-custom-graph --new-graph      # new graph for the existing user
    just demo-custom-graph --new-user       # new user (implies --new-graph)
    just demo-custom-graph --skip-queries   # load only, no verification queries

Edit `schema.json` to model your own node types and relationships.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


DEMO_DIR = Path(__file__).parent
DEFAULT_CREDENTIALS_FILE = Path(__file__).resolve().parents[2] / ".local" / "config.json"


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
    default=os.environ.get("ROBOSYSTEMS_API_URL", "http://localhost:8000"),
    help="API base URL (default: $ROBOSYSTEMS_API_URL or http://localhost:8000)",
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

  # Creating a new user always implies provisioning a fresh graph.
  if args.new_user:
    args.new_graph = True

  step1_args = ["--base-url", args.base_url]
  if args.new_user:
    step1_args.append("--force")
    args.new_graph = True

  step1_args.extend(["--credentials-file", str(credentials_path)])
  run_script("setup_credentials.py", step1_args)

  print("\n" + "=" * 70)
  print("📊 Custom Graph Demo - Complete Workflow")
  print("=" * 70)
  print(f"Base URL: {args.base_url}")
  print(f"Create new user: {args.new_user}")
  print(f"Create new graph: {args.new_graph}")
  print("Regenerate data: True (always)")
  print("=" * 70)

  step2_args = [
    "--base-url",
    args.base_url,
    "--credentials-file",
    str(credentials_path),
  ]
  if not args.new_graph:
    step2_args.append("--reuse")
  run_script("create_graph.py", step2_args)

  # Regenerate data every run to align parquet identifiers with the current graph.
  step3_args = ["--regenerate", "--credentials-file", str(credentials_path)]
  run_script("generate_data.py", step3_args)

  step4_args = [
    "--base-url",
    args.base_url,
    "--credentials-file",
    str(credentials_path),
  ]
  run_script("upload_ingest.py", step4_args)

  if not args.skip_queries:
    step5_args = [
      "--all",
      "--base-url",
      args.base_url,
      "--credentials-file",
      str(credentials_path),
    ]
    run_script("query_graph.py", step5_args)

  # Step 6: Upload project management documents (if documents/ directory exists)
  docs_dir = DEMO_DIR / "documents"
  if docs_dir.exists() and list(docs_dir.glob("*.md")):
    step6_args = ["--base-url", args.base_url]
    run_script("upload_documents.py", step6_args)

  # Step 7: Create memory subgraph
  step7_args = [
    "--base-url",
    args.base_url,
    "--credentials-file",
    str(credentials_path),
  ]
  run_script("memory_subgraph.py", step7_args)

  print("\n" + "=" * 70)
  print("✅ Custom Graph Demo - Complete!")
  print("=" * 70)
  print("\n💡 Next steps:")
  print("   - Run custom queries: uv run query_graph.py")
  print("   - Interactive mode: uv run query_graph.py")
  print("   - Search documents: uv run upload_documents.py")
  print("   - Memory subgraph: uv run memory_subgraph.py")
  print("   - Create another graph: uv run main.py --new-graph")
  print("=" * 70 + "\n")


if __name__ == "__main__":
  main()
