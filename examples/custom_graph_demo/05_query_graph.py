#!/usr/bin/env python3
"""
Custom Graph Demo - Query Helpers

Provides preset queries and interactive exploration for the generic custom
graph demo (people, companies, projects).

Usage:
    uv run 05_query_graph.py                      # Interactive mode
    uv run 05_query_graph.py --all                # Run all preset queries
    uv run 05_query_graph.py --preset teams       # Run a specific preset
    uv run 05_query_graph.py --query "MATCH (n) RETURN count(n)"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from robosystems_client.extensions import (
  RoboSystemsExtensions,
  RoboSystemsExtensionConfig,
)
from robosystems.utils.query_output import (
  print_error,
  print_info_section,
  print_table,
  print_warning,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id

DEFAULT_CREDENTIALS_FILE = Path(__file__).resolve().parents[1] / "credentials" / "config.json"
DEMO_NAME = "custom_graph_demo"


PRESET_QUERIES = {
  "summary": {
    "description": "Overview of node and relationship counts",
    "query": """
MATCH (n)
WITH labels(n) AS label, count(n) AS count
RETURN label, count
ORDER BY count DESC
        """,
  },
  "people": {
    "description": "List all people with their roles and interests",
    "query": """
MATCH (p:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN
    p.identifier AS person_id,
    p.name AS name,
    p.title AS role,
    c.name AS company,
    p.interests AS interests
ORDER BY name
LIMIT 25
        """,
  },
  "companies": {
    "description": "Company overview with team sizes and sponsored projects",
    "query": """
MATCH (c:Company)
OPTIONAL MATCH (c)<-[:PERSON_WORKS_FOR_COMPANY]-(p:Person)
OPTIONAL MATCH (c)-[:COMPANY_SPONSORS_PROJECT]->(proj:Project)
RETURN
    c.name AS company,
    c.industry AS industry,
    c.location AS location,
    count(DISTINCT p) AS team_members,
    count(DISTINCT proj) AS sponsored_projects
ORDER BY team_members DESC
        """,
  },
  "projects": {
    "description": "Active projects with sponsors and team members",
    "query": """
MATCH (proj:Project)<-[:PERSON_WORKS_ON_PROJECT]-(p:Person)
MATCH (proj)<-[:COMPANY_SPONSORS_PROJECT]-(c:Company)
RETURN
    proj.name AS project,
    proj.status AS status,
    c.name AS sponsor,
    proj.budget AS budget,
    count(DISTINCT p) AS team_members
ORDER BY project
        """,
  },
  "teams": {
    "description": "Cross-company teams working on the same project",
    "query": """
MATCH (p1:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c1:Company),
      (p2:Person)-[:PERSON_WORKS_FOR_COMPANY]->(c2:Company),
      (p1)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project),
      (p2)-[:PERSON_WORKS_ON_PROJECT]->(proj)
WHERE p1.identifier < p2.identifier AND c1.identifier <> c2.identifier
RETURN
    proj.name AS project,
    c1.name AS company_a,
    p1.name AS teammate_a,
    c2.name AS company_b,
    p2.name AS teammate_b
ORDER BY proj.name, company_a, company_b
LIMIT 50
        """,
  },
  "interests": {
    "description": "Top technical interests across the organization",
    "query": """
MATCH (p:Person)
RETURN p.interests AS interest_list, count(*) AS people
ORDER BY people DESC, interest_list ASC
LIMIT 20
        """,
  },
  "graphviz": {
    "description": "Lightweight subgraph to visualize project connections",
    "query": """
MATCH (p:Person)-[:PERSON_WORKS_ON_PROJECT]->(proj:Project)
MATCH (p)-[:PERSON_WORKS_FOR_COMPANY]->(c:Company)
RETURN
    p.name AS person,
    c.name AS company,
    proj.name AS project
ORDER BY proj.name, company
LIMIT 40
        """,
  },
}


def load_credentials(credentials_path: Path) -> dict:
  if not credentials_path.exists():
    raise FileNotFoundError(
      f"No credentials found at {credentials_path}. "
      "Run 01_setup_credentials.py first."
    )
  with credentials_path.open() as fh:
    return json.load(fh)


def build_client(api_key: str, base_url: str) -> RoboSystemsExtensions:
  config = RoboSystemsExtensionConfig(
    base_url=base_url,
    headers={"X-API-Key": api_key},
  )
  return RoboSystemsExtensions(config)


def execute_query(client: RoboSystemsExtensions, graph_id: str, cypher: str) -> dict:
  response = client.query.query(graph_id, cypher)
  return {
    "columns": getattr(response, "columns", []),
    "data": getattr(response, "data", []),
    "row_count": getattr(response, "row_count", 0),
    "execution_time_ms": getattr(response, "execution_time_ms", 0.0),
  }


def run_and_display(
  client: RoboSystemsExtensions,
  graph_id: str,
  name: str,
  query: str,
  description: str | None = None,
) -> None:
  print_info_section(name, subtitle=description)

  try:
    result = execute_query(client, graph_id, query)
    if not result["data"]:
      print_warning("No rows returned.")
      return
    print_table(
      result["data"],
      title=name,
      row_count_label=f"Rows returned (execution {result['execution_time_ms']:.1f} ms)",
    )
  except Exception as exc:  # noqa: BLE001
    print_error(f"Query failed: {exc}")


def run_presets(
  client: RoboSystemsExtensions, graph_id: str, presets: list[str]
) -> None:
  for preset in presets:
    details = PRESET_QUERIES.get(preset)
    if not details:
      print_warning(f"Unknown preset: {preset}")
      continue
    run_and_display(
      client,
      graph_id,
      name=f"Preset: {preset}",
      query=details["query"],
      description=details.get("description"),
    )


def interactive_mode(client: RoboSystemsExtensions, graph_id: str) -> None:
  print_info_section(
    "Interactive mode",
    subtitle="Enter Cypher queries, or type 'help', 'presets', or 'quit'.",
  )

  while True:
    try:
      user_input = input("\ncypher> ").strip()
    except (EOFError, KeyboardInterrupt):  # noqa: PERF203
      print("\nExiting interactive mode.")
      break

    if not user_input:
      continue
    if user_input.lower() in {"quit", "exit"}:
      print("Goodbye!")
      break
    if user_input.lower() in {"help", "?"}:
      print("Commands:")
      print("  presets         - list available preset queries")
      print("  preset <name>   - run a specific preset")
      print("  quit / exit     - leave interactive mode")
      print("  Any other input is treated as a Cypher query.")
      continue
    if user_input.lower() == "presets":
      print("Available presets:")
      for preset, details in PRESET_QUERIES.items():
        print(f"  - {preset}: {details['description']}")
      continue
    if user_input.lower().startswith("preset "):
      preset_name = user_input.split(maxsplit=1)[1]
      run_presets(client, graph_id, [preset_name])
      continue

    # Treat as a cypher query
    run_and_display(client, graph_id, "Custom Query", user_input)


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Run preset queries against the custom graph demo"
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument(
    "--all",
    action="store_true",
    help="Run all preset queries",
  )
  parser.add_argument(
    "--preset",
    help="Run a specific preset (see --list for options)",
  )
  parser.add_argument(
    "--list",
    action="store_true",
    help="List available presets",
  )
  parser.add_argument(
    "--query",
    help="Run a custom Cypher query and exit",
  )
  parser.add_argument(
    "--credentials-file",
    default=str(DEFAULT_CREDENTIALS_FILE),
    help="Path to credentials file (default: credentials/config.json)",
  )
  return parser.parse_args()


def main() -> None:
  args = parse_args()
  credentials_path = Path(args.credentials_file).expanduser()

  try:
    credentials = load_credentials(credentials_path)
  except Exception as exc:  # noqa: BLE001
    print_error(str(exc))
    sys.exit(1)

  api_key = credentials.get("api_key")
  graph_id = get_graph_id(DEFAULT_CREDENTIALS_FILE, DEMO_NAME)

  if not api_key or not graph_id:
    print_error("Missing API key or graph ID in credentials.")
    sys.exit(1)

  if args.list:
    print_info_section("Available presets")
    for name, details in PRESET_QUERIES.items():
      print(f"- {name}: {details['description']}")
    return

  client = build_client(api_key, args.base_url)

  if args.query:
    run_and_display(client, graph_id, "Custom Query", args.query)
    return

  if args.preset:
    run_presets(client, graph_id, [args.preset])
    return

  if args.all:
    run_presets(client, graph_id, list(PRESET_QUERIES))
    return

  interactive_mode(client, graph_id)


if __name__ == "__main__":
  main()
