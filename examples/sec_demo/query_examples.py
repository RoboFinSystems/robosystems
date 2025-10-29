#!/usr/bin/env python3
"""
SEC Repository Demo - Query Examples

Provides preset queries and interactive exploration for the SEC shared repository.

Usage:
    uv run query_examples.py                      # Interactive mode
    uv run query_examples.py --all                # Run all preset queries
    uv run query_examples.py --preset entities    # Run a specific preset
    uv run query_examples.py --query "MATCH (n:Entity) RETURN n.name LIMIT 10"
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

DEFAULT_CREDENTIALS_FILE = (
  Path(__file__).resolve().parents[1] / "credentials" / "config.json"
)
DEMO_NAME = "sec"


PRESET_QUERIES = {
  "summary": {
    "description": "Overview of node and relationship counts in SEC repository",
    "query": """
MATCH (n)
WITH labels(n) AS label, count(n) AS count
RETURN label, count
ORDER BY count DESC
        """,
  },
  "entities": {
    "description": "List public companies with their basic information",
    "query": """
MATCH (e:Entity)
WHERE e.entity_type = 'operating'
RETURN
    e.ticker AS ticker,
    e.name AS company_name,
    e.cik AS cik,
    e.industry AS industry,
    e.state_of_incorporation AS state,
    e.fiscal_year_end AS fiscal_year_end
ORDER BY e.ticker
LIMIT 20
        """,
  },
  "recent_reports": {
    "description": "Most recent SEC filings by entity",
    "query": """
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)
RETURN
    e.ticker AS ticker,
    e.name AS company,
    r.form AS form_type,
    r.report_date AS report_date,
    r.filing_date AS filing_date
ORDER BY r.filing_date DESC
LIMIT 25
        """,
  },
  "report_types": {
    "description": "Count of reports by form type (10-K, 10-Q, etc.)",
    "query": """
MATCH (r:Report)
RETURN
    r.form AS form_type,
    count(*) AS report_count
ORDER BY report_count DESC
        """,
  },
  "financial_facts": {
    "description": "Sample of financial facts and their metadata",
    "query": """
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
WHERE f.numeric_value IS NOT NULL
RETURN
    r.report_date AS report_date,
    e.name AS element_name,
    f.numeric_value AS value,
    f.decimals AS decimals,
    f.fact_type AS fact_type
ORDER BY r.report_date DESC, e.name
LIMIT 30
        """,
  },
  "fact_dimensions": {
    "description": "Explore dimensional qualifiers on facts",
    "query": """
MATCH (f:Fact)-[:FACT_HAS_DIMENSION]->(fd:FactDimension)
RETURN
    fd.axis_uri AS axis,
    fd.member_uri AS member,
    fd.type AS dimension_type,
    count(f) AS fact_count
ORDER BY fact_count DESC
LIMIT 20
        """,
  },
  "fact_periods": {
    "description": "Facts grouped by time periods",
    "query": """
MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
WHERE p.end_date IS NOT NULL
WITH p.fiscal_year AS year, p.end_date AS period_end, count(f) AS fact_count
RETURN
    year AS fiscal_year,
    period_end AS period_end,
    fact_count
ORDER BY year DESC, period_end DESC
LIMIT 20
        """,
  },
  "elements": {
    "description": "Most commonly used XBRL elements in reports",
    "query": """
MATCH (e:Element)<-[:FACT_HAS_ELEMENT]-(f:Fact)
RETURN
    e.name AS element_name,
    count(f) AS usage_count
ORDER BY usage_count DESC
LIMIT 30
        """,
  },
  "report_structure": {
    "description": "Count of facts per report",
    "query": """
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
RETURN
    r.form AS form_type,
    r.report_date AS report_date,
    count(f) AS fact_count
ORDER BY fact_count DESC
LIMIT 20
        """,
  },
  "entity_overview": {
    "description": "Summary of entities and their report counts",
    "query": """
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)
RETURN
    e.ticker AS ticker,
    e.name AS company_name,
    e.industry AS industry,
    count(r) AS report_count
ORDER BY report_count DESC
LIMIT 20
        """,
  },
  "fact_aspects": {
    "description": "Facts with all their aspects (Element, Period, Unit, Dimensions)",
    "query": """
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
OPTIONAL MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
OPTIONAL MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
RETURN
    r.form AS form,
    r.report_date AS report_date,
    e.name AS element,
    f.numeric_value AS value,
    p.fiscal_year AS fiscal_year,
    p.start_date AS period_start,
    p.end_date AS period_end,
    u.measure AS unit
ORDER BY r.report_date DESC, e.name
LIMIT 30
        """,
  },
  "fact_with_dimensions": {
    "description": "Facts with dimensional qualifiers showing complete context",
    "query": """
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
MATCH (f)-[:FACT_HAS_DIMENSION]->(fd:FactDimension)
OPTIONAL MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
RETURN
    r.report_date AS report_date,
    e.name AS element,
    f.numeric_value AS value,
    p.fiscal_year AS fiscal_year,
    fd.axis_uri AS dimension_axis,
    fd.member_uri AS dimension_member
ORDER BY r.report_date DESC, e.name
LIMIT 20
        """,
  },
  "taxonomy_structures": {
    "description": "Report taxonomy structures (Balance Sheet, Income Statement, etc.)",
    "query": """
MATCH (r:Report)-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure)
RETURN
    r.form AS form,
    r.report_date AS report_date,
    s.name AS structure_name,
    s.type AS structure_type,
    s.definition AS definition
ORDER BY s.name
LIMIT 25
        """,
  },
  "element_hierarchy": {
    "description": "Element parent-child relationships within taxonomy structures",
    "query": """
MATCH (s:Structure)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)
MATCH (a)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(parent:Element)
MATCH (a)-[:ASSOCIATION_HAS_TO_ELEMENT]->(child:Element)
RETURN
    s.name AS structure,
    s.type AS structure_type,
    parent.name AS parent_element,
    child.name AS child_element,
    a.order_value AS display_order,
    a.weight AS calculation_weight
ORDER BY s.name, a.order_value
LIMIT 30
        """,
  },
  "report_taxonomy_detail": {
    "description": "Complete report taxonomy structure with element associations",
    "query": """
MATCH (r:Report)-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure)
MATCH (s)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)
MATCH (a)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(parent:Element)
MATCH (a)-[:ASSOCIATION_HAS_TO_ELEMENT]->(child:Element)
RETURN
    r.form AS form,
    r.report_date AS report_date,
    s.name AS structure,
    parent.name AS parent_element,
    child.name AS child_element,
    a.order_value AS display_order
ORDER BY r.report_date DESC, s.name, a.order_value
LIMIT 25
        """,
  },
}


def load_credentials(credentials_path: Path) -> dict:
  if not credentials_path.exists():
    raise FileNotFoundError(
      f"No credentials found at {credentials_path}. "
      "Run demo-sec first to setup SEC access."
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
  except Exception as exc:
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
    "SEC Repository Interactive Mode",
    subtitle="Enter Cypher queries, or type 'help', 'presets', or 'quit'.",
  )

  while True:
    try:
      user_input = input("\ncypher> ").strip()
    except (EOFError, KeyboardInterrupt):
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

    run_and_display(client, graph_id, "Custom Query", user_input)


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Run preset queries against the SEC shared repository"
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
  except Exception as exc:
    print_error(str(exc))
    sys.exit(1)

  api_key = credentials.get("api_key")
  graph_id = get_graph_id(DEFAULT_CREDENTIALS_FILE, DEMO_NAME)

  if not api_key:
    print_error("Missing API key in credentials.")
    sys.exit(1)

  if not graph_id:
    print_error(
      "SEC repository not configured. Run 'just demo-sec' first to setup SEC access."
    )
    sys.exit(1)

  if args.list:
    print_info_section("Available SEC Query Presets")
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
