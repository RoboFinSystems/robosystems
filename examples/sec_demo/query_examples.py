#!/usr/bin/env python3
"""
SEC Repository Demo - Query Examples

Provides preset queries and interactive exploration for the SEC shared repository.
Includes both graph queries (Cypher) and document search (OpenSearch).

Usage:
    uv run query_examples.py                      # Interactive mode
    uv run query_examples.py --all                # Run all preset queries
    uv run query_examples.py --preset entities    # Run a specific preset
    uv run query_examples.py --query "MATCH (n:Entity) RETURN n.name LIMIT 10"
    uv run query_examples.py --search "tariff risk"          # Text search
    uv run query_examples.py --search "AI strategy" --entity NVDA
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from robosystems_client.clients import (
  RoboSystemsClients,
  RoboSystemsClientConfig,
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

from examples._common.config import get_graph_id

DEFAULT_CREDENTIALS_FILE = Path(__file__).resolve().parents[2] / ".local" / "config.json"
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
MATCH (f:Fact)-[:FACT_HAS_DIMENSION]->(d:Dimension)
RETURN
    d.axis AS axis,
    d.member AS member,
    d.dimension_type AS dimension_type,
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
WITH p.calendar_year AS year, p.end_date AS period_end, count(f) AS fact_count
RETURN
    year AS calendar_year,
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
    p.calendar_year AS calendar_year,
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
MATCH (f)-[:FACT_HAS_DIMENSION]->(d:Dimension)
OPTIONAL MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
RETURN
    r.report_date AS report_date,
    e.name AS element,
    f.numeric_value AS value,
    p.calendar_year AS calendar_year,
    d.axis AS dimension_axis,
    d.member AS dimension_member
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


# ── Document Search Presets (OpenSearch) ──────────────────────────────────────
# These use hybrid search (BM25 + KNN) over indexed filing text content.
# Three source types are available:
#   - xbrl_textblock: XBRL text blocks (MD&A, policies, accounting narratives)
#   - narrative_section: 10-K/10-Q narrative sections (Item 1, 1A, 7, etc.)
#   - ixbrl_disclosure: iXBRL disclosure sections with XBRL element metadata

SEARCH_PRESETS = {
  "risk_factors": {
    "description": "Search for risk factor disclosures across all entities",
    "params": {
      "query": "material risks and uncertainties",
      "form_type": "10-K",
      "section": "item_1a",
      "size": 10,
    },
  },
  "ai_strategy": {
    "description": "Find mentions of AI and machine learning strategy",
    "params": {
      "query": "artificial intelligence machine learning strategy",
      "size": 10,
    },
  },
  "revenue_recognition": {
    "description": "Search for revenue recognition policy disclosures",
    "params": {
      "query": "revenue recognition policy",
      "source_type": "xbrl_textblock",
      "size": 10,
    },
  },
  "cybersecurity": {
    "description": "Find cybersecurity risk and incident disclosures",
    "params": {
      "query": "cybersecurity risk management incident",
      "size": 10,
    },
  },
  "goodwill_impairment": {
    "description": "Search for goodwill and impairment discussions",
    "params": {
      "query": "goodwill impairment testing",
      "element": "us-gaap:Goodwill",
      "size": 10,
    },
  },
  "segment_reporting": {
    "description": "Find operating segment descriptions and breakdowns",
    "params": {
      "query": "operating segments reportable segment",
      "source_type": "ixbrl_disclosure",
      "size": 10,
    },
  },
  "mda_overview": {
    "description": "Management discussion and analysis highlights (10-K Item 7)",
    "params": {
      "query": "management discussion analysis results operations",
      "form_type": "10-K",
      "section": "item_7",
      "size": 10,
    },
  },
  "mda_quarterly": {
    "description": "Quarterly MD&A highlights (10-Q Item 2)",
    "params": {
      "query": "management discussion analysis results operations",
      "form_type": "10-Q",
      "section": "item_2",
      "size": 10,
    },
  },
  "supply_chain": {
    "description": "Search for supply chain and sourcing disclosures",
    "params": {
      "query": "supply chain sourcing manufacturing concentration",
      "size": 10,
    },
  },
  "tariff_trade": {
    "description": "Search for tariff, trade policy, and geopolitical risk mentions (10-K)",
    "params": {
      "query": "tariff trade policy geopolitical international",
      "form_type": "10-K",
      "section": "item_1a",
      "size": 10,
    },
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


def build_client(api_key: str, base_url: str) -> RoboSystemsClients:
  config = RoboSystemsClientConfig(
    base_url=base_url,
    headers={"X-API-Key": api_key},
  )
  return RoboSystemsClients(config)


def execute_query(client: RoboSystemsClients, graph_id: str, cypher: str) -> dict:
  response = client.query.query(graph_id, cypher)
  return {
    "columns": getattr(response, "columns", []),
    "data": getattr(response, "data", []),
    "row_count": getattr(response, "row_count", 0),
    "execution_time_ms": getattr(response, "execution_time_ms", 0.0),
  }


def run_and_display(
  client: RoboSystemsClients,
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


def execute_search(
  client: RoboSystemsClients,
  graph_id: str,
  query: str,
  entity: str | None = None,
  form_type: str | None = None,
  section: str | None = None,
  element: str | None = None,
  source_type: str | None = None,
  size: int = 10,
) -> dict:
  """Execute a document search via OpenSearch."""
  response = client.documents.search(
    graph_id,
    query=query,
    entity=entity,
    form_type=form_type,
    section=section,
    element=element,
    source_type=source_type,
    size=size,
  )
  hits = []
  for hit in getattr(response, "hits", []):
    hits.append({
      "score": f"{getattr(hit, 'score', 0):.3f}",
      "entity": getattr(hit, "entity_ticker", "") or "",
      "form": getattr(hit, "form_type", "") or "",
      "section": getattr(hit, "section_label", "") or getattr(hit, "element_qname", "") or "",
      "source": getattr(hit, "source_type", "") or "",
      "filing_date": getattr(hit, "filing_date", "") or "",
      "snippet": (getattr(hit, "snippet", "") or "")[:120],
    })
  return {
    "total": getattr(response, "total", 0),
    "hits": hits,
  }


def run_search_and_display(
  client: RoboSystemsClients,
  graph_id: str,
  name: str,
  params: dict,
  description: str | None = None,
) -> None:
  """Run a document search and display results."""
  print_info_section(name, subtitle=description)

  try:
    result = execute_search(client, graph_id, **params)
    if not result["hits"]:
      print_warning("No search results.")
      return
    print_table(
      result["hits"],
      title=name,
      row_count_label=f"{result['total']} total matches",
    )
  except Exception as exc:
    print_error(f"Search failed: {exc}")


def run_presets(
  client: RoboSystemsClients, graph_id: str, presets: list[str]
) -> None:
  for preset in presets:
    # Check graph query presets first, then search presets
    details = PRESET_QUERIES.get(preset)
    if details:
      run_and_display(
        client,
        graph_id,
        name=f"Preset: {preset}",
        query=details["query"],
        description=details.get("description"),
      )
      continue

    search_details = SEARCH_PRESETS.get(preset)
    if search_details:
      run_search_and_display(
        client,
        graph_id,
        name=f"Search: {preset}",
        params=search_details["params"],
        description=search_details.get("description"),
      )
      continue

    print_warning(f"Unknown preset: {preset}")


def interactive_mode(client: RoboSystemsClients, graph_id: str) -> None:
  print_info_section(
    "SEC Repository Interactive Mode",
    subtitle="Enter Cypher queries, search commands, or type 'help'.",
  )

  while True:
    try:
      user_input = input("\nsec> ").strip()
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
      print("  presets              - list all presets (graph + search)")
      print("  preset <name>        - run a specific preset")
      print("  search <query>       - full-text search across filings")
      print("  search <query> @NVDA - search filtered by entity")
      print("  quit / exit          - leave interactive mode")
      print("  Any other input is treated as a Cypher query.")
      continue
    if user_input.lower() == "presets":
      print("\nGraph Query Presets:")
      for preset, details in PRESET_QUERIES.items():
        print(f"  {preset}: {details['description']}")
      print("\nDocument Search Presets:")
      for preset, details in SEARCH_PRESETS.items():
        print(f"  {preset}: {details['description']}")
      continue
    if user_input.lower().startswith("preset "):
      preset_name = user_input.split(maxsplit=1)[1]
      run_presets(client, graph_id, [preset_name])
      continue
    if user_input.lower().startswith("search "):
      search_text = user_input.split(maxsplit=1)[1]
      # Parse optional @ENTITY filter (e.g., "tariff risk @NVDA")
      entity_filter = None
      if " @" in search_text:
        search_text, entity_filter = search_text.rsplit(" @", 1)
        entity_filter = entity_filter.strip()
      run_search_and_display(
        client,
        graph_id,
        name="Search",
        params={"query": search_text.strip(), "entity": entity_filter},
      )
      continue

    run_and_display(client, graph_id, "Custom Query", user_input)


def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Run preset queries against the SEC shared repository"
  )
  parser.add_argument(
    "--base-url",
    default=os.environ.get("ROBOSYSTEMS_API_URL", "http://localhost:8000"),
    help="API base URL (default: $ROBOSYSTEMS_API_URL or http://localhost:8000)",
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
    "--search",
    help="Run a document text search and exit",
  )
  parser.add_argument(
    "--entity",
    help="Filter search by entity ticker (e.g., NVDA). Used with --search.",
  )
  parser.add_argument(
    "--form-type",
    help="Filter search by form type (e.g., 10-K). Used with --search.",
  )
  parser.add_argument(
    "--section",
    help="Filter search by section (e.g., item_1a). Used with --search.",
  )
  parser.add_argument(
    "--credentials-file",
    default=str(DEFAULT_CREDENTIALS_FILE),
    help="Path to credentials file (default: .local/config.json)",
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
    print("\nGraph Queries (Cypher):")
    for name, details in PRESET_QUERIES.items():
      print(f"  {name}: {details['description']}")
    print("\nDocument Search (OpenSearch):")
    for name, details in SEARCH_PRESETS.items():
      print(f"  {name}: {details['description']}")
    return

  client = build_client(api_key, args.base_url)

  if args.search:
    run_search_and_display(
      client,
      graph_id,
      name="Search",
      params={
        "query": args.search,
        "entity": args.entity,
        "form_type": args.form_type,
        "section": args.section,
      },
    )
    return

  if args.query:
    run_and_display(client, graph_id, "Custom Query", args.query)
    return

  if args.preset:
    run_presets(client, graph_id, [args.preset])
    return

  if args.all:
    run_presets(client, graph_id, list(PRESET_QUERIES))
    run_presets(client, graph_id, list(SEARCH_PRESETS))
    return

  interactive_mode(client, graph_id)


if __name__ == "__main__":
  main()
