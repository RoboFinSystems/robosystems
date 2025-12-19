#!/usr/bin/env python3
"""
Graph Database Query Tool

Generic command-line tool to execute queries against graph databases through the Graph API.
Works with any backend (LadybugDB, Neo4j, etc.) that implements the Graph API.

Usage:
    # Health check
    python graph_query.py --url http://localhost:8001 --command health

    # Database info
    python graph_query.py --url http://localhost:8001 --graph-id sec --command info

    # Execute query (single quotes auto-converted to double quotes for Cypher)
    python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (c:Entity) RETURN c.name LIMIT 5"
    python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (c:Entity {ticker: 'NVDA'}) RETURN c.name"

    # Parameterized query (best practice for security and flexibility)
    python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (e:Entity {ticker: $ticker}) RETURN e.name" --params '{"ticker": "NVDA"}'

    # JSON output
    python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (c:Entity) RETURN c.name" --format json
"""

import argparse
import json
import sys

import requests

from robosystems.utils.query_output import (
  print_csv,
  print_error,
  print_info_field,
  print_info_section,
  print_json,
  print_success,
  print_table,
  print_warning,
)


def normalize_cypher_quotes(query: str) -> str:
  """
  Convert single quotes to double quotes in Cypher queries.

  This makes it easier to write queries in the shell since single quotes
  don't need escaping. Cypher requires double quotes for string literals.

  Examples:
      MATCH (e:Entity {ticker: 'NVDA'}) -> MATCH (e:Entity {ticker: "NVDA"})

  Args:
      query: Cypher query string

  Returns:
      Query with single quotes converted to double quotes
  """
  return query.replace("'", '"')


def health_check(api_url: str) -> bool:
  try:
    response = requests.get(f"{api_url}/health", timeout=10)
    response.raise_for_status()

    data = response.json()
    status = data.get("status", "unknown")

    print_info_section("HEALTH CHECK")
    print_info_field("Status", status)

    if "uptime_seconds" in data:
      uptime_hours = data["uptime_seconds"] / 3600
      print_info_field(
        "Uptime", f"{uptime_hours:.2f} hours ({data['uptime_seconds']:.0f} seconds)"
      )

    if "database_count" in data:
      print_info_field("Database count", data["database_count"])

    if "memory_rss_mb" in data:
      print_info_field("Memory (RSS)", f"{data['memory_rss_mb']:.1f} MB")
      print_info_field("Memory (VMS)", f"{data.get('memory_vms_mb', 0):.1f} MB")
      print_info_field("Memory %", f"{data.get('memory_percent', 0):.2f}%")

    if status == "healthy":
      print()
      print_success("Graph API is healthy")
      return True
    else:
      print()
      print_error(f"Graph API is unhealthy: {status}")
      return False

  except requests.exceptions.RequestException as e:
    print_error(f"Health check failed: {e}")
    return False


def get_database_info(api_url: str, graph_id: str) -> bool:
  try:
    response = requests.get(f"{api_url}/databases/{graph_id}", timeout=10)
    response.raise_for_status()

    data = response.json()

    print_info_section(f"DATABASE INFO: {graph_id}")
    print_info_field("Path", data.get("database_path", "N/A"))
    print_info_field("Created", data.get("created_at", "N/A"))
    print_info_field("Healthy", "✓" if data.get("is_healthy") else "✗")
    print_info_field("Read-only", "Yes" if data.get("read_only") else "No")

    size_bytes = data.get("size_bytes", 0)
    size_mb = size_bytes / (1024 * 1024)
    print_info_field("Size", f"{size_mb:.2f} MB ({size_bytes:,} bytes)")

    if data.get("last_accessed"):
      print_info_field("Last accessed", data["last_accessed"])

    print()
    print("Querying database for node/relationship counts...")
    try:
      count_query = """
        MATCH (n)
        RETURN count(n) as node_count
      """
      count_response = requests.post(
        f"{api_url}/databases/{graph_id}/query",
        json={"cypher": count_query, "database": graph_id},
        timeout=30,
      )
      count_response.raise_for_status()
      count_data = count_response.json()
      results = count_data.get("data", count_data.get("results", []))
      node_count = results[0].get("node_count", 0) if results else 0

      rel_query = """
        MATCH ()-[r]->()
        RETURN count(r) as rel_count
      """
      rel_response = requests.post(
        f"{api_url}/databases/{graph_id}/query",
        json={"cypher": rel_query, "database": graph_id},
        timeout=30,
      )
      rel_response.raise_for_status()
      rel_data = rel_response.json()
      rel_results = rel_data.get("data", rel_data.get("results", []))
      rel_count = rel_results[0].get("rel_count", 0) if rel_results else 0

      print_info_field("Node count", f"{node_count:,}")
      print_info_field("Relationship count", f"{rel_count:,}")
    except Exception as e:
      print_warning(f"Could not query counts: {e}")

    return True

  except requests.exceptions.RequestException as e:
    print_error(f"Failed to get database info: {e}")
    return False


def execute_query(
  api_url: str,
  graph_id: str,
  query: str,
  format_output: str = "table",
  timeout: int = 300,
  parameters: dict | None = None,
) -> bool:
  try:
    # Normalize single quotes to double quotes for Cypher
    normalized_query = normalize_cypher_quotes(query)

    # Build request payload
    payload = {"cypher": normalized_query, "database": graph_id}
    if parameters:
      payload["parameters"] = parameters

    response = requests.post(
      f"{api_url}/databases/{graph_id}/query",
      json=payload,
      timeout=timeout,
    )
    response.raise_for_status()

    data = response.json()
    results = data.get("data", data.get("results", []))

    if format_output == "table":
      query_display = f"{normalized_query}"
      if parameters:
        query_display += f"\nParameters: {parameters}"
      print_info_section(f"QUERY: {query_display}")
      print_table(results, title=f"Query Results ({len(results)} rows)")

    elif format_output == "json":
      print_json(results)

    elif format_output == "csv":
      print_csv(results)

    return True

  except requests.exceptions.RequestException as e:
    print_error(f"Query execution failed: {e}")
    if hasattr(e, "response") and e.response is not None:
      try:
        error_detail = e.response.json()
        print(f"Error details: {error_detail}")
      except Exception:
        print(f"Response text: {e.response.text}")
    return False
  except Exception as e:
    print_error(f"Error: {e}")
    return False


def interactive_mode(
  api_url: str, graph_id: str, format_output: str = "table", timeout: int = 300
):
  """Interactive query mode."""
  print_info_section("📊 Interactive Graph Query Mode")
  print(f"\nGraph ID: {graph_id}")
  print(f"API URL: {api_url}")
  print("\nCommands:")
  print("   health         - Check Graph API health")
  print("   info           - Show database information")
  print("   quit/exit      - Exit interactive mode")
  print("\nOr enter a custom Cypher query:")

  while True:
    try:
      query_input = input("\n> ").strip()

      if not query_input:
        continue

      if query_input.lower() in ["quit", "exit", "q"]:
        print("\nGoodbye!")
        break

      if query_input.lower() == "health":
        health_check(api_url)
        continue

      if query_input.lower() == "info":
        get_database_info(api_url, graph_id)
        continue

      execute_query(api_url, graph_id, query_input, format_output, timeout)

    except KeyboardInterrupt:
      print("\n\nGoodbye!")
      break
    except EOFError:
      print("\n\nGoodbye!")
      break


def main():
  parser = argparse.ArgumentParser(
    description="Execute queries against graph databases through Graph API",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Health check
  python graph_query.py --url http://localhost:8001 --command health

  # Database info
  python graph_query.py --url http://localhost:8001 --graph-id sec --command info

  # Execute query (single quotes auto-converted to double quotes)
  python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (c:Entity {ticker: 'NVDA'}) RETURN c.name LIMIT 5"

  # Parameterized query (best practice - no escaping needed!)
  python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (e:Entity {ticker: $ticker}) RETURN e.name, e.ticker LIMIT $limit" --params '{"ticker": "AAPL", "limit": 10}'

  # JSON output
  python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (c:Entity) RETURN c.name" --format json

  # Query Neo4j (change URL to Neo4j API)
  python graph_query.py --url http://localhost:8001 --graph-id sec --query "MATCH (n) RETURN labels(n)[0] as label, count(n) as count"
        """,
  )

  parser.add_argument(
    "--url",
    default="http://localhost:8001",
    help="Graph API URL (default: http://localhost:8001 for LadybugDB, use http://localhost:8001 for Neo4j)",
  )

  parser.add_argument("--graph-id", help="Graph database identifier (e.g., 'sec')")

  parser.add_argument(
    "--command", choices=["health", "info"], help="Command to execute"
  )

  parser.add_argument("--query", help="Cypher query to execute")

  parser.add_argument(
    "--params",
    "--parameters",
    dest="params",
    help='Query parameters as JSON (e.g., \'{"ticker": "NVDA", "limit": 10}\')',
  )

  parser.add_argument(
    "--format",
    choices=["table", "json", "csv"],
    default="table",
    help="Output format (default: table)",
  )

  parser.add_argument(
    "--timeout",
    type=int,
    default=300,
    help="Query timeout in seconds (default: 300)",
  )

  args = parser.parse_args()

  # Parse parameters if provided
  parameters = None
  if args.params:
    try:
      parameters = json.loads(args.params)
    except json.JSONDecodeError as e:
      print_error(f"Invalid JSON in --params: {e}")
      sys.exit(1)

  if args.command == "health":
    success = health_check(args.url)
  elif args.command == "info":
    if not args.graph_id:
      print("Error: --graph-id is required for 'info' command")
      sys.exit(1)
    success = get_database_info(args.url, args.graph_id)
  elif args.query:
    if not args.graph_id:
      print("Error: --graph-id is required for query execution")
      sys.exit(1)
    success = execute_query(
      args.url, args.graph_id, args.query, args.format, args.timeout, parameters
    )
  else:
    if not args.graph_id:
      print("Error: --graph-id is required for interactive mode")
      sys.exit(1)
    interactive_mode(args.url, args.graph_id, args.format, args.timeout)
    success = True

  if not success:
    sys.exit(1)


if __name__ == "__main__":
  main()
