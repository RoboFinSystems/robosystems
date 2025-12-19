#!/usr/bin/env python3
"""
DuckDB Staging Tables Query Tool

Command-line tool to execute SQL queries against DuckDB staging tables through the Graph API.
Queries the /tables/query endpoint which uses the DuckDB connection pool.

Usage:
    uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT * FROM information_schema.tables"
    uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT * FROM Entity LIMIT 10"
    uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT COUNT(*) FROM Entity" --format json
"""

import argparse
import sys

import requests

from robosystems.utils.query_output import (
  print_csv,
  print_error,
  print_info_section,
  print_json,
  print_table,
)


def execute_query(
  api_url: str, graph_id: str, query: str, format_output: str = "table"
) -> bool:
  try:
    response = requests.post(
      f"{api_url}/databases/{graph_id}/tables/query",
      json={"graph_id": graph_id, "sql": query},
      timeout=300,
    )
    response.raise_for_status()

    data = response.json()
    columns = data.get("columns", [])
    rows = data.get("rows", [])
    row_count = data.get("row_count", len(rows))
    execution_time_ms = data.get("execution_time_ms", 0)

    results = []
    for row in rows:
      row_dict = {}
      for i, col_name in enumerate(columns):
        row_dict[col_name] = row[i] if i < len(row) else None
      results.append(row_dict)

    if format_output == "table":
      print_info_section(f"QUERY: {query}")
      print(f"Execution time: {execution_time_ms:.2f}ms\n")
      print_table(
        results,
        title=f"DuckDB Staging Tables Query Results ({row_count} rows)",
      )

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


def interactive_mode(api_url: str, graph_id: str, format_output: str = "table"):
  """Interactive query mode."""
  print_info_section("📊 Interactive Tables Query Mode")
  print(f"\nGraph ID: {graph_id}")
  print(f"API URL: {api_url}")
  print("\nCommands:")
  print("   quit/exit      - Exit interactive mode")
  print("\nEnter a SQL query:")

  while True:
    try:
      query_input = input("\n> ").strip()

      if not query_input:
        continue

      if query_input.lower() in ["quit", "exit", "q"]:
        print("\nGoodbye!")
        break

      execute_query(api_url, graph_id, query_input, format_output)

    except KeyboardInterrupt:
      print("\n\nGoodbye!")
      break
    except EOFError:
      print("\n\nGoodbye!")
      break


def main():
  parser = argparse.ArgumentParser(
    description="Execute SQL queries against DuckDB staging tables through Graph API",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # List all tables
  uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT * FROM information_schema.tables"

  # Query a specific table
  uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT * FROM Entity LIMIT 10"

  # Count rows
  uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT COUNT(*) as total FROM Entity"

  # JSON output
  uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT * FROM Entity LIMIT 5" --format json

  # CSV output
  uv run python -m robosystems.scripts.tables_query --url http://localhost:8001 --graph-id sec --query "SELECT * FROM Entity LIMIT 5" --format csv
        """,
  )

  parser.add_argument(
    "--url",
    default="http://localhost:8001",
    help="Graph API URL (default: http://localhost:8001)",
  )

  parser.add_argument(
    "--graph-id", required=True, help="Graph database identifier (e.g., 'sec')"
  )

  parser.add_argument("--query", help="SQL query to execute")

  parser.add_argument(
    "--format",
    choices=["table", "json", "csv"],
    default="table",
    help="Output format (default: table)",
  )

  args = parser.parse_args()

  if args.query:
    success = execute_query(args.url, args.graph_id, args.query, args.format)
    if not success:
      sys.exit(1)
  else:
    interactive_mode(args.url, args.graph_id, args.format)


if __name__ == "__main__":
  main()
