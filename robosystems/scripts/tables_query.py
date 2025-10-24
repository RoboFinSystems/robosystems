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

    if format_output == "table":
      print("\n" + "=" * 60)
      print(f"QUERY RESULTS ({row_count} rows, {execution_time_ms:.2f}ms)")
      print("=" * 60)
      print(f"Query: {query}")
      print("=" * 60)

      if columns and rows:
        header_row = " | ".join(columns)
        print(header_row)
        print("-" * len(header_row))

        for row in rows:
          row_data = []
          for value in row:
            if value is None:
              row_data.append("NULL")
            else:
              row_data.append(str(value))

          print(" | ".join(row_data))

        print("-" * len(header_row))

      print(f"Total rows: {row_count}")
      print(f"Execution time: {execution_time_ms:.2f}ms")

    elif format_output == "json":
      import json

      results = []
      for row in rows:
        row_dict = {}
        for i, col_name in enumerate(columns):
          row_dict[col_name] = row[i] if i < len(row) else None
        results.append(row_dict)

      print(json.dumps(results, indent=2, default=str))

    elif format_output == "csv":
      import csv
      import io

      output = io.StringIO()
      writer = csv.writer(output)

      writer.writerow(columns)
      for row in rows:
        writer.writerow(row)

      print(output.getvalue())

    return True

  except requests.exceptions.RequestException as e:
    print(f"❌ Query execution failed: {e}")
    if hasattr(e, "response") and e.response is not None:
      try:
        error_detail = e.response.json()
        print(f"Error details: {error_detail}")
      except Exception:
        print(f"Response text: {e.response.text}")
    return False
  except Exception as e:
    print(f"❌ Error: {e}")
    return False


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

  parser.add_argument("--query", required=True, help="SQL query to execute")

  parser.add_argument(
    "--format",
    choices=["table", "json", "csv"],
    default="table",
    help="Output format (default: table)",
  )

  args = parser.parse_args()

  success = execute_query(args.url, args.graph_id, args.query, args.format)

  if not success:
    sys.exit(1)


if __name__ == "__main__":
  main()
