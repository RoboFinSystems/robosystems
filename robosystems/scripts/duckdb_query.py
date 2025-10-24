#!/usr/bin/env python3
"""
DuckDB Database Query Tool

Simple command-line tool to execute SQL queries against DuckDB databases.

Usage:
    uv run python robosystems/scripts/duckdb_query.py --db-path ./data/staging/sec.duckdb --query "SELECT * FROM information_schema.tables"
    uv run python robosystems/scripts/duckdb_query.py --db-path ./data/staging/sec.duckdb --query "SELECT COUNT(*) FROM Entity"
"""

import argparse
import sys
from pathlib import Path
import duckdb


def execute_query(db_path: str, query: str, format_output: str = "table") -> bool:
  """Execute a SQL query against the DuckDB database."""
  conn = None
  try:
    db_path_obj = Path(db_path)
    if not db_path_obj.exists():
      print(f"Error: Database path does not exist: {db_path}")
      return False

    print(f"Connecting to database: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)

    print(f"Executing query: {query}")
    result = conn.execute(query)

    if format_output == "table":
      print("\n" + "=" * 60)
      print("RESULTS:")
      print("=" * 60)

      column_names = [desc[0] for desc in result.description]
      header_row = ""

      if column_names:
        header_row = " | ".join(column_names)
        print(header_row)
        print("-" * len(header_row))

      rows = result.fetchall()
      for row in rows:
        row_data = []
        for value in row:
          if value is None:
            row_data.append("NULL")
          else:
            row_data.append(str(value))
        print(" | ".join(row_data))

      print("-" * (len(header_row) if column_names else 60))
      print(f"Total rows: {len(rows)}")

    elif format_output == "json":
      import json

      column_names = [desc[0] for desc in result.description]
      rows = result.fetchall()

      results = []
      for row in rows:
        row_dict = {}
        for i, value in enumerate(row):
          row_dict[column_names[i]] = value
        results.append(row_dict)

      print(json.dumps(results, indent=2, default=str))

    elif format_output == "csv":
      import csv
      import io

      output = io.StringIO()
      writer = csv.writer(output)

      column_names = [desc[0] for desc in result.description]
      writer.writerow(column_names)

      rows = result.fetchall()
      for row in rows:
        writer.writerow(row)

      print(output.getvalue())

    return True

  except Exception as e:
    print(f"Error executing query: {e}")
    return False
  finally:
    if conn is not None:
      conn.close()


def main():
  parser = argparse.ArgumentParser(
    description="Execute SQL queries against DuckDB databases",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # List all tables
  uv run python robosystems/scripts/duckdb_query.py --db-path ./data/staging/sec.duckdb --query "SELECT * FROM information_schema.tables"

  # Query a specific table
  uv run python robosystems/scripts/duckdb_query.py --db-path ./data/staging/sec.duckdb --query "SELECT * FROM Entity LIMIT 10"

  # Count rows in a table
  uv run python robosystems/scripts/duckdb_query.py --db-path ./data/staging/sec.duckdb --query "SELECT COUNT(*) as total FROM Entity"

  # JSON output
  uv run python robosystems/scripts/duckdb_query.py --db-path ./data/staging/sec.duckdb --query "SELECT * FROM Entity LIMIT 5" --format json
        """,
  )

  parser.add_argument(
    "--db-path", required=True, help="Path to the DuckDB database file"
  )

  parser.add_argument("--query", required=True, help="SQL query to execute")

  parser.add_argument(
    "--format",
    choices=["table", "json", "csv"],
    default="table",
    help="Output format (default: table)",
  )

  args = parser.parse_args()

  success = execute_query(args.db_path, args.query, args.format)

  if not success:
    sys.exit(1)


if __name__ == "__main__":
  main()
