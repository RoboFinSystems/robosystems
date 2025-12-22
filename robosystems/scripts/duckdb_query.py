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

from robosystems.utils.query_output import (
  print_csv,
  print_error,
  print_info_section,
  print_json,
  print_table,
)


def execute_query(db_path: str, query: str, format_output: str = "table") -> bool:
  """Execute a SQL query against the DuckDB database."""
  conn = None
  try:
    db_path_obj = Path(db_path)
    if not db_path_obj.exists():
      print_error(f"Database path does not exist: {db_path}")
      return False

    print(f"Connecting to database: {db_path}")
    conn = duckdb.connect(str(db_path), read_only=True)

    print(f"Executing query: {query}")
    result = conn.execute(query)

    column_names = [desc[0] for desc in result.description]
    rows = result.fetchall()

    results = []
    for row in rows:
      row_dict = {}
      for i, value in enumerate(row):
        row_dict[column_names[i]] = value
      results.append(row_dict)

    if format_output == "table":
      print_info_section(f"QUERY: {query}")
      print_table(results, title="DuckDB Query Results")

    elif format_output == "json":
      print_json(results)

    elif format_output == "csv":
      print_csv(results)

    return True

  except Exception as e:
    print_error(f"Error executing query: {e}")
    return False
  finally:
    if conn is not None:
      conn.close()


def interactive_mode(db_path: str, format_output: str = "table"):
  """Interactive query mode."""
  print_info_section("📊 Interactive DuckDB Query Mode")
  print(f"\nDatabase: {db_path}")
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

      execute_query(db_path, query_input, format_output)

    except KeyboardInterrupt:
      print("\n\nGoodbye!")
      break
    except EOFError:
      print("\n\nGoodbye!")
      break


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

  parser.add_argument("--query", help="SQL query to execute")

  parser.add_argument(
    "--format",
    choices=["table", "json", "csv"],
    default="table",
    help="Output format (default: table)",
  )

  args = parser.parse_args()

  if args.query:
    success = execute_query(args.db_path, args.query, args.format)
    if not success:
      sys.exit(1)
  else:
    interactive_mode(args.db_path, args.format)


if __name__ == "__main__":
  main()
