#!/usr/bin/env python3
# type: ignore
"""
Kuzu Database Query Tool

Simple command-line tool to execute queries against embedded Kuzu databases.

Usage:
    # Single quotes are auto-converted to double quotes for easier shell usage
    python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity) RETURN c.name LIMIT 5"
    python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity {ticker: 'NVDA'}) RETURN c.name"
    python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (n) RETURN labels(n)[0] as type, count(*) as count"
"""

import argparse
import sys
from pathlib import Path
import kuzu

from robosystems.utils.query_output import (
  print_csv,
  print_error,
  print_info_section,
  print_json,
  print_table,
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


def execute_query(db_path: str, query: str, format_output: str = "table"):
  """Execute a query against the Kuzu database."""
  try:
    db_path_obj = Path(db_path)
    if not db_path_obj.exists():
      print_error(f"Database path does not exist: {db_path}")
      return False

    print(f"Connecting to database: {db_path}")
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)

    normalized_query = normalize_cypher_quotes(query)
    print(f"Executing query: {normalized_query}")
    result = conn.execute(normalized_query)

    column_names = result.get_column_names()

    results = []
    while result.has_next():
      row = result.get_next()
      row_dict = {}
      for i in range(len(row)):
        col_name = (
          column_names[i] if column_names and i < len(column_names) else f"col_{i}"
        )
        row_dict[col_name] = row[i]
      results.append(row_dict)

    if format_output == "table":
      print_info_section(f"QUERY: {normalized_query}")
      print_table(results, title="Kuzu Query Results")

    elif format_output == "json":
      print_json(results)

    elif format_output == "csv":
      print_csv(results)

    conn.close()
    return True

  except Exception as e:
    print_error(f"Error executing query: {e}")
    return False


def interactive_mode(db_path: str, format_output: str = "table"):
  """Interactive query mode."""
  print_info_section("📊 Interactive Kuzu Query Mode")
  print(f"\nDatabase: {db_path}")
  print("\nCommands:")
  print("   quit/exit      - Exit interactive mode")
  print("\nEnter a Cypher query:")

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
    description="Execute queries against embedded Kuzu databases",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Basic query
  python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity) RETURN c.name LIMIT 5"

  # Single quotes auto-converted to double quotes for easier shell usage
  python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity {ticker: 'NVDA'}) RETURN c.name"

  # Count nodes by type
  python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (n) RETURN labels(n)[0] as type, count(*) as count"

  # JSON output
  python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity) RETURN c.name" --format json
        """,
  )

  parser.add_argument(
    "--db-path", required=True, help="Path to the Kuzu database directory"
  )

  parser.add_argument("--query", help="Cypher query to execute")

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
