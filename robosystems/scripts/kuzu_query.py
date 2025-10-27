#!/usr/bin/env python3
# type: ignore
"""
Kuzu Database Query Tool

Simple command-line tool to execute queries against embedded Kuzu databases.

Usage:
    python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity) RETURN c.name LIMIT 5"
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

    print(f"Executing query: {query}")
    result = conn.execute(query)

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
      print_info_section(f"QUERY: {query}")
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


def main():
  parser = argparse.ArgumentParser(
    description="Execute queries against embedded Kuzu databases",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Basic query
  python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity) RETURN c.name LIMIT 5"

  # Count nodes by type
  python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (n) RETURN labels(n)[0] as type, count(*) as count"

  # JSON output
  python kuzu_query.py --db-path ./data/kuzu-dbs/sec --query "MATCH (c:Entity) RETURN c.name" --format json
        """,
  )

  parser.add_argument(
    "--db-path", required=True, help="Path to the Kuzu database directory"
  )

  parser.add_argument("--query", required=True, help="Cypher query to execute")

  parser.add_argument(
    "--format",
    choices=["table", "json", "csv"],
    default="table",
    help="Output format (default: table)",
  )

  args = parser.parse_args()

  # Execute the query
  success = execute_query(args.db_path, args.query, args.format)

  if not success:
    sys.exit(1)


if __name__ == "__main__":
  main()
