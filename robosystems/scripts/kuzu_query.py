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


def execute_query(db_path: str, query: str, format_output: str = "table"):
  """Execute a query against the Kuzu database."""
  try:
    # Validate database path
    db_path_obj = Path(db_path)
    if not db_path_obj.exists():
      print(f"Error: Database path does not exist: {db_path}")
      return False

    # Connect to database
    print(f"Connecting to database: {db_path}")
    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)

    # Execute query
    print(f"Executing query: {query}")
    result = conn.execute(query)

    # Format and display results
    if format_output == "table":
      print("\n" + "=" * 60)
      print("RESULTS:")
      print("=" * 60)

      # Get column names from the result
      column_names = result.get_column_names()

      # Print column headers
      if column_names:
        header_row = " | ".join(column_names)
        print(header_row)
        print("-" * len(header_row))

      row_count = 0
      while result.has_next():
        row = result.get_next()

        # Print row data
        row_data = []
        for i in range(len(row)):
          value = row[i]
          if value is None:
            row_data.append("NULL")
          else:
            row_data.append(str(value))

        print(" | ".join(row_data))
        row_count += 1

      print("-" * (len(header_row) if column_names else 60))
      print(f"Total rows: {row_count}")

    elif format_output == "json":
      import json

      # Get column names
      column_names = result.get_column_names()

      results = []
      while result.has_next():
        row = result.get_next()
        row_dict = {}
        for i in range(len(row)):
          # Use actual column name if available, otherwise fallback to col_i
          col_name = (
            column_names[i] if column_names and i < len(column_names) else f"col_{i}"
          )
          row_dict[col_name] = row[i]
        results.append(row_dict)

      print(json.dumps(results, indent=2, default=str))

    elif format_output == "csv":
      import csv
      import io

      output = io.StringIO()
      writer = csv.writer(output)

      # Get column names and write header row
      column_names = result.get_column_names()
      if column_names:
        writer.writerow(column_names)

      # Write data rows
      while result.has_next():
        row = result.get_next()
        writer.writerow([row[i] for i in range(len(row))])

      print(output.getvalue())

    conn.close()
    return True

  except Exception as e:
    print(f"Error executing query: {e}")
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
