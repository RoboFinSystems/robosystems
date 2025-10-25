#!/usr/bin/env python3
"""
Graph Database Query Tool

Generic command-line tool to execute queries against graph databases through the Graph API.
Works with any backend (Kuzu, Neo4j, etc.) that implements the Graph API.

Usage:
    # Health check
    python graph_query.py --url http://localhost:8002 --command health

    # Database info
    python graph_query.py --url http://localhost:8002 --graph-id sec --command info

    # Execute query
    python graph_query.py --url http://localhost:8002 --graph-id sec --query "MATCH (c:Entity) RETURN c.name LIMIT 5"

    # JSON output
    python graph_query.py --url http://localhost:8002 --graph-id sec --query "MATCH (c:Entity) RETURN c.name" --format json
"""

import argparse
import sys
import requests


def health_check(api_url: str) -> bool:
  try:
    response = requests.get(f"{api_url}/health", timeout=10)
    response.raise_for_status()

    data = response.json()
    status = data.get("status", "unknown")

    print("=" * 60)
    print("HEALTH CHECK")
    print("=" * 60)
    print(f"Status: {status}")

    if "uptime_seconds" in data:
      uptime_hours = data["uptime_seconds"] / 3600
      print(f"Uptime: {uptime_hours:.2f} hours ({data['uptime_seconds']:.0f} seconds)")

    if "database_count" in data:
      print(f"Database count: {data['database_count']}")

    if "memory_rss_mb" in data:
      print(f"Memory (RSS): {data['memory_rss_mb']:.1f} MB")
      print(f"Memory (VMS): {data.get('memory_vms_mb', 0):.1f} MB")
      print(f"Memory %: {data.get('memory_percent', 0):.2f}%")

    if status == "healthy":
      print("\n✅ Graph API is healthy")
      return True
    else:
      print(f"\n❌ Graph API is unhealthy: {status}")
      return False

  except requests.exceptions.RequestException as e:
    print(f"❌ Health check failed: {e}")
    return False


def get_database_info(api_url: str, graph_id: str) -> bool:
  try:
    response = requests.get(f"{api_url}/databases/{graph_id}", timeout=10)
    response.raise_for_status()

    data = response.json()

    print("=" * 60)
    print(f"DATABASE INFO: {graph_id}")
    print("=" * 60)
    print(f"Path: {data.get('database_path', 'N/A')}")
    print(f"Created: {data.get('created_at', 'N/A')}")
    print(f"Healthy: {'✅' if data.get('is_healthy') else '❌'}")
    print(f"Read-only: {'Yes' if data.get('read_only') else 'No'}")

    size_bytes = data.get("size_bytes", 0)
    size_mb = size_bytes / (1024 * 1024)
    print(f"Size: {size_mb:.2f} MB ({size_bytes:,} bytes)")

    if data.get("last_accessed"):
      print(f"Last accessed: {data['last_accessed']}")

    # Query for node and relationship counts
    print("\nQuerying database for node/relationship counts...")
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

      print(f"Node count: {node_count:,}")
      print(f"Relationship count: {rel_count:,}")
    except Exception as e:
      print(f"⚠️  Could not query counts: {e}")

    return True

  except requests.exceptions.RequestException as e:
    print(f"❌ Failed to get database info: {e}")
    return False


def execute_query(
  api_url: str, graph_id: str, query: str, format_output: str = "table"
) -> bool:
  try:
    response = requests.post(
      f"{api_url}/databases/{graph_id}/query",
      json={"cypher": query, "database": graph_id},
      timeout=300,
    )
    response.raise_for_status()

    data = response.json()
    results = data.get("data", data.get("results", []))

    if format_output == "table":
      print("\n" + "=" * 60)
      print(f"QUERY RESULTS ({len(results)} rows)")
      print("=" * 60)
      print(f"Query: {query}")
      print("=" * 60)

      if results:
        column_names = list(results[0].keys())

        header_row = " | ".join(column_names)
        print(header_row)
        print("-" * len(header_row))

        for row in results:
          row_data = []
          for col in column_names:
            value = row.get(col)
            if value is None:
              row_data.append("NULL")
            else:
              row_data.append(str(value))

          print(" | ".join(row_data))

        print("-" * len(header_row))

      print(f"Total rows: {len(results)}")

    elif format_output == "json":
      import json

      print(json.dumps(results, indent=2, default=str))

    elif format_output == "csv":
      import csv
      import io

      if not results:
        print("")
        return True

      output = io.StringIO()
      writer = csv.DictWriter(output, fieldnames=results[0].keys())

      writer.writeheader()
      for row in results:
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
    description="Execute queries against graph databases through Graph API",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Health check
  python graph_query.py --url http://localhost:8002 --command health

  # Database info
  python graph_query.py --url http://localhost:8002 --graph-id sec --command info

  # Execute query
  python graph_query.py --url http://localhost:8002 --graph-id sec --query "MATCH (c:Entity) RETURN c.name LIMIT 5"

  # JSON output
  python graph_query.py --url http://localhost:8002 --graph-id sec --query "MATCH (c:Entity) RETURN c.name" --format json

  # Query Neo4j (change URL to Neo4j API)
  python graph_query.py --url http://localhost:8002 --graph-id sec --query "MATCH (n) RETURN labels(n)[0] as label, count(n) as count"
        """,
  )

  parser.add_argument(
    "--url",
    default="http://localhost:8001",
    help="Graph API URL (default: http://localhost:8001 for Kuzu, use http://localhost:8002 for Neo4j)",
  )

  parser.add_argument("--graph-id", help="Graph database identifier (e.g., 'sec')")

  parser.add_argument(
    "--command", choices=["health", "info"], help="Command to execute"
  )

  parser.add_argument("--query", help="Cypher query to execute")

  parser.add_argument(
    "--format",
    choices=["table", "json", "csv"],
    default="table",
    help="Output format (default: table)",
  )

  args = parser.parse_args()

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
    success = execute_query(args.url, args.graph_id, args.query, args.format)
  else:
    parser.print_help()
    print("\nError: Must specify either --command or --query")
    sys.exit(1)

  if not success:
    sys.exit(1)


if __name__ == "__main__":
  main()
