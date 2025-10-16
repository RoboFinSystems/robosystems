"""
Kuzu API Client - Command-line interface for Kuzu API

Usage:
    python -m robosystems.graph_api query "MATCH (c:Entity) RETURN c.name LIMIT 5"
    python -m robosystems.graph_api health --url $KUZU_API_URL

This module provides a CLI interface using the new unified client structure.
"""

import argparse
import json
# typing imports removed - unused

from robosystems.logger import logger
from robosystems.config import env
from .client.sync_client import KuzuSyncClient
from .client.exceptions import KuzuAPIError


# For backward compatibility, keep the old class name but use new implementation
class KuzuAPIClient(KuzuSyncClient):
  """Backward compatibility wrapper for KuzuAPIClient."""

  pass


def main():
  """Main CLI interface."""
  parser = argparse.ArgumentParser(description="Kuzu API Client")
  parser.add_argument(
    "--url",
    default=env.KUZU_API_URL or "http://localhost:8001",
    help="Kuzu API base URL",
  )
  parser.add_argument(
    "--timeout", type=int, default=30, help="Request timeout in seconds"
  )

  subparsers = parser.add_subparsers(dest="command", help="Available commands")

  # Health check command
  subparsers.add_parser("health", help="Check API health")

  # Query command
  query_parser = subparsers.add_parser("query", help="Execute Cypher query")
  query_parser.add_argument("cypher", help="Cypher query to execute")
  query_parser.add_argument("--database", default="sec", help="Target database name")
  query_parser.add_argument("--parameters", help="Query parameters as JSON string")
  query_parser.add_argument(
    "--format", choices=["json", "table"], default="table", help="Output format"
  )

  # Info command
  subparsers.add_parser("info", help="Get database information")

  # Ingest command
  ingest_parser = subparsers.add_parser("ingest", help="Trigger data ingestion")
  ingest_parser.add_argument("pipeline_run_id", help="Pipeline run ID")
  ingest_parser.add_argument("bucket", help="S3 bucket name")
  ingest_parser.add_argument("--prefix", default="processed/", help="S3 prefix")
  ingest_parser.add_argument(
    "--files", nargs="+", help="Specific files to ingest", required=True
  )

  # Task status command
  task_parser = subparsers.add_parser("task-status", help="Get task status")
  task_parser.add_argument("task_id", help="Task ID")

  # List databases command
  subparsers.add_parser("databases", help="List all databases")

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    return 1

  # Initialize client
  client = KuzuSyncClient(base_url=args.url, timeout=args.timeout)

  try:
    if args.command == "health":
      result = client.health_check()
      print(json.dumps(result, indent=2))

    elif args.command == "query":
      parameters = None
      if args.parameters:
        try:
          parameters = json.loads(args.parameters)
        except json.JSONDecodeError as e:
          logger.error(f"Invalid parameters JSON: {e}")
          return 1

      result = client.query(args.cypher, args.database, parameters)

      if args.format == "json":
        print(json.dumps(result, indent=2))
      else:
        # Table format
        data = result.get("data", [])
        columns = result.get("columns", [])

        if not data:
          print("No results returned.")
          return 0

        # Print column headers
        print(" | ".join(columns))
        print("-" * (len(" | ".join(columns))))

        # Print data rows
        for row in data:
          values = [str(row.get(col, "")) for col in columns]
          print(" | ".join(values))

        print(
          f"\n{len(data)} rows returned in {result.get('execution_time_ms', 0):.2f}ms"
        )

    elif args.command == "info":
      result = client.get_info()
      print(json.dumps(result, indent=2))

    elif args.command == "databases":
      result = client.list_databases()
      print(json.dumps(result, indent=2))

    elif args.command == "ingest":
      result = client.ingest(
        graph_id="sec",  # Default to sec database
        pipeline_run_id=args.pipeline_run_id,
        bucket=args.bucket,
        files=args.files,
        mode="sync",  # CLI defaults to synchronous mode
      )
      print(json.dumps(result, indent=2))

    elif args.command == "task-status":
      result = client.get_task_status(args.task_id)
      print(json.dumps(result, indent=2))

    return 0

  except KuzuAPIError as e:
    logger.error(f"API error: {e}")
    if hasattr(e, "response_data") and e.response_data:
      logger.error(f"Response: {json.dumps(e.response_data, indent=2)}")
    return 1
  except Exception as e:
    logger.error(f"Command failed: {e}")
    return 1
  finally:
    client.close()


if __name__ == "__main__":
  exit(main())
