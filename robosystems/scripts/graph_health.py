#!/usr/bin/env python3
# type: ignore
"""
Graph Health Check - Comprehensive validation for LadybugDB graph databases.

Tests both direct database access and Graph API connectivity to ensure
graph databases are properly accessible through all interfaces.

Supports shared repositories (SEC, industry data) and user graphs.

Usage:
    just graph-health sec              # Check SEC repository
    just graph-health kg123abc...      # Check user graph
    just graph-health sec --verbose    # Detailed report
    just graph-health sec --json       # JSON output
"""

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import httpx

from robosystems.logger import logger

# Schema definitions for different repository types
# Each defines the node types and sample queries for that schema
REPOSITORY_SCHEMAS = {
  "sec": {
    "name": "SEC EDGAR",
    "description": "SEC XBRL financial filings",
    "node_counts": {
      "entities": "MATCH (e:Entity) RETURN count(e) as count",
      "reports": "MATCH (r:Report) RETURN count(r) as count",
      "facts": "MATCH (f:Fact) RETURN count(f) as count",
      "elements": "MATCH (e:Element) RETURN count(e) as count",
    },
    "sample_query": "MATCH (e:Entity) RETURN e.name as name, e.ticker as ticker, e.cik as cik LIMIT 5",
    "sample_fields": ["name", "ticker", "cik"],
    "primary_node": "Entity",
  },
  "default": {
    "name": "Generic Graph",
    "description": "User graph database",
    "node_counts": {
      "nodes": "MATCH (n) RETURN count(n) as count",
    },
    "sample_query": "MATCH (n) RETURN labels(n)[0] as label, count(*) as count LIMIT 10",
    "sample_fields": ["label", "count"],
    "primary_node": None,
  },
}


@dataclass
class HealthCheckResult:
  """Result from a health check."""

  status: str  # healthy, empty, unhealthy, error
  connection: bool
  data: dict = field(default_factory=dict)
  errors: list = field(default_factory=list)


class GraphHealthChecker:
  """Comprehensive health checker for LadybugDB graph databases."""

  def __init__(self, graph_id: str, api_url: str = "http://localhost:8001"):
    self.graph_id = graph_id
    self.api_url = api_url
    self.database_path = f"./data/lbug-dbs/{graph_id}.lbug"

    # Determine schema based on graph_id
    if graph_id in REPOSITORY_SCHEMAS:
      self.schema = REPOSITORY_SCHEMAS[graph_id]
    else:
      self.schema = REPOSITORY_SCHEMAS["default"]

    self.results = {
      "timestamp": datetime.now().isoformat(),
      "graph_id": graph_id,
      "schema": self.schema["name"],
      "direct_access": {},
      "api_access": {},
      "comparison": {},
    }

  def check_direct_access(self) -> HealthCheckResult:
    """Check database health via direct LadybugDB connection."""
    logger.info(f"Testing direct database access to {self.graph_id}...")
    result = HealthCheckResult(status="unknown", connection=False)

    # Check if database file exists
    if not Path(self.database_path).exists():
      logger.warning(f"  Database file not found: {self.database_path}")
      result.status = "not_found"
      result.errors.append(f"Database file not found: {self.database_path}")
      return result

    try:
      import real_ladybug as lbug

      db = lbug.Database(self.database_path)
      conn = lbug.Connection(db)
      result.connection = True
      logger.info(f"  Connected to {self.database_path}")

      # Run node count queries
      for name, query in self.schema["node_counts"].items():
        try:
          query_result = conn.execute(query)
          if query_result.has_next():
            count = query_result.get_next()[0]
            result.data[name] = count
            logger.info(f"    {name}: {count:,}")
          else:
            result.data[name] = 0
        except Exception as e:
          logger.warning(f"    Failed to query {name}: {e}")
          result.data[name] = None
          result.errors.append(f"Query {name}: {e!s}")

      # Run relationship count
      try:
        rel_result = conn.execute("MATCH ()-[r]->() RETURN count(r) as count")
        if rel_result.has_next():
          result.data["relationships"] = rel_result.get_next()[0]
          logger.info(f"    relationships: {result.data['relationships']:,}")
      except Exception as e:
        result.errors.append(f"Relationship count: {e!s}")

      # Run sample query
      try:
        sample_result = conn.execute(self.schema["sample_query"])
        samples = []
        while sample_result.has_next():
          row = sample_result.get_next()
          sample = {}
          for i, field in enumerate(self.schema["sample_fields"]):
            sample[field] = row[i] if i < len(row) else None
          samples.append(sample)
        result.data["samples"] = samples

        if samples:
          logger.info(f"    Sample data retrieved ({len(samples)} records)")
      except Exception as e:
        result.errors.append(f"Sample query: {e!s}")

      # Determine status
      primary_count = None
      if self.schema["primary_node"]:
        # Check primary node type count
        for key, count in result.data.items():
          if (
            key != "relationships"
            and key != "samples"
            and count is not None
            and count > 0
          ):
            primary_count = count
            break
      else:
        primary_count = result.data.get("nodes", 0)

      if primary_count and primary_count > 0:
        result.status = "healthy"
      elif result.connection:
        result.status = "empty"
      else:
        result.status = "unhealthy"

      conn.close()
      logger.info(f"  Status: {result.status}")

    except ImportError:
      logger.warning("  real_ladybug not available for direct access")
      result.status = "unavailable"
      result.errors.append("real_ladybug module not available")
    except Exception as e:
      logger.error(f"  Direct access failed: {e}")
      result.status = "error"
      result.errors.append(str(e))

    return result

  async def check_api_access(self) -> HealthCheckResult:
    """Check database health via Graph API."""
    logger.info(f"Testing API access to {self.graph_id}...")
    result = HealthCheckResult(status="unknown", connection=False)

    async with httpx.AsyncClient() as client:
      # Test API connectivity
      try:
        response = await client.get(f"{self.api_url}/health", timeout=10.0)
        if response.status_code == 200:
          result.connection = True
          logger.info(f"  Connected to API at {self.api_url}")
        else:
          result.errors.append(f"Health check: HTTP {response.status_code}")
      except Exception as e:
        logger.error(f"  API connection failed: {e}")
        result.errors.append(f"Connection: {e!s}")
        result.status = "error"
        return result

      # Check if database exists
      try:
        response = await client.get(f"{self.api_url}/databases", timeout=10.0)
        if response.status_code == 200:
          data = response.json()
          databases = [db["graph_id"] for db in data.get("databases", [])]
          if self.graph_id not in databases:
            logger.warning(f"  Database {self.graph_id} not found in API")
            result.status = "not_found"
            result.errors.append(f"Database {self.graph_id} not registered")
            return result
      except Exception as e:
        result.errors.append(f"Database list: {e!s}")

      # Run queries through API
      query_url = f"{self.api_url}/databases/{self.graph_id}/query"

      for name, query in self.schema["node_counts"].items():
        try:
          response = await client.post(
            query_url,
            json={"cypher": query},
            timeout=30.0,
          )
          if response.status_code == 200:
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
              count = data["data"][0].get("count", 0)
              result.data[name] = count
              logger.info(f"    {name}: {count:,}")
            else:
              result.data[name] = 0
          else:
            result.data[name] = None
            result.errors.append(f"Query {name}: HTTP {response.status_code}")
        except Exception as e:
          result.data[name] = None
          result.errors.append(f"Query {name}: {e!s}")

      # Relationship count
      try:
        response = await client.post(
          query_url,
          json={"cypher": "MATCH ()-[r]->() RETURN count(r) as count"},
          timeout=30.0,
        )
        if response.status_code == 200:
          data = response.json()
          if "data" in data and len(data["data"]) > 0:
            result.data["relationships"] = data["data"][0].get("count", 0)
            logger.info(f"    relationships: {result.data['relationships']:,}")
      except Exception as e:
        result.errors.append(f"Relationship count: {e!s}")

      # Sample query
      try:
        response = await client.post(
          query_url,
          json={"cypher": self.schema["sample_query"]},
          timeout=30.0,
        )
        if response.status_code == 200:
          data = response.json()
          if "data" in data:
            result.data["samples"] = data["data"][:5]
            logger.info(
              f"    Sample data retrieved ({len(result.data['samples'])} records)"
            )
      except Exception as e:
        result.errors.append(f"Sample query: {e!s}")

      # Determine status
      has_data = any(
        v is not None and v > 0
        for k, v in result.data.items()
        if k not in ("relationships", "samples")
      )

      if has_data:
        result.status = "healthy"
      elif result.connection:
        result.status = "empty"
      else:
        result.status = "unhealthy"

      logger.info(f"  Status: {result.status}")

    return result

  def compare_results(self, direct: HealthCheckResult, api: HealthCheckResult) -> dict:
    """Compare direct and API access results."""
    comparison = {
      "match": True,
      "discrepancies": [],
      "analysis": "",
    }

    # Compare data counts
    all_keys = set(direct.data.keys()) | set(api.data.keys())
    for key in all_keys:
      if key == "samples":
        continue
      direct_val = direct.data.get(key)
      api_val = api.data.get(key)

      if direct_val != api_val and direct_val is not None and api_val is not None:
        comparison["match"] = False
        comparison["discrepancies"].append(
          {
            "metric": key,
            "direct": direct_val,
            "api": api_val,
          }
        )

    # Analyze
    if comparison["match"]:
      if direct.status == "healthy" and api.status == "healthy":
        comparison["analysis"] = "Both access methods working correctly"
      elif direct.status == "empty" and api.status == "empty":
        comparison["analysis"] = "Database is empty but accessible"
      elif direct.status == "unavailable":
        comparison["analysis"] = "Direct access unavailable, API working"
      else:
        comparison["analysis"] = "Status mismatch but data counts match"
    else:
      comparison["analysis"] = "Discrepancies detected between access methods"

    return comparison

  async def run(self) -> dict:
    """Run comprehensive health check."""
    logger.info("=" * 60)
    logger.info(f"Graph Health Check: {self.graph_id}")
    logger.info(f"Schema: {self.schema['name']}")
    logger.info("=" * 60)

    # Direct access check
    direct_result = self.check_direct_access()
    self.results["direct_access"] = {
      "status": direct_result.status,
      "connection": direct_result.connection,
      "data": direct_result.data,
      "errors": direct_result.errors,
    }

    # API access check
    api_result = await self.check_api_access()
    self.results["api_access"] = {
      "status": api_result.status,
      "connection": api_result.connection,
      "data": api_result.data,
      "errors": api_result.errors,
    }

    # Compare
    self.results["comparison"] = self.compare_results(direct_result, api_result)

    # Overall status (prefer API status)
    if api_result.status == "healthy" or direct_result.status == "healthy":
      overall = "healthy"
    elif api_result.status == "empty" or direct_result.status == "empty":
      overall = "empty"
    elif api_result.status == "not_found" and direct_result.status == "not_found":
      overall = "not_found"
    else:
      overall = "unhealthy"

    self.results["overall_status"] = overall

    logger.info(f"\n{'=' * 60}")
    logger.info(f"Overall Status: {overall.upper()}")
    logger.info(f"{'=' * 60}")

    return self.results

  def print_report(self):
    """Print detailed health report."""
    print(f"\n{'=' * 60}")
    print(f"GRAPH HEALTH REPORT: {self.graph_id}")
    print(f"Schema: {self.schema['name']}")
    print(f"{'=' * 60}")

    # Direct Access
    print("\nDIRECT DATABASE ACCESS:")
    direct = self.results.get("direct_access", {})
    print(f"  Status: {direct.get('status', 'unknown').upper()}")
    print(f"  Path: {self.database_path}")

    if direct.get("data"):
      print("\n  Counts:")
      for key, value in direct["data"].items():
        if key != "samples" and value is not None:
          print(f"    {key}: {value:,}")

    # API Access
    print("\nAPI ACCESS:")
    api = self.results.get("api_access", {})
    print(f"  Status: {api.get('status', 'unknown').upper()}")
    print(f"  Endpoint: {self.api_url}")

    if api.get("data"):
      print("\n  Counts:")
      for key, value in api["data"].items():
        if key != "samples" and value is not None:
          print(f"    {key}: {value:,}")

    # Comparison
    print("\nCOMPARISON:")
    comp = self.results.get("comparison", {})
    print(f"  {comp.get('analysis', 'N/A')}")

    # Samples
    samples = direct.get("data", {}).get("samples") or api.get("data", {}).get(
      "samples"
    )
    if samples:
      print(f"\nSAMPLE DATA ({len(samples)} records):")
      for sample in samples[:5]:
        if isinstance(sample, dict):
          parts = [f"{k}={v}" for k, v in sample.items() if v is not None]
          print(f"  {', '.join(parts)}")

    print(f"\n{'=' * 60}")
    print(f"OVERALL: {self.results.get('overall_status', 'unknown').upper()}")
    print(f"{'=' * 60}\n")


async def main():
  parser = argparse.ArgumentParser(
    description="Graph Health Check - LadybugDB database validation",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
    # Check SEC repository
    %(prog)s sec

    # Check user graph
    %(prog)s kg01234567890abcdef

    # Verbose output
    %(prog)s sec --verbose

    # JSON output
    %(prog)s sec --json
""",
  )

  parser.add_argument(
    "graph_id", help="Graph/repository ID to check (e.g., 'sec', 'kg123...')"
  )
  parser.add_argument(
    "--api-url", default="http://localhost:8001", help="Graph API URL"
  )
  parser.add_argument("--json", action="store_true", help="Output as JSON")
  parser.add_argument("-v", "--verbose", action="store_true", help="Detailed report")

  args = parser.parse_args()

  checker = GraphHealthChecker(args.graph_id, api_url=args.api_url)
  results = await checker.run()

  if args.json:
    print(json.dumps(results, indent=2, default=str))
  elif args.verbose:
    checker.print_report()

  status = results.get("overall_status", "unknown")
  sys.exit(0 if status in ("healthy", "empty") else 1)


if __name__ == "__main__":
  asyncio.run(main())
