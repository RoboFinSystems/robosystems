#!/usr/bin/env python3
"""
SEC Local Health Check - Comprehensive validation tool

Tests both direct database access and Graph API connectivity to ensure
the SEC database is properly accessible through all interfaces.
"""

import asyncio
import sys
from typing import Dict, Any
import kuzu
import httpx
import json
from datetime import datetime

from robosystems.logger import logger


class SECHealthChecker:
  """Comprehensive health checker for SEC database."""

  def __init__(self):
    """Initialize the health checker."""
    self.database_path = "./data/kuzu-dbs/sec.kuzu"
    self.api_url = "http://localhost:8001"
    self.results = {
      "timestamp": datetime.now().isoformat(),
      "direct_access": {},
      "api_access": {},
      "comparison": {},
    }

  def check_direct_access(self) -> Dict[str, Any]:
    """
    Check database health via direct Kuzu connection.

    Returns:
        Dictionary with direct access test results
    """
    logger.info("🔍 Testing direct database access...")
    results = {
      "status": "unknown",
      "connection": False,
      "data": {},
      "errors": [],
    }

    try:
      # Connect directly to database file
      db = kuzu.Database(self.database_path)
      conn = kuzu.Connection(db)
      results["connection"] = True
      logger.info(f"  ✅ Connected to {self.database_path}")

      # Define queries to test
      queries = {
        "entities": "MATCH (e:Entity) RETURN count(e) as count",
        "reports": "MATCH (r:Report) RETURN count(r) as count",
        "facts": "MATCH (f:Fact) RETURN count(f) as count LIMIT 1",
        "elements": "MATCH (e:Element) RETURN count(e) as count",
        "relationships": "MATCH ()-[r]->() RETURN count(r) as count LIMIT 1",
      }

      # Execute each query
      for name, query in queries.items():
        try:
          result = conn.execute(query)
          if result.has_next():
            count = result.get_next()[0]
            results["data"][name] = count
            logger.info(f"    {name}: {count:,}")
          else:
            results["data"][name] = 0
        except Exception as e:
          logger.warning(f"    Failed to query {name}: {e}")
          results["data"][name] = None
          results["errors"].append(f"Query {name}: {str(e)}")

      # Get sample companies
      try:
        result = conn.execute(
          "MATCH (e:Entity) RETURN e.name as name, e.ticker as ticker, e.cik as cik LIMIT 5"
        )
        companies = []
        while result.has_next():
          row = result.get_next()
          companies.append(
            {
              "name": row[0],
              "ticker": row[1],
              "cik": row[2],
            }
          )
        results["data"]["sample_companies"] = companies
        for company in companies:
          logger.info(f"    Company: {company['ticker']} - {company['name']}")
      except Exception as e:
        logger.warning(f"    Failed to get sample companies: {e}")
        results["errors"].append(f"Sample companies: {str(e)}")

      # Determine overall status
      if results["data"].get("entities", 0) > 0:
        results["status"] = "healthy"
      elif results["connection"]:
        results["status"] = "empty"
      else:
        results["status"] = "unhealthy"

      conn.close()
      logger.info(f"  Status: {results['status']}")

    except Exception as e:
      logger.error(f"  ❌ Direct access failed: {e}")
      results["status"] = "error"
      results["errors"].append(str(e))

    return results

  async def check_api_access(self) -> Dict[str, Any]:
    """
    Check database health via Graph API.

    Returns:
        Dictionary with API access test results
    """
    logger.info("🔍 Testing API access...")
    results = {
      "status": "unknown",
      "connection": False,
      "data": {},
      "errors": [],
    }

    async with httpx.AsyncClient() as client:
      try:
        # Test API connectivity
        health_url = f"{self.api_url}/health"
        response = await client.get(health_url)
        if response.status_code == 200:
          results["connection"] = True
          logger.info(f"  ✅ Connected to API at {self.api_url}")
        else:
          logger.warning(f"  ⚠️ API health check returned {response.status_code}")
          results["errors"].append(f"Health check: HTTP {response.status_code}")

      except Exception as e:
        logger.error(f"  ❌ API connection failed: {e}")
        results["errors"].append(f"Connection: {str(e)}")
        return results

      # Define queries to test
      queries = {
        "entities": "MATCH (e:Entity) RETURN count(e) as count",
        "reports": "MATCH (r:Report) RETURN count(r) as count",
        "facts": "MATCH (f:Fact) RETURN count(f) as count LIMIT 1",
        "elements": "MATCH (e:Element) RETURN count(e) as count",
        "relationships": "MATCH ()-[r]->() RETURN count(r) as count LIMIT 1",
      }

      # Execute each query through API
      query_url = f"{self.api_url}/databases/sec/query"
      for name, query in queries.items():
        try:
          response = await client.post(
            query_url,
            json={"database": "sec", "cypher": query},
            timeout=30.0,
          )
          if response.status_code == 200:
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
              count = data["data"][0].get("count", 0)
              results["data"][name] = count
              logger.info(f"    {name}: {count:,}")
            else:
              results["data"][name] = 0
          else:
            logger.warning(f"    Query {name} returned {response.status_code}")
            results["data"][name] = None
            results["errors"].append(f"Query {name}: HTTP {response.status_code}")
        except Exception as e:
          logger.warning(f"    Failed to query {name}: {e}")
          results["data"][name] = None
          results["errors"].append(f"Query {name}: {str(e)}")

      # Get sample companies through API
      try:
        response = await client.post(
          query_url,
          json={
            "database": "sec",
            "cypher": "MATCH (e:Entity) RETURN e.name as name, e.ticker as ticker, e.cik as cik LIMIT 5",
          },
          timeout=30.0,
        )
        if response.status_code == 200:
          data = response.json()
          if "data" in data:
            companies = []
            for row in data["data"]:
              companies.append(
                {
                  "name": row.get("name"),
                  "ticker": row.get("ticker"),
                  "cik": row.get("cik"),
                }
              )
            results["data"]["sample_companies"] = companies
            for company in companies:
              if company["name"]:
                logger.info(f"    Company: {company['ticker']} - {company['name']}")
        else:
          logger.warning(f"    Sample companies query returned {response.status_code}")
          results["errors"].append(f"Sample companies: HTTP {response.status_code}")
      except Exception as e:
        logger.warning(f"    Failed to get sample companies: {e}")
        results["errors"].append(f"Sample companies: {str(e)}")

      # Determine overall status
      if results["data"].get("entities", 0) > 0:
        results["status"] = "healthy"
      elif results["connection"]:
        results["status"] = "empty"
      else:
        results["status"] = "unhealthy"

      logger.info(f"  Status: {results['status']}")

    return results

  def compare_results(self, direct: Dict, api: Dict) -> Dict[str, Any]:
    """
    Compare direct and API access results.

    Args:
        direct: Direct access results
        api: API access results

    Returns:
        Comparison analysis
    """
    comparison = {
      "match": True,
      "discrepancies": [],
      "analysis": "",
    }

    # Compare data counts
    for key in ["entities", "reports", "facts", "elements", "relationships"]:
      direct_val = direct.get("data", {}).get(key)
      api_val = api.get("data", {}).get(key)

      if direct_val != api_val:
        comparison["match"] = False
        comparison["discrepancies"].append(
          {
            "metric": key,
            "direct": direct_val,
            "api": api_val,
          }
        )

    # Analyze the situation
    if comparison["match"]:
      if direct["status"] == "healthy" and api["status"] == "healthy":
        comparison["analysis"] = "✅ Both access methods working perfectly"
      elif direct["status"] == "empty" and api["status"] == "empty":
        comparison["analysis"] = "⚠️ Database is empty but accessible"
      else:
        comparison["analysis"] = "⚠️ Status mismatch but data counts match"
    else:
      if direct["status"] == "healthy" and api["status"] != "healthy":
        comparison["analysis"] = (
          "❌ API access issue - data visible directly but not through API"
        )
      elif direct["status"] != "healthy" and api["status"] == "healthy":
        comparison["analysis"] = (
          "⚠️ Direct access issue - API working but direct access failing"
        )
      else:
        comparison["analysis"] = "❌ Discrepancies detected between access methods"

    return comparison

  async def run_health_check(self) -> Dict[str, Any]:
    """
    Run comprehensive health check.

    Returns:
        Complete health check results
    """
    logger.info("=" * 60)
    logger.info("SEC Database Comprehensive Health Check")
    logger.info("=" * 60)

    # Run direct access check
    self.results["direct_access"] = self.check_direct_access()

    # Run API access check
    self.results["api_access"] = await self.check_api_access()

    # Compare results
    self.results["comparison"] = self.compare_results(
      self.results["direct_access"], self.results["api_access"]
    )

    # Print comparison
    logger.info("\n📊 Comparison Results:")
    logger.info(f"  {self.results['comparison']['analysis']}")

    if self.results["comparison"]["discrepancies"]:
      logger.warning("\n  Discrepancies found:")
      for disc in self.results["comparison"]["discrepancies"]:
        logger.warning(
          f"    {disc['metric']}: Direct={disc['direct']}, API={disc['api']}"
        )

    # Overall health determination
    # Priority: API health is primary indicator (direct access expected to fail when API running)
    if self.results["api_access"]["status"] == "healthy":
      overall = "healthy"
      symbol = "✅"
    elif self.results["direct_access"]["status"] == "healthy":
      overall = "healthy"
      symbol = "✅"
    elif (
      self.results["api_access"]["status"] == "empty"
      or self.results["direct_access"]["status"] == "empty"
    ):
      overall = "empty"
      symbol = "⚠️"
    else:
      overall = "unhealthy"
      symbol = "❌"

    logger.info("\n" + "=" * 60)
    logger.info(f"Overall Health: {symbol} {overall.upper()}")
    logger.info("=" * 60)

    self.results["overall_status"] = overall
    return self.results

  def print_detailed_report(self):
    """Print detailed health report in a formatted way."""
    print("\n" + "=" * 60)
    print("SEC DATABASE HEALTH REPORT")
    print("=" * 60)

    # Direct Access Section
    print("\n📁 DIRECT DATABASE ACCESS:")
    direct = self.results.get("direct_access", {})
    print(f"  Status: {direct.get('status', 'unknown').upper()}")
    print(f"  Connection: {'✅' if direct.get('connection') else '❌'}")

    if direct.get("data"):
      print("\n  Data Counts:")
      for key, value in direct["data"].items():
        if key != "sample_companies" and value is not None:
          print(f"    {key.capitalize()}: {value:,}")

    if direct.get("errors"):
      print("\n  Errors:")
      for error in direct["errors"]:
        print(f"    - {error}")

    # API Access Section
    print("\n🌐 API ACCESS:")
    api = self.results.get("api_access", {})
    print(f"  Status: {api.get('status', 'unknown').upper()}")
    print(f"  Connection: {'✅' if api.get('connection') else '❌'}")
    print(f"  Endpoint: {self.api_url}")

    if api.get("data"):
      print("\n  Data Counts:")
      for key, value in api["data"].items():
        if key != "sample_companies" and value is not None:
          print(f"    {key.capitalize()}: {value:,}")

    if api.get("errors"):
      print("\n  Errors:")
      for error in api["errors"]:
        print(f"    - {error}")

    # Comparison Section
    print("\n🔄 COMPARISON:")
    comp = self.results.get("comparison", {})
    print(f"  {comp.get('analysis', 'No comparison available')}")

    if comp.get("discrepancies"):
      print("\n  Discrepancies:")
      for disc in comp["discrepancies"]:
        print(f"    {disc['metric']}: Direct={disc['direct']}, API={disc['api']}")

    # Sample Companies (if available)
    companies = direct.get("data", {}).get("sample_companies") or api.get(
      "data", {}
    ).get("sample_companies")
    if companies:
      print("\n📊 SAMPLE COMPANIES:")
      for company in companies:
        if company.get("name"):
          print(f"  {company['ticker']} - {company['name']} (CIK: {company['cik']})")

    print("\n" + "=" * 60)
    print(f"OVERALL STATUS: {self.results.get('overall_status', 'unknown').upper()}")
    print("=" * 60 + "\n")


async def main():
  """Main entry point for health check."""
  import argparse

  parser = argparse.ArgumentParser(
    description="SEC Database Comprehensive Health Check",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Run standard health check
  %(prog)s
  
  # Output JSON format
  %(prog)s --json
  
  # Verbose output with detailed report
  %(prog)s --verbose
""",
  )

  parser.add_argument("--json", action="store_true", help="Output results as JSON")
  parser.add_argument("--verbose", action="store_true", help="Show detailed report")

  args = parser.parse_args()

  # Run health check
  checker = SECHealthChecker()
  results = await checker.run_health_check()

  # Output results
  if args.json:
    print(json.dumps(results, indent=2, default=str))
  elif args.verbose:
    checker.print_detailed_report()

  # Exit with appropriate code
  status = results.get("overall_status", "unknown")
  if status == "healthy":
    sys.exit(0)
  elif status == "empty":
    sys.exit(0)  # Empty is not an error
  else:
    sys.exit(1)


if __name__ == "__main__":
  asyncio.run(main())
