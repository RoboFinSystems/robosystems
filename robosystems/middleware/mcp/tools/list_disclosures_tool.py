"""
List Disclosures Tool — Lists disclosure types from Association Classifications.

Uses the Association Classification system to enumerate disclosure types
(e.g. AssetsRollUp, RevenueBreakdown) with fact counts.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class ListDisclosuresTool(BaseTool):
  """MCP tool that lists all disclosure types in the graph with counts."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-disclosures",
      "description": """List all disclosure types available in the graph, with counts.

**WHEN TO USE:**
- To discover what kinds of disclosures are available (e.g. roll-ups, breakdowns, schedules)
- Before using get-disclosure-detail to know which disclosure_type values are valid
- To understand the breadth of structured data in the graph

**WITH TICKER:**
- Returns disclosure types filtered to a specific company, with fact counts

**WITHOUT TICKER:**
- Returns all disclosure types across the entire graph, with association counts

**RETURNS:**
- List of disclosure types with counts, ordered by count descending
- Total number of distinct disclosure types

**TIP:**
Use the returned disclosure_type values as input to get-disclosure-detail.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "ticker": {
            "type": "string",
            "description": "Optional: filter to disclosures for a specific company ticker (e.g. 'NVDA')",
          },
        },
        "required": [],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("list-disclosures", arguments)

    ticker = (
      arguments.get("ticker", "").strip().upper() if arguments.get("ticker") else None
    )

    return await self._list(ticker)

  async def _list(self, ticker: str | None) -> dict[str, Any]:
    result: dict[str, Any] = {
      "ticker": ticker,
      "disclosures": [],
      "total_disclosures": 0,
    }

    if ticker:
      query = (
        "MATCH (s:Structure)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)"
        "-[:ASSOCIATION_HAS_CLASSIFICATION]->(c:Classification), "
        "(s)-[:STRUCTURE_HAS_FACT_SET]->(fs:FactSet)"
        "-[:FACT_SET_CONTAINS_FACT]->(f:Fact)-[:FACT_HAS_ENTITY]->(ent:Entity) "
        "WHERE c.source = 'disclosure_mechanics' AND ent.ticker = $ticker "
        "RETURN c.type AS disclosure_type, count(DISTINCT f) AS fact_count "
        "ORDER BY fact_count DESC"
      )
    else:
      query = (
        "MATCH (a:Association)-[:ASSOCIATION_HAS_CLASSIFICATION]->(c:Classification) "
        "WHERE c.source = 'disclosure_mechanics' "
        "RETURN c.type AS disclosure_type, count(DISTINCT a) AS association_count "
        "ORDER BY association_count DESC"
      )

    try:
      rows = await self.client.execute_query(
        query,
        parameters={"ticker": ticker} if ticker else None,
      )
      if rows:
        for row in rows:
          disclosure = {"type": row.get("disclosure_type")}
          if ticker:
            disclosure["count"] = row.get("fact_count", 0)
          else:
            disclosure["count"] = row.get("association_count", 0)
          result["disclosures"].append(disclosure)
        result["total_disclosures"] = len(result["disclosures"])
    except Exception as e:
      logger.error(f"List disclosures query failed: {e}")
      result["error"] = f"Query failed: {e}"

    return result
