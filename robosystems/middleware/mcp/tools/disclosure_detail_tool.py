"""
Get Disclosure Detail Tool — Returns facts for a specific disclosure type.

Uses Association Classification → Structure → FactSet → Fact traversal to
return all facts belonging to a specific disclosure category.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class GetDisclosureDetailTool(BaseTool):
  """MCP tool that returns facts for a specific disclosure type."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-disclosure-detail",
      "description": """Return facts for a specific disclosure type (e.g. AssetsRollUp, RevenueBreakdown).

**WHEN TO USE:**
- After using list-disclosures to find available disclosure types
- To drill into a specific disclosure category and see its line items
- To get the actual financial data for a disclosure

**INPUTS:**
- disclosure_type: The classification type from list-disclosures (e.g. 'AssetsRollUp')
- ticker: Optional company filter
- include_dimensions: Include dimensional/segment breakdowns (default: false)

**RETURNS:**
- Facts with element names, values, periods
- Ordered by period end date (most recent first)

**TIP:**
Run list-disclosures first to see valid disclosure_type values.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "disclosure_type": {
            "type": "string",
            "description": "The disclosure classification type (e.g. 'AssetsRollUp', 'RevenueBreakdown'). Use list-disclosures to see available types.",
          },
          "ticker": {
            "type": "string",
            "description": "Optional: filter to facts for a specific company ticker (e.g. 'NVDA')",
          },
          "accession_number": {
            "type": "string",
            "description": "Optional: filter to a specific report/filing by accession number",
          },
          "include_dimensions": {
            "type": "boolean",
            "description": "Include dimensional/segment breakdown facts (default: false)",
            "default": False,
          },
          "limit": {
            "type": "integer",
            "description": "Maximum number of facts to return (default: 100)",
            "default": 100,
          },
        },
        "required": ["disclosure_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("get-disclosure-detail", arguments)

    disclosure_type = arguments.get("disclosure_type", "").strip()
    ticker = (
      arguments.get("ticker", "").strip().upper() if arguments.get("ticker") else None
    )
    accession_number = (
      arguments.get("accession_number", "").strip()
      if arguments.get("accession_number")
      else None
    )
    include_dimensions = arguments.get("include_dimensions", False)
    limit = max(1, min(int(arguments.get("limit", 100)), 1000))

    if not disclosure_type:
      return {"error": "disclosure_type is required"}

    return await self._get_detail(
      disclosure_type, ticker, accession_number, include_dimensions, limit
    )

  async def _get_detail(
    self,
    disclosure_type: str,
    ticker: str | None,
    accession_number: str | None,
    include_dimensions: bool,
    limit: int,
  ) -> dict[str, Any]:
    result: dict[str, Any] = {
      "disclosure_type": disclosure_type,
      "ticker": ticker,
      "accession_number": accession_number,
      "facts": [],
      "fact_count": 0,
    }

    # Build match clause and filters
    match_parts = [
      "(s:Structure)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)"
      "-[:ASSOCIATION_HAS_CLASSIFICATION]->(c:Classification)",
      "(s)-[:STRUCTURE_HAS_FACT_SET]->(fs:FactSet)"
      "-[:FACT_SET_CONTAINS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)",
      "(f)-[:FACT_HAS_PERIOD]->(p:Period)",
    ]

    where_parts = [
      "c.source = 'disclosure_mechanics'",
      "c.type = $disclosure_type",
      "f.numeric_value IS NOT NULL",
    ]

    params: dict[str, Any] = {"disclosure_type": disclosure_type}

    if ticker:
      match_parts.append("(f)-[:FACT_HAS_ENTITY]->(ent:Entity)")
      where_parts.append("ent.ticker = $ticker")
      params["ticker"] = ticker

    if accession_number:
      match_parts.append("(r:Report)-[:REPORT_HAS_FACT]->(f)")
      where_parts.append("r.accession_number = $accession_number")
      params["accession_number"] = accession_number

    if not include_dimensions:
      where_parts.append("f.has_dimensions = false")

    params["limit"] = limit

    query = (
      f"MATCH {', '.join(match_parts)} "
      f"WHERE {' AND '.join(where_parts)} "
      "RETURN DISTINCT e.canonical_concept AS canonical_concept, e.qname AS qname, "
      "e.name AS name, f.numeric_value AS value, f.has_dimensions AS has_dimensions, "
      "p.end_date AS end_date, p.period_type AS period_type, "
      "p.duration_type AS duration_type "
      "ORDER BY end_date DESC "
      "LIMIT $limit"
    )

    try:
      rows = await self.client.execute_query(query, parameters=params)
      if rows:
        for row in rows:
          fact: dict[str, Any] = {
            "canonical_concept": row.get("canonical_concept"),
            "qname": row.get("qname"),
            "name": row.get("name"),
            "value": row.get("value"),
            "end_date": row.get("end_date"),
            "period_type": row.get("period_type"),
            "duration_type": row.get("duration_type"),
          }
          if include_dimensions:
            fact["has_dimensions"] = row.get("has_dimensions")
          result["facts"].append(fact)
        result["fact_count"] = len(result["facts"])
    except Exception as e:
      logger.error(f"Disclosure detail query failed: {e}")
      result["error"] = f"Query failed: {e}"

    return result
