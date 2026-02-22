"""
Get Financial Statement Tool — Returns structured financial data via FactSets.

Uses Structure → FactSet → Fact traversal (one-hop from FactSet) instead of
the 6-hop taxonomy crawl. Requires that the graph has been enriched with
Structure-level FactSets and canonical_type classification.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool

VALID_STATEMENT_TYPES = (
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "stockholders_equity",
)


class GetFinancialStatementTool(BaseTool):
  """MCP tool that returns a structured financial statement for a company/period."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-financial-statement",
      "description": """Return a structured financial statement (income statement, balance sheet, etc.) for a company.

**WHEN TO USE:**
- When the user asks for a financial statement by name (e.g. "show me NVDA's income statement")
- To get all line items for a specific statement type and company
- When you need structured financial data without writing Cypher

**STATEMENT TYPES:**
- income_statement — Revenue, expenses, net income
- balance_sheet — Assets, liabilities, equity (instant periods)
- cash_flow_statement — Operating, investing, financing activities
- stockholders_equity — Equity components and changes

**RETURNS:**
- Facts with element names, values, periods, and period types
- Ordered by period end date (most recent first)
- Filters out dimensional/segment breakdowns by default

**TIP:**
For balance sheets, only instant-period facts are returned. For other statements, duration-period facts are returned by default. Use period_type to override.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "ticker": {
            "type": "string",
            "description": "Company ticker symbol (e.g. 'NVDA', 'AAPL')",
          },
          "statement_type": {
            "type": "string",
            "description": "Type of financial statement",
            "enum": list(VALID_STATEMENT_TYPES),
          },
          "period_type": {
            "type": "string",
            "description": "Filter by period type: 'annual' (duration facts only), 'quarterly' (duration facts only), 'instant' (point-in-time facts). Default depends on statement type.",
            "enum": ["annual", "quarterly", "instant"],
          },
          "limit": {
            "type": "integer",
            "description": "Maximum number of facts to return (default: 50)",
            "default": 50,
          },
        },
        "required": ["ticker", "statement_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("get-financial-statement", arguments)

    ticker = arguments.get("ticker", "").strip().upper()
    statement_type = arguments.get("statement_type", "").strip()
    period_type = arguments.get("period_type")
    limit = arguments.get("limit", 50)

    if not ticker:
      return {"error": "ticker is required"}
    if not statement_type:
      return {"error": "statement_type is required"}
    if statement_type not in VALID_STATEMENT_TYPES:
      return {
        "error": f"Unknown statement_type '{statement_type}'. "
        f"Valid types: {', '.join(VALID_STATEMENT_TYPES)}"
      }

    return await self._get_statement(ticker, statement_type, period_type, limit)

  async def _get_statement(
    self,
    ticker: str,
    statement_type: str,
    period_type: str | None,
    limit: int,
  ) -> dict[str, Any]:
    result: dict[str, Any] = {
      "ticker": ticker,
      "statement_type": statement_type,
      "facts": [],
      "fact_count": 0,
    }

    # Build period filter
    period_filter = ""
    if period_type == "instant":
      period_filter = " AND p.period_type = 'instant'"
    elif period_type == "annual":
      period_filter = " AND p.duration_type = 'annual'"
    elif period_type == "quarterly":
      period_filter = " AND p.duration_type = 'quarterly'"
    elif statement_type == "balance_sheet":
      # Balance sheets default to instant periods
      period_filter = " AND p.period_type = 'instant'"

    query = (
      "MATCH (s:Structure)-[:STRUCTURE_HAS_FACT_SET]->(fs:FactSet)"
      "-[:FACT_SET_CONTAINS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
      "(f)-[:FACT_HAS_PERIOD]->(p:Period), "
      "(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
      f"WHERE s.canonical_type = $statement_type"
      f" AND ent.ticker = $ticker"
      f" AND f.has_dimensions = false"
      f" AND f.numeric_value IS NOT NULL"
      f"{period_filter} "
      "RETURN DISTINCT e.canonical_concept AS canonical_concept, e.qname AS qname, "
      "e.name AS name, f.numeric_value AS value, "
      "p.end_date AS end_date, p.period_type AS period_type, "
      "p.duration_type AS duration_type "
      "ORDER BY end_date DESC "
      f"LIMIT {limit}"
    )

    try:
      rows = await self.client.execute_query(
        query,
        parameters={"statement_type": statement_type, "ticker": ticker},
      )
      if rows:
        for row in rows:
          result["facts"].append(
            {
              "canonical_concept": row.get("canonical_concept"),
              "qname": row.get("qname"),
              "name": row.get("name"),
              "value": row.get("value"),
              "end_date": row.get("end_date"),
              "period_type": row.get("period_type"),
              "duration_type": row.get("duration_type"),
            }
          )
        result["fact_count"] = len(result["facts"])
    except Exception as e:
      logger.error(f"Financial statement query failed: {e}")
      result["error"] = f"Query failed: {e}"

    if not result["facts"] and "error" not in result:
      result["tip"] = (
        f"No facts found for {ticker} {statement_type}. "
        "The company may not be loaded, or try a different statement_type."
      )

    return result
