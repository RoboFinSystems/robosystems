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
  "equity_statement",
)

# Forms used by domestic filers
ANNUAL_FORMS = ["10-K", "20-F", "40-F"]
# Quarterly forms (include annual since international filers may only have annual)
QUARTERLY_FORMS = ["10-K", "20-F", "40-F", "10-Q"]


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
- equity_statement — Equity components and changes

**AUTO-RESOLVE BEHAVIOR:**
When no report_id is provided, the tool automatically finds the most recent
relevant filing based on period_type:
- annual → latest 10-K, 20-F, or 40-F
- quarterly → latest 10-Q (or 10-K/20-F/40-F for international filers)
- Use fiscal_year to target a specific year

**RETURNS:**
- Facts with element names, values, periods, and period types
- Ordered by period end date (most recent first)
- Deduplicated by element and period (keeps most recent filing)
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
          "report_id": {
            "type": "string",
            "description": (
              "Optional: filter to a specific report by its identifier. "
              "When omitted, the latest relevant filing "
              "is auto-resolved based on period_type."
            ),
          },
          "fiscal_year": {
            "type": "integer",
            "description": (
              "Optional: filter to a specific fiscal year (e.g. 2025). "
              "Used during auto-resolve to find the right filing."
            ),
          },
          "period_type": {
            "type": "string",
            "description": (
              "Filter by period type: 'annual' (10-K/20-F/40-F, duration facts), "
              "'quarterly' (10-Q or annual filings, duration facts), "
              "'instant' (point-in-time facts). Default depends on statement type."
            ),
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
    report_id = (
      arguments.get("report_id", "").strip() if arguments.get("report_id") else None
    )
    fiscal_year = arguments.get("fiscal_year")
    period_type = arguments.get("period_type")
    limit = max(1, min(int(arguments.get("limit", 50)), 1000))

    if not ticker:
      return {"error": "ticker is required"}
    if not statement_type:
      return {"error": "statement_type is required"}
    if statement_type not in VALID_STATEMENT_TYPES:
      return {
        "error": f"Unknown statement_type '{statement_type}'. "
        f"Valid types: {', '.join(VALID_STATEMENT_TYPES)}"
      }

    return await self._get_statement(
      ticker, statement_type, report_id, fiscal_year, period_type, limit
    )

  async def _resolve_report(
    self,
    ticker: str,
    period_type: str | None,
    fiscal_year: int | None,
  ) -> dict[str, Any] | None:
    """Auto-resolve the most recent relevant report for the given filters."""
    # Determine which forms to look for
    if period_type == "annual":
      forms = ANNUAL_FORMS
    elif period_type in ("quarterly", "instant"):
      # quarterly: 10-Q + annual forms for international filers
      # instant: balance sheet data appears in both annual and quarterly filings
      forms = QUARTERLY_FORMS
    else:
      # No period_type specified — default to annual forms
      forms = ANNUAL_FORMS

    where_parts = ["ent.ticker = $ticker", "r.form IN $forms"]
    params: dict[str, Any] = {"ticker": ticker, "forms": forms}

    if fiscal_year is not None:
      where_parts.append("r.fiscal_year_focus = $fiscal_year")
      params["fiscal_year"] = fiscal_year

    query = (
      "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report) "
      f"WHERE {' AND '.join(where_parts)} "
      "RETURN r.identifier AS identifier, "
      "r.form AS form, r.filing_date AS filing_date, "
      "r.fiscal_year_focus AS fiscal_year, r.fiscal_period_focus AS fiscal_period "
      "ORDER BY r.filing_date DESC LIMIT 1"
    )

    try:
      rows = await self.client.execute_query(query, parameters=params)
      if rows:
        return rows[0]
    except Exception as e:
      logger.warning(f"Report auto-resolve failed for {ticker}: {e}")

    # If quarterly didn't find a 10-Q, the company may only file annual forms
    # (international filers with 20-F/40-F). Already covered since QUARTERLY_FORMS
    # includes annual forms.
    return None

  async def _get_statement(
    self,
    ticker: str,
    statement_type: str,
    report_id: str | None,
    fiscal_year: int | None,
    period_type: str | None,
    limit: int,
  ) -> dict[str, Any]:
    resolved_report = None

    # Auto-resolve report when no report_id provided
    if not report_id:
      resolved_report = await self._resolve_report(
        ticker, period_type, fiscal_year
      )
      if resolved_report:
        report_id = resolved_report["identifier"]

    result: dict[str, Any] = {
      "ticker": ticker,
      "statement_type": statement_type,
      "report_id": report_id,
      "facts": [],
      "fact_count": 0,
    }

    if resolved_report:
      result["resolved_report"] = {
        "report_id": resolved_report.get("identifier"),
        "form": resolved_report.get("form"),
        "filing_date": resolved_report.get("filing_date"),
        "fiscal_year": resolved_report.get("fiscal_year"),
        "fiscal_period": resolved_report.get("fiscal_period"),
      }

    # Build match parts and filters
    match_parts = [
      "(s:Structure)-[:STRUCTURE_HAS_FACT_SET]->(fs:FactSet)"
      "-[:FACT_SET_CONTAINS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)",
      "(f)-[:FACT_HAS_PERIOD]->(p:Period)",
      "(f)-[:FACT_HAS_ENTITY]->(ent:Entity)",
    ]

    where_parts = [
      "s.canonical_type = $statement_type",
      "ent.ticker = $ticker",
      "f.has_dimensions = false",
      "f.numeric_value IS NOT NULL",
    ]

    params: dict[str, Any] = {"statement_type": statement_type, "ticker": ticker}

    if report_id:
      match_parts.append("(r:Report)-[:REPORT_HAS_FACT]->(f)")
      where_parts.append("r.identifier = $report_id")
      params["report_id"] = report_id

    # Period filter
    if period_type == "instant":
      where_parts.append("p.period_type = 'instant'")
    elif period_type == "annual":
      where_parts.append("p.duration_type = 'annual'")
    elif period_type == "quarterly":
      where_parts.append("p.duration_type = 'quarterly'")
    elif statement_type == "balance_sheet":
      where_parts.append("p.period_type = 'instant'")

    # Request extra rows for dedup, then trim to limit
    fetch_limit = min(limit * 3, 1000)
    params["limit"] = fetch_limit

    query = (
      f"MATCH {', '.join(match_parts)} "
      f"WHERE {' AND '.join(where_parts)} "
      "RETURN DISTINCT e.canonical_concept AS canonical_concept, e.qname AS qname, "
      "e.name AS name, f.numeric_value AS value, "
      "p.end_date AS end_date, p.period_type AS period_type, "
      "p.duration_type AS duration_type "
      "ORDER BY end_date DESC "
      "LIMIT $limit"
    )

    try:
      rows = await self.client.execute_query(query, parameters=params)
      if rows:
        deduped = self._deduplicate_facts(rows)
        for row in deduped[:limit]:
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

  @staticmethod
  def _deduplicate_facts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate facts by (qname, end_date), keeping the first occurrence.

    Since results are ordered by end_date DESC, the first occurrence for each
    (qname, end_date) pair comes from the most recent filing.
    """
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
      key = (row.get("qname", ""), row.get("end_date", ""))
      if key not in seen:
        seen.add(key)
        deduped.append(row)
    return deduped
