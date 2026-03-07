"""
Resolve Structure Tool — Maps financial statement types to XBRL structures.

Queries the canonical_type property on Structure nodes to find income statements,
balance sheets, cash flow statements, etc.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class ResolveStructureTool(BaseTool):
  """MCP tool that resolves a statement type to matching XBRL structures."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "resolve-structure",
      "description": """Find XBRL taxonomy structures (statement networks) by type.

**WHEN TO USE:**
- To find the income statement, balance sheet, or cash flow statement structures
- Before traversing structure → association → element hierarchies
- To understand how a company's filing is organized

**STATEMENT TYPES:**
- income_statement
- balance_sheet
- cash_flow_statement
- equity_statement
- comprehensive_income

**RETURNS:**
- Matching structures with definitions, canonical types, and confidence scores
- Filtered to exclude parenthetical variants by default

**TIP:**
Use returned structure identifiers to explore element hierarchies via STRUCTURE_HAS_ASSOCIATION.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "statement_type": {
            "type": "string",
            "description": "Type of financial statement: 'income_statement', 'balance_sheet', 'cash_flow_statement', 'equity_statement', 'comprehensive_income'",
            "enum": [
              "income_statement",
              "balance_sheet",
              "cash_flow_statement",
              "equity_statement",
              "comprehensive_income",
            ],
          },
          "ticker": {
            "type": "string",
            "description": "Optional: filter to structures from a specific company's filings (e.g. 'NVDA')",
          },
          "accession_number": {
            "type": "string",
            "description": "Optional: filter to structures from a specific filing (e.g. '0001045810-25-000023')",
          },
          "include_parenthetical": {
            "type": "boolean",
            "description": "Include parenthetical variants (default: false)",
            "default": False,
          },
        },
        "required": ["statement_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("resolve-structure", arguments)

    statement_type = arguments.get("statement_type", "").strip()
    ticker = (
      arguments.get("ticker", "").strip().upper() if arguments.get("ticker") else None
    )
    accession_number = (
      arguments.get("accession_number", "").strip()
      if arguments.get("accession_number")
      else None
    )
    include_parenthetical = arguments.get("include_parenthetical", False)

    if not statement_type:
      return {"error": "statement_type is required"}

    return await self._resolve(
      statement_type, ticker, accession_number, include_parenthetical
    )

  async def _resolve(
    self,
    statement_type: str,
    ticker: str | None,
    accession_number: str | None,
    include_parenthetical: bool,
  ) -> dict[str, Any]:
    result: dict[str, Any] = {
      "statement_type": statement_type,
      "ticker": ticker,
      "accession_number": accession_number,
      "structures": [],
    }

    # Build query
    parenthetical_filter = ""
    if not include_parenthetical:
      parenthetical_filter = (
        ' AND NOT s.definition CONTAINS "[Parenthetical]"'
        ' AND NOT s.definition CONTAINS "(Parenthetical)"'
      )

    # Determine if we need to join through Report → Taxonomy
    needs_report_join = ticker or accession_number

    params: dict[str, Any] = {"statement_type": statement_type}

    if needs_report_join:
      # Filter via entity → report → taxonomy → structure
      match_clause = (
        "MATCH (r:Report)-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)"
        "<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure) "
      )
      where_parts = ["s.canonical_type = $statement_type"]

      if accession_number:
        where_parts.append("r.accession_number = $accession_number")
        params["accession_number"] = accession_number

      if ticker:
        # Add entity join for ticker filtering
        match_clause = (
          "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)"
          "-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)"
          "<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure) "
        )
        where_parts.append("ent.ticker = $ticker")
        params["ticker"] = ticker

      query = (
        f"{match_clause}"
        f"WHERE {' AND '.join(where_parts)}"
        f"{parenthetical_filter} "
        "RETURN s.identifier AS id, s.definition AS definition, "
        "s.name AS name, s.type AS type, s.number AS number, "
        "s.canonical_type AS canonical_type, "
        "s.canonical_confidence AS canonical_confidence, "
        "r.accession_number AS accession_number, "
        "r.form AS form, r.filing_date AS filing_date "
        "ORDER BY r.filing_date DESC LIMIT 20"
      )
    else:
      query = (
        "MATCH (s:Structure) "
        "WHERE s.canonical_type = $statement_type"
        f"{parenthetical_filter} "
        "RETURN s.identifier AS id, s.definition AS definition, "
        "s.name AS name, s.type AS type, s.number AS number, "
        "s.canonical_type AS canonical_type, "
        "s.canonical_confidence AS canonical_confidence "
        "ORDER BY s.canonical_confidence DESC LIMIT 20"
      )

    try:
      rows = await self.client.execute_query(query, parameters=params)
      if rows:
        for row in rows:
          structure = {
            "identifier": row.get("id"),
            "name": row.get("name"),
            "type": row.get("type"),
            "definition": row.get("definition"),
            "canonical_type": row.get("canonical_type"),
            "canonical_confidence": row.get("canonical_confidence"),
          }
          if needs_report_join:
            structure["accession_number"] = row.get("accession_number")
            structure["form"] = row.get("form")
            structure["filing_date"] = row.get("filing_date")
          result["structures"].append(structure)
    except Exception as e:
      logger.error(f"Structure resolution query failed: {e}")
      result["error"] = f"Query failed: {e}"

    return result
