"""
Resolve Structure Tool — Maps financial statement types to XBRL structures.

Supports two modes:
1. Canonical lookup (default): Queries the canonical_type property on Structure
   nodes to find income statements, balance sheets, cash flow statements, etc.
2. Vector search: DuckDB cosine similarity on Structure.embedding for free-text
   queries like "cash flow" or "segment disclosures". Useful when canonical
   classifications are imprecise or when searching for non-standard structures.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class ResolveStructureTool(BaseTool):
  """MCP tool that resolves a statement type to matching XBRL structures."""

  def __init__(self, client):
    super().__init__(client)
    self._enricher = None

  @property
  def enricher(self):
    """Lazy-load the SemanticEnricher."""
    if self._enricher is None:
      from robosystems.adapters.sec.enrichment import SemanticEnricher

      self._enricher = SemanticEnricher()
    return self._enricher

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "resolve-structure",
      "description": """Find XBRL taxonomy structures (statement networks) by type or semantic search.

**WHEN TO USE:**
- To find the income statement, balance sheet, or cash flow statement structures
- Before traversing structure → association → element hierarchies
- To understand how a company's filing is organized
- To find disclosures or non-standard structures by description

**MODES:**
1. **statement_type** (canonical): Pick from a known list of statement types
2. **query** (vector search): Free-text search like "cash flow", "segment information", "revenue disaggregation"

Provide either statement_type OR query, not both.

**STATEMENT TYPES (for canonical mode):**
- income_statement
- balance_sheet
- cash_flow_statement
- equity_statement
- comprehensive_income

**RETURNS:**
- Matching structures with definitions, canonical types, and confidence scores
- Vector search also returns similarity scores
- Filtered to exclude parenthetical variants by default

**TIP:**
Use returned structure identifiers to explore element hierarchies via STRUCTURE_HAS_ASSOCIATION.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "statement_type": {
            "type": "string",
            "description": "Type of financial statement (canonical lookup): 'income_statement', 'balance_sheet', 'cash_flow_statement', 'equity_statement', 'comprehensive_income'",
            "enum": [
              "income_statement",
              "balance_sheet",
              "cash_flow_statement",
              "equity_statement",
              "comprehensive_income",
            ],
          },
          "query": {
            "type": "string",
            "description": "Free-text search for structures by description (vector search). E.g. 'cash flow statement', 'segment disclosures', 'revenue disaggregation'. Use this instead of statement_type for flexible matching.",
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
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("resolve-structure", arguments)

    statement_type = (
      arguments.get("statement_type", "").strip()
      if arguments.get("statement_type")
      else None
    )
    query = arguments.get("query", "").strip() if arguments.get("query") else None
    ticker = (
      arguments.get("ticker", "").strip().upper() if arguments.get("ticker") else None
    )
    accession_number = (
      arguments.get("accession_number", "").strip()
      if arguments.get("accession_number")
      else None
    )
    include_parenthetical = arguments.get("include_parenthetical", False)

    if statement_type and query:
      return {"error": "Provide either statement_type or query, not both"}

    if not statement_type and not query:
      return {"error": "Either statement_type or query is required"}

    if query:
      return await self._resolve_vector(
        query, ticker, accession_number, include_parenthetical
      )

    return await self._resolve_canonical(
      statement_type, ticker, accession_number, include_parenthetical
    )

  # ---------------------------------------------------------------------------
  # Vector search via DuckDB staging
  # ---------------------------------------------------------------------------

  async def _resolve_vector(
    self,
    query: str,
    ticker: str | None,
    accession_number: str | None,
    include_parenthetical: bool,
  ) -> dict[str, Any]:
    """Resolve using DuckDB vector similarity search on staging Structure table."""
    result: dict[str, Any] = {
      "query": query,
      "ticker": ticker,
      "accession_number": accession_number,
      "structures": [],
    }

    # Step 1: Embed the query
    try:
      query_embedding = self.enricher.embed_batch([query])[0]
    except Exception as e:
      logger.error(f"Failed to embed structure query: {e}")
      result["error"] = f"Embedding failed: {e}"
      return result

    # Step 2: DuckDB vector similarity search on staging Structure table
    graph_id = self._get_graph_id()
    try:
      search_sql = (
        "SELECT identifier, definition, name, type, "
        "  canonical_type, canonical_confidence, "
        "  list_cosine_similarity(embedding, $1) AS score "
        "FROM Structure "
        "WHERE embedding IS NOT NULL "
        "ORDER BY score DESC LIMIT 40"
      )
      search_response = await self.client.query_table(
        graph_id=graph_id,
        sql=search_sql,
        parameters=[query_embedding],
      )
      raw_rows = self._table_rows_to_dicts(search_response)
    except Exception as e:
      logger.warning(f"DuckDB structure vector search failed: {e}")
      result["error"] = f"Vector search failed: {e}"
      return result

    if not raw_rows:
      return result

    # Step 3: Filter parentheticals in Python (DuckDB definition column)
    if not include_parenthetical:
      raw_rows = [
        r for r in raw_rows if not self._is_parenthetical(r.get("definition", ""))
      ]

    # Step 4: If ticker or accession_number, filter to structures that belong
    # to matching reports via graph lookup
    if ticker or accession_number:
      # Get the set of structure identifiers that match the report filter
      valid_ids = await self._fetch_structure_ids_for_report(ticker, accession_number)
      if valid_ids is not None:
        filtered = [r for r in raw_rows if r.get("identifier") in valid_ids]
        # Also enrich with report metadata
        report_meta = await self._fetch_report_metadata_for_structures(
          [r["identifier"] for r in filtered if r.get("identifier")],
          ticker,
          accession_number,
        )
        for row in filtered:
          sid = row.get("identifier")
          structure = {
            "identifier": sid,
            "name": row.get("name"),
            "type": row.get("type"),
            "definition": row.get("definition"),
            "canonical_type": row.get("canonical_type"),
            "canonical_confidence": row.get("canonical_confidence"),
            "score": round(row.get("score", 0), 4),
          }
          if sid in report_meta:
            structure.update(report_meta[sid])
          result["structures"].append(structure)
      result["structures"] = result["structures"][:20]
    else:
      for row in raw_rows[:20]:
        result["structures"].append(
          {
            "identifier": row.get("identifier"),
            "name": row.get("name"),
            "type": row.get("type"),
            "definition": row.get("definition"),
            "canonical_type": row.get("canonical_type"),
            "canonical_confidence": row.get("canonical_confidence"),
            "score": round(row.get("score", 0), 4),
          }
        )

    return result

  # ---------------------------------------------------------------------------
  # Canonical lookup (original implementation)
  # ---------------------------------------------------------------------------

  async def _resolve_canonical(
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

  # ---------------------------------------------------------------------------
  # Shared helpers
  # ---------------------------------------------------------------------------

  def _get_graph_id(self) -> str:
    """Get the current graph ID from the client."""
    return getattr(self.client, "_database_name", None) or self.client.graph_id or "sec"

  @staticmethod
  def _table_rows_to_dicts(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert query_table response to list of dicts."""
    columns = response.get("columns", [])
    rows = response.get("rows", [])
    return [dict(zip(columns, row, strict=False)) for row in rows]

  @staticmethod
  def _is_parenthetical(definition: str | None) -> bool:
    """Check if a structure definition is a parenthetical variant."""
    if not definition:
      return False
    return "[Parenthetical]" in definition or "(Parenthetical)" in definition

  async def _fetch_structure_ids_for_report(
    self, ticker: str | None, accession_number: str | None
  ) -> set[str] | None:
    """Fetch structure identifiers that belong to matching reports."""
    try:
      params: dict[str, Any] = {}

      if ticker and accession_number:
        query = (
          "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)"
          "-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)"
          "<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure) "
          "WHERE ent.ticker = $ticker AND r.accession_number = $accession_number "
          "RETURN s.identifier AS id"
        )
        params = {"ticker": ticker, "accession_number": accession_number}
      elif accession_number:
        query = (
          "MATCH (r:Report)-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)"
          "<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure) "
          "WHERE r.accession_number = $accession_number "
          "RETURN s.identifier AS id"
        )
        params = {"accession_number": accession_number}
      elif ticker:
        query = (
          "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)"
          "-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)"
          "<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure) "
          "WHERE ent.ticker = $ticker "
          "RETURN s.identifier AS id"
        )
        params = {"ticker": ticker}
      else:
        return None

      rows = await self.client.execute_query(query, parameters=params) or []
      return {r["id"] for r in rows if r.get("id")}
    except Exception as e:
      logger.warning(f"Failed to fetch structure IDs for report filter: {e}")
      return None

  async def _fetch_report_metadata_for_structures(
    self,
    structure_ids: list[str],
    ticker: str | None,
    accession_number: str | None,
  ) -> dict[str, dict[str, Any]]:
    """Fetch report metadata (accession_number, form, filing_date) for structures."""
    if not structure_ids:
      return {}
    try:
      params: dict[str, Any] = {"structure_ids": structure_ids}

      if ticker:
        query = (
          "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)"
          "-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)"
          "<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure) "
          "WHERE s.identifier IN $structure_ids AND ent.ticker = $ticker "
          "RETURN s.identifier AS id, r.accession_number AS accession_number, "
          "r.form AS form, r.filing_date AS filing_date "
          "ORDER BY r.filing_date DESC"
        )
        params["ticker"] = ticker
      elif accession_number:
        query = (
          "MATCH (r:Report)-[:REPORT_USES_TAXONOMY]->(t:Taxonomy)"
          "<-[:STRUCTURE_HAS_TAXONOMY]-(s:Structure) "
          "WHERE s.identifier IN $structure_ids AND r.accession_number = $accession_number "
          "RETURN s.identifier AS id, r.accession_number AS accession_number, "
          "r.form AS form, r.filing_date AS filing_date"
        )
        params["accession_number"] = accession_number
      else:
        return {}

      rows = await self.client.execute_query(query, parameters=params) or []
      result: dict[str, dict[str, Any]] = {}
      for row in rows:
        sid = row.get("id")
        if sid and sid not in result:  # Keep first (most recent filing_date)
          result[sid] = {
            "accession_number": row.get("accession_number"),
            "form": row.get("form"),
            "filing_date": row.get("filing_date"),
          }
      return result
    except Exception as e:
      logger.debug(f"Report metadata enrichment failed: {e}")
      return {}
