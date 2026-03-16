"""
Data operation MCP tools for financial reporting.

Provides tools for:
- Fact grid construction
"""

from typing import Any

from robosystems.logger import logger


class BuildFactGridTool:
  """Build multidimensional fact grid from graph data."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "build-fact-grid",
      "description": """Construct multidimensional fact grid from graph data. Retrieves facts based on elements, periods, and optional dimensions. Returns structured data with element names, values, and periods. Use include_summary=true to add aggregated statistics (count, total, avg, min, max) by element.

**WHEN TO USE:**
- When the user asks to compare specific financial metrics across periods or companies
- To build custom cross-company comparisons (e.g., "compare NVDA and AAPL total assets")
- When you need precise control over which elements, periods, and entities to include
- For ad-hoc financial analysis that doesn't fit a standard statement format

**HOW IT DIFFERS FROM get-financial-statement:**
- get-financial-statement returns ALL line items for a standard statement (income statement, balance sheet, etc.)
- build-fact-grid returns SPECIFIC elements you choose, across any combination of periods and entities

**INPUTS:**
- elements: XBRL qnames (e.g., 'us-gaap:Assets'). Use resolve-element to find qnames.
- canonical_concepts: Normalized concept names (e.g., 'revenue', 'net_income'). Matches ALL element qnames mapped to each concept across companies. Can be combined with elements.
- periods: End dates in YYYY-MM-DD format
- entity/entities: Filter by ticker, CIK, or name. Use 'entities' array for multi-company.
- period_type: 'annual', 'quarterly', or 'instant'. Important for duration elements (revenue, net income).
- form: SEC filing type ('10-K', '10-Q')
- fiscal_year/fiscal_period: Filter by report fiscal context

**RETURNS:**
- Deduplicated facts with element qnames, names, values, periods, and units
- Entity ticker and name included when entity filters are used
- Only consolidated totals (dimensional breakdowns excluded)

**TIP:**
For income statement items (revenue, net income), always specify period_type='annual' or 'quarterly' to avoid mixing duration types. Use canonical_concepts for cross-company comparisons where companies may use different XBRL tags for the same concept.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "elements": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Element qnames to include in the grid (e.g., 'us-gaap:Assets', 'us-gaap:Revenues'). Can be combined with canonical_concepts.",
          },
          "canonical_concepts": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Canonical concept names (e.g., 'revenue', 'net_income', 'total_assets'). Matches ALL element qnames mapped to each concept, solving cross-company variation (e.g., 'revenue' matches both us-gaap:Revenues and us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax). Can be combined with elements.",
          },
          "periods": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Period end dates (YYYY-MM-DD format) or quarters (YYYY-QN)",
          },
          "entity": {
            "type": "string",
            "description": "Filter by entity ticker (e.g., 'NVDA'), CIK, or name. Matches against Entity ticker, cik, and name properties.",
          },
          "entities": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Filter by multiple entity tickers (e.g., ['NVDA', 'AAPL']). Use instead of 'entity' for multi-company comparisons.",
          },
          "form": {
            "type": "string",
            "description": "Filter by SEC filing form type (e.g., '10-K', '10-Q')",
          },
          "fiscal_year": {
            "type": "integer",
            "description": "Filter by fiscal year (e.g., 2024). Uses Report.fiscal_year_focus.",
          },
          "fiscal_period": {
            "type": "string",
            "description": "Filter by fiscal period (e.g., 'FY', 'Q1', 'Q2', 'Q3'). Uses Report.fiscal_period_focus.",
          },
          "period_type": {
            "type": "string",
            "description": "Filter by period type: 'annual' (duration facts only), 'quarterly' (duration facts only), 'instant' (point-in-time facts). Default depends on statement type.",
            "enum": ["annual", "quarterly", "instant"],
          },
          "dimensions": {
            "type": "object",
            "description": "Optional dimensional filters (e.g., segment, geography)",
            "default": {},
          },
          "rows": {
            "type": "array",
            "description": "Optional axis configuration for rows",
            "default": [],
          },
          "columns": {
            "type": "array",
            "description": "Optional axis configuration for columns",
            "default": [],
          },
          "include_summary": {
            "type": "boolean",
            "description": "Include summary statistics (count, total, avg, min, max) by element",
            "default": False,
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    """
    Execute fact grid construction using FactGridBuilder.

    Args:
        arguments: Tool arguments with elements, periods, dimensions

    Returns:
        Dict with fact grid data and metadata
    """
    elements = arguments.get("elements", [])
    canonical_concepts = arguments.get("canonical_concepts", [])
    periods = arguments.get("periods", [])
    entity = arguments.get("entity")
    entities = arguments.get("entities")
    form = arguments.get("form")
    fiscal_year = arguments.get("fiscal_year")
    fiscal_period = arguments.get("fiscal_period")
    period_type = arguments.get("period_type")
    rows = arguments.get("rows", [])
    columns = arguments.get("columns", [])
    include_summary = arguments.get("include_summary", False)

    if not elements and not canonical_concepts:
      return {
        "error": "missing_elements",
        "message": "Provide elements (qnames) and/or canonical_concepts",
      }

    if not periods and not period_type and fiscal_year is None:
      return {
        "error": "missing_period_filter",
        "message": "Provide periods, period_type, or fiscal_year to scope the query",
      }

    # Validate rows and columns structure
    if rows and not isinstance(rows, list):
      return {"error": "invalid_rows", "message": "Rows must be a list"}

    if columns and not isinstance(columns, list):
      return {"error": "invalid_columns", "message": "Columns must be a list"}

    # Validate each row/column config is a dict with required fields
    for i, row in enumerate(rows):
      if not isinstance(row, dict):
        return {
          "error": "invalid_row_config",
          "message": f"Row {i} must be a dictionary with axis configuration",
        }

    for i, col in enumerate(columns):
      if not isinstance(col, dict):
        return {
          "error": "invalid_column_config",
          "message": f"Column {i} must be a dictionary with axis configuration",
        }

    try:
      # Build parameterized Cypher query with comma-separated patterns
      # in a single MATCH clause for performance
      match_parts = [
        "(f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)",
        "(f)-[:FACT_HAS_PERIOD]->(p:Period)",
        "(f)-[:FACT_HAS_UNIT]->(u:Unit)",
      ]
      where_clauses = [
        "f.has_dimensions = false",
      ]
      parameters: dict[str, Any] = {}

      # Element filter: qnames, canonical concepts, or both
      if elements and canonical_concepts:
        where_clauses.append(
          "(el.qname IN $elements OR el.canonical_concept IN $canonical_concepts)"
        )
        parameters["elements"] = elements
        parameters["canonical_concepts"] = canonical_concepts
      elif elements:
        where_clauses.append("el.qname IN $elements")
        parameters["elements"] = elements
      elif canonical_concepts:
        where_clauses.append("el.canonical_concept IN $canonical_concepts")
        parameters["canonical_concepts"] = canonical_concepts

      if periods:
        where_clauses.append("p.end_date IN $periods")
        parameters["periods"] = periods

      if period_type:
        if period_type == "instant":
          where_clauses.append("p.period_type = 'instant'")
        elif period_type == "annual":
          where_clauses.append("p.duration_type = 'annual'")
        elif period_type == "quarterly":
          where_clauses.append("p.duration_type = 'quarterly'")

      # Normalize entity filter: single 'entity' or multi 'entities'
      entity_list = entities if entities else ([entity] if entity else None)

      if entity_list:
        match_parts.append("(f)-[:FACT_HAS_ENTITY]->(ent:Entity)")
        where_clauses.append(
          "(ent.ticker IN $entities OR ent.cik IN $entities OR ent.name IN $entities)"
        )
        parameters["entities"] = entity_list

      if form or fiscal_year is not None or fiscal_period:
        match_parts.append("(r:Report)-[:REPORT_HAS_FACT]->(f)")
        if form:
          where_clauses.append("r.form = $form")
          parameters["form"] = form
        if fiscal_year is not None:
          where_clauses.append("r.fiscal_year_focus = $fiscal_year")
          parameters["fiscal_year"] = fiscal_year
        if fiscal_period:
          where_clauses.append("r.fiscal_period_focus = $fiscal_period")
          parameters["fiscal_period"] = fiscal_period

      if entity_list:
        return_clause = """
        RETURN DISTINCT
          el.qname as element_id,
          el.name as element_name,
          p.end_date as period_end,
          f.numeric_value as value,
          u.value as unit,
          ent.ticker as entity_ticker,
          ent.name as entity_name
        """
      else:
        return_clause = """
        RETURN DISTINCT
          el.qname as element_id,
          el.name as element_name,
          p.end_date as period_end,
          f.numeric_value as value,
          u.value as unit
        """

      query = "MATCH " + ", ".join(match_parts)
      query += "\nWHERE " + "\n  AND ".join(where_clauses)
      query += return_clause

      # Execute query through MCP client (consistent with all other tools)
      result = await self.client.execute_query(query, parameters=parameters)

      # Deduplicate by (element, period, entity) — same fact can appear in
      # multiple filings (comparative periods). Keep first occurrence.
      if result:
        result = self._deduplicate_facts(result, has_entity=entity_list is not None)

      # Convert to DataFrame (lazy import pandas)
      import pandas as pd

      if not result:
        fact_data = pd.DataFrame()
      else:
        fact_data = pd.DataFrame(result)

      # Build fact grid using existing FactGridBuilder
      from robosystems.models.api.views import ViewAxisConfig, ViewConfig
      from robosystems.operations.views.fact_grid_builder import FactGridBuilder

      # Create view config
      row_configs = [ViewAxisConfig(**r) for r in rows] if rows else []
      column_configs = [ViewAxisConfig(**c) for c in columns] if columns else []

      view_config = ViewConfig(rows=row_configs, columns=column_configs)

      builder = FactGridBuilder()
      fact_grid = builder.build(
        fact_data=fact_data, view_config=view_config, source="mcp_tool"
      )

      logger.info(
        f"Built fact grid with {fact_grid.metadata.fact_count} facts across {fact_grid.metadata.dimension_count} dimensions"
      )

      # Convert DataFrame to serializable format
      data_records = []
      if fact_grid.facts_df is not None and not fact_grid.facts_df.empty:
        # Convert to records (list of dicts)
        data_records = fact_grid.facts_df.to_dict(orient="records")

      # Build response
      response = {
        "success": True,
        "fact_count": fact_grid.metadata.fact_count,
        "dimension_count": fact_grid.metadata.dimension_count,
        "dimensions": [
          {
            "name": d.name,
            "type": d.type,
            "members": d.members[:10] if len(d.members) > 10 else d.members,
            "total_members": len(d.members),
          }
          for d in fact_grid.dimensions
        ],
        "data": data_records,
        "construction_time_ms": fact_grid.metadata.construction_time_ms,
        "message": f"Built fact grid with {fact_grid.metadata.fact_count} facts",
      }

      # Optionally include summary statistics
      if (
        include_summary
        and fact_grid.facts_df is not None
        and not fact_grid.facts_df.empty
      ):
        df = fact_grid.facts_df
        if "element_name" in df.columns and "value" in df.columns:
          summary = {}
          for element_name in df["element_name"].unique():
            element_data = df[df["element_name"] == element_name]
            summary[element_name] = {
              "count": len(element_data),
              "total": float(element_data["value"].sum()),
              "average": float(element_data["value"].mean()),
              "min": float(element_data["value"].min()),
              "max": float(element_data["value"].max()),
            }
          response["summary"] = summary

      return response

    except Exception as e:
      logger.error(f"Failed to build fact grid: {e}")
      import traceback

      logger.error(traceback.format_exc())
      return {"error": "construction_failed", "message": str(e)}

  @staticmethod
  def _deduplicate_facts(
    rows: list[dict[str, Any]], has_entity: bool
  ) -> list[dict[str, Any]]:
    """Deduplicate facts by (element, period, entity), keeping first occurrence."""
    seen: set[tuple] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
      if has_entity:
        key = (
          row.get("element_id", ""),
          row.get("period_end", ""),
          row.get("entity_ticker", ""),
        )
      else:
        key = (row.get("element_id", ""), row.get("period_end", ""))
      if key not in seen:
        seen.add(key)
        deduped.append(row)
    return deduped
