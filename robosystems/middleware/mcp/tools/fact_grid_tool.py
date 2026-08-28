"""Fact grid MCP tool — thin wrapper over the ops-layer ``query_fact_grid``.

Mirrors the REST ``build-fact-grid`` operation (see
``routers/extensions/roboledger/views.py``). Both surfaces delegate to
``operations/roboledger/views/fact_query.py`` so the Cypher, LadybugDB
optimizations, and dedup logic stay in one place.

The tool's job is:

1. Parse MCP arguments (array-friendly, AI-centric input)
2. Call ``query_fact_grid`` + ``FactGridBuilder`` from the ops layer
3. Format the response in the flat shape MCP callers expect
"""

from __future__ import annotations

import time
from typing import Any

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.logger import logger
from robosystems.models.api.views import ViewAxisConfig, ViewConfig
from robosystems.models.api.views.view_config import (
  DEFAULT_FACT_LIMIT,
  MAX_FACT_LIMIT,
)
from robosystems.operations.roboledger.views import (
  FactGridBuilder,
  query_fact_grid,
  summarize_by_element,
)


class BuildFactGridTool:
  """Build multidimensional fact grid from graph data."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "build-fact-grid",
      "description": """Construct multidimensional fact grid from graph data. Retrieves facts based on elements, periods, and optional dimensions. Returns structured data with element names, values, and periods. Use include_summary=true to add per-element statistics: count, min, max for every element, plus total and average for duration elements only — a balance summed or averaged across periods is not a balance, so instants (e.g. total_assets) omit both.

**WHEN TO USE:**
- When the user asks to compare specific financial metrics across periods or companies
- To build custom cross-company comparisons (e.g., "compare NVDA and AAPL total assets")
- When you need precise control over which elements, periods, and entities to include
- For ad-hoc financial analysis that doesn't fit a standard statement format

**HOW IT DIFFERS FROM financial-statement-analysis / live-financial-statement:**
- financial-statement-analysis returns ALL line items for a standard statement from the graph-backed XBRL hypercube (SEC or materialized tenant)
- live-financial-statement returns ALL line items for a standard statement from the tenant's live OLTP ledger
- build-fact-grid returns SPECIFIC elements you choose, across any combination of periods and entities

**PARAMETERS:**
- elements: XBRL qnames (e.g., 'us-gaap:Assets'). Use resolve-element to find qnames.
- canonical_concepts: Normalized concept names (e.g., 'revenue', 'net_income'). Matches ALL element qnames mapped to each concept across companies. Can be combined with elements.
- periods: End dates in YYYY-MM-DD format
- entity/entities: Filter by ticker, CIK, or name. Use 'entities' array for multi-company.
- period_type: 'annual', 'quarterly', or 'instant'. Important for duration elements (revenue, net income).
- form: SEC filing type ('10-K', '10-Q')
- fiscal_year/fiscal_period: Filter by report fiscal context

**RETURNS:**
- Deduplicated facts with element qnames, names, values, periods, units, and entity ticker/name — one record per fact, not a pivot table
- Only consolidated totals (dimensional breakdowns excluded)
- `truncated: true` when more facts matched than `limit` allowed; the ones returned are the most recent by period

**NOTES:**
On shared repositories (e.g. SEC) entity or entities is REQUIRED — those graphs host thousands of filers, so an unscoped query returns an arbitrary slice of arbitrary companies. On a tenant graph the URL already scopes to one entity, so the filter is optional there.
For income statement items (revenue, net income), always specify period_type='annual' or 'quarterly' to avoid mixing duration types. Use canonical_concepts for cross-company comparisons where companies may use different XBRL tags for the same concept.
""",
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
          "rows": {
            "type": "array",
            "description": "Optional aspect scoping, e.g. [{'type': 'period', 'selected_members': ['2024-12-31']}]",
            "default": [],
          },
          "columns": {
            "type": "array",
            "description": "Optional aspect scoping; same shape as 'rows'. Rows/columns are naming conventions only — both filter.",
            "default": [],
          },
          "include_summary": {
            "type": "boolean",
            "description": "Include per-element statistics: count, min, max for every element; total and average for duration elements only (instants omit them — a balance summed across periods is not a balance).",
            "default": False,
          },
          "limit": {
            "type": "integer",
            "description": f"Maximum facts to return (default {DEFAULT_FACT_LIMIT}, max {MAX_FACT_LIMIT}). Applied after dedup and sort, so truncation keeps the most recent periods.",
            "default": DEFAULT_FACT_LIMIT,
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    """Parse args, delegate to ops layer, format the MCP response."""
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
    limit = arguments.get("limit", DEFAULT_FACT_LIMIT)

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

    # Shared repos host thousands of filers; an entity-less query there
    # returns an arbitrary slice of arbitrary companies. Tenant graphs are
    # already entity-scoped by the graph itself. Mirrors the REST route.
    if (
      is_shared_repository_or_subgraph(self.client.graph_id)
      and not entity
      and not entities
    ):
      return {
        "error": "missing_entity_filter",
        "message": (
          "entity or entities is required on shared-repository graphs "
          "(e.g. SEC). Pass a ticker, CIK, or company name."
        ),
      }

    if not isinstance(limit, int) or limit < 1 or limit > MAX_FACT_LIMIT:
      return {
        "error": "invalid_limit",
        "message": f"limit must be an integer between 1 and {MAX_FACT_LIMIT}",
      }

    if rows and not isinstance(rows, list):
      return {"error": "invalid_rows", "message": "Rows must be a list"}
    if columns and not isinstance(columns, list):
      return {"error": "invalid_columns", "message": "Columns must be a list"}

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

    start_time = time.time()

    fact_data, truncated = await query_fact_grid(
      graph_id=self.client.graph_id,
      elements=elements or None,
      canonical_concepts=canonical_concepts or None,
      periods=periods or None,
      entity=entity,
      entities=entities or None,
      form=form,
      fiscal_year=fiscal_year,
      fiscal_period=fiscal_period,
      period_type=period_type,
      limit=limit,
    )

    row_configs = [ViewAxisConfig(**r) for r in rows] if rows else []
    column_configs = [ViewAxisConfig(**c) for c in columns] if columns else []
    view_config = ViewConfig(rows=row_configs, columns=column_configs)

    builder = FactGridBuilder()
    fact_grid = builder.build(
      fact_data=fact_data, view_config=view_config, source="mcp_tool"
    )

    # Query time dominates; timing only the in-memory build reported ~1ms
    # regardless of how long the graph took.
    elapsed_ms = (time.time() - start_time) * 1000

    logger.info(
      f"Built fact grid with {fact_grid.metadata.fact_count} facts "
      f"across {fact_grid.metadata.dimension_count} dimensions "
      f"in {elapsed_ms:.0f}ms"
    )

    response: dict[str, Any] = {
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
      "data": fact_grid.facts,
      "truncated": truncated,
      "construction_time_ms": elapsed_ms,
      "message": (
        f"Built fact grid with {fact_grid.metadata.fact_count} facts"
        + (
          f" (truncated at limit={limit}; narrow the filters or raise limit)"
          if truncated
          else ""
        )
      ),
    }

    if include_summary and fact_grid.facts:
      response["summary"] = summarize_by_element(fact_grid.facts)

    return response
