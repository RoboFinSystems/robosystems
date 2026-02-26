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
      "description": "Construct multidimensional fact grid from graph data. Retrieves facts based on elements, periods, and optional dimensions. Returns structured data with element names, values, and periods. Use include_summary=true to add aggregated statistics (count, total, avg, min, max) by element.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "elements": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Element URIs or identifiers to include in the grid",
          },
          "periods": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Period end dates (YYYY-MM-DD format) or quarters (YYYY-QN)",
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
        "required": ["elements", "periods"],
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
    periods = arguments.get("periods", [])
    rows = arguments.get("rows", [])
    columns = arguments.get("columns", [])
    include_summary = arguments.get("include_summary", False)

    if not elements:
      return {
        "error": "missing_elements",
        "message": "At least one element is required",
      }

    if not periods:
      return {"error": "missing_periods", "message": "At least one period is required"}

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
      graph_id = self.client.graph_id

      # Build parameterized Cypher query to prevent injection
      query = """
      MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
      MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
      MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
      WHERE el.uri IN $elements
        AND p.end_date IN $periods
      RETURN
        el.uri as element_id,
        el.name as element_name,
        p.end_date as period_end,
        f.numeric_value as value,
        u.value as unit,
        NULL as dimension_member
      """

      # Execute query through Graph API with parameters
      from robosystems.middleware.graph import get_universal_repository

      repository = await get_universal_repository(graph_id, "read")
      parameters = {"elements": elements, "periods": periods}
      result = await repository.execute_query(query, parameters)

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
