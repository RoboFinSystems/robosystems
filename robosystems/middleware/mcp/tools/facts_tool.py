"""
Facts Tool - Discovers fact patterns and dimensional analysis capabilities.
"""

from typing import Any, Dict, Optional

from .base_tool import BaseTool
from robosystems.logger import logger


class FactsTool(BaseTool):
  """
  Tool for discovering fact patterns and their aspects.
  """

  def get_tool_definition(self) -> Dict[str, Any]:
    """Get the tool definition for facts discovery."""
    return {
      "name": "discover-facts",
      "description": """Discover fact patterns and their aspects (dimensions, periods, entities, units).

**WHEN TO USE:**
- To understand how facts are structured with their dimensional breakdowns
- To find segment revenue or other dimensional analyses patterns
- To discover what periods and entities have data
- Before writing complex multi-aspect queries
- To understand the difference between numeric and text facts

**RETURNS:**
- Fact patterns grouped by their aspects
- Dimensional breakdowns (segments, products, geographic regions)
- Common fact combinations (Element + Period + Dimension)
- Sample queries for complex analyses
- Statistics on numeric vs non-numeric facts

**ASPECTS EXPLAINED:**
- **Element**: The metric being measured (Revenue, Assets, etc.)
- **Period**: Time context (instant date or duration)
- **Entity**: Company or subsidiary reporting the fact
- **FactDimension**: Segment/breakdown (by product, geography, etc.)
- **Unit**: How it's measured (USD, shares, pure number)

**FACT TYPES:**
- **Numeric**: Facts with numeric_value (financial amounts, quantities)
- **Nonnumeric**: Text facts (policies, descriptions)
- **Textblock**: Long-form text (stored externally, referenced by URI)

**EXAMPLE USE CASES:**
- Find revenue by segment across periods
- Discover geographic breakdowns
- Identify dimensional reporting patterns
- Understand fact completeness by period""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "focus": {
            "type": "string",
            "description": "Focus area: 'dimensional' (segment analysis), 'temporal' (time series), 'completeness' (data coverage), or 'all'",
            "default": "all",
          },
          "element_filter": {
            "type": "string",
            "description": "Optional: Filter to specific element qname (e.g., 'us-gaap:Revenues')",
          },
          "include_samples": {
            "type": "boolean",
            "description": "Include sample fact values with all aspects",
            "default": True,
          },
          "limit": {
            "type": "integer",
            "description": "Number of patterns to return",
            "default": 10,
            "maximum": 50,
          },
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the facts discovery tool."""
    self._log_tool_execution("discover-facts", arguments)

    focus = arguments.get("focus", "all")
    element_filter = arguments.get("element_filter")
    include_samples = arguments.get("include_samples", True)
    limit = arguments.get("limit", 10)

    return await self._discover_facts(focus, element_filter, include_samples, limit)

  async def _discover_facts(
    self,
    focus: str = "all",
    element_filter: Optional[str] = None,
    include_samples: bool = True,
    limit: int = 10,
  ) -> Dict[str, Any]:
    """
    Discover fact patterns with their aspects for complex queries.

    Args:
        focus: Area to focus on (dimensional, temporal, completeness, all)
        element_filter: Optional element qname to filter to
        include_samples: Whether to include sample values
        limit: Number of patterns to return

    Returns:
        Dictionary with fact discovery results
    """
    result = {
      "focus": focus,
      "total_facts": 0,
      "fact_types": {},
      "dimensional_patterns": [],
      "temporal_coverage": [],
      "common_aspects": [],
      "sample_queries": [],
      "tips": [],
    }

    try:
      # Check if Fact nodes exist
      count_query = "MATCH (f:Fact) RETURN count(f) as count"
      count_result = await self.client.execute_query(count_query)

      if not count_result or count_result[0].get("count", 0) == 0:
        result["error"] = "No Fact nodes found in this graph"
        result["tips"].append("This might not be a financial graph with XBRL data")
        result["tips"].append("Use get-graph-schema to see available node types")
        return result

      result["total_facts"] = count_result[0]["count"]

      # Analyze fact types
      fact_types_query = """
            MATCH (f:Fact)
            RETURN 
                count(CASE WHEN f.numeric_value IS NOT NULL THEN 1 END) as numeric_facts,
                count(CASE WHEN f.nonnumeric_value IS NOT NULL THEN 1 END) as text_facts,
                count(CASE WHEN f.textblock_uri IS NOT NULL THEN 1 END) as textblock_facts
            """
      fact_types_result = await self.client.execute_query(fact_types_query)
      if fact_types_result:
        result["fact_types"] = fact_types_result[0]

      # Element filter clause
      element_clause = ""
      if element_filter:
        element_clause = f"AND e.qname = '{element_filter}'"

      # Dimensional analysis
      if focus in ["all", "dimensional"]:
        dim_query = f"""
                MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
                OPTIONAL MATCH (f)-[:FACT_HAS_DIMENSION]->(d:FactDimension)
                WHERE f.numeric_value IS NOT NULL {element_clause}
                RETURN e.qname as element, d.dimension_type as dim_type, 
                       d.dimension_value as dim_value, count(f) as fact_count
                ORDER BY fact_count DESC
                LIMIT {limit}
                """
        dim_result = await self.client.execute_query(dim_query)

        for row in dim_result:
          if row.get("dim_type"):  # Has dimensional breakdown
            pattern = {
              "element": row["element"],
              "dimension_type": row["dim_type"],
              "dimension_value": row["dim_value"],
              "fact_count": row["fact_count"],
            }

            if include_samples:
              sample_query = f"""
                            MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
                            MATCH (f)-[:FACT_HAS_DIMENSION]->(d:FactDimension)
                            WHERE e.qname = '{row["element"]}' 
                              AND d.dimension_type = '{row["dim_type"]}'
                              AND f.numeric_value IS NOT NULL
                            RETURN f.numeric_value as value, f.currency_code as currency
                            LIMIT 3
                            """
              samples = await self.client.execute_query(sample_query)
              pattern["sample_values"] = samples

            result["dimensional_patterns"].append(pattern)

      # Temporal analysis
      if focus in ["all", "temporal"]:
        temporal_query = f"""
                MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
                MATCH (f)-[:FACT_HAS_ELEMENT]->(e:Element)
                WHERE f.numeric_value IS NOT NULL {element_clause}
                RETURN p.end_date as period, e.qname as element, count(f) as fact_count
                ORDER BY p.end_date DESC
                LIMIT {limit}
                """
        temporal_result = await self.client.execute_query(temporal_query)
        result["temporal_coverage"] = temporal_result

      # Common aspect combinations
      aspect_query = f"""
            MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
            OPTIONAL MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
            OPTIONAL MATCH (f)-[:FACT_HAS_DIMENSION]->(d:FactDimension)
            OPTIONAL MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
            WHERE f.numeric_value IS NOT NULL {element_clause}
            RETURN e.qname as element,
                   count(DISTINCT p) as periods,
                   count(DISTINCT d) as dimensions,
                   count(DISTINCT u) as units,
                   count(f) as total_facts
            ORDER BY total_facts DESC
            LIMIT {limit}
            """
      aspect_result = await self.client.execute_query(aspect_query)
      result["common_aspects"] = aspect_result

      # Sample queries
      result["sample_queries"].extend(
        [
          {
            "name": "Single-Dimension Revenue Analysis (Safe)",
            "query": """MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
MATCH (f)-[:FACT_HAS_DIMENSION]->(d:FactDimension)
WHERE e.qname CONTAINS 'Revenue' AND f.numeric_value IS NOT NULL
RETURN d.dimension_type, d.dimension_value, sum(f.numeric_value) as total
ORDER BY total DESC LIMIT 10""",
            "explanation": "Revenue by segment with safe aggregation",
          },
          {
            "name": "Multi-Dimension Warning Query",
            "query": """MATCH (f:Fact)-[:FACT_HAS_DIMENSION]->(d1:FactDimension)
MATCH (f)-[:FACT_HAS_DIMENSION]->(d2:FactDimension)
WHERE d1 <> d2
RETURN count(f) as multi_dimensional_facts""",
            "explanation": "⚠️ Complex: Facts with multiple dimensions",
          },
        ]
      )

      if element_filter:
        result["sample_queries"].extend(
          [
            {
              "name": "Total Values (No Dimensions)",
              "query": f"""MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
WHERE e.qname = '{element_filter}' 
  AND f.numeric_value IS NOT NULL
  AND NOT EXISTS((f)-[:FACT_HAS_DIMENSION]->())
RETURN sum(f.numeric_value) as total, count(f) as fact_count""",
              "explanation": "Total for element without dimensional breakdown",
            },
            {
              "name": "Time Series for Element",
              "query": f"""MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
WHERE e.qname = '{element_filter}' AND f.numeric_value IS NOT NULL
RETURN p.end_date as period, sum(f.numeric_value) as value
ORDER BY p.end_date""",
              "explanation": "Time series data for specific element",
            },
            {
              "name": "All Aspects for a Fact",
              "query": f"""MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
OPTIONAL MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
OPTIONAL MATCH (f)-[:FACT_HAS_DIMENSION]->(d:FactDimension)
OPTIONAL MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
WHERE e.qname = '{element_filter}' AND f.numeric_value IS NOT NULL
RETURN f.numeric_value, p.end_date, d.dimension_type, d.dimension_value, u.unit_type
LIMIT 10""",
              "explanation": "Complete fact context with all aspects",
            },
          ]
        )

      # Add tips
      result["tips"].extend(
        [
          "Start with single elements before complex dimensional queries",
          "Use IS NOT NULL filters for numeric analysis",
          "Period nodes provide time context for facts",
          "FactDimension nodes enable segment analysis",
        ]
      )

    except Exception as e:
      logger.error(f"Error discovering facts: {e}")
      result["error"] = f"Failed to analyze facts: {str(e)}"
      result["tips"].append("Use get-graph-schema first to understand the structure")

    return result
