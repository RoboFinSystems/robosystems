"""
Elements Tool - Discovers commonly used Element nodes (financial metrics).
"""

from typing import Any, Dict

from .base_tool import BaseTool
from robosystems.logger import logger


class ElementsTool(BaseTool):
  """
  Tool for discovering commonly used Element nodes in financial graphs.
  """

  def get_tool_definition(self) -> Dict[str, Any]:
    """Get the tool definition for element discovery."""
    return {
      "name": "discover-common-elements",
      "description": """Discover commonly used Element nodes in the graph (financial metrics/tags).

**WHEN TO USE:**
- To understand what financial metrics are available in the database
- To discover XBRL tags (us-gaap, ifrs, custom) being used
- To find the most frequently reported metrics
- Before querying for specific financial data
- To explore custom "qb:" or "robo:" tags from non-SEC sources

**RETURNS:**
- Top elements by frequency with their qnames and labels
- Element categories (us-gaap, ifrs, qb, robo, custom)
- Usage statistics and sample facts
- Common patterns for querying these elements

**EXAMPLES:**
For SEC data, you might see:
- us-gaap:Revenues (Revenue)
- us-gaap:NetIncomeLoss (Net Income/Loss)
- us-gaap:Assets (Total Assets)

For QuickBooks/custom data:
- qb:AccountsReceivable
- robo:CustomerLifetimeValue
- custom:MonthlyRecurringRevenue

**TIP:**
Use the discovered qnames in your Fact queries to get actual values.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "limit": {
            "type": "integer",
            "description": "Number of top elements to return (default: 20, max: 100)",
            "default": 20,
            "maximum": 100,
          },
          "category": {
            "type": "string",
            "description": "Filter by element category: 'us-gaap', 'ifrs', 'qb', 'robo', 'custom', or 'all'",
            "default": "all",
          },
          "include_samples": {
            "type": "boolean",
            "description": "Include sample fact values for each element",
            "default": True,
          },
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the elements discovery tool."""
    self._log_tool_execution("discover-common-elements", arguments)

    limit = min(arguments.get("limit", 20), 100)
    category = arguments.get("category", "all")
    include_samples = arguments.get("include_samples", True)

    return await self._discover_common_elements(limit, category, include_samples)

  async def _discover_common_elements(
    self, limit: int = 20, category: str = "all", include_samples: bool = True
  ) -> Dict[str, Any]:
    """
    Discover commonly used Element nodes in the graph.

    Args:
        limit: Number of elements to return
        category: Element category filter
        include_samples: Whether to include sample values

    Returns:
        Dictionary with element discovery results
    """
    result = {
      "category_filter": category,
      "total_elements": 0,
      "common_elements": [],
      "categories_found": {},
      "usage_patterns": [],
      "tips": [],
    }

    try:
      # Check if Element nodes exist
      count_query = "MATCH (e:Element) RETURN count(e) as count"
      count_result = await self.client.execute_query(count_query)

      if not count_result or count_result[0].get("count", 0) == 0:
        result["error"] = "No Element nodes found in this graph"
        result["tips"].append("This might not be a financial graph with XBRL data")
        result["tips"].append("Use get-graph-schema to see available node types")
        return result

      result["total_elements"] = count_result[0]["count"]

      # Build query based on category filter
      where_clause = ""
      if category != "all":
        if category == "us-gaap":
          where_clause = "WHERE e.qname STARTS WITH 'us-gaap:'"
        elif category == "ifrs":
          where_clause = "WHERE e.qname STARTS WITH 'ifrs:'"
        elif category == "qb":
          where_clause = "WHERE e.qname STARTS WITH 'qb:'"
        elif category == "robo":
          where_clause = "WHERE e.qname STARTS WITH 'robo:'"
        elif category == "custom":
          where_clause = "WHERE NOT (e.qname STARTS WITH 'us-gaap:' OR e.qname STARTS WITH 'ifrs:' OR e.qname STARTS WITH 'qb:' OR e.qname STARTS WITH 'robo:')"

      # Get most common elements
      # Note: Labels are separate nodes, not properties of Element
      elements_query = f"""
            MATCH (e:Element)<-[:FACT_HAS_ELEMENT]-(f:Fact)
            {where_clause}
            RETURN e.qname as qname, e.name as name, 
                   count(f) as fact_count
            ORDER BY fact_count DESC
            LIMIT {limit}
            """

      elements_result = await self.client.execute_query(elements_query)

      if not elements_result:
        result["error"] = f"No elements found for category '{category}'"
        return result

      # Process elements
      for elem in elements_result:
        element_info = {
          "qname": elem["qname"],
          "name": elem.get("name"),
          "label": elem.get(
            "name"
          ),  # Use name as label since labels are separate nodes
          "fact_count": elem["fact_count"],
          "category": self._categorize_element(elem["qname"]),
        }

        # Get sample facts if requested
        if include_samples:
          sample_query = f"""
                    MATCH (e:Element)<-[:FACT_HAS_ELEMENT]-(f:Fact)
                    WHERE e.qname = '{elem["qname"]}' AND f.numeric_value IS NOT NULL
                    RETURN f.numeric_value as value, f.currency_code as currency
                    LIMIT 3
                    """
          samples = await self.client.execute_query(sample_query)
          element_info["sample_values"] = samples

        result["common_elements"].append(element_info)

        # Track categories
        cat = element_info["category"]
        if cat not in result["categories_found"]:
          result["categories_found"][cat] = 0
        result["categories_found"][cat] += 1

      # Add usage patterns
      result["usage_patterns"].extend(
        [
          "Query facts for element: MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) WHERE e.qname = 'element_qname'",
          "Find elements by name: MATCH (e:Element) WHERE e.name CONTAINS 'Revenue'",
          "Get element with all facts: MATCH (e:Element)<-[:FACT_HAS_ELEMENT]-(f:Fact) WHERE e.qname = 'qname'",
        ]
      )

      # Add tips
      result["tips"].extend(
        [
          "Use qname for precise matching in queries",
          "Filter by category to focus on specific taxonomies",
          "Check fact_count to understand data availability",
        ]
      )

    except Exception as e:
      logger.error(f"Error discovering elements: {e}")
      result["error"] = f"Failed to analyze elements: {str(e)}"
      result["tips"].append("Use get-graph-schema first to verify Element nodes exist")

    return result

  def _categorize_element(self, qname: str) -> str:
    """Categorize an element by its qname prefix."""
    if qname.startswith("us-gaap:"):
      return "us-gaap"
    elif qname.startswith("ifrs:"):
      return "ifrs"
    elif qname.startswith("qb:"):
      return "qb"
    elif qname.startswith("robo:"):
      return "robo"
    else:
      return "custom"
