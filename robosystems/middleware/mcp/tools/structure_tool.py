"""
Structure Tool - Provides natural language description of graph structure.
"""

from typing import Any, Dict

from .base_tool import BaseTool
from robosystems.logger import logger


class StructureTool(BaseTool):
  """
  Tool for generating natural language descriptions of graph structure.
  """

  def get_tool_definition(self) -> Dict[str, Any]:
    """Get the tool definition for structure description."""
    return {
      "name": "describe-graph-structure",
      "description": """Get a natural language description of the graph database structure and contents.

**WHEN TO USE:**
- When you need a high-level overview of what's in the database
- For understanding the business domain and data relationships
- When onboarding new users to the graph structure
- To quickly assess what type of analysis is possible

**RETURNS:** Human-readable description including:
- Summary of the graph type (SEC, QuickBooks, Custom, etc.)
- Key entity counts and statistics
- Primary data relationships and patterns
- Available analysis capabilities
- Data quality indicators

**EXAMPLE OUTPUT:**
"This is a financial graph database containing:
- 150 companies with SEC filings
- 2,500 XBRL reports (10-K and 10-Q filings)
- 1.2M financial facts with standardized elements
- Supports: peer analysis, trend analysis, ratio calculations" """,
      "inputSchema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> str:
    """Execute the structure description tool."""
    self._log_tool_execution("describe-graph-structure", arguments)
    return await self._describe_graph_structure()

  async def _describe_graph_structure(self) -> str:
    """
    Generate a natural language description of the graph structure.

    Returns:
        Human-readable description of the graph
    """
    try:
      # Special handling for SEC database
      if self.client.graph_id == "sec":
        # Get schema information
        schema = await self.client.get_schema()

        # Count entities
        entity_counts = {}
        for item in schema:
          if item["type"] == "node" and item.get("count", 0) > 0:
            entity_counts[item["label"]] = item["count"]

        description = "This is the SEC shared repository containing public company financial data:\n\n"

        # Add specific counts
        if "Entity" in entity_counts:
          description += f"- {entity_counts['Entity']:,} public companies\n"
        if "Report" in entity_counts:
          description += (
            f"- {entity_counts['Report']:,} SEC filings (10-K, 10-Q, 8-K reports)\n"
          )
        if "Fact" in entity_counts:
          description += (
            f"- {entity_counts['Fact']:,} financial facts (XBRL data points)\n"
          )
        if "Element" in entity_counts:
          description += f"- {entity_counts['Element']:,} financial concepts/metrics\n"

        # Add capabilities
        description += "\n**Analysis Capabilities:**\n"
        description += "- Financial ratio analysis and peer comparisons\n"
        description += "- Time series analysis across reporting periods\n"
        description += "- Industry benchmarking and sector analysis\n"
        description += "- XBRL taxonomy exploration\n"
        description += "- Regulatory filing analysis\n"

        # Add data patterns
        description += "\n**Data Structure:**\n"
        description += "- Facts linked to Elements (financial metrics)\n"
        description += "- Facts linked to Periods (time context)\n"
        description += "- Facts linked to Entities (companies)\n"
        description += "- Facts may have Dimensions (segments/breakdowns)\n"
        description += "- Facts include Units (currency, shares, etc.)\n"

        return description

      else:
        # Generic graph analysis
        schema = await self.client.get_schema()

        # Count entities by type
        node_counts = {}
        rel_counts = {}
        total_nodes = 0
        total_rels = 0

        for item in schema:
          if item["type"] == "node":
            count = item.get("count", 0)
            node_counts[item["label"]] = count
            total_nodes += count
          elif item["type"] == "relationship":
            count = item.get("count", 0)
            rel_counts[item["label"]] = count
            total_rels += count

        # Generate description
        description = f"This graph database contains {total_nodes:,} nodes and {total_rels:,} relationships:\n\n"

        # Top node types
        if node_counts:
          description += "**Node Types:**\n"
          sorted_nodes = sorted(node_counts.items(), key=lambda x: x[1], reverse=True)
          for label, count in sorted_nodes[:10]:  # Top 10
            description += f"- {label}: {count:,} nodes\n"

        # Top relationship types
        if rel_counts:
          description += "\n**Relationship Types:**\n"
          sorted_rels = sorted(rel_counts.items(), key=lambda x: x[1], reverse=True)
          for label, count in sorted_rels[:10]:  # Top 10
            description += f"- {label}: {count:,} relationships\n"

        # Data patterns
        description += "\n**Analysis Capabilities:**\n"
        description += "- Network analysis and relationship traversal\n"
        description += "- Pattern matching and graph algorithms\n"
        description += "- Centrality and connectivity analysis\n"
        description += "- Path finding and shortest path queries\n"

        return description

    except Exception as e:
      logger.error(f"Error describing graph structure: {e}")
      return f"Unable to fully analyze graph structure: {str(e)}\n\nUse 'get-graph-schema' tool for detailed schema information."
