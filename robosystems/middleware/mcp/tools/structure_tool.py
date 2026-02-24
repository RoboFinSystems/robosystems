"""
Structure Tool - Provides natural language description of graph structure.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool
from .constants import PERIOD_TYPE_GUIDANCE, QUERY_PATTERN_GUIDANCE


class StructureTool(BaseTool):
  """
  Tool for generating natural language descriptions of graph structure.
  """

  def get_tool_definition(self) -> dict[str, Any]:
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
- Summary of the graph contents and domain
- Key entity counts and statistics
- Primary data relationships and patterns
- Available analysis capabilities""",
      "inputSchema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> str:
    """Execute the structure description tool."""
    self._log_tool_execution("describe-graph-structure", arguments)
    return await self._describe_graph_structure()

  def _get_manifest(self):
    """Get the shared repository manifest for this graph, if any."""
    try:
      from robosystems.config.shared_repositories import (
        get_manifest,
        is_shared_repository_or_subgraph,
        resolve_shared_repository_parent,
      )

      if is_shared_repository_or_subgraph(self.client.graph_id):
        parent_id = resolve_shared_repository_parent(self.client.graph_id)
        return get_manifest(parent_id)
    except Exception:
      pass
    return None

  async def _describe_graph_structure(self) -> str:
    """
    Generate a natural language description of the graph structure.

    Description adapts based on graph context:
    - Shared repositories: Uses manifest name/description
    - RoboLedger graphs: Financial data structure, dimensional fact warnings
    - Generic graphs: Node/relationship counts and general analysis capabilities

    Returns:
        Human-readable description of the graph
    """
    try:
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

      # Detect graph context
      is_roboledger = "Fact" in node_counts and "Element" in node_counts
      manifest = self._get_manifest()
      is_shared_repo = manifest is not None

      # Build description header
      if is_shared_repo:
        description = (
          f"This is the {manifest.name} shared repository ({manifest.description}):\n\n"
        )
      else:
        description = f"This graph database contains {total_nodes:,} nodes and {total_rels:,} relationships:\n\n"

      # Node counts
      if node_counts:
        description += "**Node Types:**\n"
        sorted_nodes = sorted(node_counts.items(), key=lambda x: x[1], reverse=True)
        for label, count in sorted_nodes[:10]:
          description += f"- {label}: {count:,} nodes\n"

      # Relationship types
      if rel_counts:
        description += "\n**Relationship Types:**\n"
        sorted_rels = sorted(rel_counts.items(), key=lambda x: x[1], reverse=True)
        for label, count in sorted_rels[:10]:
          description += f"- {label}: {count:,} relationships\n"

      # Shared repository: mention sibling workspaces from manifest
      if is_shared_repo and manifest.sibling_subgraphs:
        siblings = [f"`{manifest.id}_{name}`" for name in manifest.sibling_subgraphs]
        description += (
          f"\n**Available workspaces:** {', '.join(siblings)} "
          f"(use `switch-workspace` to access).\n"
        )

      # RoboLedger financial graphs: data structure and dimensional warning
      if is_roboledger:
        description += "\n**Data Structure:**\n"
        description += "- Facts linked to Elements (financial metrics)\n"
        description += "- Facts linked to Periods (time context)\n"
        description += "- Facts linked to Entities (companies/organizations)\n"
        description += "- Facts may have Dimensions (segments/breakdowns)\n"
        description += "- Facts include Units (currency, shares, etc.)\n"

        description += "\n**IMPORTANT - Dimensional Facts:**\n"
        description += "Many facts have dimensional breakdowns (by segment, geography, product line).\n"
        description += "To get consolidated totals only, filter with:\n"
        description += "- `WHERE f.has_dimensions = false` (recommended)\n"
        description += (
          "Without this filter, you may get duplicate values for the same metric.\n"
        )

        description += "\n" + QUERY_PATTERN_GUIDANCE + "\n"
        description += "\n" + PERIOD_TYPE_GUIDANCE + "\n"

        description += "\n**Analysis Capabilities:**\n"
        description += "- Financial ratio analysis and comparisons\n"
        description += "- Time series analysis across reporting periods\n"
        description += "- Industry benchmarking and sector analysis\n"
      else:
        description += "\n**Analysis Capabilities:**\n"
        description += "- Network analysis and relationship traversal\n"
        description += "- Pattern matching and graph algorithms\n"
        description += "- Centrality and connectivity analysis\n"
        description += "- Path finding and shortest path queries\n"

      return description

    except Exception as e:
      logger.error(f"Error describing graph structure: {e}")
      return f"Unable to fully analyze graph structure: {e!s}\n\nUse 'get-graph-schema' tool for detailed schema information."
