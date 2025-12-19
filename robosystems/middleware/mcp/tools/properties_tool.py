"""
Properties Tool - Discovers available properties for specific node types.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class PropertiesTool(BaseTool):
  """
  Tool for discovering properties of node types by sampling actual data.
  """

  def get_tool_definition(self) -> dict[str, Any]:
    """Get the tool definition for property discovery."""
    return {
      "name": "discover-properties",
      "description": """Discover available properties for a specific node type or relationship.

**WHEN TO USE:**
- Before writing queries to understand what properties are available
- When you get "property not found" errors
- To understand the data model of specific node types
- To find the right property names for filtering or returning data

**RETURNS:**
Detailed information about properties including:
- All available property names
- Sample values for each property
- Data types when discoverable
- Which properties are commonly used

**TIP:**
This is more thorough than the schema tool - it actually samples the data to show you real property values.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "node_type": {
            "type": "string",
            "description": "Node label to discover properties for (e.g., 'Entity', 'Fact')",
          },
          "sample_size": {
            "type": "integer",
            "description": "Number of nodes to sample (default: 5, max: 20)",
            "default": 5,
            "maximum": 20,
          },
        },
        "required": ["node_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    """Execute the properties discovery tool."""
    self._log_tool_execution("discover-properties", arguments)

    node_type = arguments.get("node_type")
    if not node_type:
      raise ValueError("node_type parameter is required")

    sample_size = min(arguments.get("sample_size", 5), 20)
    return await self._discover_properties(node_type, sample_size)

  async def _discover_properties(
    self, node_type: str, sample_size: int = 5
  ) -> dict[str, Any]:
    """
    Discover properties for a node type by sampling actual data.

    Args:
        node_type: Node label to discover
        sample_size: Number of nodes to sample

    Returns:
        Detailed property information
    """
    result = {
      "node_type": node_type,
      "properties": {},
      "total_properties": 0,
      "samples_analyzed": 0,
      "common_patterns": [],
      "usage_tips": [],
    }

    try:
      # First check if the node type exists
      count_query = f"MATCH (n:{node_type}) RETURN count(n) as count"
      count_result = await self.client.execute_query(count_query)
      if not count_result or count_result[0].get("count", 0) == 0:
        result["error"] = f"No nodes found with label '{node_type}'"
        result["usage_tips"].append("Use get-graph-schema to see available node types")
        return result

      # Get sample nodes
      sample_query = f"MATCH (n:{node_type}) RETURN n LIMIT {sample_size}"
      samples = await self.client.execute_query(sample_query)

      if not samples:
        result["error"] = "Could not retrieve sample nodes"
        return result

      result["samples_analyzed"] = len(samples)

      # Analyze properties across all samples
      all_props = {}
      for sample in samples:
        node = sample.get("n", {})
        for prop_name, prop_value in node.items():
          if prop_name.startswith("_"):  # Skip internal properties
            continue

          if prop_name not in all_props:
            all_props[prop_name] = {
              "name": prop_name,
              "values": [],
              "types": set(),
              "nullable": False,
              "sample_values": [],
            }

          if prop_value is not None:
            all_props[prop_name]["values"].append(prop_value)
            all_props[prop_name]["types"].add(type(prop_value).__name__)

            # Store first 3 sample values for each property
            if len(all_props[prop_name]["sample_values"]) < 3:
              all_props[prop_name]["sample_values"].append(prop_value)
          else:
            all_props[prop_name]["nullable"] = True

      # Process final property info
      for prop_name, prop_info in all_props.items():
        prop_info["frequency"] = len(prop_info["values"]) / len(samples)
        prop_info["data_types"] = list(prop_info["types"])

        # Clean up for output
        del prop_info["values"]
        del prop_info["types"]

        result["properties"][prop_name] = prop_info

      result["total_properties"] = len(all_props)

      # Add usage tips
      result["usage_tips"].extend(
        [
          f"Query with: MATCH (n:{node_type}) WHERE n.property_name = value",
          f"Get all properties: MATCH (n:{node_type}) RETURN n",
          "Check property existence: WHERE n.property_name IS NOT NULL",
        ]
      )

      # Identify common patterns
      if result["properties"]:
        common_props = [
          prop for prop, info in result["properties"].items() if info["frequency"] > 0.8
        ]
        if common_props:
          result["common_patterns"].append(
            f"Properties present in >80% of nodes: {', '.join(common_props)}"
          )

        nullable_props = [
          prop for prop, info in result["properties"].items() if info["nullable"]
        ]
        if nullable_props:
          result["common_patterns"].append(
            f"Nullable properties (use IS NOT NULL): {', '.join(nullable_props)}"
          )

    except Exception as e:
      logger.error(f"Error discovering properties for {node_type}: {e}")
      result["error"] = f"Failed to analyze properties: {e!s}"
      result["usage_tips"].append(
        "Use get-graph-schema first to verify node types exist"
      )

    return result
