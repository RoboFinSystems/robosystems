"""
Example Queries Tool - Provides example Cypher queries for graph exploration.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class ExampleQueriesTool(BaseTool):
  """
  Tool for generating example Cypher queries based on the graph schema.
  """

  def get_tool_definition(self) -> dict[str, Any]:
    """Get the tool definition for example queries."""
    return {
      "name": "get-example-queries",
      "description": """Get example Cypher queries for this graph database.

**WHEN TO USE:**
- When starting to explore a new graph
- When you need query patterns for specific node types
- When learning the relationship structure
- After getting errors to see correct syntax

**RETURNS:**
List of example queries with explanations, tailored to the actual schema present in this graph.

**BENEFITS:**
- See real working queries for this specific graph
- Learn property names and relationships
- Understand query patterns that work
- Copy and modify examples for your needs""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "category": {
            "type": "string",
            "description": "Optional category filter (e.g., 'entity', 'financial', 'relationships', 'aggregations')",
          }
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
    """Execute the example queries tool."""
    self._log_tool_execution("get-example-queries", arguments)
    category = arguments.get("category")
    return await self._get_example_queries(category)

  async def _get_example_queries(
    self, category: str | None = None
  ) -> list[dict[str, Any]]:
    """
    Generate example queries based on the actual graph schema.

    Args:
        category: Optional filter for query category

    Returns:
        List of example queries with descriptions
    """
    examples = []
    try:
      # Get schema to understand what's available
      schema = await self.client.get_schema()

      # Find available node types
      node_types = [item["label"] for item in schema if item["type"] == "node"]
      rel_types = [item["label"] for item in schema if item["type"] == "relationship"]

      # Basic exploration queries (always include)
      if not category or category == "exploration":
        examples.append(
          {
            "category": "exploration",
            "description": "Count all nodes by type",
            "query": "MATCH (n:Fact) RETURN 'Fact' as node_type, count(n) as count UNION ALL MATCH (n:Element) RETURN 'Element' as node_type, count(n) as count UNION ALL MATCH (n:Entity) RETURN 'Entity' as node_type, count(n) as count",
            "explanation": "Shows distribution of data across node types",
          }
        )
        examples.append(
          {
            "category": "exploration",
            "description": "Get sample nodes to understand structure",
            "query": "MATCH (n) RETURN n LIMIT 5",
            "explanation": "Returns full node objects to see all properties",
          }
        )
        examples.append(
          {
            "category": "exploration",
            "description": "Discover properties of a node type",
            "query": f"MATCH (n:{node_types[0] if node_types else 'Node'}) RETURN keys(n) as properties LIMIT 1",
            "explanation": "Use keys() to find what properties are available",
          }
        )

      # SEC-specific queries
      if self.client.graph_id == "sec" and (not category or category == "financial"):
        examples.extend(
          [
            {
              "category": "financial",
              "description": "Get company information",
              "query": "MATCH (e:Entity) RETURN e.name, e.cik, e.ticker, e.sic_description",
              "explanation": "Entity nodes contain company master data",
            },
            {
              "category": "financial",
              "description": "Find financial facts with values",
              "query": """MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
WHERE f.numeric_value IS NOT NULL
RETURN e.name as metric, f.numeric_value as value
LIMIT 10""",
              "explanation": "Facts are linked to Elements that define the metric",
            },
            {
              "category": "financial",
              "description": "Get facts for a specific period",
              "query": """MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
WHERE p.end_date >= '2024-01-01'
RETURN f.identifier, f.numeric_value, p.end_date
LIMIT 10""",
              "explanation": "Facts are linked to Period nodes for time analysis",
            },
            {
              "category": "financial",
              "description": "Find reports by form type",
              "query": """MATCH (r:Report)
WHERE r.form = '10-K' OR r.form = '10-Q'
RETURN r.form, r.filing_date, r.accession_number
LIMIT 10""",
              "explanation": "Report nodes contain SEC filing metadata",
            },
          ]
        )

      # Entity-based queries (common pattern)
      if "Entity" in node_types and (not category or category == "entity"):
        examples.extend(
          [
            {
              "category": "entity",
              "description": "Find entities by name pattern",
              "query": "MATCH (e:Entity) WHERE e.name CONTAINS 'TECH' RETURN e.name, e.identifier",
              "explanation": "Use CONTAINS for substring matching",
            },
            {
              "category": "entity",
              "description": "Get entity with all its properties",
              "query": "MATCH (e:Entity) WHERE e.identifier = 'some_id' RETURN e",
              "explanation": "Return full node to see all available data",
            },
          ]
        )

      # Relationship queries
      if rel_types and (not category or category == "relationships"):
        examples.extend(
          [
            {
              "category": "relationships",
              "description": "Find all relationships from a node",
              "query": "MATCH (n)-[r]->(m) WHERE id(n) = 0 RETURN type(r) as rel_type, labels(m)[0] as target_type",
              "explanation": "Discover what a node is connected to",
            },
            {
              "category": "relationships",
              "description": "Count relationships by type",
              "query": "MATCH ()-[r]->() RETURN type(r) as rel_type, count(r) as count",
              "explanation": "Understand the relationship distribution",
            },
          ]
        )

      # Aggregation examples
      if not category or category == "aggregations":
        examples.extend(
          [
            {
              "category": "aggregations",
              "description": "Group and sum values",
              "query": """MATCH (f:Fact)
WHERE f.numeric_value IS NOT NULL
RETURN 'Fact' as type, sum(f.numeric_value) as total, count(f) as count""",
              "explanation": "LadybugDB supports aggregation functions like sum(), avg(), count()",
            },
            {
              "category": "aggregations",
              "description": "Find min/max values",
              "query": """MATCH (n)
WHERE n.numeric_value IS NOT NULL
RETURN min(n.numeric_value) as min_val, max(n.numeric_value) as max_val""",
              "explanation": "Use min() and max() for range analysis",
            },
          ]
        )

      # Add note about available nodes and relationships
      examples.append(
        {
          "category": "reference",
          "description": "Available node types in this graph",
          "info": f"Node types: {', '.join(node_types[:10])}",
          "explanation": "Use these labels in your MATCH patterns",
        }
      )
      if rel_types:
        examples.append(
          {
            "category": "reference",
            "description": "Available relationship types",
            "info": f"Relationships: {', '.join(rel_types[:10])}",
            "explanation": "Use these in relationship patterns like -[:TYPE]->",
          }
        )

    except Exception as e:
      logger.warning(f"Error generating examples: {e}")
      # Return basic examples even if schema fetch fails
      examples = [
        {
          "category": "basic",
          "description": "Count all nodes",
          "query": "MATCH (n) RETURN COUNT(*) as total_nodes",
          "explanation": "Basic query that should always work",
        },
        {
          "category": "basic",
          "description": "Get sample data",
          "query": "MATCH (n) RETURN n LIMIT 10",
          "explanation": "Explore what's in the graph",
        },
      ]

    return examples
