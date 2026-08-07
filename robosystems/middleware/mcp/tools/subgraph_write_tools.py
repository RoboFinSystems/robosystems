"""
Subgraph write MCP tools.

Provides tools for AI agents to dynamically extend schema and write data
to subgraphs. All tools enforce subgraph-only access — the main graph is
read-only to raw statements (writes go through structured extension ops).
These predate LanceDB semantic memory; the "memory" label moved there.
"""

import re
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.utils import is_subgraph

from ..exceptions import GraphAPIError
from .base_tool import BaseTool

# Lazy import to avoid circular imports with adapter chain
_is_shared_repository_or_subgraph = None


def _get_is_shared_repository_or_subgraph():
  global _is_shared_repository_or_subgraph
  if _is_shared_repository_or_subgraph is None:
    from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

    _is_shared_repository_or_subgraph = is_shared_repository_or_subgraph
  return _is_shared_repository_or_subgraph


# Valid LadybugDB property types
VALID_PROPERTY_TYPES = {"STRING", "INT32", "INT64", "DOUBLE", "BOOLEAN"}

# Valid table name pattern
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")

# Valid property name pattern
PROPERTY_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def _validate_subgraph_context(graph_id: str) -> dict[str, Any] | None:
  """Validate the active graph is a writable subgraph. Returns error dict if not.

  Blocks writes on:
  - Parent graphs (must use a subgraph)
  - Shared repository graphs and their subgraphs (always read-only)
  """
  if not is_subgraph(graph_id):
    return {
      "error": "subgraph_required",
      "message": "This tool only works on subgraphs, not the parent graph. "
      "Use create-subgraph to create a subgraph first, "
      "then connect to that subgraph's own MCP endpoint "
      "(resolve-subgraph returns its URL).",
    }

  # Shared repository subgraphs are always read-only
  if _get_is_shared_repository_or_subgraph()(graph_id):
    return {
      "error": "read_only",
      "message": "Shared repository graphs are read-only. "
      "Write operations are not allowed on shared repositories or their subgraphs.",
    }

  return None


def _validate_write_query(query: str) -> str | None:
  """Validate a Cypher write query. Returns error message if invalid.

  Uses the central security analyzer (the same predicates the StatementKernel
  composes) rather than hand-rolled keyword/pattern regexes, so this tool-layer
  guard — which also protects the Operator path — can't diverge from the kernel.
  """
  from robosystems.security.cypher_analyzer import (
    is_admin_operation,
    is_bulk_operation,
    is_schema_ddl,
    is_write_operation,
  )

  # Block DDL / bulk / admin — the same dangerous categories the kernel and the
  # read tool reject (DROP/ALTER/TABLE/INDEX, LOAD CSV/COPY, CALL DB./APOC.).
  if is_schema_ddl(query) or is_bulk_operation(query) or is_admin_operation(query):
    return "Blocked operation detected. DDL and system operations are not allowed in write queries."

  # A write tool requires an actual write operation.
  if not is_write_operation(query):
    return (
      "Query must contain a write operation (CREATE, MERGE, SET, DELETE, REMOVE). "
      "For read queries, use read-graph-cypher instead."
    )

  return None


class WriteCypherTool(BaseTool):
  """Execute write Cypher queries on subgraphs."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "write-graph-cypher",
      "description": (
        "Execute a write Cypher query on the active subgraph. "
        "Creates, updates, or deletes data in the subgraph. "
        "Only works on subgraphs, not the parent graph.\n\n"
        "**Allowed operations:** CREATE, MERGE, SET, DELETE, REMOVE\n\n"
        "**Examples:**\n"
        "```cypher\n"
        "// Create a node\n"
        "CREATE (c:Concept {identifier: 'c1', name: 'revenue', category: 'metric'})\n\n"
        "// Create a relationship\n"
        "MATCH (a:Concept {identifier: 'c1'}), (b:Concept {identifier: 'c2'}) "
        "CREATE (a)-[:CONCEPT_RELATES_TO {relationship_type: 'depends_on'}]->(b)\n\n"
        "// Update a property\n"
        "MATCH (c:Concept {identifier: 'c1'}) SET c.confidence = 0.95\n\n"
        "// Delete a node\n"
        "MATCH (o:Observation {identifier: 'obs1'}) DETACH DELETE o\n"
        "```"
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": "Cypher write query (CREATE, MERGE, SET, DELETE)",
          },
          "parameters": {
            "type": "object",
            "description": "Optional query parameters",
            "additionalProperties": True,
          },
        },
        "required": ["query"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("write-graph-cypher", arguments)

    # Validate subgraph context
    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error

    query = arguments.get("query", "").strip()
    if not query:
      return {"error": "invalid_query", "message": "Query parameter is required"}

    # Validate write query
    validation_error = _validate_write_query(query)
    if validation_error:
      return {"error": "invalid_query", "message": validation_error}

    parameters = arguments.get("parameters", {})

    try:
      result = await self.client.execute_query(query, parameters)
      return {
        "success": True,
        "result": result,
        "message": "Write query executed successfully",
      }
    except Exception as e:
      logger.error(f"Write query failed: {e}")
      raise GraphAPIError(f"Write query failed: {e}")


class AddNodeTableTool(BaseTool):
  """Dynamically add a new node table to a subgraph."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "add-node-table",
      "description": (
        "Add a new node type to the subgraph schema. "
        "Only works on subgraphs, not the parent graph. "
        "Uses IF NOT EXISTS so it's safe to call multiple times.\n\n"
        "**Example:** Add a CompanyProfile node type:\n"
        "```json\n"
        '{"table_name": "CompanyProfile", "properties": [\n'
        '  {"name": "identifier", "type": "STRING", "is_primary_key": true},\n'
        '  {"name": "ticker", "type": "STRING"},\n'
        '  {"name": "sector", "type": "STRING"}\n'
        "]}\n"
        "```"
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "table_name": {
            "type": "string",
            "description": "Name for the node table (e.g., 'CompanyProfile', 'ResearchFinding')",
          },
          "properties": {
            "type": "array",
            "description": "Property definitions for the node",
            "items": {
              "type": "object",
              "properties": {
                "name": {"type": "string"},
                "type": {
                  "type": "string",
                  "enum": list(VALID_PROPERTY_TYPES),
                },
                "is_primary_key": {
                  "type": "boolean",
                  "default": False,
                },
              },
              "required": ["name", "type"],
            },
          },
        },
        "required": ["table_name", "properties"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("add-node-table", arguments)

    # Validate subgraph context
    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error

    table_name = arguments.get("table_name", "")
    properties = arguments.get("properties", [])

    # Validate table name
    if not TABLE_NAME_PATTERN.match(table_name):
      return {
        "error": "invalid_table_name",
        "message": "Table name must start with a letter, contain only letters/numbers/underscores, and be 1-64 characters",
      }

    # Validate properties
    if not properties:
      return {
        "error": "invalid_properties",
        "message": "At least one property is required",
      }

    # Check for primary key
    has_pk = any(p.get("is_primary_key") for p in properties)
    if not has_pk:
      return {
        "error": "missing_primary_key",
        "message": "At least one property must be marked as is_primary_key: true",
      }

    # Validate each property
    for prop in properties:
      name = prop.get("name", "")
      ptype = prop.get("type", "")
      if not PROPERTY_NAME_PATTERN.match(name):
        return {
          "error": "invalid_property_name",
          "message": f"Property name '{name}' must start with a lowercase letter and contain only lowercase letters/numbers/underscores",
        }
      if ptype not in VALID_PROPERTY_TYPES:
        return {
          "error": "invalid_property_type",
          "message": f"Property type '{ptype}' is not valid. Use one of: {', '.join(sorted(VALID_PROPERTY_TYPES))}",
        }

    # Build DDL with backtick-quoted identifiers for defense-in-depth
    prop_parts = [f"`{p['name']}` {p['type']}" for p in properties]
    pk_names = [p["name"] for p in properties if p.get("is_primary_key")]
    prop_parts.append(f"PRIMARY KEY({', '.join(f'`{n}`' for n in pk_names)})")
    props_str = ", ".join(prop_parts)
    ddl = f"CREATE NODE TABLE IF NOT EXISTS `{table_name}`({props_str})"

    try:
      result = await self.client.graph_client.install_schema(
        graph_id=self.client.graph_id,
        custom_ddl=ddl,
      )
      success = result.get("success", False)
      if success:
        # Clear schema cache so get-graph-schema reflects the new table
        if hasattr(self.client, "_mcp_tools"):
          self.client._mcp_tools.clear_schema_cache()

        return {
          "success": True,
          "table_name": table_name,
          "properties": [p["name"] for p in properties],
          "primary_key": pk_names,
          "message": f"Node table '{table_name}' created successfully",
        }
      else:
        return {
          "error": "schema_install_failed",
          "message": result.get("message", "Schema installation failed"),
        }
    except Exception as e:
      logger.error(
        f"Failed to create node table {table_name}: {e}",
        extra={"table_name": table_name, "ddl": ddl, "error_type": type(e).__name__},
      )
      raise GraphAPIError(f"Failed to create node table '{table_name}': {e}")


class AddRelationshipTableTool(BaseTool):
  """Dynamically add a new relationship table to a subgraph."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "add-relationship-table",
      "description": (
        "Add a new relationship type to the subgraph schema. "
        "Only works on subgraphs, not the parent graph. "
        "Uses IF NOT EXISTS so it's safe to call multiple times.\n\n"
        "**Example:** Add a FINDING_SUPPORTS relationship:\n"
        "```json\n"
        '{"table_name": "FINDING_SUPPORTS", "from_node": "ResearchFinding", '
        '"to_node": "Concept", "properties": [\n'
        '  {"name": "strength", "type": "DOUBLE"}\n'
        "]}\n"
        "```"
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "table_name": {
            "type": "string",
            "description": "Name for the relationship table (e.g., 'FINDING_SUPPORTS')",
          },
          "from_node": {
            "type": "string",
            "description": "Source node table name",
          },
          "to_node": {
            "type": "string",
            "description": "Target node table name",
          },
          "properties": {
            "type": "array",
            "description": "Optional property definitions for the relationship",
            "items": {
              "type": "object",
              "properties": {
                "name": {"type": "string"},
                "type": {
                  "type": "string",
                  "enum": list(VALID_PROPERTY_TYPES),
                },
              },
              "required": ["name", "type"],
            },
          },
        },
        "required": ["table_name", "from_node", "to_node"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("add-relationship-table", arguments)

    # Validate subgraph context
    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error

    table_name = arguments.get("table_name", "")
    from_node = arguments.get("from_node", "")
    to_node = arguments.get("to_node", "")
    properties = arguments.get("properties", [])

    # Validate table name
    if not TABLE_NAME_PATTERN.match(table_name):
      return {
        "error": "invalid_table_name",
        "message": "Relationship table name must start with a letter, contain only letters/numbers/underscores, and be 1-64 characters",
      }

    # Validate node references
    if not TABLE_NAME_PATTERN.match(from_node):
      return {
        "error": "invalid_from_node",
        "message": f"from_node '{from_node}' is not a valid table name",
      }
    if not TABLE_NAME_PATTERN.match(to_node):
      return {
        "error": "invalid_to_node",
        "message": f"to_node '{to_node}' is not a valid table name",
      }

    # Validate properties
    for prop in properties:
      name = prop.get("name", "")
      ptype = prop.get("type", "")
      if not PROPERTY_NAME_PATTERN.match(name):
        return {
          "error": "invalid_property_name",
          "message": f"Property name '{name}' must start with a lowercase letter and contain only lowercase letters/numbers/underscores",
        }
      if ptype not in VALID_PROPERTY_TYPES:
        return {
          "error": "invalid_property_type",
          "message": f"Property type '{ptype}' is not valid. Use one of: {', '.join(sorted(VALID_PROPERTY_TYPES))}",
        }

    # Build DDL with backtick-quoted identifiers for defense-in-depth
    props_str = ""
    if properties:
      prop_parts = [f"`{p['name']}` {p['type']}" for p in properties]
      props_str = ", " + ", ".join(prop_parts)
    ddl = f"CREATE REL TABLE IF NOT EXISTS `{table_name}`(FROM `{from_node}` TO `{to_node}`{props_str})"

    try:
      result = await self.client.graph_client.install_schema(
        graph_id=self.client.graph_id,
        custom_ddl=ddl,
      )
      success = result.get("success", False)
      if success:
        # Clear schema cache
        if hasattr(self.client, "_mcp_tools"):
          self.client._mcp_tools.clear_schema_cache()

        return {
          "success": True,
          "table_name": table_name,
          "from_node": from_node,
          "to_node": to_node,
          "properties": [p["name"] for p in properties],
          "message": f"Relationship table '{table_name}' created successfully",
        }
      else:
        return {
          "error": "schema_install_failed",
          "message": result.get("message", "Schema installation failed"),
        }
    except Exception as e:
      logger.error(
        f"Failed to create relationship table {table_name}: {e}",
        extra={"table_name": table_name, "ddl": ddl, "error_type": type(e).__name__},
      )
      raise GraphAPIError(f"Failed to create relationship table '{table_name}': {e}")
