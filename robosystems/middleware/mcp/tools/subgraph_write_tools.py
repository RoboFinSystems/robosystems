"""Schema and data write tools, for subgraphs only.

The parent graph is read-only to raw statements; its writes go through
structured extension ops.
"""

import re
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.utils import is_subgraph

from ..exceptions import GraphAPIError
from .base_tool import BaseTool

# Imported lazily: the adapter chain imports back into this package.
_is_shared_repository_or_subgraph = None


def _get_is_shared_repository_or_subgraph():
  global _is_shared_repository_or_subgraph
  if _is_shared_repository_or_subgraph is None:
    from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

    _is_shared_repository_or_subgraph = is_shared_repository_or_subgraph
  return _is_shared_repository_or_subgraph


VALID_PROPERTY_TYPES = {"STRING", "INT32", "INT64", "DOUBLE", "BOOLEAN"}

TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")

PROPERTY_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


def _validate_subgraph_context(graph_id: str) -> dict[str, Any] | None:
  """Error dict unless the graph is a non-shared subgraph."""
  if not is_subgraph(graph_id):
    return {
      "error": "subgraph_required",
      "message": "This tool only works on subgraphs, not the parent graph. "
      "Use create-subgraph to create a subgraph first, "
      "then connect to that subgraph's own MCP endpoint "
      "(list-subgraphs returns each connector_url).",
    }

  if _get_is_shared_repository_or_subgraph()(graph_id):
    return {
      "error": "read_only",
      "message": "Shared repository graphs are read-only. "
      "Write operations are not allowed on shared repositories or their subgraphs.",
    }

  return None


def _validate_write_query(query: str) -> str | None:
  """Error message, or None. Uses the kernel's analyzer predicates, since this
  guard also covers the Operator path, which bypasses the kernel."""
  from robosystems.security.cypher_analyzer import (
    has_opaque_statement_call,
    is_admin_operation,
    is_bulk_operation,
    is_schema_ddl,
    is_write_operation,
    references_catalog_label,
  )

  # A string payload (CALL GQL('...')) counts as a write but is opaque to the
  # gates below; refuse it, as the kernel does.
  if has_opaque_statement_call(query):
    return (
      "Blocked operation detected. Procedures that execute a statement passed "
      "as a string (e.g. CALL GQL), and CALLs with a quoted procedure name, are "
      "not allowed; submit the statement directly with an unquoted name."
    )

  if references_catalog_label(query):
    return (
      "Blocked operation detected. Catalog labels (schema_table, schema_rel) "
      "are not queryable; use get-graph-schema."
    )

  if is_schema_ddl(query) or is_bulk_operation(query) or is_admin_operation(query):
    return "Blocked operation detected. DDL and system operations are not allowed in write queries."

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
        "Execute a write Cypher query on the active subgraph.\n\n"
        "**WHEN TO USE:**\n"
        "- To create, update or delete data in a subgraph\n"
        "- Only on subgraphs — the parent graph is not writable here\n\n"
        "**PARAMETERS:**\n"
        "- ``query``: Cypher limited to CREATE, MERGE, SET, DELETE, REMOVE\n\n"
        "**RETURNS:** `success`, plus `result` — the rows the query itself "
        "RETURNs, which is empty for a bare CREATE or SET. There is no "
        "affected-row count; to confirm a write, RETURN what you wrote.\n\n"
        "**NOTES:** Built for crafting a graph a statement at a time. Many "
        "rows go in one call as a list parameter (`UNWIND $rows AS r MERGE "
        "...`): at most 1,000 items and 50,000 characters across them, and "
        "10,000 characters in any one string. Past a few hundred rows, or for "
        "long text, use create-table-import instead.\n\n"
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

    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error

    query = arguments.get("query", "").strip()
    if not query:
      return {"error": "invalid_query", "message": "Query parameter is required"}

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
        "Add a new node type to the subgraph schema.\n\n"
        "**WHEN TO USE:**\n"
        "- To extend a subgraph's schema with a new node table\n"
        "- Only on subgraphs — the parent graph's schema is fixed\n\n"
        "**RETURNS:** Confirmation that the node table exists.\n\n"
        "**NOTES:** Uses IF NOT EXISTS, so repeat calls are safe. Example — "
        "add a CompanyProfile node type:\n"
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

    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error

    table_name = arguments.get("table_name", "")
    properties = arguments.get("properties", [])

    if not TABLE_NAME_PATTERN.match(table_name):
      return {
        "error": "invalid_table_name",
        "message": "Table name must start with a letter, contain only letters/numbers/underscores, and be 1-64 characters",
      }

    if not properties:
      return {
        "error": "invalid_properties",
        "message": "At least one property is required",
      }

    has_pk = any(p.get("is_primary_key") for p in properties)
    if not has_pk:
      return {
        "error": "missing_primary_key",
        "message": "At least one property must be marked as is_primary_key: true",
      }

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
        "Add a new relationship type to the subgraph schema.\n\n"
        "**WHEN TO USE:**\n"
        "- To extend a subgraph's schema with a new relationship table\n"
        "- Only on subgraphs — the parent graph's schema is fixed\n\n"
        "**RETURNS:** Confirmation that the relationship table exists.\n\n"
        "**NOTES:** Uses IF NOT EXISTS, so repeat calls are safe. Example — "
        "add a FINDING_SUPPORTS relationship:\n"
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

    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error

    table_name = arguments.get("table_name", "")
    from_node = arguments.get("from_node", "")
    to_node = arguments.get("to_node", "")
    properties = arguments.get("properties", [])

    if not TABLE_NAME_PATTERN.match(table_name):
      return {
        "error": "invalid_table_name",
        "message": "Relationship table name must start with a letter, contain only letters/numbers/underscores, and be 1-64 characters",
      }

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


def _import_user_id(client) -> str | None:
  user = getattr(client, "user", None)
  return str(user.id) if user is not None else None


_NO_USER = {
  "error": "authentication_required",
  "message": "User context required for this operation.",
}

_IMPORT_TABLES_SCHEMA = {
  "type": "array",
  "items": {"type": "string"},
  "minItems": 1,
  "description": "Names of tables that exist in this subgraph's schema",
}


class CreateTableImportTool(BaseTool):
  """Presign Parquet uploads for a bulk import into a subgraph's tables."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-table-import",
      "description": (
        "Start a bulk import: get one upload URL per table, for a Parquet file "
        "of rows to add to that table.\n\n"
        "**WHEN TO USE:**\n"
        "- To load more rows than write-graph-cypher is comfortable with "
        "(hundreds or more), or rows carrying long text\n"
        "- You can run shell commands, so you can write Parquet files and PUT "
        "them. Without a shell, use write-graph-cypher\n\n"
        "**WORKFLOW:**\n"
        "1. Create the tables first (add-node-table, add-relationship-table).\n"
        "2. Call this with the table names.\n"
        "3. PUT one Parquet file to each `upload_url` with the returned "
        "`content_type` header. The bytes go straight to storage, not through "
        "this connection.\n"
        "4. Call run-table-import with the `import_id` and the table names.\n\n"
        "**PARAMETERS:** each file has columns named as the table's properties. A node "
        "table's file must carry its primary key column, named `identifier`. A "
        "relationship table's file carries `from` and `to` (the two nodes' "
        "`identifier` values), then its properties.\n\n"
        "**RETURNS:** `import_id`, `content_type`, `expires_in`, and `uploads` "
        "(`table_name`, `upload_url`). Nothing is recorded until "
        "run-table-import; an import never run expires with its files."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {"tables": _IMPORT_TABLES_SCHEMA},
        "required": ["tables"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.operations.graph.subgraph_import import (
      SubgraphImportError,
      begin_import,
    )

    self._log_tool_execution("create-table-import", arguments)

    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error
    user_id = _import_user_id(self.client)
    if user_id is None:
      return _NO_USER

    try:
      result = begin_import(
        graph_id=self.client.graph_id,
        user_id=user_id,
        tables=arguments.get("tables") or [],
      )
    except SubgraphImportError as e:
      return {"error": "invalid_input", "message": str(e)}
    result["next"] = (
      "PUT a Parquet file to each upload_url with the Content-Type header set "
      "to content_type, then call run-table-import with import_id and tables."
    )
    return result


class RunTableImportTool(BaseTool):
  """Copy the uploaded Parquet files into the subgraph's tables."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "run-table-import",
      "description": (
        "Run a bulk import started with create-table-import: add each "
        "uploaded file's rows to its table, node tables before relationship "
        "tables.\n\n"
        "**WHEN TO USE:**\n"
        "- After every file of a create-table-import has been PUT\n"
        "- Again, with `remaining`, while the result says `done` is false\n\n"
        "**RETURNS:** `tables` (per table: `status` and `rows_ingested`), the "
        "total `rows_ingested`, and `remaining` — the tables a single call did "
        "not reach. While `done` is false, call again with `tables` set to "
        "`remaining`.\n\n"
        "**NOTES:** Rows are added, never replaced: a node whose `identifier` "
        "is already in the table is skipped, and so is a relationship between "
        "two nodes that already have one in that table, so repeating a table "
        "is safe. A relationship row whose `from` or `to` node does not exist "
        "fails that table's copy. A table's `status` is `imported`, "
        "`not_uploaded`, `no_such_table` or `failed` (with `stage` and "
        "`message`)."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "import_id": {
            "type": "string",
            "description": "The import_id create-table-import returned",
          },
          "tables": _IMPORT_TABLES_SCHEMA,
        },
        "required": ["import_id", "tables"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.operations.graph.subgraph_import import (
      SubgraphImportError,
      run_import,
    )

    self._log_tool_execution("run-table-import", arguments)

    error = _validate_subgraph_context(self.client.graph_id)
    if error:
      return error
    user_id = _import_user_id(self.client)
    if user_id is None:
      return _NO_USER

    try:
      return await run_import(
        graph_id=self.client.graph_id,
        user_id=user_id,
        import_id=str(arguments.get("import_id") or ""),
        tables=arguments.get("tables") or [],
      )
    except SubgraphImportError as e:
      return {"error": "invalid_input", "message": str(e)}
    except Exception as e:
      logger.error(f"Table import failed: {e}")
      raise GraphAPIError(f"Table import failed: {e}")
