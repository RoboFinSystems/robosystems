"""The read-graph-cypher tool and its read-only guard."""

from typing import TYPE_CHECKING, Any

from robosystems.logger import logger

from ..exceptions import GraphAPIError
from .base_tool import BaseTool
from .constants import (
  INVESTOR_AMOUNT_GUIDANCE,
  LEDGER_AMOUNT_GUIDANCE,
  LEDGER_ANCHOR_GUIDANCE,
  LEDGER_STATUS_GUIDANCE,
)

if TYPE_CHECKING:
  from ..client import GraphMCPClient


class CypherTool(BaseTool):
  def __init__(
    self,
    client: "GraphMCPClient",
    schema_extensions: list[str] | tuple[str, ...] = (),
  ):
    super().__init__(client)
    self.schema_extensions: tuple[str, ...] = tuple(schema_extensions)

  def _has_ledger_spine(self) -> bool:
    """True only for tenant graphs. The SEC repo carries ``roboledger`` and
    empty REA tables but no ledger, so table presence is no signal."""
    if "roboledger" not in self.schema_extensions:
      return False
    return self._is_tenant_graph()

  def _has_investor_positions(self) -> bool:
    if "roboinvestor" not in self.schema_extensions:
      return False
    return self._is_tenant_graph()

  def _is_tenant_graph(self) -> bool:
    try:
      from robosystems.config.shared_repositories import (
        is_shared_repository_or_subgraph,
      )

      return not is_shared_repository_or_subgraph(self.client.graph_id)
    except Exception as e:
      logger.debug(f"Tenant-graph check failed for {self.client.graph_id}: {e}")
      return False

  def get_tool_definition(self) -> dict[str, Any]:
    description = """Execute read-only Cypher queries against the graph database.

**WHEN TO USE:**
- To traverse the graph directly when no typed tool covers the question
- Call `get-graph-schema` first to discover node types and relationships

**RETURNS:** The rows the query RETURNs, as records. Read-only: the write
verbs are rejected before execution, and query complexity is monitored.

**NOTES:**
- Always include LIMIT clause for large result sets
- Use WHERE clauses to filter data effectively
- Check for NULL values: WHERE n.property IS NOT NULL
- Use CONTAINS for text search: WHERE n.name CONTAINS 'keyword'
- When joining multiple relationships from the same node, use comma-separated patterns
  in a SINGLE MATCH clause: `MATCH (n)-[:R1]->(a), (n)-[:R2]->(b)` (not separate MATCH clauses)


**WORKFLOW:**
```cypher
// Count nodes by type
MATCH (n)
WITH labels(n) AS label, count(n) AS count
RETURN label, count ORDER BY count DESC

// Explore node properties
MATCH (n:Entity) RETURN keys(n) LIMIT 1

// Find relationships between node types
MATCH (a)-[r]->(b)
RETURN DISTINCT labels(a)[0] AS from_type, type(r) AS rel_type, labels(b)[0] AS to_type
```

**RELATED TOOLS:** `get-graph-schema` for what is in the graph, and
`get-example-queries` for patterns that already work against it."""

    if self._has_ledger_spine():
      # Anchor first, then status: a query anchored on the wrong node loses
      # rows outright, which no amount of correct status filtering recovers.
      description += "\n\n" + LEDGER_ANCHOR_GUIDANCE
      description += "\n\n" + LEDGER_STATUS_GUIDANCE
      description += "\n\n" + LEDGER_AMOUNT_GUIDANCE
    if self._has_investor_positions():
      description += "\n\n" + INVESTOR_AMOUNT_GUIDANCE

    return {
      "name": "read-graph-cypher",
      "description": description,
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": "Cypher read query to execute",
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

  async def execute(self, arguments: dict[str, Any]) -> list[dict[str, Any]]:
    self._log_tool_execution("read-graph-cypher", arguments)

    query = arguments.get("query", "").strip()
    if not query:
      raise ValueError("Query parameter is required")

    parameters = arguments.get("parameters", {})

    validation_result = self.validator.validate(query, parameters)

    self._validate_read_only(query)

    for warning in validation_result.warnings:
      logger.warning(f"Query warnings: {warning}")

    try:
      result = await self.client.execute_query(query, parameters)
      return result
    except Exception as e:
      error_message = self._sanitize_error_message(str(e))
      raise GraphAPIError(f"Query execution failed: {error_message}")

  def _sanitize_error_message(self, error_msg: str) -> str:
    sensitive_patterns = [
      r"/[^\s]+\.db",  # Database file paths
      r"password[=:][^\s]+",  # Password patterns
      r"token[=:][^\s]+",  # Token patterns
      r"key[=:][^\s]+",  # Key patterns
    ]

    sanitized = error_msg
    for pattern in sensitive_patterns:
      import re

      sanitized = re.sub(pattern, "[REDACTED]", sanitized, flags=re.IGNORECASE)

    error_mappings = {
      "connection": "Database connection failed",
      "timeout": "Query execution timed out",
      "syntax": "Query syntax error",
      "permission": "Insufficient permissions",
    }

    for key, friendly_msg in error_mappings.items():
      if key.lower() in sanitized.lower():
        return friendly_msg

    return sanitized

  def _validate_read_only(self, query: str) -> None:
    assert_read_only_cypher(query, self.client.graph_id)


def assert_read_only_cypher(query: str, graph_id: str | None = None) -> None:
  """Raise ValueError for anything but a read.

  Module-level so every path running read-graph-cypher shares it, including
  the MCP routers' queued strategies, which never construct the tool. With
  ``graph_id``, a shared repository's read limits apply too, since the
  Operator path bypasses the kernel that enforces them elsewhere.
  """
  # The same analyzer predicates the StatementKernel composes.
  from robosystems.security.cypher_analyzer import (
    is_admin_operation,
    is_bulk_operation,
    is_schema_ddl,
    is_write_operation,
  )

  if is_bulk_operation(query):
    logger.warning("Blocked bulk operation (COPY/LOAD/IMPORT) in read-graph-cypher")
    raise ValueError("Only read-only queries are allowed")

  if is_admin_operation(query):
    logger.warning(
      "Blocked administrative operation (EXPORT/INSTALL/ATTACH/USE) in "
      "read-graph-cypher"
    )
    raise ValueError("Only read-only queries are allowed")

  if is_schema_ddl(query):
    logger.warning("Blocked schema DDL in read-graph-cypher")
    raise ValueError("Only read-only queries are allowed")

  if is_write_operation(query):
    logger.warning(
      "Blocked write operation (CREATE/MERGE/SET/DELETE) in read-graph-cypher"
    )
    raise ValueError("Only read-only queries are allowed")

  if graph_id:
    from robosystems.middleware.graph.statement_kernel import (
      shared_repository_read_refusal,
    )

    refusal = shared_repository_read_refusal(graph_id, query)
    if refusal:
      logger.warning(f"Refused read in read-graph-cypher on {graph_id}")
      raise ValueError(refusal)
