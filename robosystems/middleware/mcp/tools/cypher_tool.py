"""
Cypher Tool - Executes read-only Cypher queries against the graph database.
"""

from typing import Any

from robosystems.logger import logger

from ..exceptions import GraphAPIError
from .base_tool import BaseTool


class CypherTool(BaseTool):
  """
  Tool for executing read-only Cypher queries.
  """

  def get_tool_definition(self) -> dict[str, Any]:
    """Get the tool definition for Cypher queries."""
    return {
      "name": "read-graph-cypher",
      "description": """Execute read-only Cypher queries against the graph database.

**OVERVIEW:**
Query the graph using Cypher syntax. Use `get-graph-schema` first to discover available node types and relationships.

**QUERY BEST PRACTICES:**
- Always include LIMIT clause for large result sets
- Use WHERE clauses to filter data effectively
- Check for NULL values: WHERE n.property IS NOT NULL
- Use CONTAINS for text search: WHERE n.name CONTAINS 'keyword'
- When joining multiple relationships from the same node, use comma-separated patterns
  in a SINGLE MATCH clause: `MATCH (n)-[:R1]->(a), (n)-[:R2]->(b)` (not separate MATCH clauses)

**SECURITY:**
- Only read operations allowed (MATCH, RETURN, WHERE, etc.)
- No write operations (CREATE, SET, DELETE, etc.)
- Query complexity is automatically monitored

**EXAMPLES:**
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

**TIP:** Use `get-graph-schema` to understand what's in the graph before writing complex queries.""",
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
    """Execute the Cypher tool."""
    self._log_tool_execution("read-graph-cypher", arguments)

    query = arguments.get("query", "").strip()
    if not query:
      raise ValueError("Query parameter is required")

    parameters = arguments.get("parameters", {})

    # Validate query for basic issues
    validation_result = self.validator.validate(query, parameters)

    # Check for write operations (read-only validation)
    self._validate_read_only(query)

    # Log query warnings
    for warning in validation_result.warnings:
      logger.warning(f"Query warnings: {warning}")

    try:
      # Execute the query through the client
      # Note: MCP queries don't consume credits but are still rate limited
      result = await self.client.execute_query(query, parameters)
      return result
    except Exception as e:
      # Sanitize error message
      error_message = self._sanitize_error_message(str(e))
      raise GraphAPIError(f"Query execution failed: {error_message}")

  def _sanitize_error_message(self, error_msg: str) -> str:
    """
    Sanitize error messages to remove sensitive information.

    Args:
        error_msg: Raw error message

    Returns:
        Sanitized error message
    """
    # Remove file paths and sensitive details
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

    # Map common errors to user-friendly messages
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
    """
    Validate that the query is read-only.

    Args:
        query: Cypher query to validate

    Raises:
        ValueError: If query contains write operations
    """
    import re

    # Remove string literals to avoid false positives
    # Replace single-quoted strings with empty placeholder
    query_without_strings = re.sub(r"'[^']*'", "''", query)
    # Replace double-quoted strings with empty placeholder
    query_without_strings = re.sub(r'"[^"]*"', '""', query_without_strings)

    query_upper = query_without_strings.upper()

    # Write operations that are not allowed - check with word boundaries
    # These need word boundary checks
    word_operations = [
      "CREATE",
      "SET",
      "DELETE",
      "REMOVE",
      "MERGE",
      "DROP",
    ]

    # These are checked as-is (with dots/spaces)
    special_operations = [
      "DETACH DELETE",
      "CALL DB.",
      "CALL APOC.",
    ]

    # Check word boundary operations
    for operation in word_operations:
      # Use word boundaries to avoid matching within words
      pattern = r"\b" + re.escape(operation) + r"\b"
      if re.search(pattern, query_upper):
        logger.warning(f"Blocked write operation '{operation}' in query")
        raise ValueError("Only read-only queries are allowed")

    # Check special operations (these include spaces/dots so less likely to have false positives)
    for operation in special_operations:
      if operation in query_upper:
        logger.warning(f"Blocked write operation '{operation}' in query")
        raise ValueError("Only read-only queries are allowed")
