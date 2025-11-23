"""
Cypher Tool - Executes read-only Cypher queries against the graph database.
"""

from typing import Any, Dict, List

from .base_tool import BaseTool
from robosystems.logger import logger
from ..exceptions import GraphAPIError


class CypherTool(BaseTool):
  """
  Tool for executing read-only Cypher queries.
  """

  def get_tool_definition(self) -> Dict[str, Any]:
    """Get the tool definition for Cypher queries."""
    return {
      "name": "read-graph-cypher",
      "description": """Execute read-only Cypher queries against the graph database.

**OVERVIEW:**
Query the graph using Cypher syntax. The database uses either RoboSystems' financial schema or custom custom schemas.

**ROBOSYSTEMS SCHEMA PATTERNS:**

1. **Financial Reporting (SEC/XBRL):**
   MATCH (e:Entity)-[:HAS_REPORT]->(r:Report)-[:REPORTED_IN]->(f:Fact)

2. **Facts with Elements (Metrics):**
   MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)

3. **Time-based Analysis:**
   MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)

4. **Dimensional Analysis (Segments):**
   MATCH (f:Fact)-[:FACT_HAS_DIMENSION]->(d:FactDimension)

5. **Units and Context:**
   MATCH (f:Fact)-[:FACT_HAS_UNIT]->(u:Unit)

**QUERY BEST PRACTICES:**
- Always include LIMIT clause for large result sets
- Use WHERE clauses to filter data effectively
- Check for NULL values: WHERE f.numeric_value IS NOT NULL
- Use CONTAINS for text search: WHERE e.name CONTAINS 'keyword'

**SECURITY:**
- Only read operations allowed (MATCH, RETURN, WHERE, etc.)
- No write operations (CREATE, SET, DELETE, etc.)
- Query complexity is automatically monitored

**EXAMPLES:**
```cypher
// Get company revenue facts
MATCH (e:Entity)-[:HAS_REPORT]->(r:Report)-[:REPORTED_IN]->(f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
WHERE el.name CONTAINS 'Revenue' AND f.numeric_value IS NOT NULL
RETURN e.name, f.numeric_value, f.currency_code
LIMIT 10

// Find facts by time period
MATCH (f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
WHERE p.end_date >= '2023-01-01'
RETURN count(f) as facts_count

// Explore available metrics
MATCH (el:Element)
RETURN el.name, count(*) as usage_count
ORDER BY usage_count DESC
LIMIT 20
```""",
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

  async def execute(self, arguments: Dict[str, Any]) -> List[Dict[str, Any]]:
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
