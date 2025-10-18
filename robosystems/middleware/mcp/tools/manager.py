"""
Kuzu MCP Tools - MCP tools implementation using Graph API.

This module contains the KuzuMCPTools class which provides all the MCP tool
functionality for interacting with Kuzu graph databases.
"""

import json
from typing import Any, Dict, List, Optional

from robosystems.logger import logger
from robosystems.middleware.mcp.query_validator import KuzuQueryValidator
from ..exceptions import (
  KuzuAPIError,
  KuzuQueryTimeoutError,
  KuzuQueryComplexityError,
  KuzuValidationError,
)
from . import (
  ExampleQueriesTool,
  CypherTool,
  SchemaTool,
  PropertiesTool,
  StructureTool,
  ElementsTool,
  FactsTool,
)


class KuzuMCPTools:
  """
  MCP tools implementation using Graph API.

  Provides the same interface as graph database MCP tools but uses Graph API backend.
  """

  def __init__(self, kuzu_client):
    # Import here to avoid circular import
    from ..client import KuzuMCPClient

    self.client: KuzuMCPClient = kuzu_client

    # Initialize query validator
    self.validator = KuzuQueryValidator()

    # Initialize individual tools
    self.example_queries_tool = ExampleQueriesTool(kuzu_client)
    self.cypher_tool = CypherTool(kuzu_client)
    self.schema_tool = SchemaTool(kuzu_client)
    self.properties_tool = PropertiesTool(kuzu_client)
    self.structure_tool = StructureTool(kuzu_client)
    self.elements_tool = ElementsTool(kuzu_client)
    self.facts_tool = FactsTool(kuzu_client)

    # Cache statistics (inherited from schema tool)
    self._cache_hits = 0
    self._cache_misses = 0

    logger.info("Initialized Kuzu MCP tools with query validator enabled")

  def _should_include_element_discovery(self) -> bool:
    """
    Check if we should include the element discovery tool.

    Returns true if:
    1. This is the SEC database (always has elements)
    2. OR if Element nodes exist in the current graph
    """
    if self.client.graph_id == "sec":
      return True

    # For other graphs, we'd need to check if Element nodes exist
    # but that requires a query which we want to avoid in __init__
    # So we'll include it by default and let the tool handle the check
    return False

  def get_tool_definitions_as_dict(self) -> List[Dict[str, Any]]:
    """
    Get MCP tool definitions for graph databases, using compatible naming.

    Returns:
        List of tool definition dictionaries
    """
    tools = [
      self.example_queries_tool.get_tool_definition(),
      self.cypher_tool.get_tool_definition(),
      self.schema_tool.get_tool_definition(),
      self.properties_tool.get_tool_definition(),
      self.structure_tool.get_tool_definition(),
    ]

    # Conditionally include element and facts tools for financial graphs
    if self._should_include_element_discovery():
      tools.extend(
        [
          self.elements_tool.get_tool_definition(),
          self.facts_tool.get_tool_definition(),
        ]
      )

    return tools

  async def call_tool(
    self, name: str, arguments: Dict[str, Any], return_raw: bool = False
  ) -> Any:
    """
    Call a specific MCP tool by name.

    Args:
        name: Tool name
        arguments: Tool arguments
        return_raw: Whether to return raw result or formatted string

    Returns:
        Tool execution result
    """
    try:
      # Route to appropriate tool
      if name == "get-example-queries":
        result = await self.example_queries_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "read-graph-cypher":
        result = await self.cypher_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-graph-schema":
        # For schema tool, we need to handle caching differently
        result = await self.schema_tool.execute(arguments)

        # Update our cache stats from schema tool
        schema_stats = self.schema_tool.get_cache_stats()
        self._cache_hits = schema_stats["cache_hits"]
        self._cache_misses = schema_stats["cache_misses"]

        if return_raw:
          return result
        else:
          cache_info = {
            "_cache_metadata": {
              "cached": schema_stats["is_cached"],
              "cache_age_seconds": schema_stats.get("cache_age_seconds"),
              "cache_hit_rate": f"{schema_stats['hit_rate_percent']:.1f}%",
            },
            "schema": result,
          }
          return json.dumps(cache_info, indent=2)

      elif name == "discover-properties":
        result = await self.properties_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "describe-graph-structure":
        result = await self.structure_tool.execute(arguments)
        return result if return_raw else result  # Already a string

      elif name == "discover-common-elements":
        result = await self.elements_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "discover-facts":
        result = await self.facts_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      else:
        raise ValueError(f"Unknown tool: {name}")

    except KuzuQueryTimeoutError as e:
      # Enhanced timeout error handling
      error_context = self._build_error_context(name, arguments, e)
      error_msg = str(e)

      # Add timeout-specific suggestions
      if name == "read-graph-cypher" and "query" in arguments:
        query = arguments["query"]
        if len(query) > 1000:
          error_msg += (
            "\n💡 Large query detected. Consider breaking into smaller parts."
          )
        if "LIMIT" not in query.upper():
          error_msg += "\n💡 Add LIMIT clause to reduce result size."

      logger.error(
        f"Query timeout in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )
      if return_raw:
        raise  # Re-raise for raw mode
      return f"Timeout: {error_msg}"

    except KuzuQueryComplexityError as e:
      # Enhanced complexity error handling
      error_context = self._build_error_context(name, arguments, e)
      error_msg = str(e)

      # Add complexity-specific suggestions
      if hasattr(e, "details") and "complexity_score" in e.details:
        score = e.details["complexity_score"]
        error_msg += f"\n💡 Complexity score: {score}. Consider simplifying the query."

      logger.error(
        f"Query complexity error in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )
      if return_raw:
        raise  # Re-raise for raw mode
      return f"Complexity Error: {error_msg}"

    except KuzuAPIError as e:
      # Enhanced error handling with more context
      error_msg = str(e)
      error_context = self._build_error_context(name, arguments, e)
      logger.error(
        f"Graph API error in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )

      # Add helpful context based on error type and tool
      enhanced_msg = self._enhance_error_message(error_msg, name, arguments)

      if return_raw:
        # Preserve original exception with enhanced message
        e.args = (enhanced_msg,) + e.args[1:] if len(e.args) > 1 else (enhanced_msg,)
        if hasattr(e, "details"):
          e.details = {**e.details, **error_context}
        raise
      return f"Error: {enhanced_msg}"

    except ValueError as e:
      # Handle argument validation errors with specific context
      error_msg = str(e)
      if "Query parameter" in error_msg or "argument" in error_msg.lower():
        error_msg = f"Invalid argument in tool '{name}': {error_msg}"
        if arguments:
          error_msg += f"\nProvided arguments: {list(arguments.keys())}"

      logger.error(f"Argument validation error in tool '{name}': {error_msg}")
      if return_raw:
        raise KuzuValidationError(error_msg, validation_errors=[error_msg])
      return f"Validation Error: {error_msg}"

    except Exception as e:
      # Handle other errors with enhanced sanitization and context
      error_context = self._build_error_context(name, arguments, e)
      error_msg = self._sanitize_error_message(str(e))

      logger.error(
        f"Tool execution failed for '{name}': {error_msg}",
        extra={"error_context": error_context, "exception_type": type(e).__name__},
      )

      if return_raw:
        raise KuzuAPIError(f"Tool execution failed: {error_msg}")
      return f"Error: {error_msg}"

  async def execute_cypher_tool(
    self, query: str, parameters: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """
    Execute Cypher tool directly.

    Args:
        query: Cypher query
        parameters: Optional query parameters

    Returns:
        Query result
    """
    arguments: Dict[str, Any] = {"query": query}
    if parameters:
      arguments["parameters"] = parameters

    return await self.call_tool("read-graph-cypher", arguments, return_raw=True)

  async def execute_schema_tool(self) -> List[Dict[str, Any]]:
    """
    Execute schema retrieval tool.

    Returns:
        Schema information list
    """
    return await self.call_tool("get-graph-schema", {}, return_raw=True)

  def _build_error_context(
    self, tool_name: str, arguments: Dict[str, Any], exception: Exception
  ) -> Dict[str, Any]:
    """Build comprehensive error context for logging and debugging."""
    context: Dict[str, Any] = {
      "tool_name": tool_name,
      "graph_id": self.client.graph_id,
      "exception_type": type(exception).__name__,
    }

    # Add argument context (sanitized)
    if arguments:
      # Don't log full query content for security, just metadata
      arg_context: Dict[str, Any] = {}
      for key, value in arguments.items():
        if key == "query" and isinstance(value, str):
          arg_context[key] = {
            "length": len(value),
            "has_limit": "LIMIT" in value.upper(),
            "has_where": "WHERE" in value.upper(),
            "has_match": "MATCH" in value.upper(),
          }
        elif key == "parameters":
          arg_context[key] = {"param_count": len(value) if value else 0}
        else:
          arg_context[key] = type(value).__name__

      context["arguments"] = arg_context

    # Add exception-specific context
    if hasattr(exception, "error_code"):
      context["error_code"] = exception.error_code
    if hasattr(exception, "details"):
      context["exception_details"] = exception.details

    return context

  def _enhance_error_message(
    self, error_msg: str, tool_name: str, arguments: Dict[str, Any]
  ) -> str:
    """Enhance error messages with tool-specific context and suggestions."""
    enhanced_msg = error_msg

    # Add tool-specific suggestions
    if tool_name == "read-graph-cypher":
      if "Parser exception" in error_msg:
        enhanced_msg += "\n\n🔧 Query Syntax Help:"
        enhanced_msg += "\n- Check node labels exist: Use get-graph-schema first"
        enhanced_msg += (
          "\n- Property access: n.property_name (use keys(n) to discover properties)"
        )
        enhanced_msg += "\n- Ensure proper Cypher syntax for graph database"

      elif "property" in error_msg.lower() and "not found" in error_msg.lower():
        enhanced_msg += "\n\n🔧 Property Help:"
        enhanced_msg += "\n- Use keys(node) to list available properties"
        enhanced_msg += "\n- Common properties: identifier, name, value, uri"
        enhanced_msg += "\n- Properties vary by node type - check schema first"

      elif "connection" in error_msg.lower():
        enhanced_msg += "\n\n🔧 Connection Help:"
        enhanced_msg += "\n- Check if Graph API service is running"
        enhanced_msg += "\n- Verify network connectivity and firewall settings"
        enhanced_msg += "\n- Ensure correct API endpoint configuration"

    elif tool_name == "get-graph-schema" and "timeout" in error_msg.lower():
      enhanced_msg += "\n\n💡 Large schema detected. Consider using discover-properties for specific node types."

    elif tool_name == "discover-properties":
      if "node_type" in arguments and arguments["node_type"]:
        node_type = arguments["node_type"]
        enhanced_msg += f"\n\n💡 If '{node_type}' doesn't exist, check available labels with get-graph-schema first."

    # Add general suggestions based on error patterns
    if "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
      enhanced_msg += "\n\n🔐 Check API permissions and authentication credentials."

    elif "rate limit" in error_msg.lower():
      enhanced_msg += (
        "\n\n⏱️ API rate limit exceeded. Wait before retrying or reduce query frequency."
      )

    return enhanced_msg

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

  def clear_schema_cache(self):
    """Clear the schema cache to force refresh on next call."""
    self.schema_tool.clear_schema_cache()
    logger.debug("Schema cache cleared")

  def get_cache_stats(self) -> Dict[str, Any]:
    """Get cache performance statistics."""
    return self.schema_tool.get_cache_stats()

  async def close(self):
    """Close MCP tools and log final statistics."""
    # Log final cache statistics
    stats = self.get_cache_stats()
    logger.info(
      f"MCP Tools cache stats - Hits: {stats['cache_hits']}, "
      f"Misses: {stats['cache_misses']}, Hit Rate: {stats['hit_rate_percent']:.1f}%"
    )
