"""
Graph MCP Tools - MCP tools implementation using Graph API.

This module contains the GraphMCPTools class which provides all the MCP tool
functionality for interacting with graph databases.

Tool availability is schema-driven:
- Core tools (cypher, schema, properties, structure) are always available
- Extension tools (financial statements, disclosures, etc.) require matching schema_extensions
- Infrastructure tools (workspace, memory) are gated by feature flags
"""

import json
from typing import Any

from robosystems.config import env
from robosystems.logger import logger
from robosystems.middleware.mcp.query_validator import GraphQueryValidator

from ..exceptions import (
  GraphAPIError,
  GraphQueryComplexityError,
  GraphQueryTimeoutError,
  GraphValidationError,
)
from .cypher_tool import CypherTool
from .elements_tool import ElementsTool
from .example_queries_tool import ExampleQueriesTool
from .memory import AddNodeTableTool, AddRelationshipTableTool, WriteCypherTool
from .properties_tool import PropertiesTool
from .schema_tool import SchemaTool
from .structure_tool import StructureTool
from .workspace import (
  CreateWorkspaceTool,
  DeleteWorkspaceTool,
  ListWorkspacesTool,
  SwitchWorkspaceTool,
)


def resolve_schema_extensions(graph_id: str) -> list[str]:
  """Resolve schema extensions for a graph.

  For shared repositories: reads from the adapter manifest (no DB query needed).
  For user graphs: queries the PostgreSQL graphs table.

  Returns:
      List of extension names (e.g., ["roboledger"]), or empty list.
  """
  # Shared repos: resolve from manifest (in-memory, no DB needed)
  try:
    from robosystems.config.shared_repositories import (
      get_manifest,
      is_shared_repository_or_subgraph,
      resolve_shared_repository_parent,
    )

    if is_shared_repository_or_subgraph(graph_id):
      parent_id = resolve_shared_repository_parent(graph_id)
      manifest = get_manifest(parent_id)
      if manifest and manifest.schema_extensions:
        return list(manifest.schema_extensions)
      return []
  except Exception:
    logger.warning(f"Manifest lookup failed for {graph_id}, trying PostgreSQL")

  # User graph: query PostgreSQL
  try:
    from robosystems.database import get_db_session
    from robosystems.models.iam import Graph

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      graph = Graph.get_by_id(graph_id, db)
      if graph and graph.schema_extensions:
        return list(graph.schema_extensions)
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
  except Exception:
    logger.warning(f"Could not resolve schema extensions for {graph_id}")

  return []


class GraphMCPTools:
  """
  MCP tools implementation using Graph API.

  Tool availability is layered:
  - Layer 1 (Core): cypher, schema, properties, structure — always available
  - Layer 2 (Schema): financial tools — only when schema_extensions includes "roboledger"
  - Layer 3 (Infrastructure): workspace, memory, data — gated by feature flags
  """

  def __init__(
    self,
    graph_client,
    schema_extensions: list[str] | tuple[str, ...] = (),
  ):
    # Import here to avoid circular import
    from ..client import GraphMCPClient

    self.client: GraphMCPClient = graph_client
    self.schema_extensions: tuple[str, ...] = tuple(schema_extensions)

    # Initialize query validator
    self.validator = GraphQueryValidator()

    # Layer 1: Core tools (always available for any graph)
    self.cypher_tool = CypherTool(graph_client)
    self.schema_tool = SchemaTool(graph_client)
    self.properties_tool = PropertiesTool(graph_client)
    self.structure_tool = StructureTool(graph_client)

    # Layer 2: Schema extension tools (gated by schema_extensions)
    self.example_queries_tool = None
    self.elements_tool = None
    self.financial_statement_tool = None
    self.list_disclosures_tool = None
    self.disclosure_detail_tool = None
    self.resolve_element_tool = None
    self.resolve_structure_tool = None

    if self._has_extension("roboledger"):
      self.example_queries_tool = ExampleQueriesTool(graph_client)
      self.elements_tool = ElementsTool(graph_client)

      from .disclosure_detail_tool import GetDisclosureDetailTool
      from .financial_statement_tool import GetFinancialStatementTool
      from .list_disclosures_tool import ListDisclosuresTool

      self.financial_statement_tool = GetFinancialStatementTool(graph_client)
      self.list_disclosures_tool = ListDisclosuresTool(graph_client)
      self.disclosure_detail_tool = GetDisclosureDetailTool(graph_client)

      # Semantic enrichment tools (roboledger + manifest flag)
      if self._should_include_semantic_tools():
        from .resolve_element_tool import ResolveElementTool
        from .resolve_structure_tool import ResolveStructureTool

        self.resolve_element_tool = ResolveElementTool(graph_client)
        self.resolve_structure_tool = ResolveStructureTool(graph_client)

    # Layer 3: Infrastructure tools (gated by feature flags)
    self.create_workspace_tool = None
    self.delete_workspace_tool = None
    self.list_workspaces_tool = None
    self.switch_workspace_tool = None
    if env.MCP_WORKSPACE_ENABLED:
      self.create_workspace_tool = CreateWorkspaceTool(graph_client)
      self.delete_workspace_tool = DeleteWorkspaceTool(graph_client)
      self.list_workspaces_tool = ListWorkspacesTool(graph_client)
      self.switch_workspace_tool = SwitchWorkspaceTool(graph_client)

    self.write_cypher_tool = None
    self.add_node_table_tool = None
    self.add_relationship_table_tool = None
    if env.MCP_MEMORY_ENABLED:
      self.write_cypher_tool = WriteCypherTool(graph_client)
      self.add_node_table_tool = AddNodeTableTool(graph_client)
      self.add_relationship_table_tool = AddRelationshipTableTool(graph_client)

    self.build_fact_grid_tool = None
    if env.FACT_GRID_ENABLED:
      from .data_tools import BuildFactGridTool

      self.build_fact_grid_tool = BuildFactGridTool(graph_client)

    # Cache statistics (inherited from schema tool)
    self._cache_hits = 0
    self._cache_misses = 0

    logger.info(
      f"Initialized Graph MCP tools (extensions={list(self.schema_extensions)})"
    )

  def _has_extension(self, extension: str) -> bool:
    """Check if the graph has a specific schema extension."""
    return extension in self.schema_extensions

  def _should_include_semantic_tools(self) -> bool:
    """Check if semantic enrichment tools should be included.

    Returns true if the manifest declares has_semantic_enrichment=True.
    """
    try:
      from robosystems.config.shared_repositories import get_manifest

      manifest = get_manifest(self.client.graph_id)
      if manifest and manifest.has_semantic_enrichment:
        return True
    except Exception as exc:
      graph_id = getattr(self.client, "graph_id", "unknown")
      logger.debug(f"Semantic enrichment check failed for {graph_id}: {exc}")
    return False

  def _get_semantic_tool_definitions(self) -> list[dict[str, Any]]:
    """Get semantic enrichment tool definitions.

    Returns:
        List of tool definitions (empty if semantic enrichment not enabled)
    """
    if self.resolve_element_tool is None:
      return []
    return [
      self.resolve_element_tool.get_tool_definition(),
      self.resolve_structure_tool.get_tool_definition(),
    ]

  def _should_include_element_discovery(self) -> bool:
    """
    Check if we should include the element discovery tool.

    Returns true if the manifest declares has_element_discovery=True.
    """
    from robosystems.config.shared_repositories import get_manifest

    manifest = get_manifest(self.client.graph_id)
    if manifest and manifest.has_element_discovery:
      return True

    # For non-shared graphs, we'd need to check if Element nodes exist
    # but that requires a query which we want to avoid in __init__
    return False

  def _get_workspace_tool_definitions(self) -> list[dict[str, Any]]:
    """
    Get workspace management tool definitions from actual tool implementations.

    Returns:
        List of workspace tool definitions (empty if MCP_WORKSPACE_ENABLED is false)
    """
    if self.create_workspace_tool is None:
      return []
    return [
      self.create_workspace_tool.get_tool_definition(),
      self.switch_workspace_tool.get_tool_definition(),
      self.delete_workspace_tool.get_tool_definition(),
      self.list_workspaces_tool.get_tool_definition(),
    ]

  def _get_memory_tool_definitions(self) -> list[dict[str, Any]]:
    """
    Get memory management tool definitions.

    Returns:
        List of memory tool definitions (empty if MCP_MEMORY_ENABLED is false)
    """
    if self.write_cypher_tool is None:
      return []
    return [
      self.write_cypher_tool.get_tool_definition(),
      self.add_node_table_tool.get_tool_definition(),
      self.add_relationship_table_tool.get_tool_definition(),
    ]

  def _get_data_tool_definitions(self) -> list[dict[str, Any]]:
    """
    Get data operation tool definitions from actual tool implementations.

    Returns:
        List of data tool definitions (empty if FACT_GRID_ENABLED is false)
    """
    tools = []
    if self.build_fact_grid_tool is not None:
      tools.append(self.build_fact_grid_tool.get_tool_definition())
    return tools

  def _get_curated_tool_definitions(self) -> list[dict[str, Any]]:
    """Get curated financial tool definitions (FactSet-powered)."""
    if self.financial_statement_tool is None:
      return []
    return [
      self.financial_statement_tool.get_tool_definition(),
      self.list_disclosures_tool.get_tool_definition(),
      self.disclosure_detail_tool.get_tool_definition(),
    ]

  def _tool_unavailable_reason(self, tool_name: str, feature_flag: str) -> str:
    """Return a context-aware error message for unavailable tools."""
    if self.read_only:
      return f"{tool_name} is not available on this read-only graph."
    return f"{tool_name} tool is not available. Set {feature_flag}=true to enable this feature."

  def get_tool_definitions_as_dict(self) -> list[dict[str, Any]]:
    """
    Get MCP tool definitions for graph databases, using compatible naming.

    Tool availability is schema-driven:
    - Core tools are always included (4 tools)
    - RoboLedger extension tools require "roboledger" in schema_extensions
    - Infrastructure tools are gated by feature flags

    Returns:
        List of tool definition dictionaries
    """
    # Layer 1: Core tools (always available)
    tools = [
      self.cypher_tool.get_tool_definition(),
      self.schema_tool.get_tool_definition(),
      self.properties_tool.get_tool_definition(),
      self.structure_tool.get_tool_definition(),
    ]

    # Layer 2: Schema extension tools (roboledger)
    if self._has_extension("roboledger"):
      tools.append(self.example_queries_tool.get_tool_definition())

      # Semantic enrichment tools (preferred path for concept resolution)
      tools.extend(self._get_semantic_tool_definitions())

      # Element discovery (conditioned on manifest flag)
      if self._should_include_element_discovery():
        tools.append(self.elements_tool.get_tool_definition())

      # Curated financial tools (FactSet-powered)
      tools.extend(self._get_curated_tool_definitions())

      # Fact grid tool (custom element/period/entity queries)
      tools.extend(self._get_data_tool_definitions())

    # Layer 3: Infrastructure tools (feature-flag gated)
    tools.extend(self._get_workspace_tool_definitions())
    tools.extend(self._get_memory_tool_definitions())

    return tools

  async def call_tool(
    self, name: str, arguments: dict[str, Any], return_raw: bool = False
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
      # Layer 1: Core tools (always available)
      if name == "read-graph-cypher":
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

      # Layer 2: Schema extension tools (roboledger-gated)
      elif name == "get-example-queries":
        if self.example_queries_tool is None:
          raise ValueError(
            "get-example-queries tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.example_queries_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "discover-common-elements":
        if self.elements_tool is None:
          raise ValueError(
            "discover-common-elements tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.elements_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "resolve-element":
        if self.resolve_element_tool is None:
          raise ValueError(
            "resolve-element tool is not available. "
            "This graph does not have semantic enrichment enabled."
          )
        result = await self.resolve_element_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "resolve-structure":
        if self.resolve_structure_tool is None:
          raise ValueError(
            "resolve-structure tool is not available. "
            "This graph does not have semantic enrichment enabled."
          )
        result = await self.resolve_structure_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-financial-statement":
        if self.financial_statement_tool is None:
          raise ValueError(
            "get-financial-statement tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.financial_statement_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-disclosures":
        if self.list_disclosures_tool is None:
          raise ValueError(
            "list-disclosures tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.list_disclosures_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-disclosure-detail":
        if self.disclosure_detail_tool is None:
          raise ValueError(
            "get-disclosure-detail tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.disclosure_detail_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Layer 3: Infrastructure tools (feature-flag gated)
      elif name == "create-workspace":
        if self.create_workspace_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("create-workspace", "MCP_WORKSPACE_ENABLED")
          )
        result = await self.create_workspace_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "delete-workspace":
        if self.delete_workspace_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("delete-workspace", "MCP_WORKSPACE_ENABLED")
          )
        result = await self.delete_workspace_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-workspaces":
        if self.list_workspaces_tool is None:
          raise ValueError(
            "list-workspaces tool is not available. "
            "Set MCP_WORKSPACE_ENABLED=true to enable this feature."
          )
        result = await self.list_workspaces_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "switch-workspace":
        # This is client-side only - should be intercepted by client
        if self.switch_workspace_tool is None:
          raise ValueError(
            "switch-workspace tool is not available. "
            "Set MCP_WORKSPACE_ENABLED=true to enable this feature."
          )
        result = await self.switch_workspace_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "write-graph-cypher":
        if self.write_cypher_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("write-graph-cypher", "MCP_MEMORY_ENABLED")
          )
        result = await self.write_cypher_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "add-node-table":
        if self.add_node_table_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("add-node-table", "MCP_MEMORY_ENABLED")
          )
        result = await self.add_node_table_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "add-relationship-table":
        if self.add_relationship_table_tool is None:
          raise ValueError(
            self._tool_unavailable_reason(
              "add-relationship-table", "MCP_MEMORY_ENABLED"
            )
          )
        result = await self.add_relationship_table_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "build-fact-grid":
        if self.build_fact_grid_tool is None:
          raise ValueError(
            "build-fact-grid tool is not available. "
            "Set FACT_GRID_ENABLED=true to enable this feature."
          )
        result = await self.build_fact_grid_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      else:
        raise ValueError(f"Unknown tool: {name}")

    except GraphQueryTimeoutError as e:
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

    except GraphQueryComplexityError as e:
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

    except GraphAPIError as e:
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
        e.args = (enhanced_msg, *e.args[1:]) if len(e.args) > 1 else (enhanced_msg,)
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
        raise GraphValidationError(error_msg, validation_errors=[error_msg])
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
        raise GraphAPIError(f"Tool execution failed: {error_msg}")
      return f"Error: {error_msg}"

  async def execute_cypher_tool(
    self, query: str, parameters: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """
    Execute Cypher tool directly.

    Args:
        query: Cypher query
        parameters: Optional query parameters

    Returns:
        Query result
    """
    arguments: dict[str, Any] = {"query": query}
    if parameters:
      arguments["parameters"] = parameters

    return await self.call_tool("read-graph-cypher", arguments, return_raw=True)

  async def execute_schema_tool(self) -> list[dict[str, Any]]:
    """
    Execute schema retrieval tool.

    Returns:
        Schema information list
    """
    return await self.call_tool("get-graph-schema", {}, return_raw=True)

  def _build_error_context(
    self, tool_name: str, arguments: dict[str, Any], exception: Exception
  ) -> dict[str, Any]:
    """Build comprehensive error context for logging and debugging."""
    context: dict[str, Any] = {
      "tool_name": tool_name,
      "graph_id": self.client.graph_id,
      "exception_type": type(exception).__name__,
    }

    # Add argument context (sanitized)
    if arguments:
      # Don't log full query content for security, just metadata
      arg_context: dict[str, Any] = {}
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
    self, error_msg: str, tool_name: str, arguments: dict[str, Any]
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
      if arguments.get("node_type"):
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

  def get_cache_stats(self) -> dict[str, Any]:
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
