"""
MCP handler implementation and helper functions.

This module provides the core MCPHandler class and related utilities
for executing MCP tools with proper lifecycle management.
"""

import json
import asyncio
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.logger import logger

# Import timeout utilities
from robosystems.middleware.robustness.timeout_coordinator import TimeoutCoordinator

# Import Kuzu MCP components
from robosystems.middleware.mcp import (
  create_kuzu_mcp_client,
  KuzuMCPTools as AdapterKuzuMCPTools,
  KuzuQueryTimeoutError,
  KuzuQueryComplexityError,
  KuzuAPIError,
)

# MCP package is no longer used - always use adapter
MCP_AVAILABLE = False

# Initialize timeout coordinator
timeout_coordinator = TimeoutCoordinator()


async def validate_mcp_access(
  graph_id: str, current_user: Any, db: Session, operation_type: str = "read"
) -> None:
  """
  Validate user access for MCP operations based on graph type.

  Args:
      graph_id: Graph database identifier
      current_user: Authenticated user
      db: Database session
      operation_type: Type of operation (read, write, admin)

  Raises:
      HTTPException: If access denied
  """
  if MultiTenantUtils.is_shared_repository(graph_id):
    # Shared repository - validate repository access
    from robosystems.middleware.auth.utils import validate_repository_access

    if not validate_repository_access(current_user, graph_id, operation_type):
      raise HTTPException(
        status_code=403,
        detail=f"{graph_id.upper()} repository {operation_type} access denied",
      )
  else:
    # User graph - validate graph access
    from robosystems.models.iam import GraphUser

    if not GraphUser.user_has_access(current_user.id, graph_id, db):
      raise HTTPException(status_code=403, detail=f"Access denied to graph {graph_id}")


class MCPHandler:
  """Handle MCP protocol operations using Graph API with proper lifecycle management."""

  def __init__(self, repository, graph_id: str, user: Any):
    self.repository = repository
    self.graph_id = graph_id
    self.user = user
    self._closed = False

    # Always use Graph API adapter
    # Get the correct API URL from the repository
    repository_url = None
    if hasattr(repository, "config") and hasattr(repository.config, "base_url"):
      repository_url = repository.config.base_url
    elif hasattr(repository, "base_url"):
      repository_url = repository.base_url
    elif hasattr(repository, "api_base_url"):
      repository_url = repository.api_base_url

    # Initialize client asynchronously with lock to prevent race conditions
    self.kuzu_client = None
    self.mcp_tools: Optional[AdapterKuzuMCPTools] = None
    self.database = None
    self._init_lock = asyncio.Lock()
    self._init_task = asyncio.create_task(self._init_async(repository_url))

  async def _init_async(self, repository_url: Optional[str]):
    """Initialize the MCP client asynchronously."""
    try:
      self.kuzu_client = await create_kuzu_mcp_client(
        self.graph_id, api_base_url=repository_url
      )
      self.mcp_tools = AdapterKuzuMCPTools(self.kuzu_client)
      logger.info(
        f"Initialized MCP handler with Kuzu adapter for graph {self.graph_id} at {repository_url or 'discovered endpoint'}"
      )
    except Exception as e:
      logger.error(f"Failed to initialize MCP client for {self.graph_id}: {e}")
      raise

  async def _ensure_initialized(self):
    """Ensure the client is initialized before use with race condition protection."""
    async with self._init_lock:
      if self._init_task:
        try:
          await self._init_task
        finally:
          self._init_task = None

  @property
  def backend_type(self) -> str:
    """Get the backend type being used."""
    return "kuzu"

  async def get_tools(self) -> List[Dict[str, Any]]:
    """Get available MCP tools with backend-specific customizations."""
    self._ensure_not_closed()
    await self._ensure_initialized()
    assert self.mcp_tools is not None, "MCP tools not initialized"
    tools = self.mcp_tools.get_tool_definitions_as_dict()

    # Determine if this is a shared repository or user graph
    is_shared_repo = MultiTenantUtils.is_shared_repository(self.graph_id)
    backend_name = "Kuzu"

    # Add graph-specific context to descriptions
    for tool in tools:
      cypher_tool_names = ["read-graph-cypher"]
      schema_tool_names = ["get-graph-schema"]

      if tool["name"] in cypher_tool_names:
        if is_shared_repo:
          tool["description"] = (
            f"Execute a Cypher query on shared {self.graph_id.upper()} repository with public data (via {backend_name})"
          )
        else:
          tool["description"] = (
            f"Execute a Cypher query on private graph {self.graph_id} (via {backend_name})"
          )
      elif tool["name"] in schema_tool_names:
        if is_shared_repo:
          tool["description"] = (
            f"List all node types, attributes and relationships in shared {self.graph_id.upper()} repository (via {backend_name})"
          )
        else:
          tool["description"] = (
            f"List all node types, attributes and relationships in private graph {self.graph_id} (via {backend_name})"
          )

    # Add custom graph info tool
    graph_type = "shared repository" if is_shared_repo else "private graph"
    tools.append(
      {
        "name": "get-graph-info",
        "description": f"Get basic information about {graph_type} {self.graph_id} (via {backend_name})",
        "inputSchema": {"type": "object", "properties": {}},
      }
    )

    # Add describe-graph-structure tool if not already present
    if not any(t["name"] == "describe-graph-structure" for t in tools):
      tools.append(
        {
          "name": "describe-graph-structure",
          "description": f"Get a natural language description of the {graph_type} {self.graph_id} structure and contents",
          "inputSchema": {"type": "object", "properties": {}},
        }
      )

    return tools

  async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Execute an MCP tool call using Graph API backend with comprehensive timeout protection."""
    self._ensure_not_closed()
    await self._ensure_initialized()

    # Use coordinated timeout from the calling context
    tool_timeout = timeout_coordinator.get_tool_timeout(name)

    # Allow timeout override from arguments for read-graph-cypher
    if name == "read-graph-cypher" and "timeout" in arguments:
      user_timeout = min(arguments.get("timeout", tool_timeout), 300)  # Cap at 5 min
      tool_timeout = user_timeout

    try:
      if name == "get-graph-info":
        # Custom graph info tool - use standard timeout
        return await asyncio.wait_for(self._get_graph_info(), timeout=tool_timeout)
      elif name == "describe-graph-structure":
        # Custom description tool
        return await asyncio.wait_for(
          self._get_graph_description(), timeout=tool_timeout
        )
      else:
        # Use coordinated timeout for tools with proper instance timeout
        instance_timeout = timeout_coordinator.get_instance_timeout(name)
        results = await execute_mcp_query_with_timeout(
          self.mcp_tools,
          name,
          arguments,
          timeout=tool_timeout,
          tool_timeout=instance_timeout,  # Pass instance timeout to the tool
        )
        return {"type": "text", "text": json.dumps(results, indent=2)}

    except asyncio.TimeoutError:
      error_msg = f"Tool '{name}' timed out after {tool_timeout} seconds"
      if name == "read-graph-cypher":
        error_msg += ". Consider simplifying your query or adding LIMIT clauses."
      logger.error(error_msg)
      return {"type": "text", "text": f"Error: {error_msg}"}

    except (KuzuQueryTimeoutError, KuzuQueryComplexityError) as e:
      # Handle specific timeout/complexity errors with user-friendly messages
      logger.warning(f"Query constraint violation for {name}: {e}")
      return {"type": "text", "text": f"Query Error: {str(e)}"}

    except KuzuAPIError as e:
      # Handle Graph API errors with their enhanced messages
      # These already include helpful context from the MCP adapter
      logger.error(f"Graph API error in tool '{name}': {e}")
      return {"type": "text", "text": str(e)}

    except Exception as e:
      logger.error(f"Tool call failed for {name} on {self.backend_type}: {e}")
      return {"type": "text", "text": f"Error: {str(e)}"}

  async def execute_query_streaming(
    self, query: str, parameters: Dict[str, Any] = None, chunk_size: int = 1000
  ):
    """Execute a query with streaming support."""
    self._ensure_not_closed()

    # Check if repository supports streaming
    if hasattr(self.repository, "execute_query_streaming"):
      async for chunk in self.repository.execute_query_streaming(
        query, parameters or {}, chunk_size=chunk_size
      ):
        yield chunk
    else:
      # Fallback to non-streaming execution using the MCP tools directly
      # This avoids recursive call_tool and properly executes the query
      try:
        logger.debug(f"Using streaming fallback for query: {query[:100]}")

        # Ensure MCP tools are initialized before using them
        await self._ensure_initialized()

        if self.mcp_tools is None:
          raise RuntimeError("MCP tools not initialized")

        # Use the MCP tools to execute the query directly
        results = await self.mcp_tools.call_tool(
          "read-graph-cypher",
          {"query": query, "parameters": parameters or {}},
          return_raw=True,
        )

        logger.debug(
          f"Fallback query returned {len(results) if isinstance(results, list) else 'non-list'} results"
        )

        # Extract columns if possible (from first result row)
        columns = []
        if results and len(results) > 0 and isinstance(results[0], dict):
          columns = list(results[0].keys())

        # Yield as a single chunk
        chunk_data = results if isinstance(results, list) else [results]
        logger.debug(
          f"Yielding chunk with {len(chunk_data)} rows and columns: {columns}"
        )

        yield {
          "data": chunk_data,
          "columns": columns,
        }
      except Exception as e:
        logger.error(f"Error in streaming fallback: {e}", exc_info=True)
        yield {"data": [], "columns": [], "error": str(e)}

  async def _get_graph_info(self) -> Dict[str, Any]:
    """Get basic graph statistics using Kuzu."""
    try:
      if self.kuzu_client:
        # Use Kuzu adapter for graph info
        info = await self.kuzu_client.get_graph_info()
      else:
        # Ensure MCP tools are initialized
        await self._ensure_initialized()
        assert self.mcp_tools is not None, "MCP tools not initialized"
        # Use direct MCP for graph info
        schema_result = await self.mcp_tools.call_tool(
          "get-graph-schema", {}, return_raw=True
        )

        # Extract basic info from schema
        node_count = len(
          [t for t in schema_result if t.get("category") == "Node Tables"]
        )
        rel_count = len(
          [t for t in schema_result if t.get("category") == "Relationship Tables"]
        )

        info = {
          "graph_id": self.graph_id,
          "node_table_count": node_count,
          "relationship_table_count": rel_count,
          "backend": "kuzu",
          "mode": "adapter" if self.kuzu_client else "direct",
        }

      return {"type": "text", "text": json.dumps(info, indent=2)}
    except Exception as e:
      logger.error(f"Error getting graph info: {e}")
      return {"type": "text", "text": f"Error getting graph info: {str(e)}"}

  async def _get_graph_description(self) -> Dict[str, Any]:
    """Get natural language description of graph structure."""
    try:
      # Use describe-graph-structure if available
      if self.kuzu_client and hasattr(self.kuzu_client, "describe_graph_structure"):
        description = await self.kuzu_client.describe_graph_structure()
      else:
        # Ensure MCP tools are initialized
        await self._ensure_initialized()
        assert self.mcp_tools is not None, "MCP tools not initialized"
        # Generate description from schema
        schema_result = await self.mcp_tools.call_tool(
          "get-graph-schema", {}, return_raw=True
        )

        node_tables = [t for t in schema_result if t.get("type") == "node"]
        rel_tables = [t for t in schema_result if t.get("type") == "relationship"]

        description = f"Graph Database: {self.graph_id}\n\n"
        description += f"This graph contains {len(node_tables)} node types and {len(rel_tables)} relationship types.\n\n"

        if node_tables:
          description += "Node Types:\n"
          for table in node_tables[:5]:  # Limit to first 5
            description += f"- {table.get('label', 'Unknown')}"
            if table.get("count", 0) > 0:
              description += f" ({table['count']} records)"
            description += "\n"
          if len(node_tables) > 5:
            description += f"  ... and {len(node_tables) - 5} more\n"

        if rel_tables:
          description += "\nRelationship Types:\n"
          for table in rel_tables[:5]:  # Limit to first 5
            description += f"- {table.get('label', 'Unknown')}\n"
          if len(rel_tables) > 5:
            description += f"  ... and {len(rel_tables) - 5} more\n"

      return {"type": "text", "text": description}
    except Exception as e:
      logger.error(f"Error getting graph description: {e}")
      return {"type": "text", "text": f"Error getting graph description: {str(e)}"}

  async def close(self):
    """Close the MCP tools and database connections."""
    if self._closed:
      return

    errors = []
    try:
      if self.kuzu_client:
        try:
          await self.kuzu_client.close()
          logger.debug(f"Closed Graph client for graph {self.graph_id}")
        except Exception as e:
          error_msg = f"Failed to close Graph client for graph {self.graph_id}: {e}"
          logger.error(error_msg, exc_info=True)
          errors.append(error_msg)

      if self.database:
        try:
          await self.database.close()
          logger.debug(f"Closed direct database connection for graph {self.graph_id}")
        except Exception as e:
          error_msg = f"Failed to close database for graph {self.graph_id}: {e}"
          logger.error(error_msg, exc_info=True)
          errors.append(error_msg)
    finally:
      self._closed = True

    # Re-raise if there were critical errors
    if errors:
      raise RuntimeError(f"Errors during MCP handler cleanup: {'; '.join(errors)}")

  async def __aenter__(self):
    """Async context manager entry."""
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Async context manager exit with guaranteed cleanup."""
    await self.close()

  def _ensure_not_closed(self):
    """Ensure handler is not closed before operations."""
    if self._closed:
      raise RuntimeError(f"MCPHandler for graph {self.graph_id} is closed")


async def execute_mcp_query_with_timeout(
  mcp_tools: Any,
  tool_name: str,
  arguments: Dict[str, Any],
  timeout: float = 60.0,
  tool_timeout: Optional[float] = None,
) -> Any:
  """
  Execute MCP tool with comprehensive timeout protection.

  Args:
      mcp_tools: MCP tools instance
      tool_name: Name of the tool to execute
      arguments: Tool arguments
      timeout: Overall timeout in seconds
      tool_timeout: Tool-specific timeout to pass to the tool

  Returns:
      Tool execution result

  Raises:
      asyncio.TimeoutError: If execution exceeds timeout
  """
  # Add timeout to arguments if tool supports it
  if tool_timeout and tool_name == "read-graph-cypher":
    arguments = {**arguments, "timeout": int(tool_timeout)}

  # Execute with timeout
  try:
    result = await asyncio.wait_for(
      mcp_tools.call_tool(tool_name, arguments, return_raw=True), timeout=timeout
    )
    return result
  except asyncio.TimeoutError:
    logger.error(f"MCP tool {tool_name} timed out after {timeout} seconds")
    raise
  except Exception as e:
    logger.error(f"MCP tool {tool_name} failed: {e}")
    raise
