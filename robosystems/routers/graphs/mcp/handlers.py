"""MCP handler implementation and helper functions.

Holds the `MCPHandler` class, which owns the Graph API MCP client lifecycle,
plus the access check and result helpers the MCP routes share.
"""

import asyncio
import json
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.middleware.mcp import (
  GraphAPIError,
  GraphQueryComplexityError,
  GraphQueryTimeoutError,
  create_graph_mcp_client,
)
from robosystems.middleware.mcp import (
  GraphMCPTools as AdapterGraphMCPTools,
)
from robosystems.middleware.robustness.timeout_coordinator import TimeoutCoordinator

# Tool execution always runs through the Graph API adapter.
MCP_AVAILABLE = False

timeout_coordinator = TimeoutCoordinator()


def tool_error_result(text: str, kind: str) -> dict[str, Any]:
  """A tool-execution failure in the handler's text-result shape, marked so
  transports can surface it as an MCP ``isError`` result and the circuit
  breaker can count it — instead of both reading it as a success.

  ``kind`` is one of:
    - ``timeout``: the tool or backend query ran out of time (breaker-relevant)
    - ``backend``: the graph API failed or an unexpected error occurred
      (breaker-relevant)
    - ``constraint``: the caller's query violated a policy/complexity limit —
      a caller error, not a backend-health signal
  """
  return {"type": "text", "text": text, "is_error": True, "error_kind": kind}


def is_tool_error_result(result: Any) -> bool:
  """True when a handler result is a marked tool-execution failure."""
  return isinstance(result, dict) and result.get("is_error") is True


def tool_error_kind(result: Any) -> str | None:
  """The failure kind of a marked tool-execution failure, else None."""
  if not is_tool_error_result(result):
    return None
  kind = result.get("error_kind")
  return kind if isinstance(kind, str) else "backend"


async def validate_mcp_access(
  graph_id: str, current_user: Any, db: Session, operation_type: str = "read"
) -> None:
  """Validate user access for MCP operations, raising 403 on denial.

  Shared repositories resolve through repository access (subgraphs resolve to
  their parent); other graphs check per-graph membership and role, then the
  graph's lifecycle/subscription state (``require_graph_access``) — the same
  pair the REST command surfaces enforce through ``require_graph_write_role``
  — so a suspended, mid-teardown or grace-period graph is no more writable
  through an MCP tool than through an operation endpoint.
  """
  # Check shared repositories (including subgraphs like "sec_historical")
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    # Shared repository - validate repository access (resolves subgraph to parent)
    from robosystems.middleware.auth.utils import validate_repository_access

    if not validate_repository_access(current_user, graph_id, operation_type):
      raise HTTPException(
        status_code=403,
        detail=f"{graph_id.upper()} repository {operation_type} access denied",
      )
  else:
    # User graph - validate graph access
    from robosystems.models.core import GraphUser

    if operation_type in ("write", "admin"):
      # Write/admin tools require the 'member' or 'admin' role; 'viewer' is
      # read-only. Bare membership is not sufficient for mutations.
      if not GraphUser.user_has_write_access(current_user.id, graph_id, db):
        # Same detective-control audit the REST write gate emits
        # (`require_graph_write_role`), so an under-privileged MCP write
        # attempt is visible in the security stream, not only in a 403.
        from robosystems.security import SecurityAuditLogger

        SecurityAuditLogger.log_authorization_denied(
          user_id=str(current_user.id),
          resource=graph_id,
          action="write",
          endpoint="mcp",
        )
        raise HTTPException(
          status_code=403,
          detail=f"Write access denied to graph {graph_id}; your role is read-only.",
        )
    elif not GraphUser.user_has_access(current_user.id, graph_id, db):
      raise HTTPException(status_code=403, detail=f"Access denied to graph {graph_id}")

    from robosystems.middleware.billing.enforcement import require_graph_access

    require_graph_access(
      graph_id, db, require_write=operation_type in ("write", "admin")
    )


class MCPHandler:
  """Handle MCP protocol operations using Graph API with proper lifecycle management."""

  def __init__(self, repository, graph_id: str, user: Any):
    self.repository = repository
    self.graph_id = graph_id
    self.user = user
    self._closed = False

    # Resolve the Graph API URL from the repository.
    repository_url = None
    if hasattr(repository, "config") and hasattr(repository.config, "base_url"):
      repository_url = repository.config.base_url
    elif hasattr(repository, "base_url"):
      repository_url = repository.base_url
    elif hasattr(repository, "api_base_url"):
      repository_url = repository.api_base_url

    # Initialize client asynchronously with lock to prevent race conditions
    self.graph_client = None
    self.mcp_tools: AdapterGraphMCPTools | None = None
    self.database = None
    self._init_lock = asyncio.Lock()
    self._init_task = asyncio.create_task(self._init_async(repository_url))

  async def _init_async(self, repository_url: str | None):
    """Initialize the MCP client asynchronously."""
    try:
      self.graph_client = await create_graph_mcp_client(
        self.graph_id, api_base_url=repository_url
      )
      # Attach the authenticated user to the client so tools can resolve it.
      # Set both the User object (workspace + GraphQL query tools) and the id
      # string: the GraphQL tool builds its auth context from the user, and
      # the registrar's `created_by` resolution reads `user_id` — without it,
      # GraphQL resolvers reject as UNAUTHENTICATED and writes are audited as
      # `mcp:{graph_id}` instead of the real user.
      self.graph_client.user = self.user
      if self.user is not None:
        self.graph_client.user_id = str(self.user.id)

      from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

      schema_extensions = resolve_schema_extensions(self.graph_id)

      # Shared repositories are read-only (no workspace mutations or write tools)
      from robosystems.config.shared_repositories import (
        is_shared_repository_or_subgraph,
      )

      read_only = is_shared_repository_or_subgraph(self.graph_id)

      self.mcp_tools = AdapterGraphMCPTools(
        self.graph_client, schema_extensions=schema_extensions, read_only=read_only
      )
      logger.info(
        f"Initialized MCP handler for graph {self.graph_id} "
        f"(extensions={schema_extensions}, read_only={read_only}, endpoint={repository_url or 'discovered'})"
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
    return "ladybug"

  async def get_tools(self) -> list[dict[str, Any]]:
    """Get available MCP tools with backend-specific customizations."""
    self._ensure_not_closed()
    await self._ensure_initialized()
    assert self.mcp_tools is not None, "MCP tools not initialized"
    tools = self.mcp_tools.get_tool_definitions_as_dict()

    from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

    is_shared_repo = is_shared_repository_or_subgraph(self.graph_id)
    backend_name = "Graph Database"

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

    graph_type = "shared repository" if is_shared_repo else "private graph"
    tools.append(
      {
        "name": "get-graph-info",
        "description": f"Get basic information about {graph_type} {self.graph_id} (via {backend_name})",
        "inputSchema": {"type": "object", "properties": {}},
      }
    )

    return tools

  def get_instructions(self, tools: list[dict[str, Any]]) -> str | None:
    """Build per-graph routing guidance for the MCP client handshake.

    Derived from the SAME signals that gate the tool list — the resolved
    tool-name set, shared-repo status, and read-only — so the instructions can
    never reference a tool this graph doesn't expose. Shared repositories use
    their manifest's authored ``agent_instructions`` verbatim; entity and
    generic graphs are generated from the live tool surface.
    """
    from robosystems.config.shared_repositories import (
      get_manifest,
      is_shared_repository_or_subgraph,
      resolve_shared_repository_parent,
    )
    from robosystems.middleware.mcp.tools.instructions import build_instructions

    is_shared_repo = is_shared_repository_or_subgraph(self.graph_id)

    authored_override: str | None = None
    if is_shared_repo:
      try:
        manifest = get_manifest(resolve_shared_repository_parent(self.graph_id))
      except ValueError:
        manifest = None
      authored_override = getattr(manifest, "agent_instructions", None)

    read_only = bool(getattr(self.mcp_tools, "read_only", is_shared_repo))

    return build_instructions(
      graph_id=self.graph_id,
      tool_names={str(tool["name"]) for tool in tools},
      is_shared_repo=is_shared_repo,
      read_only=read_only,
      authored_override=authored_override,
    )

  async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """Execute an MCP tool call against the Graph API backend, under a
    coordinated timeout."""
    self._ensure_not_closed()
    await self._ensure_initialized()

    tool_timeout = timeout_coordinator.get_tool_timeout(name)

    if name == "read-graph-cypher" and "timeout" in arguments:
      user_timeout = min(arguments.get("timeout", tool_timeout), 300)  # Cap at 5 min
      tool_timeout = user_timeout

    try:
      if name == "get-graph-info":
        # Custom graph info tool - use standard timeout
        return await asyncio.wait_for(self._get_graph_info(), timeout=tool_timeout)
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

    except TimeoutError:
      error_msg = f"Tool '{name}' timed out after {tool_timeout} seconds"
      if name == "read-graph-cypher":
        error_msg += ". Consider simplifying your query or adding LIMIT clauses."
      logger.error(error_msg)
      return tool_error_result(f"Error: {error_msg}", "timeout")

    except GraphQueryTimeoutError as e:
      logger.warning(f"Query timeout for {name}: {e}")
      return tool_error_result(f"Query Error: {e!s}", "timeout")

    except GraphQueryComplexityError as e:
      # Caller's query exceeded policy limits — an error result, but not a
      # backend-health signal.
      logger.warning(f"Query constraint violation for {name}: {e}")
      return tool_error_result(f"Query Error: {e!s}", "constraint")

    except GraphAPIError as e:
      # Graph API errors already carry adapter-supplied context.
      logger.error(f"Graph API error in tool '{name}': {e}")
      return tool_error_result(str(e), "backend")

    except Exception as e:
      logger.error(f"Tool call failed for {name} on {self.backend_type}: {e}")
      return tool_error_result(
        "Error: operation failed. Please try again or contact support.",
        "backend",
      )

  async def execute_query_streaming(
    self, query: str, parameters: dict[str, Any] | None = None, chunk_size: int = 1000
  ):
    """Execute a query with streaming support."""
    self._ensure_not_closed()

    if hasattr(self.repository, "execute_query_streaming"):
      async for chunk in self.repository.execute_query_streaming(
        query, parameters or {}, chunk_size=chunk_size
      ):
        yield chunk
    else:
      # Non-streaming fallback: call the MCP tools directly rather than
      # recursing through call_tool.
      try:
        logger.debug(f"Using streaming fallback for query: {query[:100]}")

        await self._ensure_initialized()

        if self.mcp_tools is None:
          raise RuntimeError("MCP tools not initialized")

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
        yield {
          "data": [],
          "columns": [],
          "error": "Query execution failed. Please try again.",
        }

  async def _get_graph_info(self) -> dict[str, Any]:
    """Get basic graph statistics."""
    try:
      if self.graph_client:
        info = await self.graph_client.get_graph_info()
      else:
        await self._ensure_initialized()
        assert self.mcp_tools is not None, "MCP tools not initialized"
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
          "backend": "ladybug",
          "mode": "adapter" if self.graph_client else "direct",
        }

      return {"type": "text", "text": json.dumps(info, indent=2)}
    except Exception as e:
      logger.error(f"Error getting graph info: {e}")
      return tool_error_result(
        "Error getting graph info: service temporarily unavailable.",
        "backend",
      )

  async def close(self):
    """Close the MCP tools and database connections."""
    if self._closed:
      return

    errors = []
    try:
      if self.graph_client:
        try:
          await self.graph_client.close()
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
  arguments: dict[str, Any],
  timeout: float = 60.0,
  tool_timeout: float | None = None,
) -> Any:
  """Execute an MCP tool under an overall `timeout`, in seconds.

  `tool_timeout` is passed through to tools that accept their own budget.
  Raises `asyncio.TimeoutError` when the overall timeout elapses.
  """
  if tool_timeout and tool_name == "read-graph-cypher":
    arguments = {**arguments, "timeout": int(tool_timeout)}

  try:
    result = await asyncio.wait_for(
      mcp_tools.call_tool(tool_name, arguments, return_raw=True), timeout=timeout
    )
    return result
  except TimeoutError:
    logger.error(f"MCP tool {tool_name} timed out after {timeout} seconds")
    raise
  except Exception as e:
    logger.error(f"MCP tool {tool_name} failed: {e}")
    raise
