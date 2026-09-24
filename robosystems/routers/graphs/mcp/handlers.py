"""`MCPHandler` (Graph API MCP client lifecycle) plus the access check and
result helpers the MCP routes share."""

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

timeout_coordinator = TimeoutCoordinator()


def tool_error_result(text: str, kind: str) -> dict[str, Any]:
  """A tool failure in the handler's text-result shape, marked so transports
  surface it as MCP ``isError`` and the circuit breaker counts it.

  ``kind``: ``timeout`` and ``backend`` are breaker-relevant; ``constraint``
  is a caller policy/complexity violation, not a backend-health signal.
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
  """Raise 403 unless the user may perform ``operation_type`` on the graph.

  Shared repositories check repository access (subgraphs resolve to their
  parent). Other graphs check membership and role, then lifecycle and
  subscription state via ``require_graph_access``, the same pair the REST
  command surfaces enforce.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    from robosystems.middleware.auth.utils import validate_repository_access

    if not validate_repository_access(current_user, graph_id, operation_type):
      raise HTTPException(
        status_code=403,
        detail=f"{graph_id.upper()} repository {operation_type} access denied",
      )
  else:
    from robosystems.models.core import GraphUser

    if operation_type in ("write", "admin"):
      # 'viewer' is read-only.
      if not GraphUser.user_has_write_access(current_user.id, graph_id, db):
        # Audited like the REST write gate so the attempt shows in the
        # security stream, not only as a 403.
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
      # Non-member read is the enumeration signal; audited like the write gate.
      from robosystems.security import SecurityAuditLogger

      SecurityAuditLogger.log_authorization_denied(
        user_id=str(current_user.id),
        resource=graph_id,
        action="read",
        endpoint="mcp",
      )
      raise HTTPException(status_code=403, detail=f"Access denied to graph {graph_id}")

    from robosystems.middleware.billing.enforcement import require_graph_access

    require_graph_access(
      graph_id, db, require_write=operation_type in ("write", "admin")
    )


# Core tools whose authored description gets the graph scope prepended.
SCOPED_TOOL_NAMES = ("read-graph-cypher", "get-graph-schema")

_MIN_USER_TIMEOUT_S = 10
_MAX_USER_TIMEOUT_S = 300


def _graph_scope_line(graph_id: str, is_shared_repo: bool) -> str:
  if is_shared_repo:
    return f"**GRAPH:** shared repository `{graph_id}` — public, read-only data."
  return f"**GRAPH:** private graph `{graph_id}`."


def _graph_info_tool_definition(graph_id: str, is_shared_repo: bool) -> dict[str, Any]:
  kind = "shared repository" if is_shared_repo else "private graph"
  return {
    "name": "get-graph-info",
    "description": (
      f"Basic facts about {kind} `{graph_id}`: graph id, approximate node "
      "count, node labels, relationship types, and whether it is read-only.\n\n"
      "**WHEN TO USE:**\n"
      "- To confirm which graph you are connected to and what it holds before "
      "`get-graph-schema`\n"
      "- To size a query from the node count and label list without reading the "
      "full schema\n\n"
      "**RETURNS:** A small JSON object. Takes no arguments."
    ),
    "inputSchema": {"type": "object", "properties": {}},
  }


class MCPHandler:
  """Handle MCP protocol operations using Graph API with proper lifecycle management."""

  def __init__(self, repository, graph_id: str, user: Any):
    self.repository = repository
    self.graph_id = graph_id
    self.user = user
    self._closed = False

    repository_url = None
    if hasattr(repository, "config") and hasattr(repository.config, "base_url"):
      repository_url = repository.config.base_url
    elif hasattr(repository, "base_url"):
      repository_url = repository.base_url
    elif hasattr(repository, "api_base_url"):
      repository_url = repository.api_base_url

    self.graph_client = None
    self.mcp_tools: AdapterGraphMCPTools | None = None
    self._init_lock = asyncio.Lock()
    self._init_task = asyncio.create_task(self._init_async(repository_url))

  async def _init_async(self, repository_url: str | None):
    """Initialize the MCP client asynchronously."""
    try:
      self.graph_client = await create_graph_mcp_client(
        self.graph_id, api_base_url=repository_url
      )
      # Both are needed: GraphQL tools build their auth context from the user,
      # and the registrar's `created_by` reads `user_id` (else writes audit as
      # `mcp:{graph_id}`).
      self.graph_client.user = self.user
      if self.user is not None:
        self.graph_client.user_id = str(self.user.id)

      from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

      schema_extensions = resolve_schema_extensions(self.graph_id)

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
    """Get available MCP tools, scoped to this graph.

    Authored descriptions are kept whole, with the graph scope prepended and a
    shared repository's query guidance appended. Replacing them loses the
    prompt clients write queries from (on SEC it produced wrong revenue series).
    """
    self._ensure_not_closed()
    await self._ensure_initialized()
    assert self.mcp_tools is not None, "MCP tools not initialized"
    tools = [dict(tool) for tool in self.mcp_tools.get_tool_definitions_as_dict()]

    from robosystems.config.shared_repositories import (
      get_manifest,
      is_shared_repository_or_subgraph,
      resolve_shared_repository_parent,
    )

    is_shared_repo = is_shared_repository_or_subgraph(self.graph_id)
    scope = _graph_scope_line(self.graph_id, is_shared_repo)

    query_guidance: str | None = None
    if is_shared_repo:
      try:
        manifest = get_manifest(resolve_shared_repository_parent(self.graph_id))
      except ValueError:
        manifest = None
      query_guidance = getattr(manifest, "cypher_query_guidance", None)

    for tool in tools:
      if tool["name"] in SCOPED_TOOL_NAMES:
        tool["description"] = f"{scope}\n\n{tool['description']}"
      if tool["name"] == "read-graph-cypher" and query_guidance:
        tool["description"] = f"{tool['description']}\n\n{query_guidance}"

    tools.append(_graph_info_tool_definition(self.graph_id, is_shared_repo))
    return tools

  def get_instructions(self, tools: list[dict[str, Any]]) -> str | None:
    """Per-graph routing guidance for the MCP handshake.

    Derived from the same signals that gate the tool list, so it never names a
    tool this graph doesn't expose. Shared repositories use their manifest's
    ``agent_instructions`` verbatim.
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

    if name == "read-graph-cypher":
      requested = arguments.get("timeout")
      # A caller's timeout must not fail the call on its own: that failure
      # counts against the shared per-graph breaker.
      if isinstance(requested, int | float) and not isinstance(requested, bool):
        tool_timeout = min(max(requested, _MIN_USER_TIMEOUT_S), _MAX_USER_TIMEOUT_S)

    try:
      if name == "get-graph-info":
        return await asyncio.wait_for(self._get_graph_info(), timeout=tool_timeout)
      else:
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
      logger.warning(f"Query constraint violation for {name}: {e}")
      return tool_error_result(f"Query Error: {e!s}", "constraint")

    except GraphAPIError as e:
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
    """Execute a read query with streaming support, under the same guards as
    the direct ``read-graph-cypher`` path."""
    self._ensure_not_closed()
    await self._ensure_initialized()

    from robosystems.middleware.mcp.tools.cypher_tool import assert_read_only_cypher

    assert_read_only_cypher(query, self.graph_id)
    if self.graph_client is not None:
      query = self.graph_client.prepare_read_query(query)

    if hasattr(self.repository, "execute_query_streaming"):
      async for chunk in self.repository.execute_query_streaming(
        query, parameters or {}, chunk_size=chunk_size
      ):
        yield chunk
    else:
      # Non-streaming fallback: call the tools directly, not via call_tool.
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
    """Close the MCP client."""
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
    finally:
      self._closed = True

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
