"""Remote MCP transport — Streamable HTTP (JSON-RPC 2.0) over the existing tool layer.

``POST /v1/graphs/{graph_id}/mcp`` is the wire-protocol front door for MCP
clients that connect by URL (Claude custom connectors, Cursor, ``mcp-remote``).
The npx stdio bridge translates MCP to the REST tool endpoints; this endpoint
speaks the protocol directly, so a graph is connectable by pasting its URL.

The dispatch is hand-rolled rather than mounted from the MCP SDK server: the
tool surface is dynamic per graph (shared-repo gating, ``read_only``,
extension flags), and every call must run behind the same FastAPI dependency
chain as the REST tool endpoints — auth, per-graph access, rate limits, the
shared write-classification gauntlet, and the circuit breaker.

Transport rules:

- ``graph_id`` lives in the URL path and never becomes a tool argument. The
  per-graph ``serverInfo.name`` and ``instructions`` reinforce the anchor,
  and instructions are rebuilt per ``initialize`` rather than frozen at
  client-process start.
- Stateless: no ``Mcp-Session-Id``, no GET-side SSE channel, no resumability.
  A subgraph is addressed as another connector URL, never via in-session
  switching.
- Header auth (``X-API-Key`` or JWT); OAuth is a separate, later surface.
- Excluded from the OpenAPI schema so the JSON-RPC envelope never lands in
  the generated SDK clients.
"""

import json
from importlib.metadata import version as pkg_version
from typing import Any

from fastapi import APIRouter, Depends, Path, Request, Response
from fastapi import status as http_status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse

from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.api.graphs.mcp import MCPToolCall
from robosystems.models.core import User

from .execute import (
  READ_ONLY_MCP_TOOLS,
  authorize_mcp_tool_call,
  circuit_breaker,
  execute_tool_to_json,
)
from .handlers import MCPHandler, validate_mcp_access
from .strategies import MCPStrategySelector

router = APIRouter()

# Protocol revisions this transport can negotiate. The server answers with the
# client's requested revision when supported, else its own latest.
MCP_SUPPORTED_PROTOCOL_VERSIONS = frozenset({"2024-11-05", "2025-03-26", "2025-06-18"})
MCP_LATEST_PROTOCOL_VERSION = "2025-06-18"

# JSON-RPC 2.0 error codes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# The capability profile asserted for every remote caller. Real MCP clients
# (Claude, Cursor) send neither the `robosystems-mcp` User-Agent nor the
# X-MCP-Client header the npx bridge sends, so header sniffing would classify
# them as browsers and degrade strategy selection. Anything speaking JSON-RPC
# on this endpoint IS an MCP client by construction.
_REMOTE_CLIENT_INFO: dict[str, Any] = {
  "is_mcp_client": True,
  "supports_sse": True,
  "supports_ndjson": False,
  "prefers_streaming": False,
  "client_version": "remote",
  "is_testing_tool": False,
  "is_browser": False,
  "is_interactive": False,
}


def _rpc_result(msg_id: Any, result: dict[str, Any]) -> JSONResponse:
  return JSONResponse(content={"jsonrpc": "2.0", "id": msg_id, "result": result})


def _rpc_error(
  msg_id: Any, code: int, message: str, http_code: int = http_status.HTTP_200_OK
) -> JSONResponse:
  return JSONResponse(
    status_code=http_code,
    content={
      "jsonrpc": "2.0",
      "id": msg_id,
      "error": {"code": code, "message": message},
    },
  )


def _tool_error_result(msg_id: Any, text: str) -> JSONResponse:
  """Report a tool-execution failure inside a successful JSON-RPC response.

  Per the MCP spec, failures of the tool itself (as opposed to protocol
  errors) are returned as ``result.isError`` so the model can see the message
  and react — e.g. relay a subscription or rate-limit notice to the user.
  """
  return _rpc_result(
    msg_id,
    {"content": [{"type": "text", "text": text}], "isError": True},
  )


def _to_tool_result(result: Any) -> dict[str, Any]:
  """Map an internal tool result onto the MCP ``tools/call`` result shape."""
  if isinstance(result, dict) and result.get("type") == "text" and "text" in result:
    content = [{"type": "text", "text": result["text"]}]
  else:
    content = [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]
  return {"content": content, "isError": False}


async def _validate_read_access(graph_id: str, current_user: User) -> None:
  """Read-access check on a short-lived session (initialize / tools/list)."""
  from robosystems.database import SessionFactory

  sess = SessionFactory()
  try:
    await validate_mcp_access(graph_id, current_user, sess, "read")
  finally:
    sess.close()


def _get_mcp_operation_type(graph_id: str) -> str:
  # Shared repos and their subgraphs route to the reader cluster; user graphs
  # always use the writer for consistency (matches tools.py / factory.py).
  if MultiTenantUtils.is_shared_repository_or_subgraph(graph_id):
    return "read"
  else:
    return "write"


async def _handle_initialize(
  graph_id: str, current_user: User, params: dict[str, Any]
) -> dict[str, Any]:
  requested = params.get("protocolVersion")
  protocol_version = (
    requested
    if isinstance(requested, str) and requested in MCP_SUPPORTED_PROTOCOL_VERSIONS
    else MCP_LATEST_PROTOCOL_VERSION
  )

  await _validate_read_access(graph_id, current_user)

  # Instructions are rebuilt per initialize from the live tool surface — the
  # remote transport's correctness upgrade over the npx bridge, where the
  # instructions field is frozen at client-process start.
  instructions: str | None = None
  repository = await get_graph_repository(graph_id, _get_mcp_operation_type(graph_id))
  handler = MCPHandler(repository, graph_id, current_user)
  try:
    tools = await handler.get_tools()
    try:
      instructions = handler.get_instructions(tools)
    except Exception as instructions_error:
      logger.warning(
        f"Failed to build MCP instructions for graph {graph_id}: {instructions_error}"
      )
  finally:
    await handler.close()

  result: dict[str, Any] = {
    "protocolVersion": protocol_version,
    "capabilities": {"tools": {"listChanged": False}},
    "serverInfo": {
      "name": f"robosystems-{graph_id}",
      "title": f"RoboSystems — {graph_id}",
      "version": pkg_version("robosystems"),
    },
  }
  if instructions:
    result["instructions"] = instructions
  return result


async def _handle_tools_list(graph_id: str, current_user: User) -> dict[str, Any]:
  await _validate_read_access(graph_id, current_user)

  repository = await get_graph_repository(graph_id, _get_mcp_operation_type(graph_id))
  handler = MCPHandler(repository, graph_id, current_user)
  try:
    tools = await handler.get_tools()
  finally:
    await handler.close()

  mcp_tools: list[dict[str, Any]] = []
  for tool in tools:
    entry: dict[str, Any] = {
      "name": tool["name"],
      "description": tool.get("description", ""),
      "inputSchema": tool.get("inputSchema", {"type": "object", "properties": {}}),
    }
    # Cypher read tools are deliberately unhinted: they are classified per
    # statement by the StatementKernel and can carry writes on tenant graphs.
    if tool["name"] in READ_ONLY_MCP_TOOLS:
      entry["annotations"] = {"readOnlyHint": True}
    mcp_tools.append(entry)

  return {"tools": mcp_tools}


async def _handle_tools_call(
  graph_id: str, current_user: User, msg_id: Any, params: dict[str, Any]
) -> JSONResponse:
  name = params.get("name")
  if not isinstance(name, str) or not name:
    return _rpc_error(msg_id, INVALID_PARAMS, "Invalid params: 'name' is required")
  arguments = params.get("arguments") or {}
  if not isinstance(arguments, dict):
    return _rpc_error(
      msg_id, INVALID_PARAMS, "Invalid params: 'arguments' must be an object"
    )

  tool_call = MCPToolCall(name=name, arguments=arguments)

  try:
    circuit_breaker.check_circuit(graph_id, name)
    access_type = await authorize_mcp_tool_call(graph_id, tool_call, current_user)
  except HTTPException as e:
    detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
    return _tool_error_result(msg_id, detail)

  api_logger.info(
    f"Remote MCP tool execution started: {name}",
    extra={
      "component": "mcp_remote",
      "action": "tool_started",
      "user_id": str(current_user.id),
      "database": graph_id,
      "tool_name": name,
      "access_type": access_type,
    },
  )

  repository = await get_graph_repository(graph_id, _get_mcp_operation_type(graph_id))
  handler = MCPHandler(repository, graph_id, current_user)
  try:
    from robosystems.middleware.graph.query_queue import get_query_queue

    tool_stats = get_query_queue().get_stats()
    strategy = MCPStrategySelector.select_strategy(
      tool_name=name,
      arguments=arguments,
      client_info=_REMOTE_CLIENT_INFO,
      system_state={
        "queue_size": tool_stats["queue_size"],
        "running_queries": tool_stats["running_queries"],
        "cache_available": True,
      },
      graph_id=graph_id,
      user_tier=None,
    )
    timeout = MCPStrategySelector.get_timeout_for_strategy(strategy)

    result = await execute_tool_to_json(handler, graph_id, tool_call, strategy, timeout)
  except HTTPException as e:
    detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
    return _tool_error_result(msg_id, detail)
  except Exception as e:
    circuit_breaker.record_failure(graph_id, name)
    logger.error(
      f"Remote MCP tool execution failed: {e}",
      extra={"graph_id": graph_id, "user_id": str(current_user.id), "tool_name": name},
    )
    return _tool_error_result(msg_id, "Tool execution failed.")
  finally:
    if not handler._closed:
      await handler.close()

  circuit_breaker.record_success(graph_id, name)
  return _rpc_result(msg_id, _to_tool_result(result))


@router.post("", include_in_schema=False, response_model=None)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/mcp", business_event_type="mcp_remote_request"
)
async def mcp_remote_transport(
  request: Request,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Response:
  """Streamable-HTTP MCP endpoint (JSON-RPC 2.0)."""
  try:
    message = await request.json()
  except Exception:
    return _rpc_error(
      None,
      PARSE_ERROR,
      "Parse error: request body is not valid JSON",
      http_code=http_status.HTTP_400_BAD_REQUEST,
    )

  if isinstance(message, list):
    # JSON-RPC batching was removed in the 2025-06-18 MCP revision; this
    # transport never accepted it.
    return _rpc_error(
      None,
      INVALID_REQUEST,
      "Invalid request: JSON-RPC batching is not supported",
      http_code=http_status.HTTP_400_BAD_REQUEST,
    )

  if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
    return _rpc_error(
      None,
      INVALID_REQUEST,
      "Invalid request: expected a JSON-RPC 2.0 message",
      http_code=http_status.HTTP_400_BAD_REQUEST,
    )

  method = message.get("method")
  msg_id = message.get("id")
  params = message.get("params") or {}
  if not isinstance(params, dict):
    return _rpc_error(msg_id, INVALID_PARAMS, "Invalid params: expected an object")

  # A message without a method is a client->server response; a message without
  # an id is a notification. Streamable HTTP: accept both with 202, no body.
  # (notifications/initialized and notifications/cancelled land here —
  # cancellation is best-effort only on a stateless multi-node transport.)
  if not isinstance(method, str) or "id" not in message:
    return Response(status_code=http_status.HTTP_202_ACCEPTED)

  try:
    if method == "initialize":
      return _rpc_result(
        msg_id, await _handle_initialize(graph_id, current_user, params)
      )
    elif method == "ping":
      return _rpc_result(msg_id, {})
    elif method == "tools/list":
      return _rpc_result(msg_id, await _handle_tools_list(graph_id, current_user))
    elif method == "tools/call":
      return await _handle_tools_call(graph_id, current_user, msg_id, params)
    else:
      return _rpc_error(msg_id, METHOD_NOT_FOUND, f"Method not found: {method}")
  except HTTPException as e:
    # Access denials from initialize / tools/list surface as JSON-RPC errors
    # rather than tool results (there is no tool result to attach them to).
    detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
    return _rpc_error(msg_id, INVALID_REQUEST, detail, http_code=e.status_code)
  except Exception as e:
    logger.error(
      f"Remote MCP transport error on {method}: {e}",
      extra={"graph_id": graph_id, "user_id": str(current_user.id)},
    )
    return _rpc_error(msg_id, INTERNAL_ERROR, "Internal error")
