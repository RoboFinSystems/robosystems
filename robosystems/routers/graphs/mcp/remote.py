"""Remote MCP transport — Streamable HTTP (JSON-RPC 2.0) over the existing tool layer.

``POST /v1/graphs/{graph_id}/mcp`` lets MCP clients connect to a graph by URL.
Dispatch is hand-rolled rather than mounted from the MCP SDK because the tool
surface is dynamic per graph and every call must run behind the same FastAPI
dependency chain as the REST tool endpoints (auth, graph access, rate limits,
write classification, circuit breaker).

- ``graph_id`` comes from the URL (or, on the OAuth-only ``/v1/mcp`` and
  ``/v1/mcp/roboledger`` routes, from the consent grant); never a tool argument.
- Stateless: no ``Mcp-Session-Id``, no GET-side SSE channel, no resumability.
- Credentials: ``X-API-Key`` or an OAuth bearer bound to this URL, never the
  query string.
- Excluded from OpenAPI so the JSON-RPC envelope stays out of the SDK clients.
"""

import asyncio
import json
import time
from importlib.metadata import version as pkg_version
from typing import Any

from fastapi import APIRouter, Depends, Path, Request, Response
from fastapi import status as http_status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import (
  get_current_user_with_graph_or_oauth,
  get_oauth_mcp_principal,
  get_oauth_roboledger_mcp_principal,
)
from robosystems.middleware.auth.oauth import OAuthPrincipal
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.graph.query_telemetry import (
  api_key_prefix_from_request,
  is_disrupted_aggregation,
  log_shared_query_end,
  log_shared_query_start,
  record_shared_query_outcome,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.mcp.tools.manager import ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.api.graphs.mcp import MCPToolCall
from robosystems.models.core import User

from .execute import (
  READ_ONLY_MCP_TOOLS,
  _get_mcp_operation_type,
  _get_user_priority,
  authorize_mcp_tool_call,
  circuit_breaker,
  execute_tool_to_json,
)
from .handlers import (
  MCPHandler,
  is_tool_error_result,
  tool_error_kind,
  validate_mcp_access,
)
from .strategies import MCPExecutionStrategy, MCPStrategySelector
from .streaming import aggregate_streamed_results, stream_mcp_tool_execution

router = APIRouter()
# The graph-agnostic transport, mounted at /v1/mcp (see routers/__init__.py).
agnostic_router = APIRouter()
# The RoboLedger transport, mounted at /v1/mcp/roboledger.
roboledger_router = APIRouter()

_NO_EXCLUSIONS: frozenset[str] = frozenset()

# Only the revision this dispatch implements is offered: 2024-11-05 predates
# Streamable HTTP and 2025-03-26 permitted batching, which is rejected here.
# Other clients negotiate down at initialize; requests without an
# MCP-Protocol-Version header are still served.
MCP_SUPPORTED_PROTOCOL_VERSIONS = frozenset({"2025-06-18"})
MCP_LATEST_PROTOCOL_VERSION = "2025-06-18"

# JSON-RPC 2.0 error codes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# Asserted for every remote caller: real MCP clients send neither the npx
# bridge's User-Agent nor X-MCP-Client, so header sniffing would misclassify
# them as browsers.
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


def _transport_gate(request: Request) -> None:
  """Streamable HTTP checks run before auth and dispatch.

  Origin: the MCP spec requires 403 for untrusted values; server-to-server
  callers send none, so absent is allowed. Content-Type must be exactly
  ``application/json``, which keeps the endpoint out of the browser "simple
  request" class.
  """
  from robosystems.config import env

  origin = request.headers.get("origin")
  if origin and origin not in env.get_main_cors_origins():
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Origin not allowed",
    )

  # Exact media-type match: a substring check would accept
  # `text/plain; application/json` (still a simple request).
  content_type = request.headers.get("content-type", "")
  media_type = content_type.split(";", 1)[0].strip().casefold()
  if media_type != "application/json":
    raise HTTPException(
      status_code=http_status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
      detail="Content-Type must be application/json",
    )


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


class _ReadOnlyViolation(Exception):
  """A read-graph-cypher statement refused by the read-only guard."""


def _tool_error_payload(msg_id: Any, text: str) -> dict[str, Any]:
  """A tool-execution failure as a complete JSON-RPC response payload.

  Per the MCP spec, failures of the tool itself (as opposed to protocol
  errors) are returned as ``result.isError`` so the model can see the message
  and react — e.g. relay a subscription or rate-limit notice to the user.
  """
  return {
    "jsonrpc": "2.0",
    "id": msg_id,
    "result": {"content": [{"type": "text", "text": text}], "isError": True},
  }


def _tool_error_result(msg_id: Any, text: str) -> JSONResponse:
  """`_tool_error_payload` as an HTTP response (the non-streaming path)."""
  return JSONResponse(content=_tool_error_payload(msg_id, text))


def _tool_failure(result: Any) -> tuple[bool, str | None]:
  """Classify an internal tool result as (is_error, failure_kind).

  Handles both the handler's marked text result and the streaming
  aggregator's ``success: False`` shape. ``failure_kind`` is
  ``timeout``/``backend`` (breaker-relevant) or ``constraint`` (caller error).
  """
  if is_tool_error_result(result):
    return True, tool_error_kind(result)
  if isinstance(result, dict) and result.get("success") is False and "error" in result:
    kind = result.get("error_kind")
    return True, kind if isinstance(kind, str) else "backend"
  return False, None


def _to_tool_result(result: Any) -> dict[str, Any]:
  """Map an internal tool result onto the MCP ``tools/call`` result shape."""
  is_error, _ = _tool_failure(result)
  if isinstance(result, dict) and result.get("type") == "text" and "text" in result:
    content = [{"type": "text", "text": result["text"]}]
  elif is_error and isinstance(result, dict) and isinstance(result.get("error"), str):
    content = [{"type": "text", "text": result["error"]}]
  else:
    content = [{"type": "text", "text": json.dumps(result, indent=2, default=str)}]
  return {"content": content, "isError": is_error}


async def _validate_read_access(graph_id: str, current_user: User) -> None:
  """Read-access check on a short-lived session (initialize / tools/list)."""
  from robosystems.database import SessionFactory

  sess = SessionFactory()
  try:
    await validate_mcp_access(graph_id, current_user, sess, "read")
  finally:
    sess.close()


async def _handle_initialize(
  graph_id: str,
  current_user: User,
  params: dict[str, Any],
  excluded_tools: frozenset[str] = _NO_EXCLUSIONS,
) -> dict[str, Any]:
  requested = params.get("protocolVersion")
  protocol_version = (
    requested
    if isinstance(requested, str) and requested in MCP_SUPPORTED_PROTOCOL_VERSIONS
    else MCP_LATEST_PROTOCOL_VERSION
  )

  await _validate_read_access(graph_id, current_user)

  # Rebuilt per initialize so a reconnecting client sees the live tool set.
  instructions: str | None = None
  repository = await get_graph_repository(graph_id, _get_mcp_operation_type(graph_id))
  handler = MCPHandler(repository, graph_id, current_user)
  try:
    tools = _without(await handler.get_tools(), excluded_tools)
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


def _without(
  tools: list[dict[str, Any]], excluded_tools: frozenset[str]
) -> list[dict[str, Any]]:
  """Filtered before instructions are built so they never name a withheld tool."""
  if not excluded_tools:
    return tools
  return [t for t in tools if t.get("name") not in excluded_tools]


async def _handle_tools_list(
  graph_id: str,
  current_user: User,
  excluded_tools: frozenset[str] = _NO_EXCLUSIONS,
) -> dict[str, Any]:
  await _validate_read_access(graph_id, current_user)

  repository = await get_graph_repository(graph_id, _get_mcp_operation_type(graph_id))
  handler = MCPHandler(repository, graph_id, current_user)
  try:
    tools = _without(await handler.get_tools(), excluded_tools)
  finally:
    await handler.close()

  mcp_tools: list[dict[str, Any]] = []
  for tool in tools:
    title = _tool_title(tool)
    entry: dict[str, Any] = {
      "name": tool["name"],
      "title": title,
      "description": tool.get("description", ""),
      "inputSchema": tool.get("inputSchema", {"type": "object", "properties": {}}),
      "annotations": _tool_annotations(tool["name"], title),
    }
    mcp_tools.append(entry)

  return {"tools": mcp_tools}


def _tool_title(tool: dict[str, Any]) -> str:
  """The definition's title, else the name with hyphens read as spaces.
  Directory listings require one on every tool."""
  explicit = tool.get("title")
  if isinstance(explicit, str) and explicit.strip():
    return explicit.strip()
  words = str(tool["name"]).replace("-", " ").replace("_", " ").strip()
  return words[:1].upper() + words[1:]


def _tool_annotations(name: str, title: str) -> dict[str, Any]:
  """MCP tool annotations, explicit on every tool (directory scans reject a
  tool with none).

  ``READ_ONLY_MCP_TOOLS`` and the Cypher read tools (guarded by
  ``assert_read_only_cypher`` on every path) are read-only and idempotent;
  everything else is hinted destructive, matching the authorization
  gauntlet's treatment of non-read tools as mutations.
  """
  annotations: dict[str, Any] = {"title": title, "openWorldHint": False}
  if name in READ_ONLY_MCP_TOOLS or name in _CYPHER_READ_TOOLS:
    annotations.update(
      {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
    )
  else:
    annotations.update({"readOnlyHint": False, "destructiveHint": True})
  return annotations


# Strategies that answer as SSE; everything else is a single JSON body.
_SSE_STRATEGIES = frozenset(
  {
    MCPExecutionStrategy.STREAM_AGGREGATED,
    MCPExecutionStrategy.SSE_PROGRESS,
    MCPExecutionStrategy.SSE_STREAMING,
    MCPExecutionStrategy.NDJSON_STREAMING,
  }
)

# SSE comment pings keep the stream alive through the ALB idle timeout (60s)
# without being parsed as messages.
_SSE_PING_SECONDS = 15

# Cypher read tools route through the shared query queue under load; other
# tools execute directly (mirrors the REST endpoint's queue path).
_CYPHER_READ_TOOLS = frozenset(
  {"read-graph-cypher", "read-neo4j-cypher", "read-ladybug-cypher"}
)

_QUEUE_STRATEGIES = frozenset(
  {
    MCPExecutionStrategy.QUEUE_WITH_MONITORING,
    MCPExecutionStrategy.QUEUE_SIMPLE,
  }
)

# End-to-end ceiling (queue wait + execution) for a bridged queued call:
# tools/call must resolve on this request, unlike the REST 202 polling path.
# Matches the long-tool ceiling advertised in tools/list.
_QUEUE_BRIDGE_TIMEOUT_SECONDS = 300

_QUEUE_POLL_SECONDS = 1.0


def _event_to_progress(
  token: Any, event: dict[str, Any], last_progress: float
) -> tuple[dict[str, Any] | None, float]:
  """Map one internal streaming event to a notifications/progress message.

  Internal events mix scales (percentages vs row counts), so ``progress`` is
  forced monotonic as the MCP spec requires. Data-bearing events return None;
  they go into the final response.
  """
  etype = event.get("event")
  data = event.get("data") or {}

  message: str | None = None
  value: float | None = None
  if etype == "start":
    message = data.get("message")
    value = 0.0
  elif etype == "progress":
    message = data.get("message")
    raw = data.get("progress", data.get("rows_processed"))
    value = float(raw) if raw is not None else None
  elif etype == "query_chunk":
    rows = data.get("total_rows_so_far")
    if rows is not None:
      value = float(rows)
      message = f"Fetched {rows} rows"
  else:
    return None, last_progress

  progress = value if value is not None else last_progress + 1.0
  if progress <= last_progress:
    progress = last_progress + 1.0

  params: dict[str, Any] = {"progressToken": token, "progress": progress}
  if message:
    params["message"] = message
  return (
    {"jsonrpc": "2.0", "method": "notifications/progress", "params": params},
    progress,
  )


async def _stream_tool_call(
  handler: MCPHandler,
  graph_id: str,
  tool_call: MCPToolCall,
  strategy: MCPExecutionStrategy,
  timeout: int,
  msg_id: Any,
  progress_token: Any,
  user_id: str | None = None,
  api_key_prefix: str | None = None,
  exec_id: str | None = None,
):
  """Drive one tools/call as the SSE body of the POST response.

  Emits notifications/progress only when the client sent
  ``_meta.progressToken``, then the final JSON-RPC response. A client
  disconnect cancels this generator and the in-flight tool work with it.
  """
  events: list[dict[str, Any]] = []
  last_progress = 0.0
  payload: dict[str, Any]
  started = time.monotonic()
  # Stays set if the generator is closed at a yield (client disconnect).
  outcome = "client_disconnected"
  try:
    try:
      async with asyncio.timeout(timeout):
        async for event in stream_mcp_tool_execution(
          handler, tool_call.name, tool_call.arguments, strategy.value
        ):
          events.append(event)
          if progress_token is None:
            continue
          notification, last_progress = _event_to_progress(
            progress_token, event, last_progress
          )
          if notification:
            yield {"data": json.dumps(notification)}

      result = aggregate_streamed_results(events)
      failed, failure_kind = _tool_failure(result)
      if is_disrupted_aggregation(result):
        outcome = "stream_disrupted"
        record_shared_query_outcome(
          graph_id,
          user_id,
          signal="stream_disrupted",
          disruption=True,
          api_key_prefix=api_key_prefix,
          endpoint="/v1/graphs/{graph_id}/mcp",
          source="mcp_remote",
          tool_name=tool_call.name,
        )
      elif failed and failure_kind == "timeout":
        outcome = "timeout"
        record_shared_query_outcome(
          graph_id,
          user_id,
          signal="timeout",
          api_key_prefix=api_key_prefix,
          endpoint="/v1/graphs/{graph_id}/mcp",
          source="mcp_remote",
          tool_name=tool_call.name,
        )
      elif failed:
        outcome = "tool_error"
      else:
        outcome = "completed"
      payload = {"jsonrpc": "2.0", "id": msg_id, "result": _to_tool_result(result)}
      if failed and failure_kind in ("timeout", "backend"):
        circuit_breaker.record_failure(graph_id, tool_call.name)
      else:
        circuit_breaker.record_success(graph_id, tool_call.name)
    except TimeoutError:
      outcome = "timeout"
      circuit_breaker.record_failure(graph_id, tool_call.name)
      record_shared_query_outcome(
        graph_id,
        user_id,
        signal="timeout",
        api_key_prefix=api_key_prefix,
        endpoint="/v1/graphs/{graph_id}/mcp",
        source="mcp_remote",
        tool_name=tool_call.name,
      )
      payload = {
        "jsonrpc": "2.0",
        "id": msg_id,
        "result": {
          "content": [
            {
              "type": "text",
              "text": f"Error: tool '{tool_call.name}' timed out after {timeout} seconds",
            }
          ],
          "isError": True,
        },
      }
    except Exception as e:
      outcome = "error"
      circuit_breaker.record_failure(graph_id, tool_call.name)
      record_shared_query_outcome(
        graph_id,
        user_id,
        error=e,
        api_key_prefix=api_key_prefix,
        endpoint="/v1/graphs/{graph_id}/mcp",
        source="mcp_remote",
        tool_name=tool_call.name,
      )
      logger.error(
        f"Remote MCP streamed tool execution failed: {e}",
        extra={"graph_id": graph_id, "tool_name": tool_call.name},
      )
      payload = {
        "jsonrpc": "2.0",
        "id": msg_id,
        "result": {
          "content": [{"type": "text", "text": "Tool execution failed."}],
          "isError": True,
        },
      }
    yield {"data": json.dumps(payload)}
  finally:
    log_shared_query_end(
      exec_id,
      graph_id,
      user_id,
      outcome=outcome,
      duration_ms=(time.monotonic() - started) * 1000,
      api_key_prefix=api_key_prefix,
      source="mcp_remote",
      tool_name=tool_call.name,
    )
    if not handler._closed:
      await handler.close()


async def _stream_queued_call(
  graph_id: str,
  tool_call: MCPToolCall,
  current_user: User,
  msg_id: Any,
  progress_token: Any,
  api_key_prefix: str | None = None,
  exec_id: str | None = None,
):
  """Bridge a queued cypher execution onto the SSE response.

  The REST 202 + polling URL has no MCP equivalent, so this holds the stream,
  relays queue state as progress, and emits the final response. A client
  disconnect cancels the queued query.
  """
  from robosystems.middleware.graph.query_queue import QueryStatus, get_query_queue
  from robosystems.middleware.mcp.tools.cypher_tool import assert_read_only_cypher

  queue_manager = get_query_queue()
  query: str = tool_call.arguments.get("query", "")  # type: ignore[assignment]
  parameters = tool_call.arguments.get("parameters") or {}

  payload: dict[str, Any]
  queue_id: str | None = None
  query_settled = False  # reached completed/failed/cancelled in the queue
  last_progress = 0.0
  started = time.monotonic()
  outcome = "client_disconnected"
  try:
    try:
      # The queue runs the raw statement, so the read-only guard runs first.
      try:
        assert_read_only_cypher(query, graph_id)
      except ValueError as exc:
        raise _ReadOnlyViolation(str(exc)) from exc
      queue_id = await queue_manager.submit_query(
        cypher=query,
        parameters=parameters,  # type: ignore[arg-type]
        graph_id=graph_id,
        user_id=str(current_user.id),
        credits_required=10.0,
        priority=_get_user_priority(current_user),
      )

      async with asyncio.timeout(_QUEUE_BRIDGE_TIMEOUT_SECONDS):
        while True:
          status = await queue_manager.get_query_status(queue_id)
          if not status:
            query_settled = True  # nothing left in the queue to cancel
            payload = _tool_error_payload(
              msg_id, "Queued query state was lost. Please retry."
            )
            break

          raw_state = status.get("status", "")
          state = raw_state.value if isinstance(raw_state, QueryStatus) else raw_state
          if progress_token is not None:
            position = status.get("queue_position")
            message = (
              f"Queued (position {position})"
              if state == "pending" and position
              else state.capitalize()
            )
            last_progress += 1.0
            yield {
              "data": json.dumps(
                {
                  "jsonrpc": "2.0",
                  "method": "notifications/progress",
                  "params": {
                    "progressToken": progress_token,
                    "progress": last_progress,
                    "message": message,
                  },
                }
              )
            }

          if state == "completed":
            query_settled = True
            result = await queue_manager.get_query_result(queue_id)
            failed, failure_kind = _tool_failure(result)
            outcome = "tool_error" if failed else "completed"
            payload = {
              "jsonrpc": "2.0",
              "id": msg_id,
              "result": _to_tool_result(result),
            }
            if failed and failure_kind in ("timeout", "backend"):
              circuit_breaker.record_failure(graph_id, tool_call.name)
            else:
              circuit_breaker.record_success(graph_id, tool_call.name)
            break
          if state in ("failed", "cancelled"):
            query_settled = True
            outcome = f"queue_{state}"
            error = status.get("error") or f"Query {state}"
            if state == "failed":
              circuit_breaker.record_failure(graph_id, tool_call.name)
              record_shared_query_outcome(
                graph_id,
                current_user.id,
                signal="queue_failed",
                api_key_prefix=api_key_prefix,
                endpoint="/v1/graphs/{graph_id}/mcp",
                source="mcp_remote",
                tool_name=tool_call.name,
              )
            payload = _tool_error_payload(msg_id, str(error))
            break

          await asyncio.sleep(_QUEUE_POLL_SECONDS)
    except _ReadOnlyViolation as exc:
      # A policy answer, not a backend failure: no breaker hit.
      query_settled = True
      outcome = "denied"
      payload = _tool_error_payload(msg_id, f"Error: {exc}")
    except TimeoutError:
      outcome = "bridge_timeout"
      # Counts against the breaker: under queue saturation, opening it sheds
      # the load the queue is drowning under.
      circuit_breaker.record_failure(graph_id, tool_call.name)
      payload = _tool_error_payload(
        msg_id,
        f"Error: queued query did not complete within "
        f"{_QUEUE_BRIDGE_TIMEOUT_SECONDS} seconds",
      )
    except Exception as e:
      outcome = "error"
      circuit_breaker.record_failure(graph_id, tool_call.name)
      record_shared_query_outcome(
        graph_id,
        current_user.id,
        error=e,
        api_key_prefix=api_key_prefix,
        endpoint="/v1/graphs/{graph_id}/mcp",
        source="mcp_remote",
        tool_name=tool_call.name,
      )
      logger.error(
        f"Remote MCP queued execution failed: {e}",
        extra={"graph_id": graph_id, "tool_name": tool_call.name},
      )
      payload = _tool_error_payload(msg_id, "Tool execution failed.")

    yield {"data": json.dumps(payload)}
  finally:
    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome=outcome,
      duration_ms=(time.monotonic() - started) * 1000,
      api_key_prefix=api_key_prefix,
      source="mcp_remote",
      tool_name=tool_call.name,
    )
    # Disconnect, bridge timeout, or submit/monitor failure: cancel the queued
    # work so it stops consuming queue capacity.
    if not query_settled and queue_id is not None:
      try:
        await queue_manager.cancel_query(queue_id, str(current_user.id))
      except Exception as cancel_error:
        logger.warning(
          f"Failed to cancel abandoned queued query {queue_id}: {cancel_error}"
        )


async def _handle_tools_call(
  request: Request,
  graph_id: str,
  current_user: User,
  msg_id: Any,
  params: dict[str, Any],
  excluded_tools: frozenset[str] = _NO_EXCLUSIONS,
) -> Response:
  name = params.get("name")
  if not isinstance(name, str) or not name:
    return _rpc_error(msg_id, INVALID_PARAMS, "Invalid params: 'name' is required")
  # Refused, not just hidden: a client can call a name it never listed.
  if name in excluded_tools:
    return _tool_error_result(
      msg_id, f"Tool '{name}' is not available on this connection."
    )
  arguments = params.get("arguments") or {}
  if not isinstance(arguments, dict):
    return _rpc_error(
      msg_id, INVALID_PARAMS, "Invalid params: 'arguments' must be an object"
    )

  tool_call = MCPToolCall(name=name, arguments=arguments)
  key_prefix = api_key_prefix_from_request(request)

  try:
    circuit_breaker.check_circuit(graph_id, name)
    access_type = await authorize_mcp_tool_call(graph_id, tool_call, current_user)
  except HTTPException as e:
    # Tool failures leave as HTTP 200 + isError, so status-code telemetry is
    # blind here; record the outcome at classification time.
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      signal=getattr(e, "telemetry_signal", None),
      status_code=e.status_code,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/mcp",
      source="mcp_remote",
      tool_name=name,
    )
    detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
    return _tool_error_result(msg_id, detail)

  api_logger.info(
    f"Remote MCP tool execution started: {name}",
    extra={
      "component": "mcp_remote",
      "action": "tool_started",
      "user_id": str(current_user.id),
      "database": graph_id,
      "request_id": getattr(request.state, "request_id", None),
      "metadata": {
        "tool_name": name,
        "access_type": access_type,
        "api_key_prefix": key_prefix,
      },
    },
  )

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

  query_arg = arguments.get("query")
  exec_id = log_shared_query_start(
    graph_id,
    current_user.id,
    api_key_prefix=key_prefix,
    source="mcp_remote",
    tool_name=name,
    query_length=len(query_arg) if isinstance(query_arg, str) else None,
    strategy=strategy.value,
  )

  meta = params.get("_meta")
  progress_token = meta.get("progressToken") if isinstance(meta, dict) else None
  accept_header = request.headers.get("accept", "")
  client_accepts_sse = "text/event-stream" in accept_header

  # Under load, cypher reads route through the shared query queue; the stream
  # relays queue state and resolves with the result.
  #
  # A client that doesn't accept SSE falls through to bounded direct execution,
  # bypassing queue admission. Accepted: spec-compliant clients always accept
  # SSE, and the graph API's own admission control backstops saturation.
  if (
    strategy in _QUEUE_STRATEGIES and name in _CYPHER_READ_TOOLS and client_accepts_sse
  ):
    return EventSourceResponse(
      _stream_queued_call(
        graph_id,
        tool_call,
        current_user,
        msg_id,
        progress_token,
        api_key_prefix=key_prefix,
        exec_id=exec_id,
      ),
      ping=_SSE_PING_SECONDS,
    )

  repository = await get_graph_repository(graph_id, _get_mcp_operation_type(graph_id))
  handler = MCPHandler(repository, graph_id, current_user)

  # Handler ownership passes to the generator, which closes it.
  if strategy in _SSE_STRATEGIES and client_accepts_sse:
    return EventSourceResponse(
      _stream_tool_call(
        handler,
        graph_id,
        tool_call,
        strategy,
        timeout,
        msg_id,
        progress_token,
        user_id=str(current_user.id),
        api_key_prefix=key_prefix,
        exec_id=exec_id,
      ),
      ping=_SSE_PING_SECONDS,
    )

  direct_started = time.monotonic()
  try:
    result = await execute_tool_to_json(handler, graph_id, tool_call, strategy, timeout)
  except HTTPException as e:
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      status_code=e.status_code,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/mcp",
      source="mcp_remote",
      tool_name=name,
    )
    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome=f"http_{e.status_code}",
      duration_ms=(time.monotonic() - direct_started) * 1000,
      api_key_prefix=key_prefix,
      source="mcp_remote",
      tool_name=name,
    )
    detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
    return _tool_error_result(msg_id, detail)
  except Exception as e:
    circuit_breaker.record_failure(graph_id, name)
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      error=e,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/mcp",
      source="mcp_remote",
      tool_name=name,
    )
    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome="error",
      duration_ms=(time.monotonic() - direct_started) * 1000,
      api_key_prefix=key_prefix,
      source="mcp_remote",
      tool_name=name,
    )
    logger.error(
      f"Remote MCP tool execution failed: {e}",
      extra={"graph_id": graph_id, "user_id": str(current_user.id), "tool_name": name},
    )
    return _tool_error_result(msg_id, "Tool execution failed.")
  finally:
    if not handler._closed:
      await handler.close()

  failed, failure_kind = _tool_failure(result)
  # Constraint failures mean the backend answered: they count as a success.
  if failed and failure_kind in ("timeout", "backend"):
    circuit_breaker.record_failure(graph_id, name)
  else:
    circuit_breaker.record_success(graph_id, name)

  if is_disrupted_aggregation(result):
    outcome = "stream_disrupted"
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      signal="stream_disrupted",
      disruption=True,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/mcp",
      source="mcp_remote",
      tool_name=name,
    )
  elif failed and failure_kind == "timeout":
    outcome = "timeout"
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      signal="timeout",
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/mcp",
      source="mcp_remote",
      tool_name=name,
    )
  elif failed:
    outcome = "tool_error"
  else:
    outcome = "completed"
  log_shared_query_end(
    exec_id,
    graph_id,
    current_user.id,
    outcome=outcome,
    duration_ms=(time.monotonic() - direct_started) * 1000,
    api_key_prefix=key_prefix,
    source="mcp_remote",
    tool_name=name,
  )
  return _rpc_result(msg_id, _to_tool_result(result))


async def dispatch_jsonrpc(
  request: Request,
  graph_id: str,
  current_user: User,
  excluded_tools: frozenset[str] = _NO_EXCLUSIONS,
) -> Response:
  """Parse and dispatch one JSON-RPC message (the transport's whole surface).

  ``excluded_tools`` is the route's tool profile: withheld from
  ``initialize`` and ``tools/list`` and refused on ``tools/call``.
  """
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

  # Absent header = pre-header client, allowed. An unsupported value is 400 per
  # the Streamable HTTP spec. initialize negotiates in its body instead.
  version_header = request.headers.get("mcp-protocol-version")
  if (
    method != "initialize"
    and version_header is not None
    and version_header not in MCP_SUPPORTED_PROTOCOL_VERSIONS
  ):
    supported = ", ".join(sorted(MCP_SUPPORTED_PROTOCOL_VERSIONS))
    if isinstance(method, str) and "id" in message:
      return _rpc_error(
        msg_id,
        INVALID_REQUEST,
        f"Unsupported MCP-Protocol-Version '{version_header}' (supported: {supported})",
        http_code=http_status.HTTP_400_BAD_REQUEST,
      )
    # Notifications never receive JSON-RPC replies; reject at HTTP level only.
    return Response(status_code=http_status.HTTP_400_BAD_REQUEST)

  # Responses (no method) and notifications (no id) get 202 with no body, and
  # before params validation: a notification never receives a reply
  # (JSON-RPC 2.0 §4.1). Cancellation is best-effort on a stateless transport.
  if not isinstance(method, str) or "id" not in message:
    return Response(status_code=http_status.HTTP_202_ACCEPTED)

  params = message.get("params") or {}
  if not isinstance(params, dict):
    return _rpc_error(msg_id, INVALID_PARAMS, "Invalid params: expected an object")

  try:
    if method == "initialize":
      return _rpc_result(
        msg_id,
        await _handle_initialize(graph_id, current_user, params, excluded_tools),
      )
    elif method == "ping":
      return _rpc_result(msg_id, {})
    elif method == "tools/list":
      return _rpc_result(
        msg_id, await _handle_tools_list(graph_id, current_user, excluded_tools)
      )
    elif method == "tools/call":
      return await _handle_tools_call(
        request, graph_id, current_user, msg_id, params, excluded_tools
      )
    else:
      return _rpc_error(msg_id, METHOD_NOT_FOUND, f"Method not found: {method}")
  except HTTPException as e:
    # initialize / tools/list denials have no tool result to attach to.
    detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
    return _rpc_error(msg_id, INVALID_REQUEST, detail, http_code=e.status_code)
  except Exception as e:
    logger.error(
      f"Remote MCP transport error on {method}: {e}",
      extra={"graph_id": graph_id, "user_id": str(current_user.id)},
    )
    return _rpc_error(msg_id, INTERNAL_ERROR, "Internal error")


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
  _transport: None = Depends(_transport_gate),
  current_user: User = Depends(get_current_user_with_graph_or_oauth),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Response:
  """Streamable-HTTP MCP endpoint (JSON-RPC 2.0)."""
  return await dispatch_jsonrpc(request, graph_id, current_user)


@agnostic_router.post("", include_in_schema=False, response_model=None)
@endpoint_metrics_decorator("/v1/mcp", business_event_type="mcp_remote_request")
async def mcp_agnostic_transport(
  request: Request,
  _transport: None = Depends(_transport_gate),
  principal: OAuthPrincipal = Depends(get_oauth_mcp_principal),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Response:
  """Streamable-HTTP MCP endpoint (JSON-RPC 2.0), OAuth-only.

  The grant's graph is the resolved ``graph_id``: same dispatch, same
  per-call access checks, same isolation keys as the per-graph route.
  """
  return await dispatch_jsonrpc(request, principal.graph_id, principal.user)


@roboledger_router.post("", include_in_schema=False, response_model=None)
@endpoint_metrics_decorator(
  "/v1/mcp/roboledger", business_event_type="mcp_remote_request"
)
async def mcp_roboledger_transport(
  request: Request,
  _transport: None = Depends(_transport_gate),
  principal: OAuthPrincipal = Depends(get_oauth_roboledger_mcp_principal),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Response:
  """Streamable-HTTP MCP endpoint (JSON-RPC 2.0) for RoboLedger graphs,
  OAuth-only.

  The graph-agnostic transport with the RoboLedger tool profile: the grant's
  graph (a RoboLedger graph, checked at consent) is the resolved
  ``graph_id``, and ``ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS`` is withheld.
  """
  return await dispatch_jsonrpc(
    request,
    principal.graph_id,
    principal.user,
    excluded_tools=ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS,
  )
