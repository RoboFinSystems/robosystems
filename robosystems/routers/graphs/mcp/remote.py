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

import asyncio
import json
import re
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
  get_current_user_with_graph_or_url_token,
)
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.graph.query_telemetry import (
  api_key_prefix_from_request,
  is_disrupted_aggregation,
  log_shared_query_end,
  log_shared_query_start,
  record_shared_query_outcome,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
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

# Protocol revisions this transport can negotiate. The server answers with the
# client's requested revision when supported, else its own latest. Exactly the
# revision this dispatch implements is offered — 2024-11-05 predates Streamable
# HTTP, and 2025-03-26 permitted JSON-RPC batching, which this transport
# unconditionally rejects, so advertising either would promise semantics the
# wire doesn't honor. Clients on other revisions negotiate to 2025-06-18 at
# initialize (Claude requests 2025-11-25 and accepts this fine); requests
# carrying no MCP-Protocol-Version header are still served for compatibility.
MCP_SUPPORTED_PROTOCOL_VERSIONS = frozenset({"2025-06-18"})
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


def _transport_gate(request: Request) -> None:
  """HTTP-level Streamable HTTP checks, run before auth and dispatch.

  Origin: the MCP spec requires servers to validate Origin and answer 403 for
  untrusted values. Server-to-server callers (Claude's backend, the npx
  bridge) send no Origin header — absent is allowed; a browser context must
  come from a first-party app origin.

  Content-Type: JSON-RPC bodies must be ``application/json``, which also
  keeps the endpoint out of the browser "simple request" delivery class.
  """
  from robosystems.config import env

  origin = request.headers.get("origin")
  if origin and origin not in env.get_main_cors_origins():
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Origin not allowed",
    )

  # Exact media-type essence match — a substring check would accept
  # `text/plain; application/json` (still a browser simple request) and
  # unrelated `+json` types.
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

  Two failure encodings reach this transport: the handler's marked text
  result (``is_error``/``error_kind``, see ``handlers.tool_error_result``)
  and the streaming aggregator's failure shape (``success: False`` +
  ``error`` — which also covers a stream that ended with no terminal event).
  ``failure_kind`` is ``timeout``/``backend`` (breaker-relevant) or
  ``constraint`` (caller error).
  """
  if is_tool_error_result(result):
    return True, tool_error_kind(result)
  if isinstance(result, dict) and result.get("success") is False and "error" in result:
    kind = result.get("error_kind")
    return True, kind if isinstance(kind, str) else "backend"
  return False, None


def _to_tool_result(result: Any) -> dict[str, Any]:
  """Map an internal tool result onto the MCP ``tools/call`` result shape.

  Execution failures come back as ``isError: true`` so the model sees a
  failed call rather than error prose masquerading as valid output.
  """
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


# The npx bridge's own client-side workspace tool mutates client process
# state. A remote connector has no client process and is URL-anchored to one
# graph, so the remote surface answers with an address instead — which is why
# the tool is now named for resolving rather than switching.
_REMOTE_RESOLVE_SUBGRAPH_DESCRIPTION = (
  "Resolve a subgraph in this graph's family (or `primary` for the parent) to "
  "the MCP endpoint that serves it. This connector is anchored to one graph by "
  "its URL and does not change graphs — the subgraph is a separate endpoint "
  "you add as its own connector. Within a parent's own subgraphs, this "
  "connector's API key already covers the target, so reuse it."
)


def _remote_resolve_subgraph(graph_id: str, arguments: dict[str, Any]) -> str:
  """Answer resolve-subgraph with the target's connector URL (text result).

  Targets are constrained to this connector's graph family — the parent or
  one of its subgraphs. The URL alone carries no credential, so the caller
  still has to supply one; what changes is whether they must *mint* one.

  Scope runs one way. ``validate_api_key_with_graph`` honours a scoped key
  for "its own graph and its subgraphs", so descending — a connector on the
  parent, targeting one of its subgraphs — the key already in this
  connector's URL is **guaranteed** to cover the target. Ascending or
  sideways (subgraph → parent, subgraph → sibling) it is not: a
  subgraph-scoped key reaches neither. An unscoped user key covers both
  directions, but we can't tell which kind is in play from here, so the
  message promises reuse only where it is certain and stays honest about
  "may already work" everywhere else.
  """
  from robosystems.config import env

  target = str(arguments.get("subgraph") or "").strip()
  parent = graph_id.split("_")[0]
  if not target:
    return "Error: subgraph is required (an id, a short name, or 'primary')."

  if target == "primary":
    target_id = parent
  elif "_" in target:
    target_id = target
  else:
    target_id = f"{parent}_{target}"

  if not re.fullmatch(GRAPH_OR_SUBGRAPH_ID_PATTERN, target_id):
    return f"Error: '{target}' is not a valid subgraph id or name."

  if target_id != parent and not target_id.startswith(f"{parent}_"):
    return (
      f"Error: '{target}' is outside this connector's graph family "
      f"('{parent}' and its subgraphs). A different graph needs its own "
      "connector, set up from that graph's MCP page."
    )

  connector_url = f"{env.ROBOSYSTEMS_API_URL}/v1/graphs/{target_id}/mcp"

  # Descending within the family is the common case and the certain one.
  descending = graph_id == parent and target_id != parent
  credential_step = (
    (
      "Reuse the API key this connector already uses — a key scoped to "
      f"'{graph_id}' covers its subgraphs, so it works on the new URL "
      "unchanged. No new credential is needed."
    )
    if descending
    else (
      "Supply a credential the target accepts. A key scoped to a subgraph "
      "does not reach its parent or siblings, so the current one may not "
      f"carry over; if it doesn't, generate one from the app's MCP page "
      f"(/connect) with '{target_id}' selected."
    )
  )

  return json.dumps(
    {
      "switched": False,
      "reason": "url_anchored_connector",
      "target_graph_id": target_id,
      "connector_url": connector_url,
      "credential_reuse": "guaranteed" if descending else "depends_on_key_scope",
      "message": (
        f"This connector is anchored to graph '{graph_id}' by its URL and "
        f"does not change graphs. Add an MCP connector for "
        f"{connector_url}. {credential_step}"
      ),
    },
    indent=2,
  )


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
    if tool["name"] == "resolve-subgraph":
      entry["description"] = _REMOTE_RESOLVE_SUBGRAPH_DESCRIPTION
    mcp_tools.append(entry)

  return {"tools": mcp_tools}


# Strategies whose work is long or chunked enough to earn the SSE response
# mode. Everything else answers as a single application/json body — Streamable
# HTTP lets the server choose per call.
_SSE_STRATEGIES = frozenset(
  {
    MCPExecutionStrategy.STREAM_AGGREGATED,
    MCPExecutionStrategy.SSE_PROGRESS,
    MCPExecutionStrategy.SSE_STREAMING,
    MCPExecutionStrategy.NDJSON_STREAMING,
  }
)

# Keepalive interval for SSE responses. Comment pings (`: ping …`) keep the
# stream alive through the ALB idle timeout (default 60s) without being
# parsed as messages by MCP clients.
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

# Explicit end-to-end ceiling (queue wait + execution) for a bridged queued
# call. MCP tools/call must resolve on this request, so unlike the REST 202
# path — where the client owns the polling budget — the held stream needs its
# own deliberate ceiling. Matches the long-tool ceiling advertised in
# tools/list capabilities.
_QUEUE_BRIDGE_TIMEOUT_SECONDS = 300

# Poll cadence for bridged queue monitoring (same as the REST monitor loop).
_QUEUE_POLL_SECONDS = 1.0


def _event_to_progress(
  token: Any, event: dict[str, Any], last_progress: float
) -> tuple[dict[str, Any] | None, float]:
  """Map one internal streaming event to a notifications/progress message.

  Internal progress events mix scales (percentages vs row counts), so the
  emitted `progress` value is clamped monotonically increasing as the MCP
  spec requires. Data-bearing events (chunks, results) return None here —
  they are aggregated into the final response, never sent as notifications.
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

  Emits notifications/progress while the tool runs (only when the client sent
  `_meta.progressToken` — the MCP contract), then the final JSON-RPC response,
  then ends the stream. A client disconnect cancels this generator and with it
  the in-flight tool work — the transport-level closure of the abandoned-CPU
  gap. sse_starlette's comment ping carries the stream across the ALB idle
  timeout while the tool is silent.
  """
  events: list[dict[str, Any]] = []
  last_progress = 0.0
  payload: dict[str, Any]
  started = time.monotonic()
  # Default covers the generator being closed at a yield (client disconnect
  # cancels in-flight work); every settled path overwrites it.
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

  The REST endpoint answers queue strategies with 202 + a polling URL, which
  has no MCP equivalent — tools/call must resolve on this request. So the
  bridge submits to the shared query queue, holds the stream, relays queue
  state as notifications/progress, and emits the final JSON-RPC response on
  completion. A client disconnect cancels the queued query so abandoned work
  stops consuming queue capacity.
  """
  from robosystems.middleware.graph.query_queue import get_query_queue

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

          state = str(status.get("status", ""))
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
    except TimeoutError:
      outcome = "bridge_timeout"
      # Deliberate: the bridge ceiling exhausting counts against the breaker.
      # Whether the 300s went to queue wait or execution, the backend didn't
      # produce a result in time — and if the queue is saturated, opening the
      # breaker sheds exactly the load the queue is drowning under.
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
    # Reached without a settled queue state on disconnect (generator closed
    # at a yield), bridge timeout, or submit/monitor failure: stop the queued
    # work so abandoned queries don't burn queue capacity.
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
) -> Response:
  name = params.get("name")
  if not isinstance(name, str) or not name:
    return _rpc_error(msg_id, INVALID_PARAMS, "Invalid params: 'name' is required")
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
    # Tool failures leave this transport as HTTP 200 + isError per the MCP
    # contract, so outcomes must be recorded here at classification time —
    # status-code-level telemetry is blind to this surface.
    record_shared_query_outcome(
      graph_id,
      current_user.id,
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
      "tool_name": name,
      "access_type": access_type,
    },
  )

  # resolve-subgraph never reaches the tool layer on this transport: the
  # connector is URL-anchored (a subgraph is another connector URL), so the
  # answer is the target's URL. Runs after the gauntlet — access to THIS
  # graph is still required to resolve its family's URLs.
  if name == "resolve-subgraph":
    return _rpc_result(
      msg_id,
      _to_tool_result(
        {"type": "text", "text": _remote_resolve_subgraph(graph_id, arguments)}
      ),
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

  # Queue bridge: under load, cypher reads route through the shared query
  # queue. The stream relays queue state as progress and resolves with the
  # result — no handler needed, the queue executes the query itself.
  #
  # DELIBERATE: a client that doesn't accept SSE falls through to bounded
  # direct execution below, bypassing queue admission during exactly the load
  # window the queue exists for. Accepted because spec-compliant MCP clients
  # always send `Accept: …, text/event-stream` (the fallback is for curl-grade
  # callers), the direct path is still bounded by its strategy timeout, and
  # the graph API's own admission control backstops instance saturation.
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

  # SSE-on-POST: long/streaming strategies answer as text/event-stream when
  # the client accepts it (MCP clients advertise both). Handler ownership
  # passes to the generator, which closes it when the stream ends.
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
  # Constraint failures mean the backend answered fine — they reset the
  # breaker like a success; only timeout/backend failures count against it.
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
  request: Request, graph_id: str, current_user: User
) -> Response:
  """Parse and dispatch one JSON-RPC message (the transport's whole surface)."""
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

  # Post-negotiation requests must carry a supported MCP-Protocol-Version when
  # they send the header at all (absent = pre-header clients, allowed for
  # backwards compatibility). Unsupported values answer 400 per the spec.
  # initialize is exempt — negotiation happens in its body.
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

  # A message without a method is a client->server response; a message without
  # an id is a notification. Streamable HTTP: accept both with 202, no body.
  # This runs BEFORE params validation — a notification must never receive a
  # reply, not even an error for malformed params (JSON-RPC 2.0 §4.1).
  # (notifications/initialized and notifications/cancelled land here —
  # cancellation is best-effort only on a stateless multi-node transport.)
  if not isinstance(method, str) or "id" not in message:
    return Response(status_code=http_status.HTTP_202_ACCEPTED)

  params = message.get("params") or {}
  if not isinstance(params, dict):
    return _rpc_error(msg_id, INVALID_PARAMS, "Invalid params: expected an object")

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
      return await _handle_tools_call(request, graph_id, current_user, msg_id, params)
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
  current_user: User = Depends(get_current_user_with_graph_or_url_token),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Response:
  """Streamable-HTTP MCP endpoint (JSON-RPC 2.0)."""
  return await dispatch_jsonrpc(request, graph_id, current_user)
