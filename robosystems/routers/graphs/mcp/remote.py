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
- Two credential carriages on the per-graph route: ``X-API-Key`` and an
  OAuth ``Authorization: Bearer`` bound to this exact URL — nothing in the
  query string. The graph-agnostic ``POST /v1/mcp`` (``agnostic_router``)
  is OAuth-only: the consent grant names the graph, and the transport
  dispatches on that resolved ``graph_id`` exactly as the per-graph route
  dispatches on the URL's.
- Excluded from the OpenAPI schema so the JSON-RPC envelope never lands in
  the generated SDK clients.
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

  # Instructions are rebuilt on every initialize from the live tool surface,
  # so a reconnecting client picks up changes to the graph's tool set.
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
  """A human title for a tool: the definition's own when it has one, else
  its name with the hyphens read as spaces (``close-period`` → ``Close
  period``). Directory listings require one on every tool."""
  explicit = tool.get("title")
  if isinstance(explicit, str) and explicit.strip():
    return explicit.strip()
  words = str(tool["name"]).replace("-", " ").replace("_", " ").strip()
  return words[:1].upper() + words[1:]


def _tool_annotations(name: str, title: str) -> dict[str, Any]:
  """MCP tool annotations, explicit on every tool.

  Reads (the ``READ_ONLY_MCP_TOOLS`` allowlist, which is also the
  authorization classification) are read-only and idempotent; everything
  else is a write and is hinted destructive — conservatively, since the
  authorization gauntlet treats every non-read tool as a mutation. The
  Cypher read tools are hinted read-only too: ``assert_read_only_cypher``
  refuses write, bulk, admin and schema-DDL statements on every path that
  executes on their behalf, so the hint promises exactly what the tool
  enforces (``write-graph-cypher`` stays destructive). Directory scans —
  ChatGPT's in particular — reject a tool that carries no hints. All tools
  act on this graph alone (closed world).
  """
  annotations: dict[str, Any] = {"title": title, "openWorldHint": False}
  if name in READ_ONLY_MCP_TOOLS or name in _CYPHER_READ_TOOLS:
    annotations.update(
      {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True}
    )
  else:
    annotations.update({"readOnlyHint": False, "destructiveHint": True})
  return annotations


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
      # Same read-only guard the tool applies on the direct path; the queue
      # runs the raw statement, so it must be refused before submission.
      try:
        assert_read_only_cypher(query)
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
    except _ReadOnlyViolation as exc:
      # A denied statement is a policy answer, not a backend failure: no
      # breaker hit, and the model sees why so it can rephrase.
      query_settled = True
      outcome = "denied"
      payload = _tool_error_payload(msg_id, f"Error: {exc}")
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
  # backwards compatibility). Unsupported values answer 400, as the MCP
  # Streamable HTTP transport specification requires. initialize is exempt —
  # negotiation happens in its body.
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
