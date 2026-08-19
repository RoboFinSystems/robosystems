"""MCP tool execution: `POST /v1/graphs/{graph_id}/mcp/call-tool`.

Runs the shared authorization gauntlet (`authorize_mcp_tool_call`), selects an
execution strategy, and answers as JSON, SSE, NDJSON, or a 202 queue handle
depending on the strategy and what the client accepts.
"""

import asyncio
import json
import sys
from datetime import UTC, datetime
from typing import Any

from fastapi import (
  APIRouter,
  Body,
  Depends,
  HTTPException,
  Path,
  Request,
)
from fastapi import (
  Query as QueryParam,
)
from fastapi import status as http_status
from fastapi.responses import JSONResponse, StreamingResponse
from sse_starlette.sse import EventSourceResponse

from robosystems.config.query_queue import QueryQueueConfig
from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_graph_repository
from robosystems.middleware.graph.query_queue import get_query_queue
from robosystems.middleware.graph.query_telemetry import (
  api_key_prefix_from_request,
  is_disrupted_aggregation,
  log_shared_query_end,
  log_shared_query_start,
  record_shared_query_outcome,
)
from robosystems.middleware.graph.statement_kernel import (
  StatementEngine,
  statement_kernel,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  OperationStatus,
  OperationType,
  record_operation_metric,
)
from robosystems.middleware.sse.operation_manager import create_operation_response
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.mcp import MCPToolCall, MCPToolResult
from robosystems.models.core import User

from .handlers import MCPHandler, is_tool_error_result, validate_mcp_access
from .strategies import (
  MCPClientDetector,
  MCPExecutionStrategy,
  MCPStrategySelector,
)
from .streaming import (
  aggregate_streamed_results,
  stream_mcp_tool_execution,
)

router = APIRouter()

circuit_breaker = CircuitBreakerManager()

# MCP result cache TTLs (seconds).
# To manually invalidate: just admin dev cache flush mcp_cache
_MCP_INFO_CACHE_TTL = 1800  # 30 minutes
_MCP_SCHEMA_CACHE_TTL = 3600  # 1 hour

# Shared module-level Redis client for MCP cache — created once, reuses connection pool.
# Lazily initialized on first use to avoid import-time URL resolution issues.
_mcp_redis_client: Any = None

# Write classification is FAIL-CLOSED: any tool NOT in this read-only allowlist
# is treated as a write and must pass the member/admin role check (a `viewer`
# is read-only). This is deliberately an inverted allowlist so that every new
# tool — including every registrar-generated OLTP command op — defaults to
# write and cannot silently become viewer-writable.
#
# The `read-*-cypher` tools are intentionally ABSENT: they are classified per
# query by the StatementKernel (they can carry a write statement), handled
# separately below.
#
# When adding a new READ tool, add it here (otherwise viewers can't call it —
# safe, but over-restrictive). New WRITE tools need no change.
READ_ONLY_MCP_TOOLS: frozenset[str] = frozenset(
  {
    # Graph introspection / exploration
    "get-graph-info",
    "get-graph-schema",
    "get-graphql-schema",
    "get-graph-sync-status",
    "get-example-queries",
    "query-graphql",
    "list-subgraphs",
    # Financial analysis / reporting reads
    "financial-statement-analysis",
    "live-financial-statement",
    "build-fact-grid",
    "resolve-element",
    # Fiscal calendar / close reads
    "get-fiscal-calendar",
    "get-period-close-status",
    "get-close-playbook",
    "list-period-drafts",
    # Mapping reads
    "get-unmapped-elements",
    "suggest-mapping",
    "list-mapping-structures",
    "get-mapping-summary",
    # Agent reads
    "get-agent",
    "list-agents",
    "agent-activity",
    # Event block / handler reads
    "get-event-block",
    "list-event-blocks",
    "get-event-handler",
    "list-event-handlers",
    # Information block reads
    "get-information-block",
    "list-information-blocks",
    # Document / memory reads
    "get-document",
    "list-documents",
    "get-document-section",
    "search-documents",
    "recall",
  }
)


def _get_mcp_redis_client() -> Any:
  """Return the shared async Redis client for MCP_CACHE, creating it on first call."""
  global _mcp_redis_client
  if _mcp_redis_client is None:
    from robosystems.config.valkey_registry import (
      ValkeyDatabase,
      create_async_redis_client,
    )

    _mcp_redis_client = create_async_redis_client(ValkeyDatabase.MCP_CACHE)
  return _mcp_redis_client


def _mcp_cache_key(graph_id: str, tool_name: str) -> str:
  """Build a Valkey cache key for MCP tool results.

  Only the argument-free tools (get-graph-schema, get-graph-info) are cached,
  so graph_id plus tool_name identifies a result uniquely.
  """
  return f"mcp:{graph_id}:{tool_name}"


async def _get_mcp_cache(graph_id: str, tool_name: str) -> dict[str, Any] | None:
  """Get a cached MCP tool result from Valkey. Returns None on miss or error."""
  try:
    data = await _get_mcp_redis_client().get(_mcp_cache_key(graph_id, tool_name))
    if data:
      logger.debug(f"MCP cache hit for {tool_name} on {graph_id}")
      return json.loads(data)
  except Exception as e:
    logger.debug(f"MCP cache read error for {tool_name} on {graph_id}: {e}")
  return None


async def _set_mcp_cache(
  graph_id: str, tool_name: str, result: dict[str, Any], ttl: int
) -> None:
  """Store an MCP tool result in Valkey cache."""
  try:
    await _get_mcp_redis_client().set(
      _mcp_cache_key(graph_id, tool_name),
      json.dumps(result),
      ex=ttl,
    )
    logger.debug(f"MCP cache set for {tool_name} on {graph_id} (ttl={ttl}s)")
  except Exception as e:
    logger.debug(f"MCP cache write error for {tool_name} on {graph_id}: {e}")


def _get_user_priority(user: User) -> int:
  """Get query priority based on user subscription tier."""
  if hasattr(user, "subscription") and user.subscription:
    tier = (
      user.subscription.billing_plan.name if user.subscription.billing_plan else None
    )
    return QueryQueueConfig.get_priority_for_user(tier)
  return QueryQueueConfig.DEFAULT_PRIORITY


def _get_mcp_operation_type(graph_id: str) -> str:
  """Determine the correct operation type for MCP operations."""
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    return "read"
  else:
    return "write"


async def execute_tool_directly(
  handler: MCPHandler,
  tool_call: MCPToolCall,
  timeout: int = 60,
) -> dict[str, Any]:
  """Execute MCP tool directly without queuing."""
  try:
    result = await asyncio.wait_for(
      handler.call_tool(tool_call.name, tool_call.arguments), timeout=timeout
    )
    return result
  except TimeoutError:
    raise HTTPException(
      status_code=http_status.HTTP_408_REQUEST_TIMEOUT,
      detail=f"Tool execution timed out after {timeout} seconds",
    )


async def authorize_mcp_tool_call(
  graph_id: str,
  tool_call: MCPToolCall,
  current_user: User,
) -> str:
  """Run the shared MCP tool-call authorization gauntlet.

  Used by both the REST endpoint (POST /mcp/call-tool) and the remote JSON-RPC
  transport (POST /mcp) so the two surfaces cannot drift: fail-closed write
  classification, StatementKernel statement policy for cypher read tools,
  per-graph role validation, shared-repo subscription lookup, and dual-layer
  volume rate limiting.

  Returns the resolved access type, `"read"` or `"write"`. Raises 403 on
  access or subscription denial and 429 on volume limits.
  """
  from robosystems.config import env
  from robosystems.database import SessionFactory

  is_cypher_read_tool = tool_call.name in (
    "read-graph-cypher",
    "read-neo4j-cypher",
    "read-ladybug-cypher",
  )
  # Fail-closed write classification: a tool is a WRITE unless it appears on
  # the read-only allowlist, so every non-read tool — including all
  # registrar-generated OLTP command ops (update-journal-entry, close-period,
  # execute-event-block, the content-op writes) — requires the member/admin
  # role via `validate_mcp_access(..., "write")` below. Cypher read tools are
  # classified per statement by the StatementKernel, since they can carry a
  # write.
  is_write_query = (not is_cypher_read_tool) and (
    tool_call.name not in READ_ONLY_MCP_TOOLS
  )

  # Validate access using a short-lived session.  The MCP endpoint's
  # tool execution can take minutes; using a scoped session or FastAPI
  # db dependency would hold a pool connection for the entire duration.
  # A plain SessionFactory() session is closed immediately after the
  # DB work, returning the connection to the pool before tool execution.
  repo_access = None
  sess = SessionFactory()
  try:
    if is_cypher_read_tool:
      cypher_query: str = tool_call.arguments.get("query", "")  # type: ignore[assignment]
      # Only authorize an actual statement. An empty/missing query is a
      # validation error the tool surfaces ("Query parameter is required"),
      # not a policy decision — and is_write_operation fail-safes empty input
      # to "write", which would otherwise mis-trigger the write block.
      if cypher_query.strip():
        authz = statement_kernel.authorize(
          engine=StatementEngine.CYPHER,
          graph_id=graph_id,
          statement=cypher_query,
          user=current_user,
          session=sess,
        )
        is_write_query = authz.is_write

    access_type = "write" if is_write_query else "read"
    await validate_mcp_access(graph_id, current_user, sess, access_type)

    # Look up shared-repo subscription while session is still open.
    # Covers subgraphs (e.g. sec_historical) too — subscriptions live on
    # the parent, so resolve to it before the lookup, matching the query
    # path in query/execute.py::_check_shared_repository_limits.
    if MultiTenantUtils.is_shared_repository_or_subgraph(graph_id):
      from robosystems.config.shared_repositories import (
        resolve_shared_repository_parent,
      )
      from robosystems.models.core.user.user_repository import UserRepository

      repo_access = UserRepository.get_by_user_and_repository(
        current_user.id, resolve_shared_repository_parent(graph_id), sess
      )
  finally:
    sess.close()

  # Apply dual-layer rate limiting for shared repositories (incl. subgraphs)
  if (
    MultiTenantUtils.is_shared_repository_or_subgraph(graph_id)
    and env.RATE_LIMIT_ENABLED
  ):
    from robosystems.config.valkey_registry import (
      ValkeyDatabase,
      create_async_redis_client,
    )
    from robosystems.middleware.rate_limits import DualLayerRateLimiter

    if not repo_access:
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail=f"Access to {graph_id.upper()} repository requires a subscription. Visit {env.ROBOSYSTEMS_URL}/repositories/browse",
      )

    redis_client = create_async_redis_client(ValkeyDatabase.RATE_LIMITS)

    try:
      limiter = DualLayerRateLimiter(redis_client)

      # Check shared-repository per-plan volume limits (burst protection is
      # already enforced upstream by subscription_aware_rate_limit_dependency).
      limit_check = await limiter.check_limits(
        user_id=str(current_user.id),
        graph_id=graph_id,
        operation="mcp",
        endpoint=f"mcp/call-tool/{tool_call.name}",
        repository_plan=repo_access.repository_plan,
      )

      if not limit_check["allowed"]:
        reason = limit_check.get("reason", "unknown")
        message = limit_check.get("message", "Rate limit exceeded")

        if reason == "no_access":
          raise HTTPException(
            status_code=http_status.HTTP_403_FORBIDDEN,
            detail=f"{message}. Subscribe at {env.ROBOSYSTEMS_URL}/repositories/browse",
          )
        elif reason == "endpoint_not_allowed":
          raise HTTPException(
            status_code=http_status.HTTP_403_FORBIDDEN,
            detail=message,
          )
        elif reason == "repository_limit":
          detail = limit_check.get("detail", {})
          raise HTTPException(
            status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"{message}. Limit: {detail.get('limit', 0)} per {detail.get('window', 'period')}. "
            f"Upgrade for higher limits at {env.ROBOSYSTEMS_URL}/repositories/browse",
            headers={
              "Retry-After": str(detail.get("retry_after", 60)),
              "X-RateLimit-Repository": graph_id,
              "X-RateLimit-Plan": str(repo_access.repository_plan),
            },
          )
        else:
          raise HTTPException(
            status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
            detail=message,
          )
    finally:
      await redis_client.close()

  return access_type


async def execute_tool_to_json(
  handler: MCPHandler,
  graph_id: str,
  tool_call: MCPToolCall,
  strategy: MCPExecutionStrategy,
  timeout: int,
) -> dict[str, Any]:
  """Execute a tool call to a single JSON result for the given strategy.

  The remote JSON-RPC transport must return one result per `tools/call`
  (MCP has no partial-result mechanism), so streaming strategies are
  aggregated server-side and cacheable strategies go through the Valkey MCP
  cache. Queue strategies fall through to direct execution here; the remote
  transport bridges the queue over its SSE response mode instead.
  """
  if strategy == MCPExecutionStrategy.SCHEMA_CACHED:
    cached = await _get_mcp_cache(graph_id, tool_call.name)
    if cached is not None:
      return cached
    result = await execute_tool_directly(handler, tool_call, timeout)
    # Never cache an execution failure — a transient backend error would be
    # served as the schema for the full cache TTL.
    if not is_tool_error_result(result):
      await _set_mcp_cache(graph_id, tool_call.name, result, _MCP_SCHEMA_CACHE_TTL)
    return result

  if strategy == MCPExecutionStrategy.INFO_CACHED:
    cached = await _get_mcp_cache(graph_id, tool_call.name)
    if cached is not None:
      return cached
    result = await execute_tool_directly(handler, tool_call, timeout)
    if not is_tool_error_result(result):
      await _set_mcp_cache(graph_id, tool_call.name, result, _MCP_INFO_CACHE_TTL)
    return result

  if strategy in (
    MCPExecutionStrategy.STREAM_AGGREGATED,
    MCPExecutionStrategy.SSE_PROGRESS,
    MCPExecutionStrategy.SSE_STREAMING,
    MCPExecutionStrategy.NDJSON_STREAMING,
  ):
    events = [
      event
      async for event in stream_mcp_tool_execution(
        handler, tool_call.name, tool_call.arguments, strategy.value
      )
    ]
    return aggregate_streamed_results(events)

  return await execute_tool_directly(handler, tool_call, timeout)


async def stream_sse_response(request: Request, events_generator):
  """Generate SSE response with disconnect detection."""
  async for event in events_generator:
    if await request.is_disconnected():
      break

    if "event" in event:
      yield {"event": event["event"], "data": json.dumps(event.get("data", {}))}
    else:
      yield {"data": json.dumps(event)}


async def stream_ndjson_response(events_generator):
  """Generate NDJSON response."""
  async for event in events_generator:
    yield json.dumps(event) + "\n"


@router.post(
  "/call-tool",
  response_model=None,
  summary="Execute MCP Tool",
  description="Strategy auto-selected by tool type and load: JSON for fast tools, SSE for long queries, NDJSON for large results. Database operations (Cypher, schema, info) consume no credits — only AI LLM calls cost credits.",
  operation_id="callMcpTool",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {"description": "Tool queued for execution"},
    408: {"description": "Execution timeout"},
    503: {"description": "Service unavailable"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/mcp/call-tool", business_event_type="mcp_tool_called"
)
async def call_mcp_tool(
  full_request: Request,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  tool_call: MCPToolCall = Body(
    ...,
    openapi_examples={
      "cypher_query": {
        "summary": "Execute Cypher Query",
        "description": "Query companies by ticker symbol with parameters",
        "value": {
          "name": "read-graph-cypher",
          "arguments": {
            "query": "MATCH (c:Company {ticker: $ticker})-[:FILED]->(f:Filing) RETURN c.name, f.form_type, f.filing_date LIMIT 10",
            "parameters": {"ticker": "AAPL"},
          },
        },
      },
      "get_schema": {
        "summary": "Get Graph Schema",
        "description": "Retrieve the complete schema of the graph database",
        "value": {
          "name": "get-graph-schema",
          "arguments": {},
        },
      },
      "get_info": {
        "summary": "Get Graph Info",
        "description": "Get statistics and metadata about the graph",
        "value": {
          "name": "get-graph-info",
          "arguments": {},
        },
      },
    },
  ),
  format: str | None = QueryParam(
    default=None,
    description="Response format override (json, sse, ndjson)",
  ),
  test_mode: bool = QueryParam(
    default=False,
    description="Enable test mode for debugging",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> MCPToolResult | JSONResponse | StreamingResponse | EventSourceResponse:
  start_time = datetime.now(UTC)

  from robosystems.config import env

  # Shared-repository telemetry context (no-ops on user graphs)
  key_prefix = api_key_prefix_from_request(full_request)
  exec_id: str | None = None
  # Branches whose true outcome differs from the strategy-based inference in
  # the finally block set this explicitly (server-side aggregation completes
  # inline; queue submission only hands off).
  cost_outcome: str | None = None

  record_operation_metric(
    operation_type=OperationType.TOOL_EXECUTION,
    status=OperationStatus.SUCCESS,  # Will be updated on completion
    duration_ms=0.0,  # Will be updated on completion
    endpoint=f"/v1/graphs/{graph_id}/mcp/call-tool",
    graph_id=graph_id,
    operation_name=tool_call.name,
    user_id=str(current_user.id),
    metadata={
      "operation": "start",
      "arguments_count": len(tool_call.arguments) if tool_call.arguments else 0,
    },
  )

  circuit_breaker.check_circuit(graph_id, tool_call.name)

  try:
    # The full authorization gauntlet — fail-closed write classification,
    # StatementKernel policy for cypher tools, role validation, shared-repo
    # subscription lookup, and per-plan volume limits — is shared with the
    # remote JSON-RPC transport (remote.py) so the two surfaces cannot drift.
    access_type = await authorize_mcp_tool_call(graph_id, tool_call, current_user)
    is_write_query = access_type == "write"

    operation_type = _get_mcp_operation_type(graph_id)
    repository = await get_graph_repository(graph_id, operation_type)

    # The structured formatter keeps only its known top-level fields plus
    # `metadata`; the tool name and access class live there so the read-tool
    # trail (who ran which tool on which graph, under which credential) is
    # searchable rather than buried in the message text.
    api_logger.info(
      f"MCP tool execution started: {tool_call.name}",
      extra={
        "component": "mcp_api",
        "action": "tool_started",
        "user_id": str(current_user.id),
        "database": graph_id,
        "request_id": getattr(full_request.state, "request_id", None),
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/mcp/call-tool",
          "tool_name": tool_call.name,
          "access_type": access_type,
          "is_write": is_write_query,
          "api_key_prefix": getattr(full_request.state, "api_key_prefix", None),
          "arguments_size": len(str(tool_call.arguments)) if tool_call.arguments else 0,
        },
      },
    )

    headers = dict(full_request.headers)
    client_info = MCPClientDetector.detect_client_type(headers)

    if test_mode:
      client_info["is_mcp_client"] = False
      client_info["prefers_streaming"] = True

    query_queue = get_query_queue()
    tool_stats = query_queue.get_stats()

    if tool_call.name in [
      "read-graph-cypher",
      "read-neo4j-cypher",
      "read-ladybug-cypher",
    ]:
      query_queue = get_query_queue()
      query_stats = query_queue.get_stats()
      system_state = {
        "queue_size": tool_stats["queue_size"] + query_stats["queue_size"],
        "running_queries": tool_stats["running_queries"]
        + query_stats["running_queries"],
        "cache_available": True,
      }
    else:
      system_state = {
        "queue_size": tool_stats["queue_size"],
        "running_queries": tool_stats["running_queries"],
        "cache_available": True,
      }

    user_tier = None
    if hasattr(current_user, "subscription") and current_user.subscription:
      user_tier = (
        current_user.subscription.billing_plan.name
        if current_user.subscription.billing_plan
        else None
      )

    strategy = MCPStrategySelector.select_strategy(
      tool_name=tool_call.name,
      arguments=tool_call.arguments,
      client_info=client_info,
      system_state=system_state,
      graph_id=graph_id,
      user_tier=user_tier,
    )

    if format:
      if format == "sse":
        strategy = MCPExecutionStrategy.SSE_PROGRESS
      elif format == "ndjson":
        strategy = MCPExecutionStrategy.STREAM_AGGREGATED
      elif format == "json":
        strategy = MCPExecutionStrategy.JSON_COMPLETE

    query_arg = tool_call.arguments.get("query") if tool_call.arguments else None
    exec_id = log_shared_query_start(
      graph_id,
      current_user.id,
      api_key_prefix=key_prefix,
      source="mcp_rest",
      tool_name=tool_call.name,
      query_length=len(query_arg) if isinstance(query_arg, str) else None,
      strategy=strategy.value,
    )

    # Log execution attempt (debug level for production to reduce verbosity)
    if env.ENVIRONMENT in ["staging", "prod"]:
      logger.debug(
        f"Executing MCP tool '{tool_call.name}' with strategy {strategy.value}",
        extra={
          "graph_id": graph_id,
          "user_id": str(current_user.id),
          "tool_name": tool_call.name,
          "strategy": strategy.value,
          "is_mcp_client": client_info.get("is_mcp_client", False),
        },
      )
    else:
      logger.info(
        f"Executing MCP tool '{tool_call.name}' with strategy {strategy.value}",
        extra={
          "graph_id": graph_id,
          "user_id": str(current_user.id),
          "tool_name": tool_call.name,
          "strategy": strategy.value,
          "is_mcp_client": client_info.get("is_mcp_client", False),
        },
      )

    handler = MCPHandler(repository, graph_id, current_user)

    try:
      timeout = MCPStrategySelector.get_timeout_for_strategy(strategy)

      if strategy == MCPExecutionStrategy.JSON_IMMEDIATE:
        # Cheap tool: run it inline and answer in the same response.
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()

        return MCPToolResult(result=result)

      elif strategy == MCPExecutionStrategy.JSON_COMPLETE:
        # Costlier tool the client did not ask to stream: run to completion
        # and answer with the whole result.
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()

        return MCPToolResult(result=result)

      elif strategy == MCPExecutionStrategy.STREAM_AGGREGATED:
        if client_info.get("is_mcp_client"):
          events = []
          async for event in stream_mcp_tool_execution(
            handler, tool_call.name, tool_call.arguments, strategy.value
          ):
            events.append(event)

          await handler.close()

          aggregated = aggregate_streamed_results(events)
          cost_outcome = "completed"
          if is_disrupted_aggregation(aggregated):
            cost_outcome = "stream_disrupted"
            record_shared_query_outcome(
              graph_id,
              current_user.id,
              signal="stream_disrupted",
              disruption=True,
              api_key_prefix=key_prefix,
              endpoint="/v1/graphs/{graph_id}/mcp/call-tool",
              source="mcp_rest",
              tool_name=tool_call.name,
            )
          return MCPToolResult(result=aggregated)
        else:

          async def generate():
            try:
              async for line in stream_ndjson_response(
                stream_mcp_tool_execution(
                  handler, tool_call.name, tool_call.arguments, strategy.value
                )
              ):
                yield line
            finally:
              # Close handler after all streaming is complete
              if handler and not handler._closed:
                await handler.close()

          return StreamingResponse(generate(), media_type="application/x-ndjson")

      elif strategy == MCPExecutionStrategy.SSE_PROGRESS:

        async def generate_sse():
          try:
            async for event in stream_sse_response(
              full_request,
              stream_mcp_tool_execution(
                handler, tool_call.name, tool_call.arguments, strategy.value
              ),
            ):
              yield event
          finally:
            # Close handler after all streaming is complete
            if handler and not handler._closed:
              await handler.close()

        return EventSourceResponse(generate_sse())

      elif strategy in [
        MCPExecutionStrategy.QUEUE_WITH_MONITORING,
        MCPExecutionStrategy.QUEUE_SIMPLE,
      ]:
        if tool_call.name in [
          "read-graph-cypher",
          "read-neo4j-cypher",
          "read-ladybug-cypher",
        ]:
          query: str = tool_call.arguments.get("query", "")  # type: ignore[assignment]
          parameters = tool_call.arguments.get("parameters", {})

          # The queue executes the raw statement without constructing the
          # tool, so the read-only guard `CypherTool` applies on the direct
          # path has to run here — a read tool must not become a write path
          # for a write-role caller by picking a queue strategy.
          from robosystems.middleware.mcp.tools.cypher_tool import (
            assert_read_only_cypher,
          )

          try:
            assert_read_only_cypher(query)
          except ValueError as exc:
            await handler.close()
            raise HTTPException(status_code=403, detail=str(exc)) from exc

          queue_manager = get_query_queue()
          queue_id = await queue_manager.submit_query(
            cypher=query,
            parameters=parameters,
            graph_id=graph_id,
            user_id=current_user.id,
            credits_required=10.0,  # Calculated by decorator
            priority=_get_user_priority(current_user),
          )

          sse_response = await create_operation_response(
            operation_type="mcp_tool_call",
            user_id=current_user.id,
            graph_id=graph_id,
          )

          operation_id = sse_response["operation_id"]

          if strategy == MCPExecutionStrategy.QUEUE_WITH_MONITORING:

            async def monitor_queue():
              while True:
                status = await queue_manager.get_query_status(queue_id)
                if not status:
                  break

                yield {"event": "queue_status", "data": status}

                if status["status"] in ["completed", "failed", "cancelled"]:
                  if status["status"] == "completed":
                    result = await queue_manager.get_query_result(queue_id)
                    yield {"event": "result", "data": result}
                  break

                await asyncio.sleep(1)

            await handler.close()
            cost_outcome = "queued"
            return EventSourceResponse(monitor_queue())
          else:
            await handler.close()
            cost_outcome = "queued"
            return JSONResponse(
              status_code=http_status.HTTP_202_ACCEPTED,
              content={
                "queued": True,
                "operation_id": operation_id,
                "monitor_url": sse_response["_links"]["stream"],
                "message": "Tool execution queued. Monitor via SSE endpoint.",
              },
            )
        else:
          # Execute other tools directly (non-Cypher tools don't need queuing)
          try:
            result = await handler.call_tool(tool_call.name, tool_call.arguments)
            await handler.close()
            return JSONResponse(
              status_code=http_status.HTTP_200_OK, content={"result": result}
            )
          except Exception as e:
            await handler.close()
            logger.error(f"Direct tool execution failed: {e}", exc_info=True)
            raise HTTPException(
              status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
              detail="Tool execution failed.",
            )

      elif strategy == MCPExecutionStrategy.SCHEMA_CACHED:
        cached = await _get_mcp_cache(graph_id, tool_call.name)
        if cached is not None:
          await handler.close()
          return MCPToolResult(result=cached)
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()
        if not is_tool_error_result(result):
          await _set_mcp_cache(graph_id, tool_call.name, result, _MCP_SCHEMA_CACHE_TTL)
        return MCPToolResult(result=result)

      elif strategy == MCPExecutionStrategy.INFO_CACHED:
        cached = await _get_mcp_cache(graph_id, tool_call.name)
        if cached is not None:
          await handler.close()
          return MCPToolResult(result=cached)
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()
        if not is_tool_error_result(result):
          await _set_mcp_cache(graph_id, tool_call.name, result, _MCP_INFO_CACHE_TTL)
        return MCPToolResult(result=result)

      else:
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()
        return MCPToolResult(result=result)

    finally:
      # Ensure handler is closed (but not for streaming responses - they handle it themselves)
      is_streaming = strategy in [
        MCPExecutionStrategy.SSE_PROGRESS,
        MCPExecutionStrategy.SSE_STREAMING,
        MCPExecutionStrategy.NDJSON_STREAMING,
        MCPExecutionStrategy.STREAM_AGGREGATED,
      ]
      if handler and not handler._closed and not is_streaming:
        await handler.close()

      execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

      if sys.exc_info()[0] is not None:
        resolved_outcome = "error"
      elif cost_outcome is not None:
        resolved_outcome = cost_outcome
      elif is_streaming:
        resolved_outcome = "dispatched_stream"
      else:
        resolved_outcome = "completed"
      log_shared_query_end(
        exec_id,
        graph_id,
        current_user.id,
        outcome=resolved_outcome,
        duration_ms=execution_time,
        api_key_prefix=key_prefix,
        source="mcp_rest",
        tool_name=tool_call.name,
      )
      record_operation_metric(
        operation_type=OperationType.TOOL_EXECUTION,
        status=OperationStatus.SUCCESS,
        duration_ms=execution_time,
        endpoint=f"/v1/graphs/{graph_id}/mcp/call-tool",
        graph_id=graph_id,
        operation_name=tool_call.name,
        user_id=str(current_user.id),
        metadata={
          "strategy": strategy.value,
          "is_mcp_client": client_info.get("is_mcp_client", False),
        },
      )

      circuit_breaker.record_success(graph_id, tool_call.name)

  except HTTPException as exc:
    execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      status_code=exc.status_code,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/mcp/call-tool",
      source="mcp_rest",
      tool_name=tool_call.name,
    )
    record_operation_metric(
      operation_type=OperationType.TOOL_EXECUTION,
      status=OperationStatus.FAILURE,
      duration_ms=execution_time,
      endpoint=f"/v1/graphs/{graph_id}/mcp/call-tool",
      graph_id=graph_id,
      operation_name=tool_call.name,
      user_id=str(current_user.id),
      metadata={
        "error_type": "HTTPException",
      },
    )
    raise
  except Exception as e:
    circuit_breaker.record_failure(graph_id, tool_call.name)
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      error=e,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/mcp/call-tool",
      source="mcp_rest",
      tool_name=tool_call.name,
    )

    logger.error(
      f"MCP tool execution failed: {e}",
      extra={
        "graph_id": graph_id,
        "user_id": str(current_user.id),
        "tool_name": tool_call.name,
        "error": str(e),
      },
    )

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/mcp/call-tool",
      method="POST",
      event_type="mcp_tool_execution_failed",
      event_data={
        "graph_id": graph_id,
        "tool_name": tool_call.name,
        "error_type": type(e).__name__,
      },
      user_id=current_user.id,
    )

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Tool execution failed",
    )
