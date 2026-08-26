"""MCP tool authorization and execution, shared by the transports.

`authorize_mcp_tool_call` is the gauntlet every `tools/call` runs — fail-closed
write classification, StatementKernel policy for the Cypher tools, per-graph
role checks, shared-repository subscription lookup, and volume rate limits —
and `execute_tool_to_json` resolves a call to one JSON result for the strategy
the selector picked. The Streamable HTTP transport (`remote.py`) is the only
caller; the REST tool endpoints that used to live here were removed once the
transport carried all of their traffic.
"""

import asyncio
import json
from typing import Any

from fastapi import (
  HTTPException,
)
from fastapi import status as http_status

from robosystems.config.query_queue import QueryQueueConfig
from robosystems.logger import logger
from robosystems.middleware.graph.statement_kernel import (
  StatementEngine,
  statement_kernel,
)
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
)
from robosystems.models.api.graphs.mcp import MCPToolCall
from robosystems.models.core import User

from .handlers import MCPHandler, is_tool_error_result, validate_mcp_access
from .strategies import (
  MCPExecutionStrategy,
)
from .streaming import (
  aggregate_streamed_results,
  stream_mcp_tool_execution,
)

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

  Used by the remote JSON-RPC transport (POST /mcp, both the per-graph and
  the OAuth-only route) and the in-process operator path so the surfaces
  cannot drift: fail-closed write classification, StatementKernel statement policy for cypher read tools,
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
