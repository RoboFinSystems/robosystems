"""
Main MCP tool execution endpoint with intelligent routing and transparent streaming.

This module provides the primary MCP tool execution endpoint that automatically
selects the optimal execution strategy based on tool characteristics, client
capabilities, and system load. Designed for seamless AI agent integration.
"""

import asyncio
import json
from typing import Union, Dict, Any, Optional
from datetime import datetime, timezone

from fastapi import (
  APIRouter,
  Body,
  Depends,
  HTTPException,
  Path,
  Query as QueryParam,
  Request,
)
from fastapi import status as http_status
from fastapi.responses import JSONResponse, StreamingResponse
from sse_starlette.sse import EventSourceResponse
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.graph import get_graph_repository
from robosystems.models.iam import User
from robosystems.models.api.graphs.mcp import MCPToolCall, MCPToolResult
from robosystems.models.api.common import ErrorResponse
from robosystems.security.cypher_analyzer import (
  is_write_operation,
  is_bulk_operation,
  is_admin_operation,
)
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.middleware.graph.query_queue import get_query_queue
from robosystems.config.query_queue import QueryQueueConfig
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.robustness import (
  CircuitBreakerManager,
  OperationType,
  OperationStatus,
  record_operation_metric,
)
from robosystems.logger import logger, api_logger
from robosystems.middleware.sse.operation_manager import create_operation_response

# Import MCP components
from .handlers import MCPHandler, validate_mcp_access
from .strategies import (
  MCPExecutionStrategy,
  MCPClientDetector,
  MCPStrategySelector,
)
from .streaming import (
  stream_mcp_tool_execution,
  aggregate_streamed_results,
)

router = APIRouter()

# Circuit breaker instance
circuit_breaker = CircuitBreakerManager()


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
  if MultiTenantUtils.is_shared_repository(graph_id):
    return "read"
  else:
    return "write"


async def execute_tool_directly(
  handler: MCPHandler,
  tool_call: MCPToolCall,
  timeout: int = 60,
) -> Dict[str, Any]:
  """Execute MCP tool directly without queuing."""
  try:
    result = await asyncio.wait_for(
      handler.call_tool(tool_call.name, tool_call.arguments), timeout=timeout
    )
    return result
  except asyncio.TimeoutError:
    raise HTTPException(
      status_code=http_status.HTTP_408_REQUEST_TIMEOUT,
      detail=f"Tool execution timed out after {timeout} seconds",
    )


async def stream_sse_response(request: Request, events_generator):
  """Generate SSE response with disconnect detection."""
  async for event in events_generator:
    if await request.is_disconnected():
      break

    # Format as SSE
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
  response_model=None,  # Dynamic response type
  summary="Execute MCP Tool",
  description="""Execute an MCP tool with intelligent response optimization.

This endpoint automatically selects the best execution strategy based on:
- Tool type and estimated complexity
- Client capabilities (AI agent detection)
- System load and queue status
- Graph type (shared repository vs user graph)

**Response Formats:**
- **JSON**: Direct response for small/fast operations
- **SSE**: Server-Sent Events for progress monitoring
- **NDJSON**: Newline-delimited JSON for streaming
- **Queued**: Asynchronous execution with status monitoring

**SSE Streaming Support:**
- Maximum 5 concurrent SSE connections per user
- Rate limited to 10 new connections per minute
- Automatic circuit breaker for Redis failures
- Graceful degradation to direct response if SSE unavailable
- Progress events for long-running operations

**AI Agent Optimization:**
The Node.js MCP client transparently handles all response formats,
presenting a unified interface to AI agents. Streaming responses are
automatically aggregated for seamless consumption.

**Error Handling:**
- `429 Too Many Requests`: Connection limit or rate limit exceeded
- `503 Service Unavailable`: SSE system temporarily disabled
- `408 Request Timeout`: Tool execution exceeded timeout
- Clients should implement exponential backoff on errors

**Credit Model:**
MCP tool execution is included - no credit consumption required. Database
operations (queries, schema inspection, analytics) are completely free.
Only AI operations that invoke Claude or other LLM APIs consume credits,
which happens at the AI agent layer, not the MCP tool layer.""",
  operation_id="callMcpTool",
  responses={
    200: {"description": "Tool executed successfully"},
    202: {"description": "Tool queued for execution"},
    400: {"description": "Invalid tool call", "model": ErrorResponse},
    402: {"description": "Insufficient credits", "model": ErrorResponse},
    403: {"description": "Access denied", "model": ErrorResponse},
    408: {"description": "Execution timeout", "model": ErrorResponse},
    429: {"description": "Rate limit exceeded", "model": ErrorResponse},
    500: {"description": "Internal error", "model": ErrorResponse},
    503: {"description": "Service unavailable", "model": ErrorResponse},
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
    pattern="^[a-zA-Z][a-zA-Z0-9_]{2,62}$",
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
      "discover_facts": {
        "summary": "Discover Facts",
        "description": "Discover common facts and patterns in the graph",
        "value": {
          "name": "discover-facts",
          "arguments": {
            "entity_type": "Company",
            "limit": 20,
          },
        },
      },
    },
  ),
  format: Optional[str] = QueryParam(
    default=None,
    description="Response format override (json, sse, ndjson)",
  ),
  test_mode: bool = QueryParam(
    default=False,
    description="Enable test mode for debugging",
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Union[MCPToolResult, JSONResponse, StreamingResponse, EventSourceResponse]:
  """
  Execute an MCP tool with intelligent response optimization.

  This endpoint provides transparent execution of MCP tools with automatic
  strategy selection. For AI agents using the Node.js MCP client, all
  response formats are handled transparently and presented uniformly.
  """
  start_time = datetime.now(timezone.utc)

  # Import config
  from robosystems.config import env

  # Initialize monitoring and logging
  # (Operation logging is handled by circuit breaker)

  # Record metrics at start
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

  # Check circuit breaker
  circuit_breaker.check_circuit(graph_id, tool_call.name)

  try:
    # Validate write operations for Cypher tools
    is_write_query = False
    if tool_call.name in ["read-graph-cypher", "read-neo4j-cypher", "read-kuzu-cypher"]:
      query: str = tool_call.arguments.get("query", "")  # type: ignore[assignment]
      is_write_query = is_write_operation(query)

      # Check for bulk operations (COPY, LOAD, IMPORT)
      if is_bulk_operation(query):
        logger.warning(
          f"User {current_user.id} attempted bulk operation through MCP: {query[:100]}"
        )
        raise HTTPException(
          status_code=http_status.HTTP_400_BAD_REQUEST,
          detail="Bulk operations (COPY, LOAD, IMPORT) are not allowed through the MCP endpoint. "
          "Please use file upload (/v1/graphs/{graph_id}/tables/files/upload) to ingest data via DuckDB staging.",
        )

      # Check for admin operations
      if is_admin_operation(query):
        logger.warning(
          f"User {current_user.id} attempted admin operation through MCP: {query[:100]}"
        )
        # For now, block all admin operations - we can add admin flag to User model later
        raise HTTPException(
          status_code=http_status.HTTP_403_FORBIDDEN,
          detail="Administrative operations require admin privileges.",
        )

      # Block writes on shared repositories
      if is_write_query and MultiTenantUtils.is_shared_repository(graph_id):
        raise HTTPException(
          status_code=http_status.HTTP_403_FORBIDDEN,
          detail=f"Write operations not allowed on shared repository '{graph_id}'",
        )

    # Validate access
    access_type = "write" if is_write_query else "read"
    await validate_mcp_access(graph_id, current_user, db, access_type)

    # Apply dual-layer rate limiting for shared repositories
    if MultiTenantUtils.is_shared_repository(graph_id):
      from robosystems.middleware.rate_limits import DualLayerRateLimiter
      from robosystems.models.iam.user_repository import UserRepository
      from robosystems.config.valkey_registry import ValkeyDatabase
      from robosystems.config.valkey_registry import create_async_redis_client

      # Get user's repository access plan
      repo_access = UserRepository.get_by_user_and_repository(
        current_user.id, graph_id, db
      )

      if not repo_access:
        raise HTTPException(
          status_code=http_status.HTTP_403_FORBIDDEN,
          detail=f"Access to {graph_id.upper()} repository requires a subscription. Visit https://roboledger.ai/pricing",
        )

      # Get Redis client for rate limiting with proper ElastiCache support
      redis_client = create_async_redis_client(ValkeyDatabase.RATE_LIMITING)

      try:
        limiter = DualLayerRateLimiter(redis_client)

        # Get user's subscription tier for burst protection
        user_tier = "standard"  # Default for authenticated users
        if hasattr(current_user, "subscription") and current_user.subscription:
          if current_user.subscription.billing_plan:
            user_tier = current_user.subscription.billing_plan.name

        # Check both rate limit layers
        limit_check = await limiter.check_limits(
          user_id=str(current_user.id),
          graph_id=graph_id,
          operation="mcp",
          endpoint=f"mcp/call-tool/{tool_call.name}",
          user_tier=user_tier,
          repository_plan=repo_access.repository_plan,
        )

        if not limit_check["allowed"]:
          reason = limit_check.get("reason", "unknown")
          message = limit_check.get("message", "Rate limit exceeded")

          if reason == "no_access":
            raise HTTPException(
              status_code=http_status.HTTP_403_FORBIDDEN,
              detail=f"{message}. Subscribe at https://roboledger.ai/pricing",
            )
          elif reason == "endpoint_not_allowed":
            raise HTTPException(
              status_code=http_status.HTTP_403_FORBIDDEN,
              detail=message,
            )
          elif reason == "burst_limit":
            detail = limit_check.get("detail", {})
            raise HTTPException(
              status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
              detail=f"Rate limit exceeded: {detail.get('current', 0)}/{detail.get('limit', 0)} "
              f"requests per {detail.get('window', 0)} seconds",
              headers={
                "Retry-After": str(detail.get("window", 60)),
                "X-RateLimit-Limit": str(detail.get("limit", 0)),
                "X-RateLimit-Remaining": str(detail.get("remaining", 0)),
              },
            )
          elif reason == "repository_limit":
            detail = limit_check.get("detail", {})
            raise HTTPException(
              status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
              detail=f"{message}. Limit: {detail.get('limit', 0)} per {detail.get('window', 'period')}. "
              f"Upgrade for higher limits at https://roboledger.ai/pricing",
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

    # Get repository
    operation_type = _get_mcp_operation_type(graph_id)
    repository = await get_graph_repository(graph_id, operation_type)

    # Log structured attempt with business context
    api_logger.info(
      f"MCP tool execution started: {tool_call.name}",
      extra={
        "component": "mcp_api",
        "action": "tool_started",
        "user_id": str(current_user.id),
        "database": graph_id,
        "tool_name": tool_call.name,
        "access_type": access_type,
        "is_write": is_write_query,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/mcp/call-tool",
          "arguments_size": len(str(tool_call.arguments)) if tool_call.arguments else 0,
        },
      },
    )

    # Detect client capabilities
    headers = dict(full_request.headers)
    client_info = MCPClientDetector.detect_client_type(headers)

    # Override for test mode
    if test_mode:
      client_info["is_mcp_client"] = False
      client_info["prefers_streaming"] = True

    # Get system state
    query_queue = get_query_queue()
    tool_stats = query_queue.get_stats()

    # For cypher queries, also check query queue
    if tool_call.name in ["read-graph-cypher", "read-neo4j-cypher", "read-kuzu-cypher"]:
      query_queue = get_query_queue()
      query_stats = query_queue.get_stats()
      system_state = {
        "queue_size": tool_stats["queue_size"] + query_stats["queue_size"],
        "running_queries": tool_stats["running_queries"]
        + query_stats["running_queries"],
        "cache_available": True,  # TODO: Check actual cache status
      }
    else:
      system_state = {
        "queue_size": tool_stats["queue_size"],
        "running_queries": tool_stats["running_queries"],
        "cache_available": True,
      }

    # Select execution strategy
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

    # Allow format override
    if format:
      if format == "sse":
        strategy = MCPExecutionStrategy.SSE_PROGRESS
      elif format == "ndjson":
        strategy = MCPExecutionStrategy.STREAM_AGGREGATED
      elif format == "json":
        strategy = MCPExecutionStrategy.JSON_COMPLETE

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

    # Execute based on strategy
    handler = MCPHandler(repository, graph_id, current_user)

    try:
      # Get timeout for strategy
      timeout = MCPStrategySelector.get_timeout_for_strategy(strategy)

      if strategy == MCPExecutionStrategy.JSON_IMMEDIATE:
        # Direct execution with immediate response
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()

        return MCPToolResult(result=result)

      elif strategy == MCPExecutionStrategy.JSON_COMPLETE:
        # Execute and wait for complete result
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()

        return MCPToolResult(result=result)

      elif strategy == MCPExecutionStrategy.STREAM_AGGREGATED:
        # Stream with transparent aggregation for MCP clients
        if client_info.get("is_mcp_client"):
          # For MCP clients, aggregate streaming results
          events = []
          async for event in stream_mcp_tool_execution(
            handler, tool_call.name, tool_call.arguments, strategy.value
          ):
            events.append(event)

          await handler.close()

          # Aggregate and return as JSON
          aggregated = aggregate_streamed_results(events)
          return MCPToolResult(result=aggregated)
        else:
          # For other clients, return NDJSON stream
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
        # SSE streaming with progress updates
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
        # Queue for execution
        if tool_call.name in [
          "read-graph-cypher",
          "read-neo4j-cypher",
          "read-kuzu-cypher",
        ]:
          # Use query queue for cypher queries
          query: str = tool_call.arguments.get("query", "")  # type: ignore[assignment]
          parameters = tool_call.arguments.get("parameters", {})

          queue_manager = get_query_queue()
          queue_id = await queue_manager.submit_query(
            cypher=query,
            parameters=parameters,
            graph_id=graph_id,
            user_id=current_user.id,
            credits_required=10.0,  # Calculated by decorator
            priority=_get_user_priority(current_user),
          )

          # Create unified SSE operation for monitoring
          sse_response = await create_operation_response(
            operation_type="mcp_tool_call",
            user_id=current_user.id,
            graph_id=graph_id,
          )

          if strategy == MCPExecutionStrategy.QUEUE_WITH_MONITORING:
            # Return SSE stream for queue monitoring
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
            return EventSourceResponse(monitor_queue())
          else:
            # Return queue info for polling
            await handler.close()
            return JSONResponse(
              status_code=http_status.HTTP_202_ACCEPTED,
              content={
                "queued": True,
                "operation_id": sse_response.operation_id,  # Unified SSE operation ID
                "monitor_url": f"/v1/operations/{sse_response.operation_id}/stream",  # Unified monitoring only
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
            logger.error(f"Direct tool execution failed: {e}")
            raise HTTPException(
              status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
              detail=f"Tool execution failed: {str(e)}",
            )

      elif strategy == MCPExecutionStrategy.SCHEMA_CACHED:
        # Use cached schema if available
        # TODO: Implement schema caching
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()
        return MCPToolResult(result=result)

      elif strategy == MCPExecutionStrategy.INFO_CACHED:
        # Use cached info if available
        # TODO: Implement info caching
        result = await execute_tool_directly(handler, tool_call, timeout)
        await handler.close()
        return MCPToolResult(result=result)

      else:
        # Fallback to direct execution
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

      # Record success metrics
      execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
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

      # Record success in circuit breaker
      circuit_breaker.record_success(graph_id, tool_call.name)

  except HTTPException:
    # Record failure metrics
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
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
    # Record failure
    circuit_breaker.record_failure(graph_id, tool_call.name)

    logger.error(
      f"MCP tool execution failed: {e}",
      extra={
        "graph_id": graph_id,
        "user_id": str(current_user.id),
        "tool_name": tool_call.name,
        "error": str(e),
      },
    )

    # Record error metrics
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
