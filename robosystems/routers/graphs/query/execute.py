"""
Main query execution endpoint with intelligent routing and streaming.

This module provides the primary query execution endpoint that automatically
selects the optimal execution strategy based on query characteristics,
client capabilities, and system load.
"""

import asyncio
import hashlib
from typing import Optional, Union
from datetime import datetime, timezone

from fastapi import (
  APIRouter,
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
from robosystems.models.iam import User
from robosystems.models.iam.graph import GraphTier
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.api.graphs.query import (
  CypherQueryRequest,
  CypherQueryResponse,
  DEFAULT_QUERY_TIMEOUT,
)
from robosystems.security.cypher_analyzer import (
  is_write_operation,
  is_bulk_operation,
  is_admin_operation,
  is_schema_ddl,
)
from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.middleware.graph.query_queue import get_query_queue
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.logger import logger, api_logger, log_metric

from .strategies import (
  ExecutionStrategy,
  ResponseMode,
  QueryAnalyzer,
  ClientDetector,
  StrategySelector,
  QueryTimeoutCoordinator,
)
from .streaming import (
  execute_query_with_timeout,
  stream_ndjson_response,
  stream_sse_response,
  stream_sse_with_queue,
)
from .handlers import (
  get_query_operation_type,
  get_user_priority as get_user_priority_from_handler,
)
from robosystems.middleware.sse.operation_manager import create_operation_response
from robosystems.middleware.graph.types import GRAPH_ID_PATTERN

# Initialize circuit breaker
circuit_breaker = CircuitBreakerManager()


# Use helper functions from handlers module
_get_user_priority = get_user_priority_from_handler
_get_query_operation_type = get_query_operation_type


# Create router for execute endpoint
router = APIRouter()


@router.post(
  "/query",  # Full path without trailing slash
  response_model=None,  # Dynamic response type
  summary="Execute Cypher Query (Read-Only)",
  description="""Execute a read-only Cypher query with intelligent response optimization.

**IMPORTANT: This endpoint is READ-ONLY.** Write operations (CREATE, MERGE, SET, DELETE) are not allowed.
To load data into your graph, use the staging pipeline:
1. Create file upload: `POST /v1/graphs/{graph_id}/tables/{table_name}/files`
2. Ingest to graph: `POST /v1/graphs/{graph_id}/tables/ingest`

**Security Best Practice - Use Parameterized Queries:**
ALWAYS use query parameters instead of string interpolation to prevent injection attacks:
- ✅ SAFE: `MATCH (n:Entity {type: $entity_type}) RETURN n` with `parameters: {"entity_type": "Company"}`
- ❌ UNSAFE: `MATCH (n:Entity {type: "Company"}) RETURN n` with user input concatenated into query string

Query parameters provide automatic escaping and type safety. All examples in this API use parameterized queries.

This endpoint automatically selects the best execution strategy based on:
- Query characteristics (size, complexity)
- Client capabilities (SSE, NDJSON, JSON)
- System load (queue status, concurrent queries)
- User preferences (mode parameter, headers)

**Response Modes:**
- `auto` (default): Intelligent automatic selection
- `sync`: Force synchronous JSON response (best for testing)
- `async`: Force queued response with SSE monitoring endpoints (no polling needed)
- `stream`: Force streaming response (SSE or NDJSON)

**Client Detection:**
- Automatically detects testing tools (Postman, Swagger UI)
- Adjusts behavior for better interactive experience
- Respects Accept and Prefer headers for capabilities

**Streaming Support (SSE):**
- Real-time events with progress updates
- Maximum 5 concurrent SSE connections per user
- Rate limited to 10 new connections per minute
- Automatic circuit breaker for Redis failures
- Graceful degradation if event system unavailable
- 30-second keepalive to prevent timeouts

**Streaming Support (NDJSON):**
- Efficient line-delimited JSON for large results
- Automatic chunking (configurable 10-10000 rows)
- No connection limits (stateless streaming)

**Queue Management:**
- Automatic queuing under high load
- Real-time monitoring via SSE events (no polling needed)
- Priority based on subscription tier
- Queue position and progress updates pushed via SSE
- Connect to returned `/v1/operations/{id}/stream` endpoint for updates

**Error Handling:**
- `429 Too Many Requests`: Rate limit or connection limit exceeded
- `503 Service Unavailable`: Circuit breaker open or SSE disabled
- Clients should implement exponential backoff

**Note:**
Query operations are included - no credit consumption required.
Queue position is based on subscription tier for priority.""",
  operation_id="executeCypherQuery",
  responses={
    200: {
      "description": "Query executed successfully",
      "content": {
        "application/json": {
          "schema": {
            "type": "object",
            "properties": {
              "success": {"type": "boolean"},
              "data": {"type": "array", "items": {"type": "object"}},
              "columns": {"type": "array", "items": {"type": "string"}},
              "row_count": {"type": "integer"},
              "execution_time_ms": {"type": "number"},
              "graph_id": {"type": "string"},
              "timestamp": {"type": "string"},
            },
          }
        },
        "application/x-ndjson": {
          "schema": {
            "type": "string",
            "description": "Newline-delimited JSON chunks with streaming results",
          }
        },
        "text/event-stream": {
          "schema": {
            "type": "string",
            "description": "Server-Sent Events stream with real-time progress updates",
          }
        },
      },
    },
    202: {"description": "Query queued for execution"},
    400: {"description": "Invalid query or parameters"},
    403: {"description": "Access denied"},
    408: {"description": "Query timeout"},
    429: {"description": "Rate limit exceeded"},
    500: {"description": "Internal error"},
    503: {"description": "Service unavailable"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/query", business_event_type="query_executed"
)
async def execute_cypher_query(
  request: CypherQueryRequest,
  full_request: Request,
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_ID_PATTERN
  ),
  mode: Optional[ResponseMode] = QueryParam(
    default=None, description="Response mode override"
  ),
  chunk_size: Optional[int] = QueryParam(
    default=None, ge=10, le=10000, description="Rows per chunk for streaming"
  ),
  test_mode: bool = QueryParam(
    default=False, description="Enable test mode for better debugging"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Union[CypherQueryResponse, JSONResponse, StreamingResponse, EventSourceResponse]:
  """
  Execute a Cypher query with intelligent response optimization.

  This endpoint automatically detects the best way to respond based on
  the query, client capabilities, and system state.
  """
  start_time = datetime.now(timezone.utc)

  # Check circuit breaker
  circuit_breaker.check_circuit(graph_id, "cypher_query")

  # Get the graph tier for chunk size configuration
  from robosystems.models.iam.graph import Graph
  from robosystems.config import env

  graph = session.query(Graph).filter(Graph.graph_id == graph_id).first()

  # Determine chunk size based on tier (if not explicitly provided)
  if chunk_size is None:
    if graph and graph.graph_tier:
      tier_chunk_sizes = {
        "standard": env.KUZU_STANDARD_CHUNK_SIZE,
        "enterprise": env.KUZU_ENTERPRISE_CHUNK_SIZE,
        "premium": env.KUZU_PREMIUM_CHUNK_SIZE,
      }
      chunk_size = tier_chunk_sizes.get(graph.graph_tier.lower(), 1000)
      logger.debug(f"Using tier-based chunk size for {graph.graph_tier}: {chunk_size}")
    else:
      chunk_size = 1000

  # Initialize client_info for exception handling
  client_info = {"is_interactive": False}

  try:
    # Analyze query
    is_write = is_write_operation(request.query)
    access_type = "write" if is_write else "read"

    # Block ALL write operations - query endpoint is read-only
    if is_write:
      logger.warning(
        f"User {current_user.id} attempted write operation through query endpoint: {request.query[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Write operations (CREATE, MERGE, SET, DELETE) are not allowed. "
        "The query endpoint is read-only. Use the staging pipeline to load data:\n"
        "1. Create file upload: POST /v1/graphs/{graph_id}/tables/{table_name}/files\n"
        "2. Ingest to graph: POST /v1/graphs/{graph_id}/tables/ingest\n"
        "This ensures data integrity and enables pipeline benefits (audit, rollback, validation).",
      )

    # Check for bulk operations (COPY, LOAD, IMPORT) - should never reach here due to write check above
    if is_bulk_operation(request.query):
      logger.warning(
        f"User {current_user.id} attempted bulk operation through query endpoint: {request.query[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        detail="Bulk operations (COPY, LOAD, IMPORT) are not allowed through the query endpoint. "
        "Please use the staging pipeline for data ingestion.",
      )

    # Check for admin operations (EXPORT, INSTALL, ATTACH, etc.)
    if is_admin_operation(request.query):
      logger.warning(
        f"User {current_user.id} attempted admin operation through query endpoint: {request.query[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Administrative operations (EXPORT, IMPORT DATABASE, INSTALL, ATTACH, etc.) require admin privileges.",
      )

    # Check for schema DDL operations (CREATE/DROP/ALTER TABLE, etc.)
    if is_schema_ddl(request.query):
      logger.warning(
        f"User {current_user.id} attempted schema DDL through query endpoint: {request.query[:100]}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail="Schema DDL operations (CREATE/DROP/ALTER TABLE, etc.) are not allowed. "
        "Graph schemas are immutable after creation to ensure consistency with staging tables.",
      )

    # Block writes on shared repositories
    if is_write and MultiTenantUtils.is_shared_repository(graph_id):
      logger.warning(
        f"User {current_user.id} attempted write on shared repository {graph_id}"
      )
      raise HTTPException(
        status_code=http_status.HTTP_403_FORBIDDEN,
        detail=f"Write operations not allowed on shared repository '{graph_id}'",
      )

    # Apply dual-layer rate limiting for shared repositories
    await _check_shared_repository_limits(
      graph_id=graph_id, user=current_user, session=session, endpoint="query"
    )

    # Get repository with auth
    # Convert graph tier string to GraphTier enum
    tier = GraphTier.KUZU_STANDARD
    if graph and graph.graph_tier:
      tier_map = {
        "kuzu-standard": GraphTier.KUZU_STANDARD,
        "kuzu-large": GraphTier.KUZU_LARGE,
        "kuzu-xlarge": GraphTier.KUZU_XLARGE,
        "kuzu-shared": GraphTier.KUZU_SHARED,
      }
      tier = tier_map.get(graph.graph_tier.lower(), GraphTier.KUZU_STANDARD)

    try:
      repository = await get_universal_repository(graph_id, access_type, tier)
    except HTTPException:
      # Re-raise HTTP exceptions as-is (already properly formatted)
      raise
    except Exception as e:
      # Handle repository access errors with better messaging
      error_message = str(e)
      if (
        "No access to repository" in error_message
        or "not found for user" in error_message
      ):
        logger.warning(
          f"User {current_user.id} lacks access to repository {graph_id}: {error_message}"
        )
        raise HTTPException(
          status_code=http_status.HTTP_403_FORBIDDEN,
          detail=f"You don't have access to the '{graph_id}' repository. Please contact support to request access.",
        )
      elif "Repository not found" in error_message:
        raise HTTPException(
          status_code=http_status.HTTP_404_NOT_FOUND,
          detail=f"Repository '{graph_id}' not found",
        )
      else:
        logger.error(f"Failed to get repository {graph_id}: {error_message}")
        raise HTTPException(
          status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Failed to access repository: {error_message}",
        )

    # Log structured query attempt with business context
    api_logger.info(
      f"Cypher query execution started: {request.query[:50]}...",
      extra={
        "component": "query_api",
        "action": "query_started",
        "user_id": str(current_user.id),
        "database": graph_id,
        "query_length": len(request.query),
        "access_type": access_type,
        "is_write": is_write,
        "metadata": {
          "endpoint": "/v1/graphs/{graph_id}/query",
          "query_hash": hashlib.md5(request.query.encode()).hexdigest()[:8],
        },
      },
    )

    # Analyze query characteristics
    query_analysis = QueryAnalyzer.analyze_query(request.query)

    # Detect client capabilities
    headers = dict(full_request.headers)
    client_info = ClientDetector.detect_client_type(headers)

    # Override for test mode
    if test_mode:
      client_info["is_testing_tool"] = True
      client_info["is_interactive"] = True

    # Get system state
    queue_manager = get_query_queue()
    system_state = queue_manager.get_stats()
    system_state["max_concurrent"] = 5  # Configurable threshold

    # Convert string mode to enum if provided
    mode_enum = None
    if mode:
      try:
        mode_enum = ResponseMode(mode)
      except ValueError:
        logger.warning(f"Invalid mode parameter: {mode}")

    # Select execution strategy
    strategy, metadata = StrategySelector.select_strategy(
      query_analysis=query_analysis,
      client_info=client_info,
      system_state=system_state,
      mode_override=mode_enum,
      is_write_operation=is_write,
    )

    # Calculate timeouts
    timeouts = QueryTimeoutCoordinator.calculate_timeouts(
      requested_timeout=request.timeout or DEFAULT_QUERY_TIMEOUT,
      strategy=strategy,
      is_testing=client_info["is_interactive"],
    )

    # Log strategy selection
    api_logger.info(
      f"Query execution strategy: {strategy.value}",
      extra={
        "component": "query_api",
        "action": "strategy_selected",
        "user_id": str(current_user.id),
        "database": graph_id,
        "strategy": strategy.value,
        "is_write": is_write,
        "is_testing": client_info["is_interactive"],
        "estimated_rows": query_analysis["estimated_rows"],
        "queue_size": system_state["queue_size"],
        "metadata": metadata,
      },
    )

    # Execute based on strategy
    if strategy == ExecutionStrategy.SSE_QUEUE_STREAM:
      # Queue with SSE then stream results
      # Create unified SSE operation for monitoring
      sse_response = await create_operation_response(
        operation_type="cypher_query_streaming",
        user_id=current_user.id,
        graph_id=graph_id,
      )

      # Stream with unified monitoring support
      return await stream_sse_with_queue(
        request=request,
        graph_id=graph_id,
        repository=repository,
        current_user=current_user,
        priority=_get_user_priority(current_user),
        chunk_size=chunk_size,
        operation_id=sse_response.operation_id,  # Pass operation_id for unified events
      )

    elif strategy == ExecutionStrategy.SSE_STREAMING:
      # Direct SSE streaming
      return await stream_sse_response(
        repository=repository,
        request=request,
        graph_id=graph_id,
        current_user=current_user,
        chunk_size=chunk_size,
        include_progress=True,
        start_time=start_time,
      )

    elif strategy == ExecutionStrategy.NDJSON_STREAMING:
      # NDJSON streaming
      return await stream_ndjson_response(
        repository=repository,
        request=request,
        graph_id=graph_id,
        current_user=current_user,
        chunk_size=chunk_size,
        start_time=start_time,
      )

    elif strategy in [
      ExecutionStrategy.JSON_IMMEDIATE,
      ExecutionStrategy.JSON_COMPLETE,
      ExecutionStrategy.SYNC_TESTING,
    ]:
      # Execute and return JSON
      timeout = timeouts["execution"]
      try:
        if strategy == ExecutionStrategy.SYNC_TESTING:
          # Testing mode - provide helpful feedback
          if query_analysis["estimated_rows"] > QueryAnalyzer.LARGE_RESULT:
            logger.warning(
              f"Testing mode with large query ({query_analysis['estimated_rows']} rows)"
            )

        # Execute query
        result = await execute_query_with_timeout(
          repository, request.query, request.parameters, timeout
        )

        # Calculate execution time
        execution_time = (
          datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000

        # Extract columns
        columns = list(result[0].keys()) if result else []

        # Check if result is too large for testing tools
        if (
          client_info["is_interactive"]
          and len(result) > 10000
          and not query_analysis["has_limit"]
        ):
          logger.warning(f"Large result ({len(result)} rows) for testing tool")

          # Truncate with warning
          return JSONResponse(
            content={
              "success": True,
              "data": result[:1000],
              "columns": columns,
              "row_count": len(result),
              "truncated": True,
              "truncated_at": 1000,
              "execution_time_ms": execution_time,
              "graph_id": graph_id,
              "warning": (
                f"Result truncated from {len(result)} to 1000 rows for testing. "
                f"Use 'LIMIT' in your query or mode=stream for full results."
              ),
              "suggestion": {
                "add_limit": f"{request.query} LIMIT 1000",
                "use_streaming": "Set mode=stream or Accept: text/event-stream",
              },
            }
          )

        # Record success
        circuit_breaker.record_success(graph_id, "cypher_query")

        # Record business event for successful execution
        metrics_instance = get_endpoint_metrics()
        metrics_instance.record_business_event(
          endpoint="/v1/graphs/{graph_id}/query",
          method="POST",
          event_type="query_executed_directly",
          event_data={
            "graph_id": graph_id,
            "query_length": len(request.query),
            "execution_time_ms": execution_time,
            "row_count": len(result),
            "is_write_operation": is_write,
            "access_type": access_type,
            "strategy": strategy.value,
            "queue_bypassed": True,
          },
          user_id=current_user.id,
        )

        # Log structured query completion
        api_logger.info(
          "Cypher query execution completed successfully",
          extra={
            "component": "query_api",
            "action": "query_completed",
            "user_id": str(current_user.id),
            "database": graph_id,
            "duration_ms": execution_time,
            "row_count": len(result),
            "access_type": access_type,
            "strategy": strategy.value,
            "success": True,
          },
        )

        # Log performance metric
        log_metric(
          "cypher_query_success",
          1,
          "count",
          "query_api",
          {
            "access_type": access_type,
            "database": graph_id,
            "execution_time_ms": execution_time,
            "strategy": strategy.value,
          },
        )

        # Return complete result
        return CypherQueryResponse(
          success=True,
          data=result,
          columns=columns,
          row_count=len(result),
          execution_time_ms=execution_time,
          graph_id=graph_id,
          timestamp=start_time.isoformat(),
        )

      except asyncio.TimeoutError:
        # Record circuit breaker failure for timeout
        circuit_breaker.record_failure(graph_id, "cypher_query")

        # Record business event for timeout
        metrics_instance = get_endpoint_metrics()
        metrics_instance.record_business_event(
          endpoint="/v1/graphs/{graph_id}/query",
          method="POST",
          event_type="query_timeout",
          event_data={
            "graph_id": graph_id,
            "query_length": len(request.query),
            "timeout_seconds": timeout,
            "access_type": access_type,
            "strategy": strategy.value,
          },
          user_id=current_user.id,
        )

        # Timeout - provide helpful error for testing
        if client_info["is_interactive"]:
          elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

          return JSONResponse(
            status_code=http_status.HTTP_408_REQUEST_TIMEOUT,
            content={
              "error": "Query execution timeout",
              "timeout_seconds": timeout,
              "elapsed_seconds": round(elapsed, 1),
              "suggestion": "Query is taking too long. Try these options:",
              "options": {
                "1_add_limit": "Add a LIMIT clause to reduce result size",
                "2_use_async": "Set mode=async to queue the query",
                "3_use_streaming": "Set mode=stream for progressive results",
                "4_increase_timeout": f"Increase timeout (current: {timeout}s)",
              },
              "examples": {
                "with_limit": f"{request.query[:50]}... LIMIT 100",
                "async_mode": "POST /v1/graphs/{graph_id}/query?mode=async",
                "streaming": "curl -H 'Accept: text/event-stream' ...",
              },
            },
          )
        else:
          # Fall through to queue
          logger.info("Direct execution timed out, falling back to queue")

    # TRADITIONAL_QUEUE or fallback
    try:
      query_id = await queue_manager.submit_query(
        cypher=request.query,
        parameters=request.parameters,
        graph_id=graph_id,
        user_id=current_user.id,
        credits_required=0.0,  # Queries are included
        priority=_get_user_priority(current_user),
      )

      # Get initial status
      status = await queue_manager.get_query_status(query_id)

      # Create unified SSE operation for monitoring
      sse_response = await create_operation_response(
        operation_type="cypher_query",
        user_id=current_user.id,
        graph_id=graph_id,
        operation_id=query_id,  # Use query_id as operation_id
      )
    except Exception as queue_error:
      # Handle queue submission errors
      metrics_instance = get_endpoint_metrics()

      if "queue is full" in str(queue_error):
        metrics_instance.record_business_event(
          endpoint="/v1/graphs/{graph_id}/query",
          method="POST",
          event_type="query_queue_full",
          event_data={
            "graph_id": graph_id,
            "query_length": len(request.query),
            "access_type": access_type,
            "is_write_operation": is_write,
            "error_message": str(queue_error),
          },
          user_id=current_user.id,
        )
        raise HTTPException(
          status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
          detail="Query queue is full. Please retry later.",
          headers={"Retry-After": "60"},
        )
      elif "query limit exceeded" in str(queue_error):
        metrics_instance.record_business_event(
          endpoint="/v1/graphs/{graph_id}/query",
          method="POST",
          event_type="query_limit_exceeded",
          event_data={
            "graph_id": graph_id,
            "query_length": len(request.query),
            "access_type": access_type,
            "is_write_operation": is_write,
            "error_message": str(queue_error),
          },
          user_id=current_user.id,
        )
        raise HTTPException(
          status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
          detail="Too many concurrent queries. Please wait for existing queries to complete.",
        )
      elif "Query rejected" in str(queue_error):
        # Admission control rejection
        metrics_instance.record_business_event(
          endpoint="/v1/graphs/{graph_id}/query",
          method="POST",
          event_type="query_admission_control_rejected",
          event_data={
            "graph_id": graph_id,
            "query_length": len(request.query),
            "access_type": access_type,
            "is_write_operation": is_write,
            "error_message": str(queue_error),
          },
          user_id=current_user.id,
        )
        raise HTTPException(
          status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
          detail=str(queue_error),
          headers={"Retry-After": "30"},
        )
      else:
        metrics_instance.record_business_event(
          endpoint="/v1/graphs/{graph_id}/query",
          method="POST",
          event_type="query_queue_submission_failed",
          event_data={
            "graph_id": graph_id,
            "query_length": len(request.query),
            "access_type": access_type,
            "is_write_operation": is_write,
            "error_type": type(queue_error).__name__,
            "error_message": str(queue_error),
          },
          user_id=current_user.id,
        )
        raise HTTPException(
          status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Failed to queue query: {str(queue_error)}",
        )

    # Continue with the successfully queued query_id and status

    # Record business event for successful queue submission
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/query",
      method="POST",
      event_type="query_queued_successfully",
      event_data={
        "graph_id": graph_id,
        "query_id": query_id,
        "query_length": len(request.query),
        "queue_position": status.get("queue_position", 0),
        "estimated_wait_seconds": status.get("estimated_wait", 10),
        "access_type": access_type,
        "is_write_operation": is_write,
        "user_priority": _get_user_priority(current_user),
        "strategy": strategy.value if strategy else "fallback_queue",
      },
      user_id=current_user.id,
    )

    # Build response with helpful instructions
    base_url = str(full_request.base_url).rstrip("/")

    response_content = {
      "status": "queued",
      "query_id": query_id,
      "operation_id": sse_response["operation_id"],  # Unified SSE operation ID
      "queue_position": status.get("queue_position", 0),
      "estimated_wait_seconds": status.get("estimated_wait", 10),
      "message": "Query has been queued for execution",
    }

    # Add helpful instructions for testing tools
    if client_info["is_interactive"]:
      response_content["instructions"] = {
        "message": "Your query is queued. Monitor via unified SSE endpoint:",
        "monitor_url": f"{base_url}/v1/operations/{sse_response['operation_id']}/stream",
        "curl": (
          f"curl -N '{base_url}/v1/operations/{sse_response['operation_id']}/stream' "
          f"-H 'Authorization: Bearer YOUR_TOKEN'"
        ),
      }

    # Machine-readable links - only unified SSE monitoring
    response_content["_links"] = {
      "self": str(full_request.url),
      "monitor": f"/v1/operations/{sse_response['operation_id']}/stream",  # Unified monitoring only
    }

    return JSONResponse(
      status_code=http_status.HTTP_202_ACCEPTED, content=response_content
    )

  except ValueError as e:
    # Handle credit-related errors (no credit pool found)
    if "No credit pool found" in str(e):
      raise HTTPException(
        status_code=http_status.HTTP_402_PAYMENT_REQUIRED,
        detail=str(e),
      )
    # Re-raise other ValueErrors
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail=str(e),
    )

  except HTTPException:
    circuit_breaker.record_failure(graph_id, "cypher_query")
    raise

  except Exception as e:
    circuit_breaker.record_failure(graph_id, "cypher_query")

    # Record business event for unexpected errors
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/query",
      method="POST",
      event_type="query_unexpected_error",
      event_data={
        "graph_id": graph_id,
        "query_length": len(request.query) if request else 0,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
      user_id=current_user.id if current_user else None,
    )

    logger.error(f"Unexpected error in query execution: {e}")

    # Provide helpful error for testing tools
    if client_info.get("is_interactive"):
      return JSONResponse(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
          "error": "Query execution failed",
          "error_type": type(e).__name__,
          "error_message": str(e),
          "suggestion": "Please check your query syntax and try again",
        },
      )

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while processing your query",
    )


async def _check_shared_repository_limits(
  graph_id: str, user: User, session: Session, endpoint: str = "query"
) -> None:
  """
  Check dual-layer rate limits for shared repositories.

  Direct API queries are included (no credits consumed) but still rate limited.

  Args:
      graph_id: The graph/repository ID
      user: Current user
      session: Database session
      endpoint: The endpoint being called

  Raises:
      HTTPException: If rate limits are exceeded or access is denied
  """
  from robosystems.middleware.rate_limits import DualLayerRateLimiter
  from robosystems.config.billing.repositories import SharedRepository
  from robosystems.models.iam.user_repository import UserRepository
  from robosystems.config.valkey_registry import ValkeyDatabase
  from robosystems.config.valkey_registry import create_async_redis_client

  # Only apply to shared repositories
  if graph_id not in [repo.value for repo in SharedRepository]:
    return

  # Get Redis client for rate limiting with proper ElastiCache support
  redis_client = create_async_redis_client(ValkeyDatabase.RATE_LIMITING)

  try:
    limiter = DualLayerRateLimiter(redis_client)

    # Get user's subscription tier (for burst protection)
    user_tier = getattr(user, "subscription_tier", "standard")

    # Get user's repository access plan
    repo_access = UserRepository.get_by_user_and_repository(user.id, graph_id, session)
    repo_plan = repo_access.repository_plan if repo_access else None

    # Check both rate limit layers
    limit_check = await limiter.check_limits(
      user_id=user.id,
      graph_id=graph_id,
      operation="query",  # Direct queries
      endpoint=endpoint,
      user_tier=user_tier,
      repository_plan=repo_plan,
    )

    if not limit_check["allowed"]:
      reason = limit_check.get("reason", "unknown")
      message = limit_check.get("message", "Rate limit exceeded")

      if reason == "no_access":
        raise HTTPException(
          status_code=http_status.HTTP_403_FORBIDDEN,
          detail=f"{message}. Subscribe at https://roboledger.ai/upgrade",
        )
      elif reason == "endpoint_not_allowed":
        raise HTTPException(status_code=http_status.HTTP_403_FORBIDDEN, detail=message)
      elif reason == "burst_limit":
        detail = limit_check.get("detail", {})
        raise HTTPException(
          status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
          detail=f"Rate limit exceeded: {detail.get('current', 0)}/{detail.get('limit', 0)} "
          f"requests per {detail.get('window', 0)} seconds",
        )
      elif reason == "repository_limit":
        detail = limit_check.get("detail", {})
        raise HTTPException(
          status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
          detail=f"{message}. Limit: {detail.get('limit', 0)} per {detail.get('window', 'period')}. "
          f"Upgrade for higher limits at https://roboledger.ai/upgrade",
        )
      else:
        raise HTTPException(
          status_code=http_status.HTTP_429_TOO_MANY_REQUESTS, detail=message
        )

  finally:
    await redis_client.close()

  # Note: Direct API queries are included - no credit consumption
  # Only MCP queries (AI-mediated) consume credits
