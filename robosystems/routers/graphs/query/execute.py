"""Cypher execution: `POST /v1/graphs/{graph_id}/query/cypher`.

Authorization runs through the shared StatementKernel, which classifies each
statement as a read or a write. The endpoint then selects an execution
strategy and answers as JSON, NDJSON, or SSE, or hands back a queue handle
under load; monitor a queued run over the unified SSE stream at
`/v1/operations/{id}/stream`.
"""

import hashlib
from datetime import UTC, datetime

from fastapi import (
  APIRouter,
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
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

from robosystems.config.graph_tier import GraphTier
from robosystems.database import get_db_session
from robosystems.graph_api.client.exceptions import GraphTransientError
from robosystems.logger import api_logger, log_metric, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.query_queue import get_query_queue
from robosystems.middleware.graph.query_telemetry import (
  api_key_prefix_from_request,
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
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.middleware.sse.operation_manager import create_operation_response
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.graphs.query import (
  DEFAULT_QUERY_TIMEOUT,
  CypherStatementRequest,
  CypherStatementResponse,
)
from robosystems.models.core import User
from robosystems.security.error_handling import safe_error_message

from .handlers import (
  get_query_operation_type,
)
from .handlers import (
  get_user_priority as get_user_priority_from_handler,
)
from .strategies import (
  ClientDetector,
  ExecutionStrategy,
  QueryAnalyzer,
  QueryTimeoutCoordinator,
  ResponseMode,
  StrategySelector,
)
from .streaming import (
  execute_query_with_timeout,
  stream_ndjson_response,
  stream_sse_response,
  stream_sse_with_queue,
)

# Initialize circuit breaker
circuit_breaker = CircuitBreakerManager()


# Use helper functions from handlers module
_get_user_priority = get_user_priority_from_handler
_get_query_operation_type = get_query_operation_type


# Create router for execute endpoint
router = APIRouter()


@router.post(
  "/query/cypher",
  response_model=None,
  summary="Execute Cypher Statement",
  description='Cypher over the graph (LadybugDB). Main graphs are **read-only** — use the staging pipeline to ingest data. Subgraphs support full writes. Always use parameterized queries (`parameters: {"key": "val"}`) to prevent injection. Response modes: `auto` (default), `sync`, `async`, `stream`. Under load, queries are queued and emit an `operation_id` for SSE monitoring at `/v1/operations/{id}/stream`.',
  operation_id="executeCypher",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    202: {
      "description": "Query queued — monitor via SSE at /v1/operations/{operation_id}/stream"
    },
    408: {"description": "Query timeout"},
    503: {"description": "Service unavailable"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/query/cypher", business_event_type="query_executed"
)
async def execute_cypher_query(
  request: CypherStatementRequest,
  full_request: Request,
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  mode: ResponseMode | None = QueryParam(
    default=None, description="Response mode override"
  ),
  chunk_size: int | None = QueryParam(
    default=None, ge=10, le=10000, description="Rows per chunk for streaming"
  ),
  test_mode: bool = QueryParam(
    default=False, description="Enable test mode for better debugging"
  ),
  current_user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> CypherStatementResponse | JSONResponse | StreamingResponse | EventSourceResponse:
  start_time = datetime.now(UTC)

  # Enforce graph lifecycle and subscription status (reads allowed)
  from robosystems.middleware.billing.enforcement import require_graph_access

  graph = require_graph_access(graph_id, session, require_write=False)

  circuit_breaker.check_circuit(graph_id, "cypher_query")

  # Determine chunk size based on tier (if not explicitly provided)
  from robosystems.config.graph_tier import GraphTierConfig

  if chunk_size is None:
    if graph and graph.graph_tier:
      chunk_size = GraphTierConfig.get_chunk_size(graph.graph_tier)
      logger.debug(f"Using tier-based chunk size for {graph.graph_tier}: {chunk_size}")
    else:
      chunk_size = GraphTierConfig.get_chunk_size(None)  # Use default

  # Initialize client_info for exception handling
  client_info = {"is_interactive": False}

  # Shared-repository telemetry context (no-ops on user graphs)
  is_shared = MultiTenantUtils.is_shared_repository_or_subgraph(graph_id)
  key_prefix = api_key_prefix_from_request(full_request)
  exec_id: str | None = None

  try:
    # Authorize the statement — write policy, engine validation, role gate.
    # Shared, transport-independent path (also used by /query/sql, MCP).
    auth = statement_kernel.authorize(
      engine=StatementEngine.CYPHER,
      graph_id=graph_id,
      statement=request.query,
      user=current_user,
      session=session,
    )
    is_write = auth.is_write
    access_type = auth.access_type

    # Apply dual-layer rate limiting for shared repositories
    await _check_shared_repository_limits(
      graph_id=graph_id, user=current_user, session=session, endpoint="query"
    )

    # Get repository with auth
    # Convert graph tier string to GraphTier enum
    tier = GraphTier.LADYBUG_STANDARD
    if graph and graph.graph_tier:
      tier_map = {
        "ladybug-standard": GraphTier.LADYBUG_STANDARD,
        "ladybug-large": GraphTier.LADYBUG_LARGE,
        "ladybug-xlarge": GraphTier.LADYBUG_XLARGE,
        "ladybug-shared": GraphTier.LADYBUG_SHARED,
      }
      tier = tier_map.get(graph.graph_tier.lower(), GraphTier.LADYBUG_STANDARD)

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
        logger.error(
          f"Failed to get repository {graph_id}: {error_message}", exc_info=True
        )
        raise HTTPException(
          status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail="Failed to access repository.",
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

    exec_id = log_shared_query_start(
      graph_id,
      current_user.id,
      api_key_prefix=key_prefix,
      source="query_cypher",
      query_length=len(request.query),
      strategy=strategy.value,
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
      log_shared_query_end(
        exec_id,
        graph_id,
        current_user.id,
        outcome="dispatched_stream",
        api_key_prefix=key_prefix,
        source="query_cypher",
      )
      return await stream_sse_with_queue(
        request=request,
        graph_id=graph_id,
        repository=repository,
        current_user=current_user,
        priority=_get_user_priority(current_user),
        chunk_size=chunk_size,
        operation_id=sse_response["operation_id"],
      )

    elif strategy == ExecutionStrategy.SSE_STREAMING:
      # Direct SSE streaming
      log_shared_query_end(
        exec_id,
        graph_id,
        current_user.id,
        outcome="dispatched_stream",
        api_key_prefix=key_prefix,
        source="query_cypher",
      )
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
      log_shared_query_end(
        exec_id,
        graph_id,
        current_user.id,
        outcome="dispatched_stream",
        api_key_prefix=key_prefix,
        source="query_cypher",
      )
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

        result = await execute_query_with_timeout(
          repository, request.query, request.parameters, timeout
        )

        # Calculate execution time
        execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

        # Extract columns
        columns = list(result[0].keys()) if result else []

        log_shared_query_end(
          exec_id,
          graph_id,
          current_user.id,
          outcome="completed",
          duration_ms=execution_time,
          row_count=len(result),
          api_key_prefix=key_prefix,
          source="query_cypher",
        )

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
        return CypherStatementResponse(
          success=True,
          data=result,
          columns=columns,
          row_count=len(result),
          execution_time_ms=execution_time,
          graph_id=graph_id,
          timestamp=start_time.isoformat(),
        )

      except TimeoutError:
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

        # Shared repositories and their subgraphs have their own scaling
        # (ALB + ASG) — queuing just delays the inevitable, so return a
        # timeout error. User graphs benefit from the queue since they have
        # limited connections (max 3) and no read replicas.
        if client_info["is_interactive"] or is_shared:
          elapsed = (datetime.now(UTC) - start_time).total_seconds()

          record_shared_query_outcome(
            graph_id,
            current_user.id,
            signal="timeout",
            status_code=http_status.HTTP_408_REQUEST_TIMEOUT,
            api_key_prefix=key_prefix,
            endpoint="/v1/graphs/{graph_id}/query/cypher",
            source="query_cypher",
          )
          log_shared_query_end(
            exec_id,
            graph_id,
            current_user.id,
            outcome="timeout",
            duration_ms=elapsed * 1000,
            api_key_prefix=key_prefix,
            source="query_cypher",
          )
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
          # User graph - fall through to queue for fair connection sharing
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
          detail=safe_error_message(queue_error) or "Failed to queue query",
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

    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome="queued",
      api_key_prefix=key_prefix,
      source="query_cypher",
    )
    return JSONResponse(
      status_code=http_status.HTTP_202_ACCEPTED, content=response_content
    )

  except ValueError as e:
    # Handle credit-related errors (no credit pool found)
    if "No credit pool found" in str(e):
      log_shared_query_end(
        exec_id,
        graph_id,
        current_user.id,
        outcome="http_402",
        duration_ms=(datetime.now(UTC) - start_time).total_seconds() * 1000,
        api_key_prefix=key_prefix,
        source="query_cypher",
      )
      raise HTTPException(
        status_code=http_status.HTTP_402_PAYMENT_REQUIRED,
        detail="No credit pool found for this graph. Please check your subscription.",
      )
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      status_code=http_status.HTTP_400_BAD_REQUEST,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/query/cypher",
      source="query_cypher",
    )
    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome="http_400",
      duration_ms=(datetime.now(UTC) - start_time).total_seconds() * 1000,
      api_key_prefix=key_prefix,
      source="query_cypher",
    )
    # Re-raise other ValueErrors — 400s are client errors, keep specific message
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail=str(e),
    )

  except HTTPException as exc:
    # 503 is deliberate backpressure (admission control, rebuilding, full
    # queue), not a fault of the graph. Counting it as a breaker failure turns
    # a resource ceiling into an outage: the rejections trip the breaker and
    # every subsequent query fails even once the pressure clears.
    if (
      exc.status_code >= 500
      and exc.status_code != http_status.HTTP_503_SERVICE_UNAVAILABLE
    ):
      circuit_breaker.record_failure(graph_id, "cypher_query")
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      status_code=exc.status_code,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/query/cypher",
      source="query_cypher",
    )
    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome=f"http_{exc.status_code}",
      duration_ms=(datetime.now(UTC) - start_time).total_seconds() * 1000,
      api_key_prefix=key_prefix,
      source="query_cypher",
    )
    raise

  except GraphTransientError as e:
    # The Graph API rejected the request rather than failing it — admission
    # control (memory/CPU/connection headroom), a 502/504, or its own client
    # breaker already being open. Surface the reason as 503 + Retry-After so
    # the caller can back off, and do not record a breaker failure: the graph
    # is healthy, just at capacity.
    retry_after = 30
    logger.warning(f"Graph API unavailable for {graph_id}: {e}")
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/query/cypher",
      source="query_cypher",
    )
    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome="http_503",
      duration_ms=(datetime.now(UTC) - start_time).total_seconds() * 1000,
      api_key_prefix=key_prefix,
      source="query_cypher",
    )
    get_endpoint_metrics().record_business_event(
      endpoint="/v1/graphs/{graph_id}/query",
      method="POST",
      event_type="query_admission_control_rejected",
      event_data={
        "graph_id": graph_id,
        "query_length": len(request.query) if request else 0,
        "error_message": str(e),
      },
      user_id=current_user.id if current_user else None,
    )
    # Transient-rejection text can name factory internals (pool state, the
    # shared master) — the caller only needs "at capacity, back off".
    raise HTTPException(
      status_code=http_status.HTTP_503_SERVICE_UNAVAILABLE,
      detail=safe_error_message(e)
      or "Graph API is temporarily unavailable; retry shortly",
      headers={"Retry-After": str(retry_after)},
    )

  except Exception as e:
    circuit_breaker.record_failure(graph_id, "cypher_query", error=e)
    record_shared_query_outcome(
      graph_id,
      current_user.id,
      error=e,
      api_key_prefix=key_prefix,
      endpoint="/v1/graphs/{graph_id}/query/cypher",
      source="query_cypher",
    )
    log_shared_query_end(
      exec_id,
      graph_id,
      current_user.id,
      outcome="error",
      duration_ms=(datetime.now(UTC) - start_time).total_seconds() * 1000,
      api_key_prefix=key_prefix,
      source="query_cypher",
    )

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

    # Provide helpful error for testing tools. is_interactive is caller-
    # controlled (test_mode / User-Agent), so it confers verbosity of shape
    # only — the message itself is sanitized like every other sink.
    if client_info.get("is_interactive"):
      return JSONResponse(
        status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
          "error": "Query execution failed",
          "error_message": safe_error_message(e)
          or "An unexpected error occurred while processing your query",
          "suggestion": "Please check your query syntax and try again",
        },
      )

    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="An unexpected error occurred while processing your query",
    )


async def _check_shared_repository_limits(
  graph_id: str,
  user: User,
  session: Session,
  endpoint: str = "query",
  operation: str = "query",
) -> None:
  from robosystems.config import env
  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph,
    resolve_shared_repository_parent,
  )
  from robosystems.models.core.user.user_repository import UserRepository

  # Only apply to shared repositories (including subgraphs like sec_historical)
  if not is_shared_repository_or_subgraph(graph_id):
    return

  # Resolve subgraph to parent for subscription lookup (subscriptions are on the parent)
  parent_repo_id = resolve_shared_repository_parent(graph_id)

  # ALWAYS check access (authorization) - this is not gated by rate limiting
  repo_access = UserRepository.get_by_user_and_repository(
    user.id, parent_repo_id, session
  )
  if not repo_access:
    logger.warning(
      f"User {user.id} attempted to access shared repository {graph_id} without access"
    )
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail=f"You don't have access to the '{graph_id}' repository. "
      f"Subscribe at {env.ROBOSYSTEMS_URL}/repositories/browse",
    )

  # Rate limiting is optional - skip if disabled (dev environments)
  if not env.RATE_LIMIT_ENABLED:
    return

  from robosystems.config.valkey_registry import (
    ValkeyDatabase,
    create_async_redis_client,
  )
  from robosystems.middleware.rate_limits import DualLayerRateLimiter

  # Get Redis client for rate limiting with proper ElastiCache support
  redis_client = create_async_redis_client(ValkeyDatabase.RATE_LIMITS)

  try:
    limiter = DualLayerRateLimiter(redis_client)

    # repo_access already fetched above for access check
    repo_plan = repo_access.repository_plan if repo_access else None

    # Check shared-repository per-plan volume limits (burst protection is
    # already enforced upstream by subscription_aware_rate_limit_dependency).
    limit_check = await limiter.check_limits(
      user_id=user.id,
      graph_id=graph_id,
      operation=operation,
      endpoint=endpoint,
      repository_plan=repo_plan,
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
        raise HTTPException(status_code=http_status.HTTP_403_FORBIDDEN, detail=message)
      elif reason == "repository_limit":
        detail = limit_check.get("detail", {})
        raise HTTPException(
          status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
          detail=f"{message}. Limit: {detail.get('limit', 0)} per {detail.get('window', 'period')}. "
          f"Upgrade for higher limits at {env.ROBOSYSTEMS_URL}/repositories/browse",
        )
      else:
        raise HTTPException(
          status_code=http_status.HTTP_429_TOO_MANY_REQUESTS, detail=message
        )

  finally:
    await redis_client.close()

  # Note: Direct API queries are included - no credit consumption
  # Only MCP queries (AI-mediated) consume credits
