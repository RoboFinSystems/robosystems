"""
Streaming response handlers for query execution.

This module provides various streaming implementations including
NDJSON and Server-Sent Events (SSE) for efficient large result handling.
It leverages the shared streaming utilities for consistency across endpoints.
"""

import asyncio
import json
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

from robosystems.models.api.graph import CypherQueryRequest, DEFAULT_QUERY_TIMEOUT
from robosystems.middleware.graph.query_queue import get_query_queue, QueryStatus
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.logger import logger, api_logger
from robosystems.middleware.sse.streaming import emit_event_to_operation
from robosystems.middleware.sse.event_storage import EventType
from robosystems.models.iam import User

# Initialize circuit breaker
circuit_breaker = CircuitBreakerManager()


async def execute_query_with_timeout(
  repository: Any, query: str, parameters: Optional[Dict[str, Any]], timeout: int
) -> List[Dict[str, Any]]:
  """
  Execute a query with timeout handling.

  This is a legacy function maintained for backward compatibility.
  Consider using QueryHandler.execute_query() instead.

  Args:
      repository: The graph repository instance
      query: The Cypher query to execute
      parameters: Optional query parameters
      timeout: Timeout in seconds

  Returns:
      Query results as a list of dictionaries

  Raises:
      TimeoutError: If query exceeds timeout
  """

  async def execute():
    # Check if repository has async execute_query method
    if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
      repository.execute_query
    ):
      return await repository.execute_query(query, parameters)
    else:
      # Fallback for sync repositories
      loop = asyncio.get_event_loop()
      return await loop.run_in_executor(
        None, repository.execute_query, query, parameters
      )

  try:
    return await asyncio.wait_for(execute(), timeout=timeout)
  except asyncio.TimeoutError:
    raise TimeoutError(f"Query exceeded timeout of {timeout} seconds")


async def stream_ndjson_response(
  repository: Any,
  request: CypherQueryRequest,
  graph_id: str,
  current_user: User,
  chunk_size: int = 1000,
  start_time: Optional[datetime] = None,
) -> StreamingResponse:
  """
  Stream query results as NDJSON (newline-delimited JSON).

  Each line is a complete JSON object representing a chunk of results.
  This format is efficient for large results and works well with many clients.

  Args:
      repository: Graph repository instance
      request: Query request
      graph_id: Graph identifier
      current_user: Current authenticated user
      chunk_size: Number of rows per chunk
      start_time: Request start time for metrics

  Returns:
      StreamingResponse with NDJSON content
  """
  if not start_time:
    start_time = datetime.now(timezone.utc)

  async def generate_ndjson():
    """Generate NDJSON chunks from query results."""
    try:
      total_rows = 0
      chunk_index = 0
      columns = None

      # Check if repository supports native streaming
      if hasattr(repository, "execute_query_streaming"):
        # Use native streaming support
        async for chunk in repository.execute_query_streaming(
          request.query, request.parameters, chunk_size=chunk_size
        ):
          # Extract columns from first chunk
          if columns is None and chunk:
            columns = (
              list(chunk[0].keys()) if isinstance(chunk, list) else chunk.get("columns")
            )

          # Handle different chunk formats
          if isinstance(chunk, dict):
            # Repository returns structured chunks
            rows = chunk.get("rows", chunk.get("data", []))
            chunk_columns = chunk.get("columns")
          else:
            # Repository returns raw rows
            rows = chunk
            chunk_columns = None

          total_rows += len(rows)

          # Create NDJSON chunk
          ndjson_chunk = {
            "chunk_index": chunk_index,
            "rows": rows,
            "row_count": len(rows),
            "total_rows_sent": total_rows,
          }

          # Include columns in first chunk
          if chunk_index == 0 and (columns or chunk_columns):
            ndjson_chunk["columns"] = columns or chunk_columns

          yield json.dumps(ndjson_chunk) + "\n"
          chunk_index += 1

          # Log progress periodically
          if chunk_index % 10 == 0:
            logger.debug(f"Streamed {total_rows} rows in {chunk_index} chunks")

      else:
        # Fallback: Execute complete query and chunk it
        result = await execute_query_with_timeout(
          repository,
          request.query,
          request.parameters,
          request.timeout or DEFAULT_QUERY_TIMEOUT,
        )

        columns = list(result[0].keys()) if result else []
        total_rows = len(result)

        # Stream in chunks
        for i in range(0, total_rows, chunk_size):
          chunk = result[i : i + chunk_size]

          ndjson_chunk = {
            "chunk_index": i // chunk_size,
            "rows": chunk,
            "row_count": len(chunk),
            "total_rows_sent": min(i + chunk_size, total_rows),
          }

          # Include columns in first chunk
          if i == 0:
            ndjson_chunk["columns"] = columns

          yield json.dumps(ndjson_chunk) + "\n"

      # Send completion metadata
      execution_time = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

      final_chunk = {
        "complete": True,
        "total_rows": total_rows,
        "execution_time_ms": execution_time,
        "graph_id": graph_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }
      yield json.dumps(final_chunk) + "\n"

      # Record success metrics
      circuit_breaker.record_success(graph_id, "cypher_query")

      api_logger.info(
        "NDJSON streaming completed successfully",
        extra={
          "component": "query_streaming",
          "action": "ndjson_stream_completed",
          "user_id": str(current_user.id),
          "database": graph_id,
          "total_rows": total_rows,
          "chunks_sent": chunk_index + 1,
          "duration_ms": execution_time,
        },
      )

    except Exception as e:
      # Stream error as NDJSON
      error_chunk = {
        "error": str(e),
        "error_type": type(e).__name__,
        "graph_id": graph_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }
      yield json.dumps(error_chunk) + "\n"

      # Record failure metrics
      circuit_breaker.record_failure(graph_id, "cypher_query")
      logger.error(f"NDJSON streaming failed: {e}")

  return StreamingResponse(
    generate_ndjson(),
    media_type="application/x-ndjson",
    headers={
      "X-Streaming": "true",
      "X-Stream-Format": "ndjson",
      "X-Graph-ID": graph_id,
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",  # Disable nginx buffering
    },
  )


async def stream_sse_response(
  repository: Any,
  request: CypherQueryRequest,
  graph_id: str,
  current_user: User,
  chunk_size: int = 100,
  include_progress: bool = True,
  start_time: Optional[datetime] = None,
) -> EventSourceResponse:
  """
  Stream query results via Server-Sent Events (SSE).

  Provides rich feedback including progress updates, chunks, and metadata.
  Best for real-time monitoring and progressive rendering.

  Args:
      repository: Graph repository instance
      request: Query request
      graph_id: Graph identifier
      current_user: Current authenticated user
      chunk_size: Number of rows per chunk
      include_progress: Whether to send progress events
      start_time: Request start time for metrics

  Returns:
      EventSourceResponse with SSE stream
  """
  if not start_time:
    start_time = datetime.now(timezone.utc)

  async def sse_generator():
    """Generate SSE events from query results."""
    try:
      total_rows = 0
      chunk_count = 0
      columns = None

      # Send start event
      yield {
        "event": "started",
        "data": json.dumps(
          {
            "graph_id": graph_id,
            "query_hash": hashlib.md5(request.query.encode()).hexdigest()[:8],
            "timestamp": start_time.isoformat(),
            "message": "Query execution started",
          }
        ),
      }

      # Check for native streaming support
      if hasattr(repository, "execute_query_streaming"):
        async for chunk in repository.execute_query_streaming(
          request.query, request.parameters, chunk_size=chunk_size
        ):
          # Extract columns from first chunk
          if columns is None and chunk:
            if isinstance(chunk, dict):
              columns = chunk.get("columns")
              rows = chunk.get("rows", chunk.get("data", []))
            else:
              columns = list(chunk[0].keys()) if chunk else []
              rows = chunk

            # Send schema event
            if columns:
              yield {"event": "schema", "data": json.dumps({"columns": columns})}
          else:
            rows = chunk.get("rows", chunk) if isinstance(chunk, dict) else chunk

          chunk_count += 1
          total_rows += len(rows)

          # Send chunk event
          yield {
            "event": "chunk",
            "data": json.dumps(
              {
                "chunk_number": chunk_count,
                "rows": rows,
                "rows_in_chunk": len(rows),
                "total_rows": total_rows,
              }
            ),
          }

          # Send progress updates
          if include_progress and chunk_count % 10 == 0:
            yield {
              "event": "progress",
              "data": json.dumps(
                {
                  "chunks_processed": chunk_count,
                  "rows_processed": total_rows,
                  "message": f"Processed {total_rows} rows...",
                }
              ),
            }

      else:
        # Execute complete query
        yield {
          "event": "executing",
          "data": json.dumps({"message": "Executing query..."}),
        }

        result = await execute_query_with_timeout(
          repository,
          request.query,
          request.parameters,
          request.timeout or DEFAULT_QUERY_TIMEOUT,
        )

        columns = list(result[0].keys()) if result else []
        total_rows = len(result)

        # Send schema
        yield {"event": "schema", "data": json.dumps({"columns": columns})}

        # Stream in chunks
        for i in range(0, total_rows, chunk_size):
          chunk = result[i : i + chunk_size]
          chunk_count += 1

          yield {
            "event": "chunk",
            "data": json.dumps(
              {
                "chunk_number": chunk_count,
                "rows": chunk,
                "rows_in_chunk": len(chunk),
                "total_rows": total_rows,
              }
            ),
          }

          # Progress updates for large results
          if include_progress and i > 0 and i % (chunk_size * 10) == 0:
            progress_percent = (i / total_rows) * 100
            yield {
              "event": "progress",
              "data": json.dumps(
                {
                  "progress_percent": round(progress_percent, 1),
                  "rows_sent": i,
                  "total_rows": total_rows,
                }
              ),
            }

      # Send completion event
      execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

      yield {
        "event": "complete",
        "data": json.dumps(
          {
            "total_rows": total_rows,
            "total_chunks": chunk_count,
            "execution_time_seconds": execution_time,
            "graph_id": graph_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
          }
        ),
      }

      # Record success metrics
      circuit_breaker.record_success(graph_id, "cypher_query")

      api_logger.info(
        "SSE streaming completed successfully",
        extra={
          "component": "query_streaming",
          "action": "sse_stream_completed",
          "user_id": str(current_user.id),
          "database": graph_id,
          "total_rows": total_rows,
          "chunks_sent": chunk_count,
          "duration_seconds": execution_time,
        },
      )

    except asyncio.TimeoutError:
      yield {
        "event": "timeout",
        "data": json.dumps(
          {
            "error": "Query execution timeout",
            "timeout_seconds": request.timeout or DEFAULT_QUERY_TIMEOUT,
          }
        ),
      }
      circuit_breaker.record_failure(graph_id, "cypher_query")

    except Exception as e:
      yield {
        "event": "error",
        "data": json.dumps(
          {
            "error": str(e),
            "error_type": type(e).__name__,
          }
        ),
      }
      circuit_breaker.record_failure(graph_id, "cypher_query")
      logger.error(f"SSE streaming failed: {e}")

  return EventSourceResponse(
    sse_generator(),
    headers={
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",  # Disable nginx buffering
      "X-Stream-Format": "sse",
      "X-Graph-ID": graph_id,
    },
    ping=15,  # Send ping every 15 seconds to keep connection alive
  )


async def stream_sse_with_queue(
  request: CypherQueryRequest,
  graph_id: str,
  repository: Any,
  current_user: User,
  priority: int = 5,
  chunk_size: int = 100,
  operation_id: Optional[str] = None,
) -> EventSourceResponse:
  """
  Handle queued queries with SSE, providing queue updates then streaming results.

  This combines queue management with result streaming in a single SSE connection,
  providing the best user experience for queries that need to be queued.

  Args:
      request: Query request
      graph_id: Graph identifier
      repository: Graph repository instance
      current_user: Current authenticated user
      priority: Query priority
      chunk_size: Number of rows per chunk
      operation_id: Optional unified SSE operation ID for monitoring

  Returns:
      EventSourceResponse with queue updates and result stream
  """

  async def sse_queue_stream_generator():
    """Generate SSE events for queued query with streaming."""
    queue_manager = get_query_queue()
    query_id = None

    try:
      # Calculate query cost
      credits_required = 0.0  # Queries are included

      # Submit to queue
      query_id = await queue_manager.submit_query(
        cypher=request.query,
        parameters=request.parameters,
        graph_id=graph_id,
        user_id=current_user.id,
        credits_required=credits_required,
        priority=priority,
      )

      # Send queued event
      initial_status = await queue_manager.get_query_status(query_id)
      queue_event_data = {
        "query_id": query_id,
        "position": initial_status.get("queue_position", 0),
        "estimated_wait_seconds": initial_status.get("estimated_wait", 10),
        "message": "Query has been queued",
      }

      # Emit to unified SSE if operation_id provided
      if operation_id:
        await emit_event_to_operation(
          operation_id,
          EventType.OPERATION_PROGRESS,
          {**queue_event_data, "progress_percent": 0, "status": "queued"},
        )

      yield {
        "event": "queued",
        "data": json.dumps(queue_event_data),
      }

      # Monitor queue position
      last_position = initial_status.get("queue_position", 0)

      while True:
        await asyncio.sleep(1)
        status = await queue_manager.get_query_status(query_id)

        if status and status["status"] == QueryStatus.PENDING:
          # Update position if changed
          current_position = status.get("queue_position", 0)
          if current_position != last_position:
            yield {
              "event": "queue_update",
              "data": json.dumps(
                {
                  "position": current_position,
                  "estimated_wait_seconds": current_position * 2,
                  "message": f"Queue position: {current_position}",
                }
              ),
            }
            last_position = current_position

        elif status and status["status"] == QueryStatus.RUNNING:
          # Query started executing
          start_event_data = {
            "query_id": query_id,
            "message": "Query execution started",
          }

          # Emit to unified SSE if operation_id provided
          if operation_id:
            await emit_event_to_operation(
              operation_id,
              EventType.OPERATION_STARTED,
              {**start_event_data, "progress_percent": 10, "status": "running"},
            )

          yield {
            "event": "started",
            "data": json.dumps(start_event_data),
          }

          # Now stream the results
          total_rows = 0
          chunk_count = 0

          # Check for native streaming support
          if hasattr(repository, "execute_query_streaming"):
            async for chunk in repository.execute_query_streaming(
              request.query, request.parameters, chunk_size=chunk_size
            ):
              chunk_count += 1
              rows = chunk.get("rows", chunk) if isinstance(chunk, dict) else chunk
              total_rows += len(rows)

              # Send chunk
              yield {
                "event": "chunk",
                "data": json.dumps(
                  {
                    "chunk_number": chunk_count,
                    "rows": rows,
                    "rows_in_chunk": len(rows),
                    "total_rows": total_rows,
                  }
                ),
              }

              # Progress updates
              if chunk_count % 10 == 0:
                yield {
                  "event": "progress",
                  "data": json.dumps(
                    {
                      "chunks": chunk_count,
                      "rows": total_rows,
                    }
                  ),
                }
          else:
            # Execute and stream complete result
            result = await execute_query_with_timeout(
              repository,
              request.query,
              request.parameters,
              request.timeout or DEFAULT_QUERY_TIMEOUT,
            )

            # Stream in chunks
            for i in range(0, len(result), chunk_size):
              chunk = result[i : i + chunk_size]
              chunk_count += 1
              total_rows += len(chunk)

              yield {
                "event": "chunk",
                "data": json.dumps(
                  {
                    "chunk_number": chunk_count,
                    "rows": chunk,
                    "total_rows": total_rows,
                  }
                ),
              }

          # Mark complete in queue
          await queue_manager.mark_completed(query_id, {"rows": total_rows})

          # Send completion
          complete_event_data = {
            "query_id": query_id,
            "total_rows": total_rows,
            "message": "Query completed successfully",
          }

          # Emit to unified SSE if operation_id provided
          if operation_id:
            await emit_event_to_operation(
              operation_id,
              EventType.OPERATION_COMPLETED,
              {**complete_event_data, "progress_percent": 100, "status": "completed"},
            )

          yield {
            "event": "complete",
            "data": json.dumps(complete_event_data),
          }
          break

        elif status and status["status"] in [QueryStatus.COMPLETED, QueryStatus.FAILED]:
          # Handle completion or failure
          event_type = (
            "complete" if status["status"] == QueryStatus.COMPLETED else "error"
          )
          yield {"event": event_type, "data": json.dumps(status)}
          break

    except Exception as e:
      logger.error(f"Queue SSE error: {e}")
      yield {
        "event": "error",
        "data": json.dumps(
          {
            "error": str(e),
            "query_id": query_id,
          }
        ),
      }

  return EventSourceResponse(
    sse_queue_stream_generator(),
    headers={
      "Cache-Control": "no-cache",
      "X-Accel-Buffering": "no",
      "X-Stream-Mode": "queue-and-stream",
      "X-Graph-ID": graph_id,
    },
    ping=10,  # Send ping every 10 seconds during queue wait
  )
