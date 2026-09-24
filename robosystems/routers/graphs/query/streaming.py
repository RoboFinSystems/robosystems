"""NDJSON and SSE streaming handlers for query execution."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

from robosystems.logger import api_logger, logger
from robosystems.middleware.graph.query_queue import QueryStatus, get_query_queue
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.middleware.sse.event_storage import EventType
from robosystems.middleware.sse.streaming import emit_event_to_operation
from robosystems.models.api.graphs.query import (
  DEFAULT_QUERY_TIMEOUT,
  CypherStatementRequest,
)
from robosystems.models.core import User
from robosystems.security.error_handling import safe_error_message

circuit_breaker = CircuitBreakerManager()


async def execute_query_with_timeout(
  repository: Any, query: str, parameters: dict[str, Any] | None, timeout: int
) -> list[dict[str, Any]]:
  """Execute a query, raising `TimeoutError` once `timeout` seconds elapse."""

  async def execute():
    if hasattr(repository, "execute_query") and asyncio.iscoroutinefunction(
      repository.execute_query
    ):
      return await repository.execute_query(query, parameters)
    else:
      loop = asyncio.get_event_loop()
      return await loop.run_in_executor(
        None, repository.execute_query, query, parameters
      )

  try:
    return await asyncio.wait_for(execute(), timeout=timeout)
  except TimeoutError:
    raise TimeoutError(f"Query exceeded timeout of {timeout} seconds")


async def stream_ndjson_response(
  repository: Any,
  request: CypherStatementRequest,
  graph_id: str,
  current_user: User,
  chunk_size: int = 1000,
  start_time: datetime | None = None,
) -> StreamingResponse:
  """Stream query results as NDJSON, one JSON chunk of `chunk_size` rows per
  line."""
  if not start_time:
    start_time = datetime.now(UTC)

  async def generate_ndjson():
    """Generate NDJSON chunks from query results."""
    try:
      total_rows = 0
      chunk_index = 0
      columns = None

      if hasattr(repository, "execute_query_streaming"):
        async for chunk in repository.execute_query_streaming(
          request.query, request.parameters, chunk_size=chunk_size
        ):
          if columns is None and chunk:
            columns = (
              list(chunk[0].keys()) if isinstance(chunk, list) else chunk.get("columns")
            )

          if isinstance(chunk, dict):
            rows = chunk.get("rows", chunk.get("data", []))
            chunk_columns = chunk.get("columns")
          else:
            rows = chunk
            chunk_columns = None

          total_rows += len(rows)

          ndjson_chunk = {
            "chunk_index": chunk_index,
            "rows": rows,
            "row_count": len(rows),
            "total_rows_sent": total_rows,
          }

          if chunk_index == 0 and (columns or chunk_columns):
            ndjson_chunk["columns"] = columns or chunk_columns

          yield json.dumps(ndjson_chunk) + "\n"
          chunk_index += 1

          if chunk_index % 10 == 0:
            logger.debug(f"Streamed {total_rows} rows in {chunk_index} chunks")

      else:
        # Fallback: execute the whole query, then chunk it.
        result = await execute_query_with_timeout(
          repository,
          request.query,
          request.parameters,
          request.timeout or DEFAULT_QUERY_TIMEOUT,
        )

        columns = list(result[0].keys()) if result else []
        total_rows = len(result)

        for i in range(0, total_rows, chunk_size):
          chunk = result[i : i + chunk_size]

          ndjson_chunk = {
            "chunk_index": i // chunk_size,
            "rows": chunk,
            "row_count": len(chunk),
            "total_rows_sent": min(i + chunk_size, total_rows),
          }

          if i == 0:
            ndjson_chunk["columns"] = columns

          yield json.dumps(ndjson_chunk) + "\n"

      execution_time = (datetime.now(UTC) - start_time).total_seconds() * 1000

      final_chunk = {
        "complete": True,
        "total_rows": total_rows,
        "execution_time_ms": execution_time,
        "graph_id": graph_id,
        "timestamp": datetime.now(UTC).isoformat(),
      }
      yield json.dumps(final_chunk) + "\n"

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
      # Headers are already sent, so the error goes in-band, sanitized:
      # infrastructure text stays in logs, the caller's query errors pass.
      error_chunk = {
        "error": safe_error_message(e) or "Query streaming failed",
        "error_type": type(e).__name__,
        "graph_id": graph_id,
        "timestamp": datetime.now(UTC).isoformat(),
      }
      yield json.dumps(error_chunk) + "\n"

      circuit_breaker.record_failure(graph_id, "cypher_query", error=e)
      logger.error(f"NDJSON streaming failed: {e}", exc_info=True)

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
  request: CypherStatementRequest,
  graph_id: str,
  current_user: User,
  chunk_size: int = 100,
  include_progress: bool = True,
  start_time: datetime | None = None,
) -> EventSourceResponse:
  """Stream query results as Server-Sent Events (progress, chunks, metadata)."""
  if not start_time:
    start_time = datetime.now(UTC)

  async def sse_generator():
    """Generate SSE events from query results."""
    try:
      total_rows = 0
      chunk_count = 0
      columns = None

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

      if hasattr(repository, "execute_query_streaming"):
        async for chunk in repository.execute_query_streaming(
          request.query, request.parameters, chunk_size=chunk_size
        ):
          if columns is None and chunk:
            if isinstance(chunk, dict):
              columns = chunk.get("columns")
              rows = chunk.get("rows", chunk.get("data", []))
            else:
              columns = list(chunk[0].keys()) if chunk else []
              rows = chunk

            if columns:
              yield {"event": "schema", "data": json.dumps({"columns": columns})}
          else:
            rows = chunk.get("rows", chunk) if isinstance(chunk, dict) else chunk

          chunk_count += 1
          total_rows += len(rows)

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

        yield {"event": "schema", "data": json.dumps({"columns": columns})}

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

      execution_time = (datetime.now(UTC) - start_time).total_seconds()

      yield {
        "event": "complete",
        "data": json.dumps(
          {
            "total_rows": total_rows,
            "total_chunks": chunk_count,
            "execution_time_seconds": execution_time,
            "graph_id": graph_id,
            "timestamp": datetime.now(UTC).isoformat(),
          }
        ),
      }

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

    except TimeoutError:
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
            "error": safe_error_message(e) or "Query streaming failed",
            "error_type": type(e).__name__,
          }
        ),
      }
      circuit_breaker.record_failure(graph_id, "cypher_query", error=e)
      logger.error(f"SSE streaming failed: {e}", exc_info=True)

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
  request: CypherStatementRequest,
  graph_id: str,
  current_user: User,
  priority: int = 5,
  chunk_size: int = 100,
  operation_id: str | None = None,
) -> EventSourceResponse:
  """Stream a queued query over one SSE connection: queue-position updates
  while it waits, then the result chunks once it runs."""

  async def sse_queue_stream_generator():
    """Generate SSE events for queued query with streaming."""
    queue_manager = get_query_queue()
    query_id = None

    try:
      credits_required = 0.0  # Queries are included

      query_id = await queue_manager.submit_query(
        cypher=request.query,
        parameters=request.parameters,
        graph_id=graph_id,
        user_id=current_user.id,
        credits_required=credits_required,
        priority=priority,
      )

      initial_status = await queue_manager.get_query_status(query_id)
      queue_event_data = {
        "query_id": query_id,
        "position": initial_status.get("queue_position", 0),
        "estimated_wait_seconds": initial_status.get("estimated_wait", 10),
        "message": "Query has been queued",
      }

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

      last_position = initial_status.get("queue_position", 0)
      announced_start = False

      while True:
        await asyncio.sleep(1)
        status = await queue_manager.get_query_status(query_id)
        state = status["status"] if status else None

        if state == QueryStatus.PENDING:
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

        elif state == QueryStatus.RUNNING:
          if announced_start:
            continue
          announced_start = True
          start_event_data = {
            "query_id": query_id,
            "message": "Query execution started",
          }
          if operation_id:
            await emit_event_to_operation(
              operation_id,
              EventType.OPERATION_STARTED,
              {**start_event_data, "progress_percent": 10, "status": "running"},
            )
          yield {"event": "started", "data": json.dumps(start_event_data)}

        elif state == QueryStatus.COMPLETED:
          # The queue worker already ran the query; stream its stored result.
          result = await queue_manager.get_query_result(query_id)
          payload = (result or {}).get("data") or {}
          rows = payload.get("data", []) if isinstance(payload, dict) else payload

          total_rows = 0
          chunk_count = 0
          for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
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

          complete_event_data = {
            "query_id": query_id,
            "total_rows": total_rows,
            "message": "Query completed successfully",
          }
          if operation_id:
            await emit_event_to_operation(
              operation_id,
              EventType.OPERATION_COMPLETED,
              {**complete_event_data, "progress_percent": 100, "status": "completed"},
            )
          yield {"event": "complete", "data": json.dumps(complete_event_data)}
          break

        else:
          # FAILED, CANCELLED, or the query aged out of the queue.
          error_event_data = {
            "query_id": query_id,
            "error": (status or {}).get("error")
            or ("Query cancelled" if state else "Queued query state was lost"),
          }
          if operation_id:
            await emit_event_to_operation(
              operation_id,
              EventType.OPERATION_ERROR,
              {**error_event_data, "status": "failed"},
            )
          yield {"event": "error", "data": json.dumps(error_event_data)}
          break

    except Exception as e:
      logger.error(f"Queue SSE error: {e}", exc_info=True)
      yield {
        "event": "error",
        "data": json.dumps(
          {
            "error": safe_error_message(e) or "Query streaming failed",
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
