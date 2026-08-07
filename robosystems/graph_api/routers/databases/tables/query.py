"""SQL against DuckDB staging tables.

``POST /query`` runs tenant SQL on a hardened read-only connection and can
answer as JSON, NDJSON, or SSE. ``POST /execute`` is the internal read-write
companion used by the materialization and ingestion paths.
"""

import json
from datetime import UTC, datetime

from fastapi import APIRouter, Body, HTTPException, Path, Query, Request
from fastapi import status as http_status
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse

from robosystems.graph_api.core.duckdb.manager import (
  DuckDBTableManager,
  TableQueryRequest,
  TableQueryResponse,
)
from robosystems.logger import logger

router = APIRouter(prefix="/databases/{graph_id}/tables")

table_manager = DuckDBTableManager()


@router.post("/query", response_model=None)
async def query_tables(
  full_request: Request,
  graph_id: str = Path(..., description="Graph database identifier"),
  request: TableQueryRequest = Body(...),
  streaming: bool = Query(
    default=None,
    description="Enable streaming response (auto-detected if not specified)",
  ),
  chunk_size: int = Query(
    default=1000, ge=10, le=10000, description="Rows per chunk for streaming"
  ),
) -> TableQueryResponse | StreamingResponse | EventSourceResponse:
  """
  Execute SQL query against DuckDB staging tables.

  **Response Modes (auto-detected from Accept header):**
  - `application/json` (default): Return all results as JSON
  - `application/x-ndjson`: Stream results as NDJSON (efficient for large results)
  - `text/event-stream`: Stream results as SSE (with progress updates)

  **Manual Override:**
  - `streaming=false`: Force JSON response
  - `streaming=true`: Force streaming (NDJSON or SSE based on Accept header)

  **When to use streaming:**
  - Queries on external tables (reading from thousands of S3 files)
  - Queries returning > 10,000 rows
  - Long-running aggregations over S3 data

  **Note:** External tables query S3 directly and can take minutes for thousands of files.
  Use streaming to get progressive results. For repeated queries, ingest data into LadybugDB graph.
  """
  start_time = datetime.now(UTC)

  accept_header = full_request.headers.get("accept", "")
  wants_sse = "text/event-stream" in accept_header
  wants_ndjson = "application/x-ndjson" in accept_header

  # An explicit `streaming` query param overrides Accept-header detection.
  if streaming is None:
    streaming = wants_sse or wants_ndjson

  logger.info(
    f"Executing {'streaming ' if streaming else ''}query for graph {graph_id}: {request.sql[:100]} "
    f"(SSE: {wants_sse}, NDJSON: {wants_ndjson})"
  )

  request.graph_id = graph_id

  try:
    if streaming:
      if wants_sse:

        async def sse_generator():
          try:
            yield {
              "event": "started",
              "data": json.dumps(
                {
                  "graph_id": graph_id,
                  "timestamp": start_time.isoformat(),
                  "message": "Query execution started",
                }
              ),
            }

            chunk_index = 0
            last_chunk = None
            for chunk in table_manager.query_table_streaming(request, chunk_size):
              last_chunk = chunk
              if "error" in chunk:
                yield {
                  "event": "error",
                  "data": json.dumps(
                    {
                      "error": chunk["error"],
                      "error_type": chunk.get("error_type"),
                      "execution_time_ms": chunk.get("execution_time_ms", 0),
                    }
                  ),
                }
                return

              yield {
                "event": "chunk",
                "data": json.dumps(
                  {
                    "chunk_index": chunk["chunk_index"],
                    "rows": chunk["rows"],
                    "row_count": chunk["row_count"],
                    "total_rows_sent": chunk["total_rows_sent"],
                    "total_rows": chunk.get("total_rows", chunk["total_rows_sent"]),
                    "is_last_chunk": chunk["is_last_chunk"],
                  }
                ),
              }

              # Progress is emitted on the first and last chunk only.
              if chunk_index == 0 or chunk["is_last_chunk"]:
                progress_percent = (
                  100
                  if chunk["is_last_chunk"]
                  else int(
                    (chunk["total_rows_sent"] / chunk.get("total_rows", 1)) * 100
                  )
                )
                yield {
                  "event": "progress",
                  "data": json.dumps(
                    {
                      "progress_percent": progress_percent,
                      "rows_processed": chunk["total_rows_sent"],
                      "total_rows": chunk.get("total_rows"),
                      "execution_time_ms": chunk["execution_time_ms"],
                    }
                  ),
                }

              chunk_index += 1

            execution_time_ms = (datetime.now(UTC) - start_time).total_seconds() * 1000
            yield {
              "event": "completed",
              "data": json.dumps(
                {
                  "message": "Query execution completed",
                  "total_rows": last_chunk.get("total_rows", 0) if last_chunk else 0,
                  "execution_time_ms": execution_time_ms,
                }
              ),
            }

          except Exception as e:
            logger.error(f"SSE streaming error: {e}")
            yield {
              "event": "error",
              "data": json.dumps({"error": str(e), "error_type": type(e).__name__}),
            }

        return EventSourceResponse(
          sse_generator(),
          headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "X-Stream-Format": "sse",
            "X-Graph-ID": graph_id,
          },
          ping=15,
        )
      else:

        def generate_ndjson():
          for chunk in table_manager.query_table_streaming(request, chunk_size):
            yield json.dumps(chunk) + "\n"

        return StreamingResponse(
          generate_ndjson(),
          media_type="application/x-ndjson",
          headers={
            "X-Streaming": "true",
            "Cache-Control": "no-cache",
            "X-Content-Type-Options": "nosniff",
          },
        )
    else:
      return table_manager.query_table(request)

  except Exception as e:
    logger.error(f"Query failed for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail=f"Query failed: {e!s}",
    )


@router.post("/execute", response_model=None)
async def execute_table_write(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: TableQueryRequest = Body(...),
) -> TableQueryResponse:
  """Execute a write/DDL statement against DuckDB staging (**internal**).

  Read-write companion to ``POST /query``. ``/query`` runs tenant SQL on the
  hardened read-only connection (SELECT/WITH only, external access off); this
  runs on the read-write connection with httpfs + postgres_scanner enabled, for
  the internal materialization + ingestion paths — ``CREATE TABLE AS
  SELECT ... postgres_scan(...)`` staging and INSERT/DELETE upserts.

  Not proxied by the tenant ``/query/sql`` surface; only the internal graph
  client calls it, alongside the other internal write endpoints
  (``POST /tables`` create, ``POST /tables/{name}/insert``).
  """
  request.graph_id = graph_id
  try:
    return table_manager.execute_write(request)
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Write failed for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST,
      detail=f"Write failed: {e!s}",
    )
