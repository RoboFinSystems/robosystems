"""
Asynchronous Graph API client.

Wraps the Graph API HTTP surface with retry/circuit-breaker handling and
SSE-based monitoring for long-running ingestion, backup and restore tasks.
"""

import asyncio
import json
import time
from collections.abc import AsyncGenerator
from typing import Any, cast

import httpx
from httpx_sse import aconnect_sse

from robosystems.logger import logger

from .base import BaseGraphClient
from .config import GraphClientConfig
from .exceptions import (
  GraphAPIError,
  GraphTimeoutError,
  GraphTransientError,
)


class GraphClient(BaseGraphClient):
  """Asynchronous client for Graph API operations."""

  def __init__(
    self,
    base_url: str | None = None,
    config: GraphClientConfig | None = None,
    **kwargs,
  ):
    """Initialize the client; ``kwargs`` override individual config fields."""
    super().__init__(base_url, config, **kwargs)

    limits = httpx.Limits(
      max_connections=self.config.max_connections,
      max_keepalive_connections=self.config.max_keepalive_connections,
      keepalive_expiry=self.config.keepalive_expiry,
    )

    self.client = httpx.AsyncClient(
      base_url=self.config.base_url,
      timeout=httpx.Timeout(self.config.timeout),
      limits=limits,
      headers=self.config.headers,
      verify=self.config.verify_ssl,
    )

    # Routing metadata (set by factory for debugging)
    self._route_target: str | None = None
    self._graph_id: str | None = None
    self._database_name: str | None = None
    self._instance_id: str | None = None
    self._purpose: str | None = None

  async def __aenter__(self):
    """Async context manager entry."""
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Async context manager exit."""
    await self.close()

  async def close(self):
    """Close the client and cleanup resources."""
    await self.client.aclose()

  async def _execute_with_retry(self, func, *args, **kwargs):
    """Run ``func`` under the circuit breaker, retrying transient failures.

    httpx transport errors are mapped to ``GraphTimeoutError`` /
    ``GraphTransientError`` before the retry decision, so only errors the
    client considers retryable consume an attempt. The last error is re-raised
    once ``max_retries`` is exhausted, and the failure is recorded against the
    circuit breaker.
    """
    last_error = None

    for attempt in range(self.config.max_retries + 1):
      try:
        self._check_circuit_breaker()

        result = await func(*args, **kwargs)

        self._record_success()

        return result

      except Exception as e:
        last_error = e

        if isinstance(e, httpx.TimeoutException):
          last_error = GraphTimeoutError(f"Request timeout: {e}")
        elif isinstance(e, httpx.ConnectError):
          last_error = GraphTransientError(f"Connection error: {e}")
        elif isinstance(e, httpx.RequestError):
          last_error = GraphTransientError(f"Request error: {e}")

        if not self._should_retry(last_error, attempt):
          self._record_failure()
          raise last_error

        if attempt < self.config.max_retries:
          delay = self._calculate_retry_delay(attempt)
          logger.warning(
            f"Request failed (attempt {attempt + 1}/{self.config.max_retries + 1}), "
            f"retrying in {delay:.2f}s: {last_error}"
          )
          await asyncio.sleep(delay)

    self._record_failure()
    if last_error is None:
      raise RuntimeError("Retry logic failed without capturing an exception")
    raise last_error

  @staticmethod
  def _classify_operation(path: str) -> str:
    """Classify a Graph API path into an operation name for metrics.

    Collapses per-graph paths to a bounded label set (``/databases/{id}/query``
    -> ``query``) so metric cardinality does not grow with the graph count.
    """
    parts = path.strip("/").split("/")
    if len(parts) == 0:
      return "unknown"
    if parts[-1] == "query":
      return "query"
    if parts[-1] == "tables" and len(parts) >= 2:
      return "table_ops"
    if parts[-1] == "materialize":
      return "materialize"
    if parts[-1] == "schema":
      return "schema"
    if parts[-1] == "backup":
      return "backup"
    if parts[-1] == "restore":
      return "restore"
    if parts[-1] == "metrics":
      return "metrics"
    if parts[-1] == "health":
      return "health"
    if parts[-1] == "info":
      return "info"
    if parts[-1] == "databases" or (len(parts) == 2 and parts[0] == "databases"):
      return "database_ops"
    if "memory" in parts[-1]:
      return "memory"
    if "fork" in parts[-1] or "copy" in parts[-1]:
      return "fork"
    if "ingest" in path or "s3" in parts[-1]:
      return "ingest"
    if parts[-1] in ("tasks", "task"):
      return "task"
    return "other"

  def _emit_graph_api_metrics(
    self,
    method: str,
    path: str,
    duration: float,
    status_code: int,
    error: bool,
    error_type: str | None,
  ):
    """Emit OTel metrics for a Graph API call."""
    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_graph_api_call(
        method=method,
        operation=self._classify_operation(path),
        duration=duration,
        status_code=status_code,
        route_target=self._route_target or "unknown",
        error=error,
        error_type=error_type,
      )
    except Exception:
      pass  # Metrics are best-effort, never break graph API calls

  async def _request(
    self,
    method: str,
    path: str,
    json_data: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    timeout: float | None = None,
    retries: int | None = None,
    headers: dict[str, str] | None = None,
  ) -> httpx.Response:
    """Make an HTTP request, timed and reported to OTel whether or not it fails.

    ``retries=None`` uses the config default; ``retries=0`` bypasses
    ``_execute_with_retry`` entirely and must be used for non-idempotent
    operations (staging inserts, materialization) where a replay would
    duplicate data. Responses with a 4xx/5xx status are converted to the
    matching ``Graph*Error`` before returning.
    """
    request_kwargs: dict[str, Any] = {
      "method": method,
      "url": path,
    }

    if json_data is not None:
      request_kwargs["json"] = json_data
    if params is not None:
      request_kwargs["params"] = params
    if timeout is not None:
      request_kwargs["timeout"] = timeout
    if headers is not None:
      request_kwargs["headers"] = headers

    async def make_request():
      logger.debug(f"Making request: {method} {path}")
      if self.client.headers:
        debug_headers = dict(self.client.headers)
        if "X-Graph-API-Key" in debug_headers:
          debug_headers["X-Graph-API-Key"] = (
            debug_headers["X-Graph-API-Key"][:8] + "..."
          )
        logger.debug(f"Client headers: {debug_headers}")

      response = await self.client.request(**request_kwargs)

      if response.status_code >= 400:
        try:
          error_data = response.json()
        except Exception:
          error_data = {"detail": response.text}

        error = self._handle_response_error(response.status_code, error_data)
        raise error

      return response

    # Timed across all retries, so the metric reflects caller-observed latency
    start_time = time.time()
    status_code = 0
    error_occurred = False
    error_type = None

    try:
      if retries == 0:
        response = await make_request()
      else:
        response = await self._execute_with_retry(make_request)

      status_code = response.status_code
      return response

    except Exception as e:
      error_occurred = True
      status_code = getattr(e, "status_code", 0)
      from .exceptions import GraphClientError, GraphTimeoutError, GraphTransientError

      if isinstance(e, GraphTimeoutError):
        error_type = "timeout"
      elif isinstance(e, GraphTransientError):
        error_type = "transient"
      elif isinstance(e, GraphClientError):
        error_type = "client"
      else:
        error_type = "server"
      raise

    finally:
      duration = time.time() - start_time
      self._emit_graph_api_metrics(
        method=method,
        path=path,
        duration=duration,
        status_code=status_code,
        error=error_occurred,
        error_type=error_type,
      )

  # API Methods

  async def health_check(self) -> dict[str, Any]:
    """Check API health status."""
    response = await self._request("GET", "/health")
    return response.json()

  async def query(
    self,
    cypher: str,
    graph_id: str = "sec",
    parameters: dict[str, Any] | None = None,
    streaming: bool = False,
  ) -> dict[str, Any] | AsyncGenerator[Any]:
    """Execute a Cypher query.

    Returns a result dict, or — when ``streaming`` is set — an async generator
    of NDJSON chunks produced by the server. Streaming lets the graph instance
    do the chunking rather than materializing the full result set in memory,
    and a malformed or empty response body degrades to an empty result rather
    than raising.
    """
    payload: dict[str, Any] = {"cypher": cypher, "database": graph_id}
    if parameters:
      payload["parameters"] = parameters

    params = {"streaming": "true"} if streaming else {}

    if not streaming:
      response = await self._request(
        "POST", f"/databases/{graph_id}/query", json_data=payload, params=params
      )
      logger.debug(f"Response status: {response.status_code}")
      logger.debug(f"Response content type: {response.headers.get('content-type')}")
      logger.debug(
        f"Response content length: {len(response.content) if response.content else 'None'}"
      )
      logger.debug(
        f"Response content (first 200 chars): {repr(response.content[:200]) if response.content else 'None'}"
      )

      if response.content is None or len(response.content) == 0:
        logger.warning("Received empty response body from Graph API")
        return {"data": [], "columns": [], "row_count": 0}

      try:
        json_result = response.json()
        logger.debug(f"Successfully parsed JSON response: {json_result}")
        return json_result
      except Exception as e:
        logger.error(
          f"Failed to parse response as JSON: {e}, content: {response.content[:100]!r}"
        )
        return {
          "error": f"Invalid JSON response: {e!s}",
          "data": [],
          "columns": [],
          "row_count": 0,
        }

    async def stream_chunks() -> AsyncGenerator[dict[str, Any]]:
      """Stream NDJSON chunks from Graph API server."""
      stream_start = time.time()
      metrics_recorded = False

      try:
        async with self.client.stream(
          "POST",
          f"/databases/{graph_id}/query",
          json=payload,
          params=params,
          timeout=httpx.Timeout(300.0, connect=10.0),  # 5 min stream timeout
        ) as response:
          if response.status_code >= 400:
            error_text = await response.aread()
            try:
              error_data = json.loads(error_text)
            except Exception:
              error_data = {
                "detail": error_text.decode()
                if isinstance(error_text, bytes)
                else error_text
              }
            raise self._handle_response_error(response.status_code, error_data)

          # Streaming has no single completion point, so time-to-first-byte
          # stands in for query duration.
          ttfb = time.time() - stream_start
          self._emit_graph_api_metrics(
            method="POST",
            path=f"/databases/{graph_id}/query",
            duration=ttfb,
            status_code=response.status_code,
            error=False,
            error_type=None,
          )
          metrics_recorded = True

          async for line in response.aiter_lines():
            if line:
              try:
                chunk = json.loads(line)
                yield chunk
              except json.JSONDecodeError as e:
                logger.error(f"Failed to parse NDJSON line: {e}, line: {line[:100]}")
                continue

      except Exception as e:
        if not metrics_recorded:
          from .exceptions import GraphTimeoutError, GraphTransientError

          if isinstance(e, GraphTimeoutError):
            error_type = "timeout"
          elif isinstance(e, GraphTransientError):
            error_type = "transient"
          else:
            error_type = "server"

          self._emit_graph_api_metrics(
            method="POST",
            path=f"/databases/{graph_id}/query",
            duration=time.time() - stream_start,
            status_code=getattr(e, "status_code", 0),
            error=True,
            error_type=error_type,
          )
        raise

    return stream_chunks()

  async def get_info(self) -> dict[str, Any]:
    """Get cluster configuration, status and capabilities."""
    response = await self._request("GET", "/info")
    return response.json()

  async def _monitor_task_sse(
    self, sse_path: str, task_id: str, task_type: str, timeout: int
  ) -> dict[str, Any]:
    """Monitor any task (ingestion, backup, restore, staging) via its SSE stream.

    ``task_type`` is a label used in log lines only.
    """
    return await self._monitor_ingestion_sse(
      sse_path=sse_path,
      task_id=task_id,
      table_name=task_type,
      timeout=timeout,
    )

  async def _monitor_ingestion_sse(
    self, sse_path: str, task_id: str, table_name: str, timeout: int
  ) -> dict[str, Any]:
    """Monitor ingestion progress via SSE, falling back to status polling.

    Both the hard timeout and any stream error fall back to
    :meth:`_poll_task_status_fallback` rather than failing the task: a dead
    stream does not mean a dead task.
    """
    start_time = time.time()

    sse_url = f"{self.config.base_url}{sse_path}"

    try:
      # The wait_for is the only timeout that fires when the server stops
      # sending events entirely — the in-loop checks run per event.
      return await asyncio.wait_for(
        self._sse_event_loop(
          sse_url=sse_url,
          task_id=task_id,
          table_name=table_name,
          timeout=timeout,
          start_time=start_time,
        ),
        timeout=timeout + 30,  # Add 30s buffer for connection setup
      )

    except TimeoutError:
      elapsed = time.time() - start_time
      logger.warning(
        f"SSE monitoring hard timeout after {elapsed:.0f}s for {table_name} "
        f"(task {task_id}), falling back to status poll"
      )
      return await self._poll_task_status_fallback(
        task_id, table_name, timeout, start_time
      )
    except Exception as e:
      logger.warning(
        f"SSE monitoring error for task {task_id}: {e}, falling back to status poll"
      )
      return await self._poll_task_status_fallback(
        task_id, table_name, timeout, start_time
      )

  async def _sse_event_loop(
    self,
    sse_url: str,
    task_id: str,
    table_name: str,
    timeout: int,
    start_time: float,
  ) -> dict[str, Any]:
    """SSE event loop, split out so the caller can wrap it in ``wait_for``.

    Uses its own httpx client so a long-lived stream does not hold a
    connection from the shared pool for the duration of the task.
    """
    last_heartbeat = start_time
    last_progress_log = start_time

    try:
      async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers=self.config.headers,
      ) as sse_client:
        async with aconnect_sse(sse_client, "GET", sse_url) as event_source:
          async for sse_event in event_source.aiter_sse():
            current_time = time.time()

            try:
              data = json.loads(sse_event.data) if sse_event.data else {}
            except json.JSONDecodeError:
              logger.warning(f"Invalid JSON in SSE event: {sse_event.data}")
              continue

            if sse_event.event == "heartbeat":
              last_heartbeat = current_time
              elapsed = current_time - start_time
              logger.debug(
                f"[Heartbeat] Task {task_id} still running after {elapsed:.0f}s"
              )

            elif sse_event.event == "progress":
              if current_time - last_progress_log > 30:
                progress = data.get("progress_percent", 0)
                records = data.get("records_processed", 0)
                estimated = data.get("estimated_records", 0)

                logger.info(
                  f"[Progress] {table_name}: {progress}% complete, "
                  f"{records:,}/{estimated:,} records processed"
                )
                last_progress_log = current_time

            elif sse_event.event == "completed":
              result = data.get("result", {})
              duration = data.get("duration_seconds", 0)
              records_loaded = result.get("records_loaded", 0)

              if records_loaded > 0:
                logger.info(
                  f"[OK] Ingestion completed for {table_name}: "
                  f"{records_loaded:,} records in {duration:.1f}s"
                )
              else:
                # With IGNORE_ERRORS, record count is not available
                logger.info(
                  f"[OK] Ingestion completed for {table_name} in {duration:.1f}s"
                )

              return {
                "status": "completed",
                "task_id": task_id,
                "records_loaded": records_loaded,
                "duration_seconds": duration,
                "result": result,
              }

            elif sse_event.event == "failed":
              error = data.get("error", "Unknown error")
              # "No files found" is expected for optional tables - don't log as error
              if "No files found" in error:
                logger.info(f"No files found for {table_name} (optional table)")
              else:
                logger.error(f"[FAILED] Ingestion failed for {table_name}: {error}")

              return {"status": "failed", "task_id": task_id, "error": error}

            elif sse_event.event == "error":
              error = data.get("error", "Stream error")
              logger.error(f"SSE stream error: {error}")

              return {
                "status": "failed",
                "task_id": task_id,
                "error": f"SSE stream error: {error}",
              }

            # Check for timeout (soft timeout - checked after each event)
            if current_time - start_time > timeout:
              logger.error(f"Ingestion timeout after {timeout}s for {table_name}")
              return {
                "status": "failed",
                "task_id": task_id,
                "error": f"Timeout after {timeout} seconds",
              }

            if current_time - last_heartbeat > 120:
              logger.warning(
                f"No heartbeat received for 2 minutes for task {task_id}, "
                f"falling back to status poll"
              )
              return await self._poll_task_status_fallback(
                task_id, table_name, timeout, start_time
              )

      logger.warning(
        f"SSE stream ended unexpectedly for task {task_id}, falling back to status poll"
      )
      return await self._poll_task_status_fallback(
        task_id, table_name, timeout, start_time
      )

    except TimeoutError:
      logger.error(f"SSE connection timeout for task {task_id}")
      return {"status": "failed", "task_id": task_id, "error": "SSE connection timeout"}

  async def _poll_task_status_fallback(
    self,
    task_id: str,
    table_name: str,
    timeout: int,
    start_time: float,
  ) -> dict[str, Any]:
    """Poll task status over HTTP when the SSE stream goes stale.

    An SSE stream can be starved (outbound network saturated by a large S3
    upload, say) while the task itself is running fine, so a dead stream is
    treated as a monitoring failure rather than a task failure.
    """
    poll_interval = 5
    max_polls = max(timeout // poll_interval, 60)  # Poll until timeout, minimum 5 min

    for i in range(max_polls):
      if time.time() - start_time > timeout:
        logger.error(f"Task {task_id} timed out after {timeout}s during status polling")
        return {
          "status": "failed",
          "task_id": task_id,
          "error": f"Timeout after {timeout} seconds",
        }

      try:
        task = await self.get_task_status(task_id)
        status = task.get("status")

        if status == "completed":
          result = task.get("result", {})
          logger.info(
            f"[OK] Task {task_id} completed (detected via status poll after SSE stale)"
          )
          return {
            "status": "completed",
            "task_id": task_id,
            "result": result,
            "duration_seconds": time.time() - start_time,
          }

        if status == "failed":
          error = task.get("error", "Unknown error")
          logger.error(f"[FAILED] Task {task_id} failed: {error}")
          return {"status": "failed", "task_id": task_id, "error": error}

        # Still running — keep polling
        if i == 0:
          logger.info(
            f"Task {task_id} still running (status={status}), "
            f"polling every {poll_interval}s..."
          )

      except Exception as e:
        logger.warning(f"Status poll failed for task {task_id}: {e}")

      await asyncio.sleep(poll_interval)

    return {
      "status": "failed",
      "task_id": task_id,
      "error": f"Task still running after {max_polls * poll_interval}s of status polling",
    }

  async def list_databases(self) -> dict[str, Any]:
    """List all databases."""
    response = await self._request("GET", "/databases")
    return response.json()

  async def get_database(self, graph_id: str) -> dict[str, Any]:
    """Get specific database information."""
    response = await self._request("GET", f"/databases/{graph_id}")
    return response.json()

  async def create_database(
    self,
    graph_id: str,
    schema_type: str = "entity",
    repository_name: str | None = None,
    custom_schema_ddl: str | None = None,
    is_subgraph: bool = False,
  ) -> dict[str, Any]:
    """Create a database.

    ``schema_type`` is one of ``entity``/``shared``/``custom``;
    ``repository_name`` applies to shared databases. ``is_subgraph`` bypasses
    the node's ``max_databases`` check, since a subgraph is accounted against
    its parent's tier limit rather than the instance's.
    """
    payload = {
      "graph_id": graph_id,
      "schema_type": schema_type,
      "is_subgraph": is_subgraph,
    }
    if repository_name:
      payload["repository_name"] = repository_name
    if custom_schema_ddl:
      payload["custom_schema_ddl"] = custom_schema_ddl

    response = await self._request("POST", "/databases", json_data=payload)
    return response.json()

  async def delete_database(
    self,
    graph_id: str,
    preserve_duckdb: bool = False,
    staging_only: bool = False,
    lock_token: str | None = None,
  ) -> dict[str, Any]:
    """Delete a database and, by default, its DuckDB staging.

    ``preserve_duckdb`` keeps staging so LadybugDB can be rebuilt from it;
    ``staging_only`` does the inverse, dropping staging and keeping the graph.
    The two are mutually exclusive.

    Deleting a ``-wip``/``-prev`` name is guarded by the base graph's
    materialization lock. A caller that already holds it (the materialize flow
    cleaning up its own WIP) passes ``lock_token`` so the endpoint does not
    re-acquire — without it, the delete 409s against the caller's own lock.
    """
    params = {}
    if preserve_duckdb:
      params["preserve_duckdb"] = "true"
    if staging_only:
      params["staging_only"] = "true"
    headers: dict[str, str] = {}
    if lock_token:
      headers["X-Materialization-Lock-Token"] = lock_token
    response = await self._request(
      "DELETE",
      f"/databases/{graph_id}",
      params=params,
      headers=headers or None,
    )
    return response.json()

  # =========================================================================
  # Memory Management
  # =========================================================================

  async def boost_memory(self, graph_id: str, target: str = "both") -> dict[str, Any]:
    """Raise memory limits for staging (DuckDB) or materialization (LadybugDB).

    Call before a batch of staging or materialization operations. The boost
    stays active until :meth:`restore_memory` or :meth:`release_memory`.
    ``target`` is ``duckdb``, ``ladybug`` or ``both``.
    """
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/memory/boost",
      json_data={"target": target},
      timeout=30.0,
    )
    return response.json()

  async def restore_memory(self, graph_id: str) -> dict[str, Any]:
    """Restore memory limits to their defaults.

    Reconfigures limits only — connections stay open and buffers stay
    allocated. Use :meth:`release_memory` to actually hand memory back.
    """
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/memory/restore",
      timeout=30.0,
    )
    return response.json()

  async def release_memory(
    self, graph_id: str, target: str = "both", aggressive: bool = True
  ) -> dict[str, Any]:
    """Close connections and return buffer memory to the OS.

    Closing connections is what forces the engines to give buffers back;
    :meth:`restore_memory` only lowers the configured ceiling. Call after
    staging or materialization completes. ``aggressive`` additionally runs GC
    and ``malloc_trim`` on the LadybugDB side.
    """
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/memory/release",
      json_data={"target": target, "aggressive": aggressive},
      timeout=60.0,  # Longer timeout for aggressive cleanup
    )
    return response.json()

  async def memory_status(self, graph_id: str) -> dict[str, Any]:
    """Report whether memory is currently boosted for a graph."""
    response = await self._request(
      "GET",
      f"/databases/{graph_id}/memory/status",
    )
    return response.json()

  async def ingest(
    self,
    graph_id: str,
    file_path: str | None = None,
    table_name: str | None = None,
    pipeline_run_id: str | None = None,
    bucket: str | None = None,
    files: list[str] | None = None,
    mode: str = "sync",
    priority: int = 5,
    ignore_errors: bool = True,
  ) -> dict[str, Any]:
    """Ingest data, either inline or as a queued background task.

    ``mode="sync"`` ingests a local file and requires ``file_path`` and
    ``table_name``; ``mode="async"`` queues an S3 batch and requires
    ``pipeline_run_id``, ``bucket`` and ``files``. Sync mode gets 30x the
    configured timeout because it blocks on the load itself.
    """
    payload = {
      "mode": mode,
      "priority": priority,
      "ignore_errors": ignore_errors,
    }

    if mode == "sync":
      if not file_path or not table_name:
        raise ValueError("Sync mode requires file_path and table_name")
      payload["file_path"] = file_path
      payload["table_name"] = table_name
    else:  # async
      if not pipeline_run_id or not bucket or not files:
        raise ValueError("Async mode requires pipeline_run_id, bucket, and files")
      payload["pipeline_run_id"] = pipeline_run_id
      payload["bucket"] = bucket
      payload["files"] = files

    timeout = self.config.timeout * 30 if mode == "sync" else self.config.timeout

    response = await self._request(
      "POST",
      f"/databases/{graph_id}/ingest",
      json_data=payload,
      timeout=timeout,
    )
    return response.json()

  async def get_task_status(self, task_id: str) -> dict[str, Any]:
    """Get background task status."""
    response = await self._request("GET", f"/tasks/{task_id}/status")
    return response.json()

  async def list_tasks(
    self, status: str | None = None, limit: int = 100
  ) -> dict[str, Any]:
    """List tasks with optional status filter."""
    params: dict[str, Any] = {"limit": limit}
    if status:
      params["status"] = status

    response = await self._request("GET", "/tasks", params=params)
    return response.json()

  async def cancel_task(self, task_id: str) -> dict[str, Any]:
    """Cancel a pending task."""
    response = await self._request("DELETE", f"/tasks/{task_id}")
    return response.json()

  async def get_queue_info(self) -> dict[str, Any]:
    """Get ingestion queue information."""
    response = await self._request("GET", "/tasks/queue/info")
    return response.json()

  # APIRepository-compatible surface

  async def execute_query(
    self, cypher: str, params: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """Execute a query and return just the data rows.

    Targets ``_database_name`` when the factory set one (subgraphs route to a
    database name distinct from the graph ID), else ``graph_id``.
    """
    database = getattr(self, "_database_name", None) or self.graph_id or "sec"

    result = cast(dict[str, Any], await self.query(cypher, database, params))
    return result.get("data", [])

  async def execute_single(
    self, cypher: str, params: dict[str, Any] | None = None
  ) -> dict[str, Any] | None:
    """Execute a query and return its first row, or None if empty."""
    results = await self.execute_query(cypher, params)
    return results[0] if results else None

  async def get_schema(self) -> list[dict[str, Any]]:
    """Get the bound graph's tables and their properties."""
    graph_id = self.graph_id or "sec"

    response = await self._request("GET", f"/databases/{graph_id}/schema")
    schema_data = response.json()

    return schema_data.get("tables", [])

  async def vector_search(
    self,
    graph_id: str,
    table_name: str,
    embedding: list[float],
    limit: int = 20,
    select: list[str] | None = None,
  ) -> list[dict[str, Any]]:
    """Search a table's LanceDB vector index for rows near ``embedding``.

    Serves from master or replica instances. The index must exist already —
    build it with :meth:`vector_build`. Each result carries the selected
    columns plus a ``distance``; ``select=None`` returns all columns.
    """
    json_data: dict[str, Any] = {"embedding": embedding, "limit": limit}
    if select is not None:
      json_data["select"] = select
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/{table_name}/vector/search",
      json_data=json_data,
      timeout=10.0,
    )
    data = response.json()
    return data.get("results", [])

  async def vector_build(
    self,
    graph_id: str,
    table_name: str,
    query: str,
    memory_limit: str = "8GB",
    timeout: int = 900,
  ) -> dict[str, Any]:
    """Build a LanceDB IVF-PQ index from a DuckDB staging query.

    ``query`` must select a ``vector`` column (e.g.
    ``embedding::FLOAT[384] AS vector``); every other column it returns
    becomes searchable metadata on the index. Builds run on the graph instance
    and can take 10+ minutes, hence the long default ``timeout``.

    Returns row_count, num_partitions, index_size_mb and duration_ms.
    """
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/{table_name}/vector/build",
      json_data={"query": query, "memory_limit": memory_limit},
      timeout=float(timeout),
    )
    return response.json()

  async def vector_export(
    self,
    graph_id: str,
    table_name: str,
    s3_bucket: str | None = None,
    s3_key: str | None = None,
    timeout: int = 300,
  ) -> dict[str, Any]:
    """Export a vector index as tar.gz, optionally uploading it to S3.

    With ``s3_bucket``/``s3_key`` the graph instance uploads the archive
    itself, since the caller has no access to the instance filesystem.

    Returns size_mb, duration_ms, and s3_uri when uploaded.
    """
    json_data: dict[str, Any] = {}
    if s3_bucket and s3_key:
      json_data["s3_bucket"] = s3_bucket
      json_data["s3_key"] = s3_key
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/{table_name}/vector/export",
      json_data=json_data if json_data else None,
      timeout=float(timeout),
    )
    return response.json()

  # ---------------------------------------------------------------------------
  # Semantic memory (per-graph LanceDB "memory" table)
  # ---------------------------------------------------------------------------

  async def memory_add(
    self,
    graph_id: str,
    records: list[dict[str, Any]],
    timeout: float = 30.0,
  ) -> dict[str, Any]:
    """Add pre-embedded memory rows to the graph's semantic-memory store."""
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/semantic-memory/rows",
      json_data={"records": records},
      timeout=timeout,
    )
    return response.json()

  async def memory_search(
    self,
    graph_id: str,
    embedding: list[float],
    limit: int = 10,
    where: str | None = None,
    select: list[str] | None = None,
    timeout: float = 10.0,
  ) -> list[dict[str, Any]]:
    """Vector recall (cosine top-k) over the graph's semantic-memory store."""
    json_data: dict[str, Any] = {"embedding": embedding, "limit": limit}
    if where is not None:
      json_data["where"] = where
    if select is not None:
      json_data["select_columns"] = select
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/semantic-memory/search",
      json_data=json_data,
      timeout=timeout,
    )
    return response.json().get("results", [])

  async def memory_list(
    self,
    graph_id: str,
    where: str | None = None,
    limit: int = 100,
    offset: int = 0,
    timeout: float = 10.0,
  ) -> dict[str, Any]:
    """List/filter memory rows (metadata scan)."""
    json_data: dict[str, Any] = {"limit": limit, "offset": offset}
    if where is not None:
      json_data["where"] = where
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/semantic-memory/list",
      json_data=json_data,
      timeout=timeout,
    )
    return response.json()

  async def memory_get(
    self,
    graph_id: str,
    memory_id: str,
    timeout: float = 10.0,
  ) -> dict[str, Any] | None:
    """Get one memory row by id, or None if it does not exist."""
    try:
      response = await self._request(
        "GET",
        f"/databases/{graph_id}/semantic-memory/rows/{memory_id}",
        timeout=timeout,
      )
    except GraphAPIError as e:
      if getattr(e, "status_code", None) == 404:
        return None
      raise
    return response.json()

  async def memory_update(
    self,
    graph_id: str,
    memory_id: str,
    row: dict[str, Any],
    timeout: float = 30.0,
  ) -> dict[str, Any]:
    """Full-row upsert on id (caller supplies a re-embedded vector)."""
    response = await self._request(
      "PATCH",
      f"/databases/{graph_id}/semantic-memory/rows/{memory_id}",
      json_data=row,
      timeout=timeout,
    )
    return response.json()

  async def memory_delete(
    self,
    graph_id: str,
    memory_id: str,
    timeout: float = 30.0,
  ) -> dict[str, Any]:
    """Delete a memory row by id."""
    response = await self._request(
      "DELETE",
      f"/databases/{graph_id}/semantic-memory/rows/{memory_id}",
      timeout=timeout,
    )
    return response.json()

  async def install_schema(
    self,
    graph_id: str,
    base_schema: str = "base",
    extensions: list[str] | None = None,
    custom_ddl: str | None = None,
  ) -> dict[str, Any]:
    """Install or update a database schema.

    Passing ``custom_ddl`` sends the statements verbatim; otherwise the base
    schema plus the named extensions are composed server-side.
    """
    if custom_ddl:
      payload = {"type": "ddl", "ddl": custom_ddl}
    else:
      payload = {
        "type": "custom",
        "metadata": {"base_schema": base_schema, "extensions": extensions or []},
      }

    response = await self._request(
      "POST", f"/databases/{graph_id}/schema", json_data=payload
    )
    return response.json()

  async def export_database(self, graph_id: str) -> bytes:
    """Export a database file and return its raw bytes."""
    response = await self._request("GET", f"/databases/{graph_id}/backup")
    return response.content

  async def get_storage_breakdown(self, graph_id: str) -> dict[str, Any]:
    """Get itemized disk usage for a graph and everything it owns.

    Covers the LadybugDB databases (graph, memory, subgraphs, WALs), the
    LanceDB vector indexes and the DuckDB staging file, so the total is real
    occupied disk rather than just the primary database.

    Returns ``{graph_id, total_bytes, items: [{type, id, bytes}]}``.
    """
    response = await self._request("GET", f"/databases/{graph_id}/storage")
    return response.json()

  async def get_database_info(self, graph_id: str) -> dict[str, Any]:
    """Get a database's size, schema and metadata."""
    response = await self._request("GET", f"/databases/{graph_id}")
    return response.json()

  async def get_database_metrics(
    self, graph_id: str, include_counts: bool = False
  ) -> dict[str, Any]:
    """Get size and modification metrics for a database.

    ``include_counts`` additionally computes node/relationship counts. Those
    are full graph scans — tens of seconds on a large database — so they are
    off by default and come back as None. Only pass True off a latency path.
    """
    params = {"include_counts": "true"} if include_counts else None
    response = await self._request(
      "GET", f"/databases/{graph_id}/metrics", params=params
    )
    return response.json()

  async def get_metrics(self) -> dict[str, Any]:
    """Get system, database, query and ingestion metrics for this node."""
    response = await self._request("GET", "/metrics")
    return response.json()

  async def database_exists(self, graph_id: str) -> bool:
    """Check if a database exists; errors other than 404 propagate."""
    try:
      await self.get_database(graph_id)
      return True
    except Exception as e:
      if hasattr(e, "status_code") and getattr(e, "status_code", None) == 404:
        return False
      raise

  async def ensure_database_exists(
    self, graph_id: str, schema_type: str = "entity"
  ) -> None:
    """Ensure a database exists, creating it if necessary."""
    if not await self.database_exists(graph_id):
      await self.create_database(graph_id, schema_type)

  async def swap_database(
    self, graph_id: str, lock_token: str | None = None
  ) -> dict[str, Any]:
    """Promote a WIP database to active (blue-green swap).

    ``graph_id`` is the base ID, not the ``-wip`` variant, and
    ``{graph_id}-wip.lbug`` must already exist on the Graph API node. Pass
    ``lock_token`` if the caller already holds the materialization lock, so
    the swap endpoint does not try to re-acquire it.
    """
    headers: dict[str, str] = {}
    if lock_token:
      headers["X-Materialization-Lock-Token"] = lock_token
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/swap",
      headers=headers or None,
    )
    return response.json()

  async def execute_ddl(self, ddl: str, graph_id: str | None = None) -> dict[str, Any]:
    """Execute a DDL statement (CREATE NODE TABLE, CREATE REL TABLE, ...).

    Defaults to the client's bound graph when ``graph_id`` is omitted.
    """
    target_graph = graph_id or self.graph_id or "sec"
    return cast(dict[str, Any], await self.query(ddl, target_graph))

  async def node_exists(
    self, label: str, filters: dict[str, Any] | None = None
  ) -> bool:
    """Check whether any node with ``label`` matches ``filters``.

    Filter values are passed as query parameters, never interpolated.
    """
    database = self.graph_id or "sec"

    where_clause = ""
    params = {}

    if filters:
      conditions = []
      for key, value in filters.items():
        param_name = f"param_{key}"
        conditions.append(f"n.{key} = ${param_name}")
        params[param_name] = value

      if conditions:
        where_clause = f"WHERE {' AND '.join(conditions)}"

    cypher = f"""
      MATCH (n:{label})
      {where_clause}
      RETURN COUNT(n) > 0 AS exists
      LIMIT 1
    """

    result = cast(dict[str, Any], await self.query(cypher, database, params))
    data = result.get("data", [])

    if data and len(data) > 0:
      return data[0].get("exists", False)

    return False

  async def create_backup(
    self,
    graph_id: str,
    backup_format: str = "full_dump",
    compression: bool = True,
    encryption: bool = False,
    backup_type: str = "standard",
    s3_destination: dict[str, str] | None = None,
    checkpoint: bool = True,
    vacuum: bool = False,
  ) -> dict[str, Any]:
    """Start a backup task and return its ``task_id`` and ``monitor_url``.

    ``backup_format`` only supports ``full_dump``. ``backup_type`` is
    ``standard``, ``replica``, ``duckdb_staging`` or ``r2_download``; every
    non-standard type requires ``s3_destination``. ``vacuum`` compacts before
    the dump and applies to DuckDB only.
    """
    payload = {
      "backup_format": backup_format,
      "compression": compression,
      "encryption": encryption,
      "backup_type": backup_type,
      "checkpoint": checkpoint,
      "vacuum": vacuum,
    }
    if s3_destination:
      payload["s3_destination"] = s3_destination

    response = await self.client.post(
      f"/databases/{graph_id}/backup",
      json=payload,
      headers=self.config.headers,
    )
    response.raise_for_status()
    return response.json()

  async def backup_with_sse(
    self,
    graph_id: str,
    backup_format: str = "full_dump",
    compression: bool = True,
    encryption: bool = False,
    timeout: int = 3600,  # 1 hour default
    backup_type: str = "standard",
    s3_destination: dict[str, str] | None = None,
    checkpoint: bool = True,
    vacuum: bool = False,
  ) -> dict[str, Any]:
    """Create a backup and block until it finishes, monitoring via SSE.

    Takes the same arguments as :meth:`create_backup`. Never raises: failures
    come back as ``{"status": "failed", "error": ...}``, and success carries
    ``backup_size_mb`` and ``duration_seconds``.
    """
    try:
      logger.info(f"Starting backup for database {graph_id}")

      start_response = await self.create_backup(
        graph_id=graph_id,
        backup_format=backup_format,
        compression=compression,
        encryption=encryption,
        backup_type=backup_type,
        s3_destination=s3_destination,
        checkpoint=checkpoint,
        vacuum=vacuum,
      )

      task_id = start_response["task_id"]
      monitor_url = start_response.get("monitor_url")

      if not monitor_url:
        monitor_url = f"/tasks/{task_id}/monitor"

      logger.info(f"Started backup task {task_id}, monitoring via SSE...")

      return await self._monitor_task_sse(
        sse_path=monitor_url, task_id=task_id, task_type="backup", timeout=timeout
      )

    except Exception as e:
      logger.error(f"Failed to start/monitor backup: {e}")
      return {"status": "failed", "error": str(e)}

  async def download_backup(
    self,
    graph_id: str,
  ) -> dict[str, Any]:
    """Download the current database, returning its bytes plus metadata.

    The whole dump is held in memory; prefer :meth:`create_backup` with an
    ``s3_destination`` for anything large.
    """
    response = await self.client.post(
      f"/databases/{graph_id}/backup-download",
      headers=self.config.headers,
    )
    response.raise_for_status()

    return {
      "backup_data": response.content,
      "size_bytes": int(response.headers.get("X-Backup-Size", len(response.content))),
      "database": response.headers.get("X-Database", graph_id),
      "format": response.headers.get("X-Backup-Format", "full_dump"),
    }

  async def restore_backup(
    self,
    graph_id: str,
    s3_bucket: str,
    s3_key: str,
    create_system_backup: bool = True,
    force_overwrite: bool = False,
    encrypted: bool = True,
    compressed: bool = True,
  ) -> dict[str, Any]:
    """Start a restore from an S3 backup; returns task_id and monitor_url.

    ``encrypted`` and ``compressed`` describe the stored artifact, not what to
    do to it. ``force_overwrite`` is required to restore over an existing
    database, and ``create_system_backup`` snapshots that database first.
    """
    data = {
      "s3_bucket": s3_bucket,
      "s3_key": s3_key,
      "create_system_backup": str(create_system_backup).lower(),
      "force_overwrite": str(force_overwrite).lower(),
      "encrypted": str(encrypted).lower(),
      "compressed": str(compressed).lower(),
    }

    response = await self.client.post(
      f"/databases/{graph_id}/restore",
      data=data,
    )
    response.raise_for_status()
    return response.json()

  async def restore_with_sse(
    self,
    graph_id: str,
    s3_bucket: str,
    s3_key: str,
    create_system_backup: bool = True,
    force_overwrite: bool = False,
    encrypted: bool = True,
    compressed: bool = True,
    timeout: int = 3600,  # 1 hour default
  ) -> dict[str, Any]:
    """Restore from an S3 backup and block until it finishes, monitoring via SSE.

    Takes the same arguments as :meth:`restore_backup`. Never raises: failures
    come back as ``{"status": "failed", "error": ...}``.
    """
    try:
      logger.info(f"Starting restore for database {graph_id} from {s3_bucket}/{s3_key}")

      restore_response = await self.restore_backup(
        graph_id=graph_id,
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        create_system_backup=create_system_backup,
        force_overwrite=force_overwrite,
        encrypted=encrypted,
        compressed=compressed,
      )

      task_id = restore_response["task_id"]
      monitor_url = restore_response.get("monitor_url", f"/tasks/{task_id}/monitor")

      logger.info(f"Started restore task {task_id}, monitoring via SSE...")

      return await self._monitor_task_sse(
        sse_path=monitor_url, task_id=task_id, task_type="restore", timeout=timeout
      )

    except Exception as e:
      logger.error(f"Failed to start/monitor restore: {e}")
      return {"status": "failed", "error": str(e)}

  # DuckDB Table Management Methods

  async def create_table(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str | list[str],
    file_id_map: dict[str, str] | None = None,
    null_columns: list[str] | None = None,
    timeout: int = 1800,  # 30 minutes default for large file sets
  ) -> dict[str, Any]:
    """Create a DuckDB staging table from S3 files, monitoring via SSE.

    The create runs as a background task on the instance, so thousands of S3
    files can be staged without hitting an HTTP timeout. ``s3_pattern`` is
    either a glob or an explicit file list, and ``file_id_map`` (s3_key ->
    file_id) carries provenance through to the staged rows.

    Never raises: failures come back as ``{"status": "failed", "error": ...}``.
    """
    try:
      file_count = len(s3_pattern) if isinstance(s3_pattern, list) else 1
      logger.info(
        f"Starting table creation for {table_name} ({file_count} {'files' if file_count > 1 else 'pattern'})"
      )

      json_data: dict[str, Any] = {
        "graph_id": graph_id,
        "table_name": table_name,
        "s3_pattern": s3_pattern,
        "timeout_seconds": timeout,
      }
      if file_id_map is not None:
        json_data["file_id_map"] = file_id_map
      if null_columns is not None:
        json_data["null_columns"] = null_columns

      start_response = await self._request(
        "POST",
        f"/databases/{graph_id}/tables",
        json_data=json_data,
        timeout=30.0,  # Short timeout for starting the task
        retries=0,  # Non-idempotent: retries could start duplicate staging tasks
      )

      start_data = start_response.json()
      task_id = start_data["task_id"]
      sse_path = start_data["sse_url"]

      logger.info(f"Started staging task {task_id}, monitoring via SSE...")

      return await self._monitor_task_sse(
        sse_path=sse_path, task_id=task_id, task_type="staging", timeout=timeout
      )

    except Exception as e:
      logger.error(f"Failed to start/monitor table creation: {e}")
      return {"status": "failed", "error": str(e)}

  async def insert_into_table(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str | list[str],
    timeout: int = 1800,  # 30 minutes default for large file sets
    deduplicate: bool = True,
    null_columns: list[str] | None = None,
    file_id_map: dict[str, str] | None = None,
  ) -> dict[str, Any]:
    """Append S3 files to an existing DuckDB staging table, monitoring via SSE.

    The table must already exist (see :meth:`create_table`) with a schema
    compatible with the incoming files. ``deduplicate`` uses NOT EXISTS on the
    dedup key — ``identifier`` for nodes, src/dst for relationships — which is
    safe for tables carrying FLOAT[384] embedding columns.

    Never raises: failures come back as ``{"status": "failed", "error": ...}``.
    """
    try:
      file_count = len(s3_pattern) if isinstance(s3_pattern, list) else 1
      logger.info(
        f"Starting table insert for {table_name} ({file_count} {'files' if file_count > 1 else 'pattern'})"
      )

      json_data: dict[str, Any] = {
        "graph_id": graph_id,
        "table_name": table_name,
        "s3_pattern": s3_pattern,
        "deduplicate": deduplicate,
        "timeout_seconds": timeout,
      }
      if null_columns is not None:
        json_data["null_columns"] = null_columns
      if file_id_map is not None:
        json_data["file_id_map"] = file_id_map

      start_response = await self._request(
        "POST",
        f"/databases/{graph_id}/tables/{table_name}/insert",
        json_data=json_data,
        timeout=30.0,  # Short timeout for starting the task
        retries=0,  # Non-idempotent: retries could insert duplicate data
      )

      start_data = start_response.json()
      task_id = start_data["task_id"]
      sse_path = start_data["sse_url"]

      logger.info(f"Started insert task {task_id}, monitoring via SSE...")

      return await self._monitor_task_sse(
        sse_path=sse_path, task_id=task_id, task_type="insert", timeout=timeout
      )

    except Exception as e:
      logger.error(f"Failed to start/monitor table insert: {e}")
      return {"status": "failed", "error": str(e)}

  async def list_tables(self, graph_id: str) -> list[dict[str, Any]]:
    """List the graph's DuckDB staging tables."""
    response = await self._request("GET", f"/databases/{graph_id}/tables")
    return response.json()

  async def query_table(
    self,
    graph_id: str,
    sql: str,
    parameters: list[Any] | None = None,
    timeout: float | None = None,
  ) -> dict[str, Any]:
    """Run a read-only SQL query against DuckDB staging.

    Hits the hardened ``/tables/query`` endpoint, which accepts SELECT/WITH
    only; use :meth:`execute_write` for DDL and writes. ``parameters`` are
    bound rather than interpolated.
    """
    json_data = {"graph_id": graph_id, "sql": sql}
    if parameters is not None:
      json_data["parameters"] = parameters

    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/query",
      json_data=json_data,
      timeout=timeout,
    )
    return response.json()

  async def execute_write(
    self,
    graph_id: str,
    sql: str,
    parameters: list[Any] | None = None,
    timeout: float | None = None,
  ) -> dict[str, Any]:
    """Execute a write/DDL statement on DuckDB staging (internal write path).

    Read-write companion to :meth:`query_table`. Hits ``/tables/execute``,
    which runs on the read-write connection with httpfs and postgres_scanner
    enabled — for ``CREATE TABLE AS SELECT ... postgres_scan(...)`` staging
    and the INSERT/DELETE upserts the materialization and ingestion pipelines
    depend on. Give DDL like CREATE TABLE AS SELECT a longer ``timeout``.
    """
    json_data = {"graph_id": graph_id, "sql": sql}
    if parameters is not None:
      json_data["parameters"] = parameters

    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/execute",
      json_data=json_data,
      timeout=timeout,
    )
    return response.json()

  async def delete_table(self, graph_id: str, table_name: str) -> dict[str, Any]:
    """Drop a DuckDB staging table."""
    response = await self._request(
      "DELETE", f"/databases/{graph_id}/tables/{table_name}"
    )
    return response.json()

  async def delete_file_data(
    self, graph_id: str, table_name: str, file_id: str
  ) -> dict[str, Any]:
    """Delete a staging table's rows for one ``file_id``; returns rows_deleted."""
    response = await self._request(
      "DELETE", f"/databases/{graph_id}/tables/{table_name}/files/{file_id}"
    )
    return response.json()

  async def materialize_table(
    self,
    graph_id: str,
    table_name: str,
    file_ids: list[str] | None = None,
    batch_num: int | None = None,
    num_batches: int | None = None,
    materialize_embeddings: bool = False,
    timeout: float = 600.0,
    source_graph_id: str | None = None,
    incremental: bool = False,
  ) -> dict[str, Any]:
    """Materialize a DuckDB staging table into the graph database.

    ``file_ids`` narrows the copy to specific files; None copies every row.
    ``batch_num``/``num_batches`` partition a large table deterministically by
    ``hash(key) % num_batches``, so batches can be issued one at a time
    against LadybugDB's single writer. ``source_graph_id`` reads staging from
    a different graph, which is how blue-green materialization feeds a
    ``-wip`` target. ``incremental`` anti-joins against a keyset snapshot so
    only new rows are copied — use it when the target graph is already
    populated, otherwise the copy assumes an empty target.

    Sent with retries disabled, since a replayed materialize duplicates rows.
    """
    json_data: dict[str, Any] = {}

    if file_ids is not None:
      json_data["file_ids"] = file_ids

    if batch_num is not None and num_batches is not None:
      json_data["batch_num"] = batch_num
      json_data["num_batches"] = num_batches

    if materialize_embeddings:
      json_data["materialize_embeddings"] = True

    if source_graph_id is not None:
      json_data["source_graph_id"] = source_graph_id

    if incremental:
      json_data["incremental"] = True

    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/{table_name}/materialize",
      json_data=json_data,
      timeout=timeout,
      retries=0,  # Non-idempotent: retries cause duplicate data
    )
    return response.json()

  async def build_vector_index(
    self,
    graph_id: str,
    table_name: str,
    backend: str = "hnsw",
    column: str = "embedding",
    query: str | None = None,
    timeout: float = 600.0,
  ) -> dict[str, Any]:
    """Build or rebuild a table's vector index.

    ``backend="hnsw"`` indexes ``column`` in place with LadybugDB's HNSW;
    ``backend="lance"`` builds a LanceDB IVF-PQ index from ``query`` and
    ignores ``column``.
    """
    json_data: dict[str, Any] = {"backend": backend}
    if backend == "hnsw":
      json_data["column"] = column
    elif query:
      json_data["query"] = query

    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/{table_name}/vector/build",
      json_data=json_data,
      timeout=timeout,
    )
    return response.json()

  async def fork_from_parent(
    self,
    parent_graph_id: str,
    subgraph_id: str,
    tables: list[str] | None = None,
  ) -> dict[str, Any]:
    """Fork the parent's DuckDB staging directly into a subgraph's LadybugDB.

    The instance attaches the parent's staging database and copies ``tables``
    (empty list means all) straight across. Both databases live on the same
    instance, so no data crosses the network.
    """
    response = await self._request(
      "POST",
      f"/databases/{subgraph_id}/tables/{subgraph_id}/fork-from/{parent_graph_id}",
      json_data={
        "tables": tables or [],
      },
    )
    return response.json()

  # Migration endpoints

  async def migration_export(
    self, source_version: str, target_version: str, bucket: str
  ) -> dict[str, Any]:
    """Start a LadybugDB migration export on this instance.

    ``bucket`` is the S3 destination for the system backup — the writer
    container does not resolve ``USER_DATA_BUCKET`` itself, so the caller
    supplies it. Returns the task ID and monitor URL.
    """
    response = await self._request(
      "POST",
      "/migration/export",
      params={
        "source_version": source_version,
        "target_version": target_version,
        "bucket": bucket,
      },
      timeout=30.0,
      retries=0,
    )
    return response.json()

  async def migration_import(self) -> dict[str, Any]:
    """Start a migration import on this instance; returns task ID and monitor URL."""
    response = await self._request(
      "POST",
      "/migration/import",
      timeout=30.0,
      retries=0,
    )
    return response.json()

  async def migration_status(self) -> dict[str, Any]:
    """Check migration status: pending flag, manifest and pre-migration files."""
    response = await self._request("GET", "/migration/status")
    return response.json()

  async def migration_cleanup(self) -> dict[str, Any]:
    """Delete this instance's ``.pre-migration`` files once migration is verified.

    The S3 system backups remain as the safety net. Returns files deleted and
    bytes freed.
    """
    response = await self._request("POST", "/migration/cleanup")
    return response.json()
