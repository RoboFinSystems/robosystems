"""
Asynchronous Graph API Client.

Provides asynchronous interface for Graph API operations including
SSE-based monitoring for long-running ingestion tasks.
"""

import asyncio
import json
import time
from typing import Any, Dict, Optional, List, Union, AsyncGenerator, cast

import httpx
from httpx_sse import aconnect_sse

from robosystems.logger import logger
from .base import BaseGraphClient
from .config import GraphClientConfig
from .exceptions import (
  GraphTimeoutError,
  GraphTransientError,
)


class GraphClient(BaseGraphClient):
  """Asynchronous client for Graph API operations."""

  def __init__(
    self,
    base_url: Optional[str] = None,
    config: Optional[GraphClientConfig] = None,
    **kwargs,
  ):
    """
    Initialize asynchronous Graph client.

    Args:
        base_url: Base URL for the API
        config: Client configuration
        **kwargs: Additional config overrides
    """
    super().__init__(base_url, config, **kwargs)

    # Configure httpx client limits
    limits = httpx.Limits(
      max_connections=self.config.max_connections,
      max_keepalive_connections=self.config.max_keepalive_connections,
      keepalive_expiry=self.config.keepalive_expiry,
    )

    # Create httpx client
    self.client = httpx.AsyncClient(
      base_url=self.config.base_url,
      timeout=httpx.Timeout(self.config.timeout),
      limits=limits,
      headers=self.config.headers,
      verify=self.config.verify_ssl,
    )

    # Routing metadata (set by factory for debugging)
    self._route_target: Optional[str] = None
    self._graph_id: Optional[str] = None
    self._database_name: Optional[str] = None
    self._instance_id: Optional[str] = None
    self._purpose: Optional[str] = None

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
    """
    Execute an async function with retry logic.

    Args:
        func: Async function to execute
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        Function result

    Raises:
        GraphAPIError: If all retries fail
    """
    last_error = None

    for attempt in range(self.config.max_retries + 1):
      try:
        # Check circuit breaker
        self._check_circuit_breaker()

        # Execute function
        result = await func(*args, **kwargs)

        # Record success
        self._record_success()

        return result

      except Exception as e:
        last_error = e

        # Convert to appropriate exception type
        if isinstance(e, httpx.TimeoutException):
          last_error = GraphTimeoutError(f"Request timeout: {e}")
        elif isinstance(e, httpx.ConnectError):
          last_error = GraphTransientError(f"Connection error: {e}")
        elif isinstance(e, httpx.RequestError):
          last_error = GraphTransientError(f"Request error: {e}")

        # Check if we should retry
        if not self._should_retry(last_error, attempt):
          self._record_failure()
          raise last_error

        # Calculate retry delay
        if attempt < self.config.max_retries:
          delay = self._calculate_retry_delay(attempt)
          logger.warning(
            f"Request failed (attempt {attempt + 1}/{self.config.max_retries + 1}), "
            f"retrying in {delay:.2f}s: {last_error}"
          )
          await asyncio.sleep(delay)

    # All retries failed
    self._record_failure()
    if last_error is None:
      raise RuntimeError("Retry logic failed without capturing an exception")
    raise last_error

  async def _request(
    self,
    method: str,
    path: str,
    json_data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None,
  ) -> httpx.Response:
    """
    Make HTTP request with retry logic.

    Args:
        method: HTTP method
        path: API path
        json_data: JSON body
        params: Query parameters
        timeout: Request timeout

    Returns:
        Response object
    """
    # Build request kwargs with proper typing
    request_kwargs: Dict[str, Any] = {
      "method": method,
      "url": path,
    }

    if json_data is not None:
      request_kwargs["json"] = json_data
    if params is not None:
      request_kwargs["params"] = params
    if timeout is not None:
      request_kwargs["timeout"] = timeout

    async def make_request():
      # Debug log the request
      logger.debug(f"Making request: {method} {path}")
      if self.client.headers:
        debug_headers = dict(self.client.headers)
        if "X-Graph-API-Key" in debug_headers:
          debug_headers["X-Graph-API-Key"] = (
            debug_headers["X-Graph-API-Key"][:8] + "..."
          )
        logger.debug(f"Client headers: {debug_headers}")

      response = await self.client.request(**request_kwargs)

      # Raise for error status codes
      if response.status_code >= 400:
        try:
          error_data = response.json()
        except Exception:
          error_data = {"detail": response.text}

        error = self._handle_response_error(response.status_code, error_data)
        raise error

      return response

    return await self._execute_with_retry(make_request)

  # API Methods

  async def health_check(self) -> Dict[str, Any]:
    """Check API health status."""
    response = await self._request("GET", "/health")
    return response.json()

  async def query(
    self,
    cypher: str,
    graph_id: str = "sec",
    parameters: Optional[Dict[str, Any]] = None,
    streaming: bool = False,
  ) -> Union[Dict[str, Any], AsyncGenerator[Any, None]]:
    """
    Execute a Cypher query.

    Args:
        cypher: Cypher query to execute
        graph_id: Target graph database ID
        parameters: Query parameters
        streaming: If True, use streaming for large result sets

    Returns:
        Query results (dict for regular, async generator for streaming)
    """
    payload: Dict[str, Any] = {"cypher": cypher, "database": graph_id}
    if parameters:
      payload["parameters"] = parameters

    # Add streaming parameter to URL
    params = {"streaming": "true"} if streaming else {}

    if not streaming:
      response = await self._request(
        "POST", f"/databases/{graph_id}/query", json_data=payload, params=params
      )
      # Debug logging for response content
      logger.debug(f"Response status: {response.status_code}")
      logger.debug(f"Response content type: {response.headers.get('content-type')}")
      logger.debug(
        f"Response content length: {len(response.content) if response.content else 'None'}"
      )
      logger.debug(
        f"Response content (first 200 chars): {repr(response.content[:200]) if response.content else 'None'}"
      )

      # Handle truly empty response body (but be more careful about false positives)
      if response.content is None or len(response.content) == 0:
        logger.warning("Received empty response body from Graph API")
        return {"data": [], "columns": [], "row_count": 0}

      try:
        json_result = response.json()
        logger.debug(f"Successfully parsed JSON response: {json_result}")
        return json_result
      except Exception as e:
        logger.error(
          f"Failed to parse response as JSON: {e}, content: {repr(response.content[:100])}"
        )
        return {
          "error": f"Invalid JSON response: {str(e)}",
          "data": [],
          "columns": [],
          "row_count": 0,
        }

    # For true streaming, use the streaming endpoint
    # This allows the graph database instance to do the chunking
    async def stream_chunks():
      """Stream NDJSON chunks from Graph API server."""
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

        # Stream NDJSON lines
        async for line in response.aiter_lines():
          if line:
            try:
              chunk = json.loads(line)
              yield chunk
            except json.JSONDecodeError as e:
              logger.error(f"Failed to parse NDJSON line: {e}, line: {line[:100]}")
              continue

    return stream_chunks()

  async def get_info(self) -> Dict[str, Any]:
    """
    Get comprehensive cluster information.

    Returns cluster configuration, status, and capabilities.
    """
    response = await self._request("GET", "/info")
    return response.json()

  async def ingest_with_sse(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str,
    s3_credentials: Optional[Dict[str, Any]] = None,
    ignore_errors: bool = True,
    timeout: int = 14400,  # 4 hours default
  ) -> Dict[str, Any]:
    """
    Start background ingestion and monitor via SSE.

    This method is designed for long-running ingestion tasks that could
    take hours to complete. It uses Server-Sent Events to receive real-time
    progress updates and heartbeat events to prevent timeouts.

    Args:
        graph_id: Target database identifier
        table_name: Table to ingest into
        s3_pattern: S3 glob pattern for files
        s3_credentials: Optional S3 credentials for LocalStack/MinIO
        ignore_errors: Whether to use IGNORE_ERRORS for duplicate handling
        timeout: Maximum time to wait for completion (seconds)

    Returns:
        Dict with ingestion results:
        - status: "completed" or "failed"
        - records_loaded: Number of records ingested (if successful)
        - duration_seconds: Total time taken
        - error: Error message (if failed)

    Example:
        >>> client = await GraphClientFactory.create_client("sec", "write")
        >>> result = await client.ingest_with_sse(
        ...     graph_id="sec",
        ...     table_name="Fact",
        ...     s3_pattern="s3://bucket/consolidated/nodes/Fact/*.parquet",
        ...     timeout=7200  # 2 hours
        ... )
        >>> if result["status"] == "completed":
        ...     print(f"Loaded {result['records_loaded']} records")
    """
    try:
      # Step 1: Start the background ingestion task
      logger.info(f"Starting background ingestion for {table_name} from {s3_pattern}")

      start_response = await self._request(
        "POST",
        f"/databases/{graph_id}/copy",
        json_data={
          "s3_pattern": s3_pattern,
          "table_name": table_name,
          "s3_credentials": s3_credentials,
          "ignore_errors": ignore_errors,
        },
        timeout=30.0,  # Short timeout for starting the task
      )

      start_data = start_response.json()
      task_id = start_data["task_id"]
      sse_path = start_data["sse_url"]

      logger.info(f"Started ingestion task {task_id}, monitoring via SSE...")

      # Step 2: Monitor via SSE
      return await self._monitor_ingestion_sse(
        sse_path=sse_path, task_id=task_id, table_name=table_name, timeout=timeout
      )

    except Exception as e:
      logger.error(f"Failed to start/monitor ingestion: {e}")
      return {"status": "failed", "error": str(e)}

  async def _monitor_task_sse(
    self, sse_path: str, task_id: str, task_type: str, timeout: int
  ) -> Dict[str, Any]:
    """
    Monitor any task progress via SSE stream (generic).

    Args:
        sse_path: SSE endpoint path
        task_id: Task ID for logging
        task_type: Type of task (ingestion, backup, restore, etc.)
        timeout: Maximum time to wait (seconds)

    Returns:
        Dict with results or error
    """
    # Use the existing monitoring logic with task_type instead of table_name
    return await self._monitor_ingestion_sse(
      sse_path=sse_path,
      task_id=task_id,
      table_name=task_type,  # Reuse existing param for task type
      timeout=timeout,
    )

  async def _monitor_ingestion_sse(
    self, sse_path: str, task_id: str, table_name: str, timeout: int
  ) -> Dict[str, Any]:
    """
    Monitor ingestion progress via SSE stream.

    Args:
        sse_path: SSE endpoint path (e.g., "/databases/sec/ingest/monitor/task123")
        task_id: Task ID for logging
        table_name: Table name for logging
        timeout: Maximum time to wait (seconds)

    Returns:
        Dict with results or error
    """
    start_time = time.time()
    last_heartbeat = start_time
    last_progress_log = start_time

    # Build full SSE URL
    sse_url = f"{self.config.base_url}{sse_path}"

    try:
      # Use a separate client for SSE to avoid interfering with main client
      # Include authentication headers from the main client config
      async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout),
        headers=self.config.headers,  # Include API key and other headers
      ) as sse_client:
        # Use aconnect_sse for async operations
        async with aconnect_sse(sse_client, "GET", sse_url) as event_source:
          async for sse_event in event_source.aiter_sse():
            current_time = time.time()

            # Parse event data
            try:
              data = json.loads(sse_event.data) if sse_event.data else {}
            except json.JSONDecodeError:
              logger.warning(f"Invalid JSON in SSE event: {sse_event.data}")
              continue

            # Handle different event types
            if sse_event.event == "heartbeat":
              last_heartbeat = current_time
              elapsed = current_time - start_time
              logger.debug(
                f"[Heartbeat] Task {task_id} still running after {elapsed:.0f}s"
              )

            elif sse_event.event == "progress":
              # Log progress every 30 seconds
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
                  f"✅ Ingestion completed for {table_name}: "
                  f"{records_loaded:,} records in {duration:.1f}s"
                )
              else:
                # With IGNORE_ERRORS, record count is not available
                logger.info(
                  f"✅ Ingestion completed for {table_name} in {duration:.1f}s"
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
              logger.error(f"❌ Ingestion failed for {table_name}: {error}")

              return {"status": "failed", "task_id": task_id, "error": error}

            elif sse_event.event == "error":
              error = data.get("error", "Stream error")
              logger.error(f"SSE stream error: {error}")

              return {
                "status": "failed",
                "task_id": task_id,
                "error": f"SSE stream error: {error}",
              }

            # Check for timeout
            if current_time - start_time > timeout:
              logger.error(f"Ingestion timeout after {timeout}s for {table_name}")
              return {
                "status": "failed",
                "task_id": task_id,
                "error": f"Timeout after {timeout} seconds",
              }

            # Check for stale connection (no heartbeat for 2 minutes)
            if current_time - last_heartbeat > 120:
              logger.warning(
                "No heartbeat received for 2 minutes, connection may be stale"
              )

      # If we exit the loop without a completion event
      return {
        "status": "failed",
        "task_id": task_id,
        "error": "SSE stream ended unexpectedly",
      }

    except asyncio.TimeoutError:
      logger.error(f"SSE connection timeout for task {task_id}")
      return {"status": "failed", "task_id": task_id, "error": "SSE connection timeout"}
    except Exception as e:
      logger.error(f"SSE monitoring error: {e}")
      return {"status": "failed", "task_id": task_id, "error": str(e)}

  async def list_databases(self) -> Dict[str, Any]:
    """List all databases."""
    response = await self._request("GET", "/databases")
    return response.json()

  async def get_database(self, graph_id: str) -> Dict[str, Any]:
    """Get specific database information."""
    response = await self._request("GET", f"/databases/{graph_id}")
    return response.json()

  async def create_database(
    self,
    graph_id: str,
    schema_type: str = "entity",
    repository_name: Optional[str] = None,
    custom_schema_ddl: Optional[str] = None,
    is_subgraph: bool = False,
  ) -> Dict[str, Any]:
    """
    Create a new database.

    Args:
        graph_id: Database identifier
        schema_type: Type of schema (entity/shared/custom)
        repository_name: Repository name for shared databases
        custom_schema_ddl: Custom DDL for database creation
        is_subgraph: Whether this is a subgraph (bypasses max_databases check)

    Returns:
        Creation result
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

  async def delete_database(self, graph_id: str) -> Dict[str, Any]:
    """Delete a database."""
    response = await self._request("DELETE", f"/databases/{graph_id}")
    return response.json()

  async def ingest(
    self,
    graph_id: str,
    file_path: Optional[str] = None,
    table_name: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
    bucket: Optional[str] = None,
    files: Optional[List[str]] = None,
    mode: str = "sync",
    priority: int = 5,
    ignore_errors: bool = True,
  ) -> Dict[str, Any]:
    """
    Unified data ingestion with flexible execution modes.

    Args:
        graph_id: Target database ID
        file_path: Local file path (for sync mode)
        table_name: Target table name (for sync mode)
        pipeline_run_id: Pipeline ID (for async mode)
        bucket: S3 bucket (for async mode)
        files: S3 file keys (for async mode)
        mode: Execution mode ("sync" or "async")
        priority: Task priority (1-10)
        ignore_errors: Use IGNORE_ERRORS for duplicates

    Returns:
        Ingestion response
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

    # Longer timeout for ingestion operations
    timeout = self.config.timeout * 30 if mode == "sync" else self.config.timeout

    response = await self._request(
      "POST",
      f"/databases/{graph_id}/ingest",
      json_data=payload,
      timeout=timeout,
    )
    return response.json()

  async def get_task_status(self, task_id: str) -> Dict[str, Any]:
    """Get background task status."""
    response = await self._request("GET", f"/tasks/{task_id}/status")
    return response.json()

  async def list_tasks(
    self, status: Optional[str] = None, limit: int = 100
  ) -> Dict[str, Any]:
    """List tasks with optional status filter."""
    params: Dict[str, Any] = {"limit": limit}
    if status:
      params["status"] = status

    response = await self._request("GET", "/tasks", params=params)
    return response.json()

  async def cancel_task(self, task_id: str) -> Dict[str, Any]:
    """Cancel a pending task."""
    response = await self._request("DELETE", f"/tasks/{task_id}")
    return response.json()

  async def get_queue_info(self) -> Dict[str, Any]:
    """Get ingestion queue information."""
    response = await self._request("GET", "/tasks/queue/info")
    return response.json()

  # Additional methods for compatibility with APIRepository

  async def execute_query(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """
    Execute a query and return data rows (APIRepository compatibility).

    Args:
        cypher: Cypher query
        params: Query parameters

    Returns:
        List of result rows
    """
    # Use _database_name if set (for subgraphs), otherwise fall back to graph_id
    database = getattr(self, "_database_name", None) or self.graph_id or "sec"

    result = cast(Dict[str, Any], await self.query(cypher, database, params))
    return result.get("data", [])

  async def execute_single(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> Optional[Dict[str, Any]]:
    """
    Execute a query expecting a single result.

    Args:
        cypher: Cypher query
        params: Query parameters

    Returns:
        Single result or None
    """
    results = await self.execute_query(cypher, params)
    return results[0] if results else None

  async def get_schema(self) -> List[Dict[str, Any]]:
    """
    Get database schema information.

    Returns:
        Schema information including tables and properties
    """
    # Get graph ID from instance variable
    graph_id = self.graph_id or "sec"

    response = await self._request("GET", f"/databases/{graph_id}/schema")
    schema_data = response.json()

    # Return just the tables array for compatibility
    return schema_data.get("tables", [])

  async def install_schema(
    self,
    graph_id: str,
    base_schema: str = "base",
    extensions: Optional[List[str]] = None,
    custom_ddl: Optional[str] = None,
  ) -> Dict[str, Any]:
    """
    Install or update database schema.

    Args:
        graph_id: Target database ID
        base_schema: Base schema type (default: "base")
        extensions: List of schema extensions to install
        custom_ddl: Custom DDL statements to execute

    Returns:
        Schema installation result
    """
    # The API expects a 'type' field in the request
    # When using base_schema and extensions, the type should be 'custom'
    # When using custom_ddl, the type should be 'ddl'
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

  # Additional endpoints not in original API

  async def export_database(self, graph_id: str) -> bytes:
    """
    Export a database file.

    Args:
        graph_id: Database to export

    Returns:
        Database file contents as bytes
    """
    response = await self._request("GET", f"/databases/{graph_id}/backup")
    return response.content

  async def get_database_info(self, graph_id: str) -> Dict[str, Any]:
    """
    Get comprehensive database information and statistics.

    Args:
        graph_id: Database to get information for

    Returns:
        Database information including size, schema, and metadata
    """
    response = await self._request("GET", f"/databases/{graph_id}")
    return response.json()

  async def get_database_metrics(self, graph_id: str) -> Dict[str, Any]:
    """
    Get metrics for a specific database (optimized for billing).

    Args:
        graph_id: Database to get metrics for

    Returns:
        Database metrics including size, counts, and timestamps
    """
    response = await self._request("GET", f"/databases/{graph_id}/metrics")
    return response.json()

  async def get_metrics(self) -> Dict[str, Any]:
    """
    Get comprehensive metrics for the entire cluster node.

    Returns:
        System, database, query, and ingestion metrics for monitoring
    """
    response = await self._request("GET", "/metrics")
    return response.json()

  # Helper methods for common operations

  async def database_exists(self, graph_id: str) -> bool:
    """Check if a database exists."""
    try:
      await self.get_database(graph_id)
      return True
    except Exception as e:
      # Check if it's an HTTP exception with 404 status
      if hasattr(e, "status_code") and getattr(e, "status_code", None) == 404:
        return False
      raise

  async def ensure_database_exists(
    self, graph_id: str, schema_type: str = "entity"
  ) -> None:
    """Ensure a database exists, creating it if necessary."""
    if not await self.database_exists(graph_id):
      await self.create_database(graph_id, schema_type)

  async def execute_ddl(
    self, ddl: str, graph_id: Optional[str] = None
  ) -> Dict[str, Any]:
    """
    Execute DDL (Data Definition Language) statements.

    This is useful for creating tables, relationships, etc.

    Args:
        ddl: DDL statement (CREATE NODE TABLE, CREATE REL TABLE, etc.)
        graph_id: Target graph ID (uses self.graph_id if not provided)

    Returns:
        Query result
    """
    target_graph = graph_id or self.graph_id or "sec"
    return cast(Dict[str, Any], await self.query(ddl, target_graph))

  async def node_exists(
    self, label: str, filters: Optional[Dict[str, Any]] = None
  ) -> bool:
    """
    Check if a node exists with the given label and filters.

    Args:
        label: Node label to check
        filters: Optional filters to match against

    Returns:
        True if node exists, False otherwise
    """
    database = self.graph_id or "sec"

    # Build WHERE clause from filters
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

    # Build query to check existence
    cypher = f"""
      MATCH (n:{label})
      {where_clause}
      RETURN COUNT(n) > 0 AS exists
      LIMIT 1
    """

    result = cast(Dict[str, Any], await self.query(cypher, database, params))
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
  ) -> Dict[str, Any]:
    """
    Create a backup of a database.

    Args:
        graph_id: Graph database identifier
        backup_format: Backup format (only 'full_dump' supported)
        compression: Enable compression
        encryption: Enable encryption

    Returns:
        Task information including task_id and monitor_url
    """
    payload = {
      "backup_format": backup_format,
      "compression": compression,
      "encryption": encryption,
    }

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
  ) -> Dict[str, Any]:
    """
    Create a backup and monitor via SSE.

    Args:
        graph_id: Graph database identifier
        backup_format: Backup format (only 'full_dump' supported)
        compression: Enable compression
        encryption: Enable encryption
        timeout: Maximum time to wait for completion (seconds)

    Returns:
        Dict with backup results:
        - status: "completed" or "failed"
        - backup_size_mb: Size of backup (if successful)
        - duration_seconds: Total time taken
        - error: Error message (if failed)
    """
    try:
      # Start the backup task
      logger.info(f"Starting backup for database {graph_id}")

      start_response = await self.create_backup(
        graph_id=graph_id,
        backup_format=backup_format,
        compression=compression,
        encryption=encryption,
      )

      task_id = start_response["task_id"]
      monitor_url = start_response.get("monitor_url")

      if not monitor_url:
        # Fallback for compatibility
        monitor_url = f"/tasks/{task_id}/monitor"

      logger.info(f"Started backup task {task_id}, monitoring via SSE...")

      # Monitor via SSE using the generic monitor
      return await self._monitor_task_sse(
        sse_path=monitor_url, task_id=task_id, task_type="backup", timeout=timeout
      )

    except Exception as e:
      logger.error(f"Failed to start/monitor backup: {e}")
      return {"status": "failed", "error": str(e)}

  async def download_backup(
    self,
    graph_id: str,
  ) -> Dict[str, Any]:
    """
    Download the current database as a backup.

    Args:
        graph_id: Graph database identifier

    Returns:
        Dict containing backup_data and metadata
    """
    response = await self.client.post(
      f"/databases/{graph_id}/backup-download",
      headers=self.config.headers,
    )
    response.raise_for_status()
    return response.json()

  async def restore_backup(
    self,
    graph_id: str,
    s3_bucket: str,
    s3_key: str,
    create_system_backup: bool = True,
    force_overwrite: bool = False,
    encrypted: bool = True,
    compressed: bool = True,
  ) -> Dict[str, Any]:
    """
    Restore a database from S3 backup.

    Args:
        graph_id: Graph database identifier
        s3_bucket: S3 bucket containing the backup
        s3_key: S3 key path to the backup
        create_system_backup: Create system backup before restore
        force_overwrite: Force overwrite existing database
        encrypted: Whether the backup is encrypted
        compressed: Whether the backup is compressed

    Returns:
        Task information including task_id and monitor_url
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
    backup_data: bytes,
    create_system_backup: bool = True,
    force_overwrite: bool = False,
    timeout: int = 3600,  # 1 hour default
  ) -> Dict[str, Any]:
    """
    Restore a database from backup and monitor via SSE.

    Args:
        graph_id: Graph database identifier
        backup_data: Backup data to restore
        create_system_backup: Create system backup before restore
        force_overwrite: Force overwrite existing database
        timeout: Maximum time to wait for completion (seconds)

    Returns:
        Dict with restore results:
        - status: "completed" or "failed"
        - duration_seconds: Total time taken
        - error: Error message (if failed)
    """
    try:
      # Start the restore task
      logger.info(f"Starting restore for database {graph_id}")

      # TODO: Upload backup_data to S3 first, then pass s3_bucket and s3_key
      # For now, this is not implemented as restore functionality is pending
      raise NotImplementedError(
        "Restore from backup data not yet implemented. "
        "Backup data must be uploaded to S3 first."
      )

    except Exception as e:
      logger.error(f"Failed to start/monitor restore: {e}")
      return {"status": "failed", "error": str(e)}

  # DuckDB Table Management Methods

  async def create_table(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str | List[str],
  ) -> Dict[str, Any]:
    """
    Create a DuckDB staging table (external view over S3).

    Args:
        graph_id: Graph database identifier
        table_name: Name for the table
        s3_pattern: S3 glob pattern (string) or list of S3 file paths

    Returns:
        Table creation response with status and metadata
    """
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables",
      json_data={
        "graph_id": graph_id,
        "table_name": table_name,
        "s3_pattern": s3_pattern,
      },
    )
    return response.json()

  async def list_tables(self, graph_id: str) -> List[Dict[str, Any]]:
    """
    List all DuckDB staging tables for a graph.

    Args:
        graph_id: Graph database identifier

    Returns:
        List of table info dictionaries
    """
    response = await self._request("GET", f"/databases/{graph_id}/tables")
    return response.json()

  async def query_table(
    self, graph_id: str, sql: str, parameters: Optional[List[Any]] = None
  ) -> Dict[str, Any]:
    """
    Execute SQL query on DuckDB staging tables.

    Args:
        graph_id: Graph database identifier
        sql: SQL query to execute
        parameters: Optional query parameters for safe value substitution

    Returns:
        Query results with columns and rows
    """
    json_data = {"graph_id": graph_id, "sql": sql}
    if parameters is not None:
      json_data["parameters"] = parameters

    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/query",
      json_data=json_data,
    )
    return response.json()

  async def delete_table(self, graph_id: str, table_name: str) -> Dict[str, Any]:
    """
    Delete a DuckDB staging table.

    Args:
        graph_id: Graph database identifier
        table_name: Table name to delete

    Returns:
        Deletion response
    """
    response = await self._request(
      "DELETE", f"/databases/{graph_id}/tables/{table_name}"
    )
    return response.json()

  async def ingest_table_to_graph(
    self,
    graph_id: str,
    table_name: str,
    ignore_errors: bool = True,
  ) -> Dict[str, Any]:
    """
    Ingest a DuckDB staging table into the Kuzu graph.

    Args:
        graph_id: Graph database identifier
        table_name: Table name to ingest
        ignore_errors: Continue on row errors

    Returns:
        Ingestion response with rows ingested and timing
    """
    response = await self._request(
      "POST",
      f"/databases/{graph_id}/tables/{table_name}/ingest",
      json_data={
        "ignore_errors": ignore_errors,
      },
    )
    return response.json()
