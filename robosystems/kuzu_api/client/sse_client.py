"""
SSE client for monitoring long-running Kuzu ingestion tasks.

This module provides an SSE client that can monitor ingestion progress
from Celery workers, handling multi-hour operations with heartbeat support.
"""

import asyncio
import json
import time
from typing import Dict, Any

import httpx
from httpx_sse import aconnect_sse

from robosystems.logger import logger


class KuzuIngestionSSEClient:
  """
  SSE client for monitoring Kuzu ingestion tasks.

  Features:
  - Connects to Kuzu API SSE endpoint
  - Handles heartbeat events to prevent timeout
  - Processes progress updates
  - Returns final result or error
  """

  def __init__(
    self,
    base_url: str,
    timeout: int = 14400,  # 4 hours default for large ingestions
  ):
    """
    Initialize SSE client.

    Args:
        base_url: Kuzu API base URL (e.g., "http://10.0.1.123:8001")
        timeout: Maximum time to wait for completion (seconds)
    """
    self.base_url = base_url.rstrip("/")
    self.timeout = timeout

  async def start_and_monitor_ingestion(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str,
    s3_credentials: Dict[str, Any] | None = None,
    ignore_errors: bool = True,
  ) -> Dict[str, Any]:
    """
    Start a background ingestion task and monitor it via SSE.

    Args:
        graph_id: Target database identifier
        table_name: Table to ingest into
        s3_pattern: S3 glob pattern for files
        s3_credentials: Optional S3 credentials
        ignore_errors: Whether to use IGNORE_ERRORS

    Returns:
        Dict with ingestion results or error
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
      try:
        # Step 1: Start the background ingestion
        logger.info(f"Starting background ingestion for {table_name} from {s3_pattern}")

        start_response = await client.post(
          f"{self.base_url}/databases/{graph_id}/ingest/background",
          json={
            "s3_pattern": s3_pattern,
            "table_name": table_name,
            "s3_credentials": s3_credentials,
            "ignore_errors": ignore_errors,
          },
        )
        start_response.raise_for_status()

        start_data = start_response.json()
        task_id = start_data["task_id"]
        sse_url = f"{self.base_url}{start_data['sse_url']}"

        logger.info(f"Started ingestion task {task_id}, monitoring via SSE...")

        # Step 2: Monitor via SSE
        return await self._monitor_via_sse(
          sse_url=sse_url, task_id=task_id, table_name=table_name
        )

      except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error starting ingestion: {e}")
        return {
          "status": "failed",
          "error": f"HTTP {e.response.status_code}: {e.response.text}",
        }
      except Exception as e:
        logger.error(f"Failed to start ingestion: {e}")
        return {"status": "failed", "error": str(e)}

  async def _monitor_via_sse(
    self, sse_url: str, task_id: str, table_name: str
  ) -> Dict[str, Any]:
    """
    Monitor ingestion progress via SSE.

    Args:
        sse_url: Full SSE endpoint URL
        task_id: Task ID to monitor
        table_name: Table name for logging

    Returns:
        Dict with results or error
    """
    start_time = time.time()
    last_heartbeat = start_time
    last_progress_log = start_time

    async with httpx.AsyncClient(timeout=httpx.Timeout(self.timeout)) as client:
      try:
        async with aconnect_sse(client, "GET", sse_url) as event_source:
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
            if current_time - start_time > self.timeout:
              logger.error(f"Ingestion timeout after {self.timeout}s for {table_name}")
              return {
                "status": "failed",
                "task_id": task_id,
                "error": f"Timeout after {self.timeout} seconds",
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
        return {
          "status": "failed",
          "task_id": task_id,
          "error": "SSE connection timeout",
        }
      except Exception as e:
        logger.error(f"SSE monitoring error: {e}")
        return {"status": "failed", "task_id": task_id, "error": str(e)}


def monitor_ingestion_sync(
  base_url: str,
  graph_id: str,
  table_name: str,
  s3_pattern: str,
  s3_credentials: Dict[str, Any] | None = None,
  ignore_errors: bool = True,
  timeout: int = 14400,  # 4 hours default
) -> Dict[str, Any]:
  """
  Synchronous wrapper for monitoring ingestion via SSE.

  This function can be called from synchronous Celery tasks.

  Args:
      base_url: Kuzu API base URL
      graph_id: Target database identifier
      table_name: Table to ingest into
      s3_pattern: S3 glob pattern for files
      s3_credentials: Optional S3 credentials
      ignore_errors: Whether to use IGNORE_ERRORS
      timeout: Maximum time to wait (seconds)

  Returns:
      Dict with ingestion results or error
  """
  client = KuzuIngestionSSEClient(base_url, timeout)

  # Run the async function in a new event loop
  return asyncio.run(
    client.start_and_monitor_ingestion(
      graph_id=graph_id,
      table_name=table_name,
      s3_pattern=s3_pattern,
      s3_credentials=s3_credentials,
      ignore_errors=ignore_errors,
    )
  )
