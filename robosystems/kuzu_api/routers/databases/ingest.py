"""
SSE-enabled ingestion endpoint for long-running operations.

This module provides:
- Background task execution for multi-hour ingestion
- Server-Sent Events (SSE) for real-time progress monitoring
- Heartbeat events to prevent connection timeouts
- Robust error handling and status tracking
"""

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from enum import Enum

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  HTTPException,
  Path as PathParam,
  status as http_status,
)
from pydantic import BaseModel, Field

from robosystems.kuzu_api.core.cluster_manager import get_cluster_service
from robosystems.kuzu_api.core.connection_pool import get_connection_pool
from robosystems.logger import logger
from robosystems.config.valkey_registry import ValkeyDatabase
from robosystems.config import env
import redis.asyncio as redis_async


# Tables that require aggressive memory cleanup after ingestion
# This is configurable via environment variable to support different use cases
# Default list is empty - specific implementations (like SEC) should define their own
import os

_large_tables_env = os.getenv("KUZU_LARGE_TABLES_REQUIRING_CLEANUP", "")
LARGE_TABLES_REQUIRING_CLEANUP = (
  [t.strip() for t in _large_tables_env.split(",") if t.strip()]
  if _large_tables_env
  else []
)


class TaskStatus(str, Enum):
  """Task execution status."""

  PENDING = "pending"
  RUNNING = "running"
  COMPLETED = "completed"
  FAILED = "failed"


class BackgroundIngestRequest(BaseModel):
  """Request for background ingestion with SSE monitoring."""

  s3_pattern: str = Field(
    ...,
    description="S3 glob pattern for bulk loading (e.g., s3://bucket/path/*.parquet)",
  )
  table_name: str = Field(..., description="Target table name")
  s3_credentials: Optional[dict] = Field(
    None, description="S3 credentials for LocalStack/MinIO"
  )
  ignore_errors: bool = Field(
    True, description="Use IGNORE_ERRORS for duplicate handling"
  )

  class Config:
    json_schema_extra = {
      "example": {
        "s3_pattern": "s3://robosystems-sec-processed/consolidated/nodes/Fact/batch_*.parquet",
        "table_name": "Fact",
        "ignore_errors": True,
        "s3_credentials": {
          "aws_access_key_id": "test",
          "aws_secret_access_key": "test",
          "endpoint_url": "http://localhost:4566",
          "region": "us-east-1",
        },
      }
    }


router = APIRouter(prefix="/databases", tags=["Data Copy"])


class IngestionTaskManager:
  """Manages background ingestion tasks and their status."""

  def __init__(self):
    self.tasks: Dict[str, Dict[str, Any]] = {}
    self._redis_client = None
    self._redis_url = None

  async def get_redis(self) -> redis_async.Redis:
    """Get async Redis client for task storage."""
    if not self._redis_client:
      # Use dedicated database for Kuzu tasks
      # Use async factory method to handle SSL params correctly
      from robosystems.config.valkey_registry import create_async_redis_client

      self._redis_client = create_async_redis_client(
        ValkeyDatabase.KUZU_CACHE, decode_responses=True
      )
    return self._redis_client

  async def create_task(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str,
    estimated_records: int = 0,
    estimated_size_mb: float = 0,
  ) -> str:
    """Create a new ingestion task."""
    task_id = f"ingest_{graph_id}_{table_name}_{uuid.uuid4().hex[:8]}"

    task_data = {
      "task_id": task_id,
      "graph_id": graph_id,
      "table_name": table_name,
      "s3_pattern": s3_pattern,
      "status": TaskStatus.PENDING,
      "created_at": datetime.now(timezone.utc).isoformat(),
      "started_at": None,
      "completed_at": None,
      "progress_percent": 0,
      "records_processed": 0,
      "estimated_records": estimated_records,
      "estimated_size_mb": estimated_size_mb,
      "last_heartbeat": time.time(),
      "error": None,
      "result": None,
    }

    # Store in Redis with 24-hour TTL
    redis_client = await self.get_redis()
    await redis_client.setex(
      f"kuzu:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

    return task_id

  async def update_task(self, task_id: str, **updates) -> None:
    """Update task status."""
    redis_client = await self.get_redis()

    # Get existing task
    task_json = await redis_client.get(f"kuzu:task:{task_id}")
    if not task_json:
      raise ValueError(f"Task {task_id} not found")

    task_data = json.loads(task_json)

    # Update fields
    task_data.update(updates)
    task_data["last_heartbeat"] = time.time()

    # Store back in Redis
    await redis_client.setex(
      f"kuzu:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

  async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
    """Get task status."""
    redis_client = await self.get_redis()
    task_json = await redis_client.get(f"kuzu:task:{task_id}")

    if task_json:
      return json.loads(task_json)
    return None


# Global task manager
task_manager = IngestionTaskManager()


async def perform_ingestion(
  task_id: str,
  graph_id: str,
  table_name: str,
  s3_pattern: str,
  s3_credentials: Optional[dict],
  ignore_errors: bool,
  connection_pool,
) -> None:
  """
  Perform the actual ingestion in the background.
  Updates task status in Redis for SSE monitoring.
  """
  try:
    # Update task status to running
    await task_manager.update_task(
      task_id,
      status=TaskStatus.RUNNING,
      started_at=datetime.now(timezone.utc).isoformat(),
    )

    # Get connection from pool using context manager
    with connection_pool.get_connection(graph_id) as conn:
      # Load httpfs extension for S3 support
      try:
        # Check if httpfs is already loaded
        result = conn.execute("CALL show_loaded_extensions() RETURN *")
        loaded_extensions = []
        while result.has_next():
          loaded_extensions.append(str(result.get_next()).lower())

        if "httpfs" not in loaded_extensions:
          # Install and load httpfs extension
          try:
            conn.execute("INSTALL httpfs")
            logger.debug("Installed httpfs extension for S3 support")
          except Exception as e:
            logger.debug(f"Could not install httpfs (may already be installed): {e}")

          conn.execute("LOAD httpfs")
          logger.debug("Loaded httpfs extension")
        else:
          logger.debug("httpfs extension already loaded")
      except Exception as e:
        if "already loaded" not in str(e).lower():
          logger.warning(f"Could not load httpfs extension: {e}")
          # Try one more time
          try:
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")
            logger.debug("Successfully loaded httpfs on retry")
          except Exception as retry_error:
            logger.error(f"Failed to load httpfs after retry: {retry_error}")
            raise Exception(f"httpfs extension required for S3 access: {retry_error}")

      # Set S3 credentials using CALL statements (Kuzu's way)
      # SECURITY: Escape single quotes to prevent SQL injection
      if s3_credentials:
        # Only set credentials that are provided
        if s3_credentials.get("aws_access_key_id"):
          # Escape single quotes to prevent SQL injection
          escaped_key = s3_credentials["aws_access_key_id"].replace("'", "''")
          conn.execute(f"CALL s3_access_key_id = '{escaped_key}'")
        if s3_credentials.get("aws_secret_access_key"):
          # Escape single quotes to prevent SQL injection
          escaped_secret = s3_credentials["aws_secret_access_key"].replace("'", "''")
          conn.execute(f"CALL s3_secret_access_key = '{escaped_secret}'")
        if s3_credentials.get("region"):
          # Escape single quotes to prevent SQL injection
          escaped_region = s3_credentials["region"].replace("'", "''")
          conn.execute(f"CALL s3_region = '{escaped_region}'")
        if s3_credentials.get("endpoint_url"):
          endpoint = s3_credentials["endpoint_url"]
          # Remove protocol prefix for Kuzu - it will add https:// by default
          # For LocalStack with HTTP, we need to strip the protocol
          if endpoint.startswith("http://"):
            endpoint = endpoint[7:]
          elif endpoint.startswith("https://"):
            endpoint = endpoint[8:]
          # Escape single quotes to prevent SQL injection
          escaped_endpoint = endpoint.replace("'", "''")
          conn.execute(f"CALL s3_endpoint = '{escaped_endpoint}'")
          conn.execute("CALL s3_url_style = 'path'")  # For LocalStack/MinIO
          logger.debug(f"Set S3 endpoint to: {endpoint} (path style URLs)")

        # Set S3 performance configurations
        conn.execute("CALL s3_uploader_threads_limit = 8")
        conn.execute("CALL s3_uploader_max_num_parts_per_file = 10000")
        conn.execute("CALL s3_uploader_max_filesize = 10737418240")  # 10GB

        # CRITICAL: Memory management settings for large ingestions
        # Ensure spill_to_disk is enabled to handle memory overflow
        conn.execute("CALL spill_to_disk = true")

        # Set timeout to 30 minutes for large COPY operations
        # The default 10-minute timeout may be too short for bulk data ingestion
        conn.execute("CALL timeout=1800000")  # 30 minutes in milliseconds

        logger.debug(
          "Configured S3 performance settings, memory management, and 30-minute timeout for bulk ingestion"
        )
      # Build COPY query with S3 pattern
      # Note: S3 credentials are already set via CALL statements above
      # Kuzu uses those for S3 access, not inline parameters
      query = f'COPY {table_name} FROM "{s3_pattern}"'

      # Add IGNORE_ERRORS for duplicate handling
      if ignore_errors:
        if "(" in query:
          query = query[:-1] + ", IGNORE_ERRORS=TRUE)"
        else:
          query += " (IGNORE_ERRORS=TRUE)"

      logger.info(f"[Task {task_id}] Executing: {query}")

      # Execute the COPY command
      # This is a blocking operation that could take hours
      start_time = time.time()
      result = conn.execute(query)

      duration = time.time() - start_time

      # Parse result to get records loaded
      records_loaded = 0
      if result and hasattr(result, "get_as_list"):
        result_list = result.get_as_list()
        if result_list and len(result_list) > 0:
          # Try to extract record count from result
          result_str = str(result_list[0])
          if "Records loaded:" in result_str:
            try:
              records_loaded = int(
                result_str.split("Records loaded:")[-1].strip().split()[0]
              )
            except (ValueError, IndexError):
              pass

      # CRITICAL: Force checkpoint after large COPY operations
      # This flushes the Write-Ahead Log (WAL) to disk and can help with memory management
      try:
        # Execute checkpoint to flush WAL to disk
        conn.execute("CHECKPOINT;")
        logger.debug(f"[Task {task_id}] Executed checkpoint to flush WAL to disk")
      except Exception as checkpoint_error:
        logger.warning(
          f"[Task {task_id}] Failed to execute checkpoint: {checkpoint_error}"
        )

      # For very large tables (>100MB), force connection pool cleanup to release memory
      # This is especially important for SEC data ingestion with multiple large files
      if table_name in LARGE_TABLES_REQUIRING_CLEANUP:
        try:
          # Force aggressive cleanup of connections and database object to release buffer pool memory
          # This includes multiple GC passes and memory trimming back to OS
          connection_pool.force_database_cleanup(graph_id, aggressive=True)
          logger.info(
            f"[Task {task_id}] Forced aggressive database cleanup for {graph_id} after loading large table {table_name}"
          )

          # Memory settlement delay after large table ingestion
          #
          # Why different settlement times?
          # - Production (10s): Kuzu's buffer pool manager needs time to release memory back to the OS
          #   after large ingestions. The admission controller monitors memory usage and will reject
          #   new tasks if memory appears high. A 10-second delay ensures memory metrics stabilize.
          # - Development (0.1s): Local development has smaller datasets and no admission controller.
          #   A minimal symbolic delay improves developer experience without affecting stability.
          import time as time_module

          if env.is_development():
            settlement_time = (
              0.1  # Minimal delay for faster local development iteration
            )
          else:
            settlement_time = (
              10  # Production delay for memory stabilization and admission controller
            )

          if settlement_time > 0:
            logger.info(
              f"[Task {task_id}] Waiting {settlement_time}s for memory to fully settle after {table_name} ingestion"
            )
            time_module.sleep(settlement_time)

          # Log memory status after settlement
          try:
            import psutil

            process = psutil.Process()
            mem_info = process.memory_info()
            logger.info(
              f"[Task {task_id}] Memory after settlement - RSS: {mem_info.rss / (1024 * 1024):.1f}MB, "
              f"VMS: {mem_info.vms / (1024 * 1024):.1f}MB"
            )
          except ImportError:
            pass

        except Exception as cleanup_error:
          logger.warning(
            f"[Task {task_id}] Could not force database cleanup: {cleanup_error}"
          )

    # Final cleanup and memory settlement before marking task complete
    # This ensures the admission controller sees low memory for the next task
    try:
      # One more aggressive cleanup at the end
      connection_pool.force_database_cleanup(graph_id, aggressive=True)
      logger.info(f"[Task {task_id}] Final cleanup completed for {graph_id}")

      # Final settlement time before task completion
      # This shorter delay (5s vs 10s) ensures the admission controller sees stable memory
      # before the next task starts, while being less aggressive than post-large-table delays
      import time as time_module

      if env.is_development():
        final_settlement = 0.1  # Minimal delay for faster local development
      else:
        final_settlement = 5  # Shorter production delay for final memory stabilization

      if final_settlement > 0:
        logger.info(
          f"[Task {task_id}] Final {final_settlement}s settlement before task completion"
        )
        time_module.sleep(final_settlement)

      # Log final memory state
      try:
        import psutil

        process = psutil.Process()
        mem_info = process.memory_info()
        logger.info(
          f"[Task {task_id}] Final memory state - RSS: {mem_info.rss / (1024 * 1024):.1f}MB, "
          f"VMS: {mem_info.vms / (1024 * 1024):.1f}MB"
        )
      except ImportError:
        pass
    except Exception as final_cleanup_error:
      logger.warning(f"[Task {task_id}] Final cleanup failed: {final_cleanup_error}")

    # Clear the ingestion flag now that we're done
    try:
      redis_client = await task_manager.get_redis()
      instance_id = env.EC2_INSTANCE_ID or "unknown"
      ingestion_key = f"kuzu:ingestion:active:{instance_id}"
      await redis_client.delete(ingestion_key)
      logger.info(f"[Task {task_id}] Cleared ingestion flag for instance {instance_id}")
    except Exception as flag_error:
      logger.warning(f"[Task {task_id}] Could not clear ingestion flag: {flag_error}")

    # Update task as completed
    await task_manager.update_task(
      task_id,
      status=TaskStatus.COMPLETED,
      completed_at=datetime.now(timezone.utc).isoformat(),
      progress_percent=100,
      records_processed=records_loaded,
      result={
        "records_loaded": records_loaded,
        "duration_seconds": duration,
        "query": query,
      },
    )

    # Log completion with appropriate message based on whether we have accurate counts
    if records_loaded > 0:
      logger.info(
        f"[Task {task_id}] Completed: {records_loaded:,} records loaded in {duration:.2f}s"
      )
    elif ignore_errors:
      # With IGNORE_ERRORS, we don't get accurate insertion counts
      logger.info(
        f"[Task {task_id}] Completed in {duration:.2f}s (with IGNORE_ERRORS - insertion count unavailable)"
      )
    else:
      # Without IGNORE_ERRORS, 0 means no new records (possibly already loaded)
      logger.info(f"[Task {task_id}] Completed in {duration:.2f}s")

  except Exception as e:
    logger.error(f"[Task {task_id}] Failed: {e}")

    # Clear the ingestion flag even on failure
    try:
      redis_client = await task_manager.get_redis()
      instance_id = env.EC2_INSTANCE_ID or "unknown"
      ingestion_key = f"kuzu:ingestion:active:{instance_id}"
      await redis_client.delete(ingestion_key)
      logger.info(
        f"[Task {task_id}] Cleared ingestion flag for instance {instance_id} after failure"
      )
    except Exception as flag_error:
      logger.warning(
        f"[Task {task_id}] Could not clear ingestion flag after failure: {flag_error}"
      )

    # Update task as failed
    await task_manager.update_task(
      task_id,
      status=TaskStatus.FAILED,
      completed_at=datetime.now(timezone.utc).isoformat(),
      error=str(e),
    )


@router.post("/{graph_id}/copy")
async def start_background_copy(
  request: BackgroundIngestRequest,
  background_tasks: BackgroundTasks,
  graph_id: str = PathParam(..., description="Graph database identifier"),
  cluster_service=Depends(get_cluster_service),
  connection_pool=Depends(get_connection_pool),
) -> Dict[str, Any]:
  """
  Start a background copy operation with SSE monitoring support.

  This endpoint:
  1. Creates a background task for data copying/ingestion
  2. Returns a task_id immediately
  3. Client can monitor progress via SSE endpoint

  Returns:
      Dict with task_id and SSE monitoring URL
  """
  if cluster_service.read_only:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Cannot ingest data: node is in read-only mode",
    )

  # Estimate data size (simplified - in production, query S3 for actual size)
  estimated_records = 1000000  # Default estimate
  estimated_size_mb = 100.0  # Default estimate

  # Create task
  task_id = await task_manager.create_task(
    graph_id=graph_id,
    table_name=request.table_name,
    s3_pattern=request.s3_pattern,
    estimated_records=estimated_records,
    estimated_size_mb=estimated_size_mb,
  )

  # Set ingestion flag in Redis for health check awareness
  # This prevents the instance from being marked unhealthy during heavy operations
  redis_client = await task_manager.get_redis()
  instance_id = env.EC2_INSTANCE_ID or "unknown"
  ingestion_key = f"kuzu:ingestion:active:{instance_id}"

  # Store ingestion metadata with 1-hour TTL (in case cleanup fails)
  ingestion_data = {
    "task_id": task_id,
    "table_name": request.table_name,
    "graph_id": graph_id,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "is_large_table": request.table_name.lower() in ["fact", "factdimension", "report"],
  }
  await redis_client.setex(ingestion_key, 3600, json.dumps(ingestion_data))
  logger.info(
    f"Set ingestion flag for instance {instance_id} - table: {request.table_name}"
  )

  # Add background task
  background_tasks.add_task(
    perform_ingestion,
    task_id=task_id,
    graph_id=graph_id,
    table_name=request.table_name,
    s3_pattern=request.s3_pattern,
    s3_credentials=request.s3_credentials,
    ignore_errors=request.ignore_errors,
    connection_pool=connection_pool,
  )

  logger.info(f"Started background ingestion task {task_id} for {request.table_name}")

  return {
    "task_id": task_id,
    "status": "started",
    "sse_url": f"/tasks/{task_id}/monitor",
    "message": f"Background ingestion started for {request.table_name}",
  }
