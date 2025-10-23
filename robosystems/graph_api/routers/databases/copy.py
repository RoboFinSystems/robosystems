"""
Direct S3 → Kuzu copy endpoint for internal/legacy operations.

This module provides:
- Direct COPY FROM S3 with user-provided credentials
- Background task execution for multi-hour ingestion
- Server-Sent Events (SSE) for real-time progress monitoring
- Heartbeat events to prevent connection timeouts
- Robust error handling and status tracking

Used by: SEC pipeline workers and other internal VPC processes.
For user-facing operations, use the file upload + DuckDB staging workflow instead.
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

from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.graph_api.core.connection_pool import get_connection_pool
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


router = APIRouter(prefix="/databases", tags=["Copy"])


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
  backend,
) -> None:
  """
  Perform the actual ingestion in the background.
  Updates task status in Redis for SSE monitoring.
  Uses backend abstraction to support both Kuzu and Neo4j.
  """
  try:
    # Update task status to running
    await task_manager.update_task(
      task_id,
      status=TaskStatus.RUNNING,
      started_at=datetime.now(timezone.utc).isoformat(),
    )

    # Use backend-specific ingestion method
    logger.info(
      f"[Task {task_id}] Starting ingestion using backend: {type(backend).__name__}"
    )

    result = await backend.ingest_from_s3(
      graph_id=graph_id,
      table_name=table_name,
      s3_pattern=s3_pattern,
      s3_credentials=s3_credentials,
      ignore_errors=ignore_errors,
    )

    records_loaded = result.get("records_loaded", 0)
    duration = result.get("duration_seconds", 0)
    query = result.get("query", "N/A")

    # For Kuzu backends, perform aggressive cleanup for large tables
    from robosystems.graph_api.backends.kuzu import KuzuBackend

    if (
      isinstance(backend, KuzuBackend) and table_name in LARGE_TABLES_REQUIRING_CLEANUP
    ):
      logger.info(
        f"[Task {task_id}] Starting aggressive memory cleanup for large table: {table_name}"
      )
      try:
        import asyncio
        import psutil
        import gc

        # Force Python garbage collection
        gc.collect()

        # Get memory stats before cleanup
        process = psutil.Process()
        mem_before = process.memory_info().rss / (1024 * 1024)

        # Allow brief settling period for memory to stabilize
        await asyncio.sleep(2)

        # Force another GC pass
        gc.collect()

        # Get memory stats after cleanup
        mem_after = process.memory_info().rss / (1024 * 1024)
        mem_freed = mem_before - mem_after

        logger.info(
          f"[Task {task_id}] Memory cleanup complete. Before: {mem_before:.1f}MB, "
          f"After: {mem_after:.1f}MB, Freed: {mem_freed:.1f}MB"
        )

        # Additional settling time for admission controller visibility
        await asyncio.sleep(3)

      except Exception as cleanup_error:
        logger.warning(
          f"[Task {task_id}] Memory cleanup encountered issue: {cleanup_error}"
        )

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

  # Get the backend instance for ingestion
  from robosystems.graph_api.backends import get_backend

  backend = get_backend()

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

  # Add background task with backend instead of connection_pool
  background_tasks.add_task(
    perform_ingestion,
    task_id=task_id,
    graph_id=graph_id,
    table_name=request.table_name,
    s3_pattern=request.s3_pattern,
    s3_credentials=request.s3_credentials,
    ignore_errors=request.ignore_errors,
    backend=backend,
  )

  logger.info(f"Started background ingestion task {task_id} for {request.table_name}")

  return {
    "task_id": task_id,
    "status": "started",
    "sse_url": f"/tasks/{task_id}/monitor",
    "message": f"Background ingestion started for {request.table_name}",
  }
