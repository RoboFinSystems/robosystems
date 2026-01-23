"""
DuckDB table management endpoints with SSE support for long-running operations.

This module provides:
- Background table creation from S3 with SSE monitoring
- Table listing, deletion, and file data management
- Heartbeat events to prevent connection timeouts

The table creation follows the same pattern as copy/backup/restore endpoints:
1. Client POSTs to create table
2. Server returns task_id + sse_url immediately
3. Client monitors via SSE until complete
"""

import asyncio
import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis_async
from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Path
from fastapi import status as http_status

from robosystems.config.valkey_registry import ValkeyDatabase
from robosystems.graph_api.core.duckdb.manager import (
  DuckDBTableManager,
  TableCreateRequest,
  TableInfo,
)
from robosystems.graph_api.models.tasks import TaskStatus
from robosystems.logger import logger

router = APIRouter(prefix="/databases/{graph_id}/tables")

table_manager = DuckDBTableManager()


class StagingTaskManager:
  """Manages background staging tasks (S3 -> DuckDB table creation) and their status."""

  def __init__(self):
    self.tasks: dict[str, dict[str, Any]] = {}
    self._redis_client = None

  async def get_redis(self) -> redis_async.Redis:
    """Get async Redis client for task storage."""
    if not self._redis_client:
      from robosystems.config.valkey_registry import create_async_redis_client

      self._redis_client = create_async_redis_client(
        ValkeyDatabase.LBUG_CACHE, decode_responses=True
      )
    return self._redis_client

  async def create_task(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str | list[str],
    file_count: int = 0,
  ) -> str:
    """Create a new staging task."""
    task_id = f"staging_{graph_id}_{table_name}_{uuid.uuid4().hex[:8]}"

    task_data = {
      "task_id": task_id,
      "graph_id": graph_id,
      "table_name": table_name,
      "s3_pattern": s3_pattern
      if isinstance(s3_pattern, str)
      else f"{len(s3_pattern)} files",
      "file_count": file_count,
      "status": TaskStatus.PENDING,
      "created_at": datetime.now(UTC).isoformat(),
      "started_at": None,
      "completed_at": None,
      "progress_percent": 0,
      "records_processed": 0,
      "estimated_records": 0,
      "last_heartbeat": time.time(),
      "error": None,
      "result": None,
      "metadata": {
        "table_name": table_name,
        "graph_id": graph_id,
      },
    }

    # Store in Redis with 24-hour TTL
    redis_client = await self.get_redis()
    await redis_client.setex(
      f"lbug:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

    return task_id

  async def update_task(self, task_id: str, **updates) -> None:
    """Update task status."""
    redis_client = await self.get_redis()

    # Get existing task
    task_json = await redis_client.get(f"lbug:task:{task_id}")
    if not task_json:
      raise ValueError(f"Task {task_id} not found")

    task_data = json.loads(task_json)

    # Update fields
    task_data.update(updates)
    task_data["last_heartbeat"] = time.time()

    # Store back in Redis
    await redis_client.setex(
      f"lbug:task:{task_id}",
      86400,  # 24 hours
      json.dumps(task_data),
    )

  async def get_task(self, task_id: str) -> dict[str, Any] | None:
    """Get task status."""
    redis_client = await self.get_redis()
    task_json = await redis_client.get(f"lbug:task:{task_id}")

    if task_json:
      return json.loads(task_json)
    return None


# Global staging task manager
staging_task_manager = StagingTaskManager()


async def perform_table_creation(
  task_id: str,
  request: TableCreateRequest,
  timeout_seconds: int = 1800,  # 30 minutes default
) -> None:
  """
  Perform the actual table creation in the background.
  Updates task status in Redis for SSE monitoring.

  Args:
      task_id: Unique task identifier for SSE tracking
      request: Table creation request with graph_id, table_name, s3_pattern
      timeout_seconds: Maximum time allowed for table creation (default 30 min)
  """
  # Import inside function to avoid circular dependency with pool initialization
  from robosystems.graph_api.core.duckdb.pool import get_duckdb_pool

  try:
    # Update task status to running
    await staging_task_manager.update_task(
      task_id,
      status=TaskStatus.RUNNING,
      started_at=datetime.now(UTC).isoformat(),
    )

    logger.info(
      f"[Task {task_id}] Starting table creation: {request.table_name} for graph {request.graph_id}"
    )

    # Perform the actual table creation with timeout
    # Run sync function in thread pool and wrap with timeout
    start_time = time.time()
    try:
      result = await asyncio.wait_for(
        asyncio.to_thread(table_manager.create_table, request),
        timeout=timeout_seconds,
      )
    except TimeoutError:
      # CRITICAL: Interrupt the DuckDB query to prevent zombie threads
      # asyncio.wait_for() raises TimeoutError but the thread keeps running
      # We must interrupt the connection to cancel the in-progress query
      try:
        pool = get_duckdb_pool()
        interrupted = pool.interrupt_connections(request.graph_id)
        logger.warning(
          f"[Task {task_id}] Timeout - interrupted {interrupted} DuckDB connection(s)"
        )
      except Exception as interrupt_err:
        logger.error(
          f"[Task {task_id}] Failed to interrupt connections: {interrupt_err}"
        )

      raise TimeoutError(
        f"Table creation timed out after {timeout_seconds}s "
        f"({timeout_seconds // 60} minutes)"
      )
    duration = time.time() - start_time

    # Update task as completed
    await staging_task_manager.update_task(
      task_id,
      status=TaskStatus.COMPLETED,
      completed_at=datetime.now(UTC).isoformat(),
      progress_percent=100,
      result={
        "status": result.status,
        "table_name": result.table_name,
        "execution_time_ms": result.execution_time_ms,
        "row_count": result.row_count,
        "duration_seconds": duration,
      },
    )

    logger.info(
      f"[Task {task_id}] Completed: table {request.table_name} created "
      f"({result.row_count:,} rows) in {duration:.2f}s"
    )

  except Exception as e:
    logger.error(f"[Task {task_id}] Failed: {e}")

    # Update task as failed
    await staging_task_manager.update_task(
      task_id,
      status=TaskStatus.FAILED,
      completed_at=datetime.now(UTC).isoformat(),
      error=str(e),
    )


async def perform_table_insert(
  task_id: str,
  request: TableCreateRequest,
  timeout_seconds: int = 1800,  # 30 minutes default
) -> None:
  """
  Perform incremental table insert in the background.
  Updates task status in Redis for SSE monitoring.

  Args:
      task_id: Unique task identifier for SSE tracking
      request: Table creation request with graph_id, table_name, s3_pattern
      timeout_seconds: Maximum time allowed for insert (default 30 min)
  """
  # Import inside function to avoid circular dependency with pool initialization
  from robosystems.graph_api.core.duckdb.pool import get_duckdb_pool

  try:
    # Update task status to running
    await staging_task_manager.update_task(
      task_id,
      status=TaskStatus.RUNNING,
      started_at=datetime.now(UTC).isoformat(),
    )

    logger.info(
      f"[Task {task_id}] Starting table insert: {request.table_name} for graph {request.graph_id}"
    )

    # Perform the actual table insert with timeout
    start_time = time.time()
    try:
      result = await asyncio.wait_for(
        asyncio.to_thread(table_manager.insert_into_table, request),
        timeout=timeout_seconds,
      )
    except TimeoutError:
      # CRITICAL: Interrupt the DuckDB query to prevent zombie threads
      try:
        pool = get_duckdb_pool()
        interrupted = pool.interrupt_connections(request.graph_id)
        logger.warning(
          f"[Task {task_id}] Timeout - interrupted {interrupted} DuckDB connection(s)"
        )
      except Exception as interrupt_err:
        logger.error(
          f"[Task {task_id}] Failed to interrupt connections: {interrupt_err}"
        )

      raise TimeoutError(
        f"Table insert timed out after {timeout_seconds}s "
        f"({timeout_seconds // 60} minutes)"
      )
    duration = time.time() - start_time

    # Update task as completed
    await staging_task_manager.update_task(
      task_id,
      status=TaskStatus.COMPLETED,
      completed_at=datetime.now(UTC).isoformat(),
      progress_percent=100,
      result={
        "status": result.status,
        "table_name": result.table_name,
        "execution_time_ms": result.execution_time_ms,
        "row_count": result.row_count,
        "duration_seconds": duration,
      },
    )

    logger.info(
      f"[Task {task_id}] Completed: inserted into {request.table_name} "
      f"({result.row_count:,} rows total) in {duration:.2f}s"
    )

  except Exception as e:
    logger.error(f"[Task {task_id}] Failed: {e}")

    # Update task as failed
    await staging_task_manager.update_task(
      task_id,
      status=TaskStatus.FAILED,
      completed_at=datetime.now(UTC).isoformat(),
      error=str(e),
    )


@router.post("")
async def create_table(
  background_tasks: BackgroundTasks,
  graph_id: str = Path(..., description="Graph database identifier"),
  request: TableCreateRequest = Body(...),
) -> dict[str, Any]:
  """
  Create a DuckDB staging table from S3 files with SSE monitoring.

  This endpoint:
  1. Creates a background task for table creation
  2. Returns a task_id immediately
  3. Client monitors progress via SSE endpoint

  For small file sets, the task completes quickly.
  For large file sets (thousands of files), it may take minutes.

  Returns:
      Dict with task_id and SSE monitoring URL
  """
  request.graph_id = graph_id

  # Calculate file count for logging
  file_count = len(request.s3_pattern) if isinstance(request.s3_pattern, list) else 1

  logger.info(
    f"Starting background table creation: {request.table_name} for graph {graph_id} "
    f"({file_count} {'files' if file_count > 1 else 'pattern'})"
  )

  # Create task
  task_id = await staging_task_manager.create_task(
    graph_id=graph_id,
    table_name=request.table_name,
    s3_pattern=request.s3_pattern,
    file_count=file_count,
  )

  # Add background task
  background_tasks.add_task(
    perform_table_creation,
    task_id=task_id,
    request=request,
  )

  logger.info(
    f"Started background staging task {task_id} for table {request.table_name}"
  )

  return {
    "task_id": task_id,
    "status": "started",
    "sse_url": f"/tasks/{task_id}/monitor",
    "message": f"Background table creation started for {request.table_name}",
  }


@router.post("/{table_name}/insert")
async def insert_into_table(
  background_tasks: BackgroundTasks,
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name to insert into"),
  request: TableCreateRequest = Body(...),
) -> dict[str, Any]:
  """
  Insert data into an existing DuckDB table from S3 files (incremental append).

  This endpoint:
  1. Creates a background task for table insert
  2. Returns a task_id immediately
  3. Client monitors progress via SSE endpoint

  Prerequisites:
  - Table must already exist (created via POST /tables)
  - Schema must be compatible with the new files

  Returns:
      Dict with task_id and SSE monitoring URL
  """
  request.graph_id = graph_id
  request.table_name = table_name

  # Calculate file count for logging
  file_count = len(request.s3_pattern) if isinstance(request.s3_pattern, list) else 1

  logger.info(
    f"Starting background table insert: {table_name} for graph {graph_id} "
    f"({file_count} {'files' if file_count > 1 else 'pattern'})"
  )

  # Create task
  task_id = await staging_task_manager.create_task(
    graph_id=graph_id,
    table_name=table_name,
    s3_pattern=request.s3_pattern,
    file_count=file_count,
  )

  # Add background task
  background_tasks.add_task(
    perform_table_insert,
    task_id=task_id,
    request=request,
  )

  logger.info(f"Started background insert task {task_id} for table {table_name}")

  return {
    "task_id": task_id,
    "status": "started",
    "sse_url": f"/tasks/{task_id}/monitor",
    "message": f"Background table insert started for {table_name}",
  }


@router.get("", response_model=list[TableInfo])
async def list_tables(
  graph_id: str = Path(..., description="Graph database identifier"),
) -> list[TableInfo]:
  logger.info(f"Listing tables for graph {graph_id}")

  try:
    return table_manager.list_tables(graph_id)
  except Exception as e:
    logger.error(f"Failed to list tables for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list tables: {e!s}",
    )


@router.delete("/{table_name}")
async def delete_table(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name"),
) -> dict:
  logger.info(f"Deleting table {table_name} from graph {graph_id}")

  try:
    return table_manager.delete_table(graph_id, table_name)
  except Exception as e:
    logger.error(f"Failed to delete table {table_name}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to delete table: {e!s}",
    )


@router.delete("/{table_name}/files/{file_id}")
async def delete_file_data(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name"),
  file_id: str = Path(..., description="File ID to delete rows for"),
) -> dict:
  logger.info(
    f"Deleting file data for file_id={file_id} from table {table_name} in graph {graph_id}"
  )

  try:
    return table_manager.delete_file_data(graph_id, table_name, file_id)
  except Exception as e:
    logger.error(
      f"Failed to delete file data for file_id={file_id} from table {table_name}: {e}"
    )
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to delete file data: {e!s}",
    )


# ============================================================================
# Staging Progress Tracking Endpoints
# ============================================================================


@router.get("/staging/progress")
async def get_staging_progress(
  graph_id: str = Path(..., description="Graph database identifier"),
) -> dict:
  """
  Get staging progress summary for a graph.

  Returns min/max filing dates staged, total dates, and total rows.
  Used by incremental staging sensor to determine what's already staged.
  """
  logger.info(f"Getting staging progress for graph {graph_id}")

  try:
    return table_manager.get_staging_progress_summary(graph_id)
  except Exception as e:
    logger.error(f"Failed to get staging progress for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to get staging progress: {e!s}",
    )


@router.get("/staging/progress/dates")
async def get_staged_dates(
  graph_id: str = Path(..., description="Graph database identifier"),
) -> list[str]:
  """
  Get all filing dates that have been successfully staged.

  Returns a list of YYYY-MM-DD date strings.
  Used by incremental staging sensor to diff against S3.
  """
  logger.info(f"Getting staged dates for graph {graph_id}")

  try:
    return table_manager.get_staged_dates(graph_id)
  except Exception as e:
    logger.error(f"Failed to get staged dates for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to get staged dates: {e!s}",
    )


@router.post("/staging/progress")
async def record_staging_progress(
  graph_id: str = Path(..., description="Graph database identifier"),
  filing_date: str = Body(..., embed=True, description="Filing date (YYYY-MM-DD)"),
  table_count: int = Body(0, embed=True, description="Number of tables staged"),
  total_rows: int = Body(0, embed=True, description="Total rows inserted"),
  status: str = Body("complete", embed=True, description="Status: complete or failed"),
) -> dict:
  """
  Record that a filing date has been successfully staged.

  Called after incremental staging completes for a specific date.
  """
  logger.info(f"Recording staging progress for {filing_date} in graph {graph_id}")

  try:
    return table_manager.record_staging_progress(
      graph_id=graph_id,
      filing_date=filing_date,
      table_count=table_count,
      total_rows=total_rows,
      status=status,
    )
  except Exception as e:
    logger.error(f"Failed to record staging progress for {filing_date}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to record staging progress: {e!s}",
    )
