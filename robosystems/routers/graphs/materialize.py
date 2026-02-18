"""
Graph Materialization Endpoint.

This module provides graph-scoped materialization from DuckDB staging tables.
Treats LadybugDB graph as a materialized view of the mutable DuckDB data lake.

Key Features:
- Full graph rebuild from all DuckDB staging tables
- Automatic table discovery and ordering (nodes before relationships)
- Staleness tracking and clearing
- Force rebuild option
- Comprehensive error handling and logging

Workflow:
1. Discover all tables for the graph from PostgreSQL registry
2. Sort tables (nodes before relationships)
3. Ingest all tables from DuckDB to LadybugDB in order
4. Clear graph staleness flag on success
5. Return detailed materialization report

Use Cases:
- After batch uploads (files uploaded with ingest_to_graph=false)
- After cascade deletions (graph marked stale)
- Periodic full refresh to ensure consistency
- Recovery from partial ingestion failures
"""

from __future__ import annotations

import uuid
from datetime import UTC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from robosystems.middleware.auth.distributed_lock import DistributedLock

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Body,
  Depends,
  HTTPException,
  Path,
  status,
)
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from robosystems.config.shared_repositories import is_shared_repository
from robosystems.database import get_db_session
from robosystems.logger import api_logger, logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import (
  GRAPH_OR_SUBGRAPH_ID_PATTERN,
  SHARED_REPO_WRITE_ERROR_MESSAGE,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.models.api.common import ErrorResponse
from robosystems.models.iam import Graph, User

router = APIRouter(
  tags=["Materialize"],
  responses={
    401: {"description": "Not authenticated"},
    403: {"description": "Access denied to graph"},
    404: {"description": "Graph not found"},
  },
)

circuit_breaker = CircuitBreakerManager()


class MaterializeRequest(BaseModel):
  force: bool = Field(
    default=False,
    description="Force materialization even if graph is not stale",
  )
  rebuild: bool = Field(
    default=False,
    description="Delete and recreate graph database before materialization",
  )
  ignore_errors: bool = Field(
    default=True,
    description="Continue ingestion on row errors",
  )
  dry_run: bool = Field(
    default=False,
    description="Validate limits without executing materialization. Returns usage, limits, and warnings.",
  )

  class Config:
    extra = "forbid"


class MaterializeResponse(BaseModel):
  """Response for queued materialization operation."""

  status: str = Field(default="queued", description="Operation status")
  graph_id: str = Field(..., description="Graph database identifier")
  operation_id: str = Field(..., description="SSE operation ID for progress tracking")
  message: str = Field(..., description="Human-readable status message")
  limit_check: dict | None = Field(
    default=None,
    description="Limit check results (only present for dry_run requests)",
  )

  class Config:
    json_schema_extra = {
      "example": {
        "status": "queued",
        "graph_id": "kg_abc123",
        "operation_id": "550e8400-e29b-41d4-a716-446655440000",
        "message": "Materialization queued. Monitor via SSE stream.",
      }
    }


class MaterializeStatusResponse(BaseModel):
  graph_id: str = Field(..., description="Graph database identifier")
  is_stale: bool = Field(..., description="Whether graph is currently stale")
  stale_reason: str | None = Field(
    None, description="Reason for staleness if applicable"
  )
  stale_since: str | None = Field(
    None, description="When graph became stale (ISO timestamp)"
  )
  last_materialized_at: str | None = Field(
    None, description="When graph was last materialized (ISO timestamp)"
  )
  materialization_count: int = Field(
    0, description="Total number of materializations performed"
  )
  hours_since_materialization: float | None = Field(
    None, description="Hours since last materialization"
  )
  message: str = Field(..., description="Human-readable status summary")


@router.get(
  "/materialize/status",
  response_model=MaterializeStatusResponse,
  operation_id="getMaterializationStatus",
  summary="Get Materialization Status",
  description="""Get current materialization status for the graph.

Shows whether the graph is stale (DuckDB has changes not yet in graph database),
when it was last materialized, and how long since last materialization.

**Status Information:**
- Whether graph is currently stale
- Reason for staleness if applicable
- When graph became stale
- When graph was last materialized
- Total materialization count
- Hours since last materialization

**Use Cases:**
- Decide if materialization is needed
- Monitor graph freshness
- Track materialization history
- Understand data pipeline state

**Important Notes:**
- Stale graph means DuckDB has changes not in graph
- Graph becomes stale after file deletions
- Materialization clears staleness
- Status retrieval is included - no credit consumption""",
  responses={
    200: {
      "description": "Materialization status retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "graph_id": "kg_abc123",
            "is_stale": True,
            "stale_reason": "file_deleted: data.parquet from tables Fact",
            "stale_since": "2025-01-20T14:30:00Z",
            "last_materialized_at": "2025-01-20T10:00:00Z",
            "materialization_count": 5,
            "hours_since_materialization": 4.5,
            "message": "Graph is stale - materialization recommended. Last materialized 4.5 hours ago.",
          }
        }
      },
    },
    403: {
      "description": "Access denied - insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "Graph not found",
      "model": ErrorResponse,
    },
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/materialize/status",
  business_event_type="materialization_status_retrieved",
)
async def get_materialization_status(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit=Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> MaterializeStatusResponse:
  """
  Get current materialization status for the graph.

  Shows staleness, last materialization time, and whether rebuild is recommended.
  """
  from datetime import datetime

  repository = await get_universal_repository(graph_id, "read")
  if not repository:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found",
    )

  graph = Graph.get_by_id(graph_id, db)
  if not graph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Graph {graph_id} not found in database",
    )

  is_stale = graph.graph_stale or False
  stale_reason = graph.graph_stale_reason
  stale_since = graph.graph_stale_at.isoformat() if graph.graph_stale_at else None

  metadata = graph.graph_metadata or {}
  last_materialized_at = metadata.get("last_materialized_at")
  materialization_count = metadata.get("materialization_count", 0)

  hours_since_materialization = None
  if last_materialized_at:
    try:
      from dateutil import parser as date_parser

      last_mat_dt = date_parser.isoparse(last_materialized_at)
      delta = datetime.now(UTC) - last_mat_dt
      hours_since_materialization = delta.total_seconds() / 3600
    except Exception:
      pass

  if is_stale:
    message = "Graph is stale - materialization recommended."
    if hours_since_materialization:
      message += f" Last materialized {hours_since_materialization:.1f} hours ago."
    else:
      message += " Never materialized."
  else:
    if hours_since_materialization:
      message = f"Graph is fresh. Last materialized {hours_since_materialization:.1f} hours ago."
    elif materialization_count > 0:
      message = "Graph is fresh. Materialized recently."
    else:
      message = "Graph is fresh. Never required materialization."

  api_logger.info(
    "Materialization status retrieved",
    extra={
      "component": "materialize_api",
      "action": "status_retrieved",
      "user_id": str(current_user.id),
      "graph_id": graph_id,
      "is_stale": is_stale,
      "hours_since_materialization": hours_since_materialization,
    },
  )

  return MaterializeStatusResponse(
    graph_id=graph_id,
    is_stale=is_stale,
    stale_reason=stale_reason,
    stale_since=stale_since,
    last_materialized_at=last_materialized_at,
    materialization_count=materialization_count,
    hours_since_materialization=hours_since_materialization,
    message=message,
  )


@router.post(
  "/materialize",
  response_model=MaterializeResponse,
  operation_id="materializeGraph",
  summary="Materialize Graph from DuckDB",
  description="""Rebuild entire graph from DuckDB staging tables (materialized view pattern).

This endpoint rebuilds the complete graph database from the current state of DuckDB
staging tables. It automatically discovers all tables, ingests them in the correct
order (nodes before relationships), and clears the staleness flag.

**When to Use:**
- After batch uploads (files uploaded with ingest_to_graph=false)
- After cascade file deletions (graph marked stale)
- To ensure graph consistency with DuckDB state
- Periodic full refresh

**What Happens:**
1. Discovers all tables for the graph from PostgreSQL registry
2. Sorts tables (nodes before relationships)
3. Ingests all tables from DuckDB to graph in order
4. Clears staleness flag on success
5. Returns detailed materialization report

**Staleness Check:**
By default, only materializes if graph is stale (after deletions or missed ingestions).
Use `force=true` to rebuild regardless of staleness.

**Rebuild Feature:**
Setting `rebuild=true` regenerates the entire graph database from scratch:
- Deletes existing graph database
- Recreates with fresh schema from active GraphSchema
- Ingests all data files
- Safe operation - DuckDB is source of truth
- Useful for schema changes or data corrections
- Graph marked as 'rebuilding' during process

**Table Ordering:**
Node tables (PascalCase) are ingested before relationship tables (UPPERCASE) to
ensure referential integrity.

**Error Handling:**
With `ignore_errors=true` (default), continues materializing even if individual
rows fail. Failed rows are logged but don't stop the process.

**Concurrency Control:**
Only one materialization can run per graph at a time. If another materialization is in progress,
you'll receive a 409 Conflict error. The distributed lock automatically expires after
the configured TTL (default: 1 hour) to prevent deadlocks from failed materializations.

**Performance:**
Full graph materialization can take minutes for large datasets. Consider running
during off-peak hours for production systems.

**Dry Run:**
Set `dry_run=true` to validate limits without executing. Returns current usage, tier limits,
and any warnings or errors. No lock is acquired, no SSE operation is created.

**Credits:**
Materialization is included - no credit consumption""",
  responses={
    200: {
      "description": "Graph materialized successfully",
      "content": {
        "application/json": {
          "example": {
            "status": "success",
            "graph_id": "kg_abc123",
            "was_stale": True,
            "stale_reason": "file_deleted: data.parquet from tables Fact",
            "tables_materialized": ["Entity", "Fact", "ENTITY_HAS_FACT"],
            "total_rows": 125000,
            "execution_time_ms": 3456.78,
            "message": "Graph materialized successfully from 3 tables",
          }
        }
      },
    },
    400: {
      "description": "Graph not stale and force=false",
      "model": ErrorResponse,
    },
    403: {
      "description": "Access denied - shared repositories or insufficient permissions",
      "model": ErrorResponse,
    },
    404: {
      "description": "Graph not found",
      "model": ErrorResponse,
    },
    409: {
      "description": "Conflict - another materialization is already in progress for this graph",
      "model": ErrorResponse,
    },
    413: {
      "description": "Graph content limit exceeded - data too large for current tier",
      "model": ErrorResponse,
    },
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/materialize", business_event_type="graph_materialized"
)
async def materialize_graph(
  background_tasks: BackgroundTasks,
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  request: MaterializeRequest = Body(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit=Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> MaterializeResponse:
  """
  Materialize complete graph from DuckDB staging tables.

  Uses direct materialization (fast path) when enabled, falling back to
  Dagster job execution. Both paths support SSE progress tracking.

  Rebuilds entire graph from current DuckDB state, treating graph database as a
  materialized view of the mutable DuckDB data lake.
  """
  from robosystems.config.constants import INGESTION_LOCK_TTL
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.middleware.auth.distributed_lock import DistributedLock

  # Enforce graph lifecycle and subscription status (write operation)
  from robosystems.middleware.billing.enforcement import require_graph_access
  from robosystems.middleware.sse.event_storage import get_event_storage

  graph = require_graph_access(graph_id, db, require_write=True)

  circuit_breaker.check_circuit(graph_id, "graph_materialization")

  # Check for shared repository access
  if is_shared_repository(graph_id.lower()):
    logger.warning(
      f"User {current_user.id} attempted materialization on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_WRITE_ERROR_MESSAGE,
    )

  # Dry run: validate limits and return without executing
  if request.dry_run:
    from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

    graph_tier = graph.graph_tier if graph.graph_tier else "ladybug-standard"
    limit_check = await IngestionLimitChecker.check_materialization_limits(
      db=db,
      graph_id=graph_id,
      tier=graph_tier,
    )

    api_logger.info(
      "Materialization dry run completed",
      extra={
        "component": "materialize_api",
        "action": "dry_run_checked",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "allowed": limit_check["allowed"],
        "tier": graph_tier,
      },
    )

    return MaterializeResponse(
      status="dry_run",
      graph_id=graph_id,
      operation_id="dry_run",
      message="Dry run completed - no materialization executed",
      limit_check=limit_check,
    )

  # Acquire distributed lock to prevent concurrent materialization
  try:
    redis_client = create_redis_client(ValkeyDatabase.LOCKS)
    lock = DistributedLock(
      redis_client, f"graph_materialize:{graph_id}", ttl_seconds=INGESTION_LOCK_TTL
    )
    lock_result = lock.acquire(blocking=False)
    if not lock_result.acquired:
      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail="Materialization already in progress for this graph",
      )
  except HTTPException:
    raise
  except Exception as e:
    logger.warning(f"Could not acquire distributed lock for {graph_id}: {e}")
    # Proceed without lock if Redis is unavailable (degraded mode)
    lock = None

  # Everything after lock acquisition must release the lock on failure,
  # since the background task (which normally releases it) won't be dispatched.
  try:
    # Check graph content limits before proceeding
    graph_tier = graph.graph_tier if graph.graph_tier else "ladybug-standard"
    from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

    limit_check = await IngestionLimitChecker.check_materialization_limits(
      db=db,
      graph_id=graph_id,
      tier=graph_tier,
    )

    if not limit_check["allowed"]:
      raise HTTPException(
        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        detail={
          "error": "Graph content limit exceeded",
          "errors": limit_check["errors"],
          "warnings": limit_check["warnings"],
          "current_usage": limit_check["current_usage"],
          "limits": limit_check["limits"],
          "tier": graph_tier,
          "upgrade_suggestion": "Consider upgrading to a higher tier for larger data volumes",
        },
      )

    if limit_check["warnings"]:
      logger.info(f"Materialization warnings for {graph_id}: {limit_check['warnings']}")

    # Create SSE operation for progress tracking
    operation_id = str(uuid.uuid4())
    event_storage = get_event_storage()
    await event_storage.create_operation(
      operation_id=operation_id,
      operation_type="graph_materialization",
      user_id=current_user.id,
      graph_id=graph_id,
    )

    # Decide materialization method: direct (fast) vs Dagster (reliable for large data)
    use_direct = _should_use_direct_materialization(db, graph_id)

    api_logger.info(
      "Materialization job queued",
      extra={
        "component": "materialize_api",
        "action": "job_queued",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "operation_id": operation_id,
        "force": request.force,
        "rebuild": request.rebuild,
        "ignore_errors": request.ignore_errors,
        "method": "direct" if use_direct else "dagster",
      },
    )

    if use_direct:
      # Direct materialization fast path
      background_tasks.add_task(
        _run_direct_materialization,
        db=db,
        graph_id=graph_id,
        force=request.force,
        rebuild=request.rebuild,
        ignore_errors=request.ignore_errors,
        operation_id=operation_id,
        lock=lock,
      )

      return MaterializeResponse(
        status="queued",
        graph_id=graph_id,
        operation_id=operation_id,
        message="Materialization started (direct). Monitor progress via SSE stream at "
        f"/v1/operations/{operation_id}/stream",
      )

    else:
      # Dagster path for large graphs or when direct is disabled
      from robosystems.middleware.sse import build_graph_job_config

      run_config = build_graph_job_config(
        "materialize_graph_job",
        graph_id=graph_id,
        user_id=str(current_user.id),
        force=request.force,
        rebuild=request.rebuild,
        ignore_errors=request.ignore_errors,
        operation_id=operation_id,
      )

      background_tasks.add_task(
        _run_dagster_materialization,
        job_name="materialize_graph_job",
        operation_id=operation_id,
        run_config=run_config,
        lock=lock,
      )

      return MaterializeResponse(
        status="queued",
        graph_id=graph_id,
        operation_id=operation_id,
        message="Materialization queued (Dagster). Monitor progress via SSE stream at "
        f"/v1/operations/{operation_id}/stream",
      )

  except Exception:
    # Release lock if background task was never dispatched
    if lock is not None:
      lock.release()
    raise


def _should_use_direct_materialization(db: Session, graph_id: str) -> bool:
  """
  Decide whether to use the direct fast path or Dagster for materialization.

  Uses the direct path when:
  - DIRECT_GRAPH_MATERIALIZATION_ENABLED is true AND
  - Total staged data size is below GRAPH_MATERIALIZATION_THRESHOLD_MB

  Large graphs automatically route to Dagster regardless of the flag,
  since API worker background tasks aren't suited for long-running work.
  """
  from sqlalchemy import func

  from robosystems.config import env
  from robosystems.config.constants import GRAPH_MATERIALIZATION_THRESHOLD_MB
  from robosystems.models.iam import GraphFile, GraphTable

  if not env.DIRECT_GRAPH_MATERIALIZATION_ENABLED:
    return False

  # Sum file sizes for all staged (not yet materialized) files in this graph
  result = (
    db.query(func.coalesce(func.sum(GraphFile.file_size_bytes), 0))
    .join(GraphTable, GraphFile.table_id == GraphTable.id)
    .filter(
      GraphTable.graph_id == graph_id,
      GraphFile.duckdb_status == "staged",
    )
    .scalar()
  )

  total_mb = (result or 0) / (1024 * 1024)
  threshold_mb = GRAPH_MATERIALIZATION_THRESHOLD_MB

  if total_mb > threshold_mb:
    logger.info(
      f"Graph {graph_id} staged data is {total_mb:.1f}MB "
      f"(threshold: {threshold_mb}MB) - routing to Dagster"
    )
    return False

  logger.info(
    f"Graph {graph_id} staged data is {total_mb:.1f}MB "
    f"(threshold: {threshold_mb}MB) - using direct materialization"
  )
  return True


async def _run_direct_materialization(
  db: Session,
  graph_id: str,
  force: bool,
  rebuild: bool,
  ignore_errors: bool,
  operation_id: str,
  lock: DistributedLock | None,
) -> None:
  """Background task wrapper for direct materialization with lock management."""
  from robosystems.operations.lbug.direct_materialization import (
    materialize_graph_directly,
  )

  try:
    await materialize_graph_directly(
      db=db,
      graph_id=graph_id,
      force=force,
      rebuild=rebuild,
      ignore_errors=ignore_errors,
      operation_id=operation_id,
    )
  finally:
    if lock is not None:
      lock.release()


async def _run_dagster_materialization(
  job_name: str,
  operation_id: str,
  run_config: dict,
  lock: DistributedLock | None,
) -> None:
  """Background task wrapper for Dagster materialization with lock management."""
  from robosystems.middleware.sse import run_and_monitor_dagster_job

  try:
    await run_and_monitor_dagster_job(
      job_name=job_name,
      operation_id=operation_id,
      run_config=run_config,
    )
  finally:
    if lock is not None:
      lock.release()
