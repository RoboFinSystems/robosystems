"""
Graph Materialization Endpoint.

This module provides graph-scoped materialization from DuckDB staging tables.
Treats Kuzu graph as a materialized view of the mutable DuckDB data lake.

Key Features:
- Full graph rebuild from all DuckDB staging tables
- Automatic table discovery and ordering (nodes before relationships)
- Staleness tracking and clearing
- Force rebuild option
- Comprehensive error handling and logging

Workflow:
1. Discover all tables for the graph from PostgreSQL registry
2. Sort tables (nodes before relationships)
3. Ingest all tables from DuckDB to Kuzu in order
4. Clear graph staleness flag on success
5. Return detailed materialization report

Use Cases:
- After batch uploads (files uploaded with ingest_to_graph=false)
- After cascade deletions (graph marked stale)
- Periodic full refresh to ensure consistency
- Recovery from partial ingestion failures
"""

from fastapi import APIRouter, Depends, HTTPException, Path, Body, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from robosystems.models.iam import Graph, GraphTable, GraphSchema, User
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.middleware.graph import get_universal_repository
from robosystems.middleware.graph.types import (
  GRAPH_OR_SUBGRAPH_ID_PATTERN,
  GraphTypeRegistry,
  SHARED_REPO_WRITE_ERROR_MESSAGE,
)
from robosystems.database import get_db_session
from robosystems.graph_api.client.factory import get_graph_client
from robosystems.logger import logger, api_logger
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.config import env
from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  create_async_redis_client,
)

router = APIRouter(
  tags=["Materialization"],
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

  class Config:
    extra = "forbid"


class MaterializeResponse(BaseModel):
  status: str = Field(..., description="Materialization status")
  graph_id: str = Field(..., description="Graph database identifier")
  was_stale: bool = Field(
    ..., description="Whether graph was stale before materialization"
  )
  stale_reason: str | None = Field(None, description="Reason graph was stale")
  tables_materialized: list[str] = Field(
    ..., description="List of tables successfully materialized"
  )
  total_rows: int = Field(..., description="Total rows materialized across all tables")
  execution_time_ms: float = Field(..., description="Total materialization time")
  message: str = Field(..., description="Human-readable status message")


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
  from datetime import datetime, timezone

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
      delta = datetime.now(timezone.utc) - last_mat_dt
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
    500: {"description": "Internal server error"},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/materialize", business_event_type="graph_materialized"
)
async def materialize_graph(
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

  Rebuilds entire graph from current DuckDB state, treating graph database as a
  materialized view of the mutable DuckDB data lake.
  """
  import time
  from datetime import datetime, timezone

  start_time_dt = datetime.now(timezone.utc)
  start_time = time.time()

  circuit_breaker.check_circuit(graph_id, "graph_materialization")

  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    logger.warning(
      f"User {current_user.id} attempted materialization on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_WRITE_ERROR_MESSAGE,
    )

  redis_client = create_async_redis_client(ValkeyDatabase.DISTRIBUTED_LOCKS)
  lock_key = f"materialization_lock:{graph_id}"
  lock_ttl = env.INGESTION_LOCK_TTL
  lock_acquired = False

  try:
    lock_acquired = await redis_client.set(
      lock_key,
      f"{current_user.id}:{start_time_dt.isoformat()}",
      nx=True,
      ex=lock_ttl,
    )

    if not lock_acquired:
      lock_info = await redis_client.get(lock_key)
      lock_timestamp = "unknown"
      if lock_info and ":" in lock_info:
        try:
          lock_timestamp = lock_info.split(":", 1)[1]
        except (IndexError, ValueError):
          lock_timestamp = "unknown"

      api_logger.warning(
        "Materialization already in progress",
        extra={
          "component": "materialize_api",
          "action": "materialization_blocked",
          "user_id": str(current_user.id),
          "graph_id": graph_id,
          "lock_holder": lock_info,
          "lock_timestamp": lock_timestamp,
        },
      )

      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"Another materialization is already in progress for graph {graph_id}. "
        f"Lock acquired at: {lock_timestamp}. "
        f"Please wait for it to complete before starting a new one. "
        f"The lock will automatically expire after {lock_ttl} seconds if the materialization fails.",
      )

    api_logger.info(
      "Materialization lock acquired",
      extra={
        "component": "materialize_api",
        "action": "lock_acquired",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "lock_ttl": lock_ttl,
      },
    )

    logger.info(
      f"Graph materialization requested for {graph_id} "
      f"(force={request.force}, rebuild={request.rebuild}, ignore_errors={request.ignore_errors})"
    )

    # Verify graph exists
    repository = await get_universal_repository(graph_id, "write")
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

    # Check staleness
    was_stale = graph.graph_stale or False
    stale_reason = graph.graph_stale_reason

    if not was_stale and not request.force and not request.rebuild:
      logger.info(
        f"Graph {graph_id} is not stale and force=false, rebuild=false - skipping materialization"
      )
      return MaterializeResponse(
        status="skipped",
        graph_id=graph_id,
        was_stale=False,
        stale_reason=None,
        tables_materialized=[],
        total_rows=0,
        execution_time_ms=0,
        message="Graph is fresh - no materialization needed. Use force=true to rebuild anyway.",
      )

    api_logger.info(
      "Materialization started",
      extra={
        "component": "materialize_api",
        "action": "materialization_started",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "was_stale": was_stale,
        "stale_reason": stale_reason,
        "force": request.force,
        "rebuild": request.rebuild,
        "ignore_errors": request.ignore_errors,
      },
    )

    logger.info(
      f"Starting full graph materialization for {graph_id} "
      f"(was_stale={was_stale}, reason={stale_reason}, rebuild={request.rebuild})"
    )

    # Get Graph API client
    client = await get_graph_client(graph_id=graph_id, operation_type="write")

    # Handle rebuild if requested
    if request.rebuild:
      logger.info(
        f"Rebuild requested for {graph_id} - regenerating entire graph database from DuckDB"
      )

      api_logger.info(
        "Database rebuild initiated",
        extra={
          "component": "materialize_api",
          "action": "rebuild_started",
          "user_id": str(current_user.id),
          "graph_id": graph_id,
        },
      )

      graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
      graph_metadata["status"] = "rebuilding"
      graph_metadata["rebuild_started_at"] = time.time()
      graph.graph_metadata = graph_metadata
      db.commit()

      try:
        logger.info(f"Deleting graph database for {graph_id}")
        await client.delete_database(graph_id)

        schema = GraphSchema.get_active_schema(graph_id, db)
        if not schema:
          raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No schema found for graph {graph_id}",
          )

        schema_type_for_rebuild = "custom" if schema.schema_ddl else "entity"
        logger.info(
          f"Recreating graph database with schema type: {schema_type_for_rebuild} (original: {schema.schema_type})"
        )
        await client.create_database(
          graph_id=graph_id,
          schema_type=schema_type_for_rebuild,
          custom_schema_ddl=schema.schema_ddl,
        )

        api_logger.info(
          "Database rebuild completed",
          extra={
            "component": "materialize_api",
            "action": "rebuild_completed",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "schema_type": schema.schema_type,
          },
        )

        logger.info(f"Graph database recreated successfully for {graph_id}")

      except Exception as e:
        graph_metadata["status"] = "rebuild_failed"
        graph_metadata["rebuild_failed_at"] = time.time()
        graph_metadata["rebuild_error"] = str(e)
        graph.graph_metadata = graph_metadata
        db.commit()

        api_logger.error(
          "Database rebuild failed",
          extra={
            "component": "materialize_api",
            "action": "rebuild_failed",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "error_type": type(e).__name__,
            "error_message": str(e),
          },
        )

        logger.error(f"Failed to rebuild graph {graph_id}: {e}")
        raise HTTPException(
          status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
          detail=f"Failed to rebuild graph database: {str(e)}",
        )

    # Get all tables for this graph
    all_tables = GraphTable.get_all_for_graph(graph_id, db)

    if not all_tables:
      logger.warning(f"No tables found for graph {graph_id}")
      return MaterializeResponse(
        status="success",
        graph_id=graph_id,
        was_stale=was_stale,
        stale_reason=stale_reason,
        tables_materialized=[],
        total_rows=0,
        execution_time_ms=(time.time() - start_time) * 1000,
        message="No tables to materialize",
      )

    # Sort tables: nodes before relationships
    # Relationship tables are typically all uppercase (e.g., ENTITY_HAS_FACT)
    # Node tables are typically PascalCase (e.g., Entity, Fact)
    table_names = [t.table_name for t in all_tables]
    node_tables = [t for t in table_names if not t.isupper()]
    rel_tables = [t for t in table_names if t.isupper()]
    ordered_tables = node_tables + rel_tables

    logger.info(
      f"Discovered {len(ordered_tables)} tables to materialize: "
      f"Node tables ({len(node_tables)}): {node_tables}, "
      f"Relationship tables ({len(rel_tables)}): {rel_tables}"
    )

    api_logger.info(
      "Processing tables for materialization",
      extra={
        "component": "materialize_api",
        "action": "tables_processing",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "total_tables": len(ordered_tables),
        "node_tables": len(node_tables),
        "relationship_tables": len(rel_tables),
      },
    )

    # Materialize each table
    tables_materialized = []
    total_rows = 0

    for table_name in ordered_tables:
      try:
        logger.info(f"Materializing table {table_name} from DuckDB to graph")

        result = await client.materialize_table(
          graph_id=graph_id,
          table_name=table_name,
          ignore_errors=request.ignore_errors,
          file_ids=None,
        )

        rows_ingested = result.get("rows_ingested", 0)
        total_rows += rows_ingested
        tables_materialized.append(table_name)

        logger.info(f"Materialized {table_name}: {rows_ingested:,} rows")

      except Exception as e:
        logger.error(f"Failed to materialize table {table_name}: {e}")
        if not request.ignore_errors:
          raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Materialization failed on table {table_name}: {str(e)}",
          )

    # Mark graph as fresh
    graph.mark_fresh(session=db)
    logger.info(f"Graph {graph_id} marked as fresh after successful materialization")

    # Update graph metadata if rebuild was performed
    if request.rebuild:
      graph_metadata = {**graph.graph_metadata} if graph.graph_metadata else {}
      graph_metadata["status"] = "available"
      graph_metadata["rebuild_completed_at"] = time.time()
      if "rebuild_started_at" in graph_metadata:
        rebuild_duration = (
          graph_metadata["rebuild_completed_at"] - graph_metadata["rebuild_started_at"]
        )
        graph_metadata["last_rebuild_duration_seconds"] = rebuild_duration
      graph.graph_metadata = graph_metadata
      db.commit()
      logger.info(f"Graph {graph_id} marked as available after rebuild")

    execution_time_ms = (time.time() - start_time) * 1000

    logger.info(
      f"Graph materialization complete: {len(tables_materialized)} tables, "
      f"{total_rows:,} rows in {execution_time_ms:.2f}ms"
    )

    circuit_breaker.record_success(graph_id, "graph_materialization")

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/materialize",
      method="POST",
      event_type="graph_materialized_success",
      event_data={
        "graph_id": graph_id,
        "total_tables": len(ordered_tables),
        "tables_materialized": len(tables_materialized),
        "total_rows": total_rows,
        "execution_time_ms": execution_time_ms,
        "was_stale": was_stale,
        "rebuild": request.rebuild,
      },
      user_id=current_user.id,
    )

    api_logger.info(
      "Materialization completed",
      extra={
        "component": "materialize_api",
        "action": "materialization_completed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time_ms,
        "total_tables": len(ordered_tables),
        "tables_materialized": len(tables_materialized),
        "total_rows": total_rows,
        "was_stale": was_stale,
        "rebuild": request.rebuild,
        "success": True,
      },
    )

    return MaterializeResponse(
      status="success",
      graph_id=graph_id,
      was_stale=was_stale,
      stale_reason=stale_reason,
      tables_materialized=tables_materialized,
      total_rows=total_rows,
      execution_time_ms=execution_time_ms,
      message=f"Graph materialized successfully from {len(tables_materialized)} tables",
    )

  except HTTPException:
    circuit_breaker.record_failure(graph_id, "graph_materialization")
    raise

  except Exception as e:
    circuit_breaker.record_failure(graph_id, "graph_materialization")

    execution_time = (datetime.now(timezone.utc) - start_time_dt).total_seconds() * 1000

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/materialize",
      method="POST",
      event_type="graph_materialization_failed",
      event_data={
        "graph_id": graph_id,
        "error_type": type(e).__name__,
        "error_message": str(e),
        "execution_time_ms": execution_time,
      },
      user_id=current_user.id,
    )

    api_logger.error(
      "Materialization failed",
      extra={
        "component": "materialize_api",
        "action": "materialization_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "duration_ms": execution_time,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    logger.error(
      f"Failed to materialize graph {graph_id}: {e}",
      extra={
        "component": "materialize_api",
        "action": "materialization_failed",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "error_type": type(e).__name__,
      },
    )

    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to materialize graph: {str(e)}",
    )

  finally:
    if lock_acquired:
      try:
        await redis_client.delete(lock_key)
        api_logger.info(
          "Materialization lock released",
          extra={
            "component": "materialize_api",
            "action": "lock_released",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "duration_ms": (datetime.now(timezone.utc) - start_time_dt).total_seconds()
            * 1000,
          },
        )
      except Exception as lock_error:
        logger.error(
          f"Failed to release materialization lock for graph {graph_id}: {lock_error}",
          extra={
            "component": "materialize_api",
            "action": "lock_release_failed",
            "user_id": str(current_user.id),
            "graph_id": graph_id,
            "error_type": type(lock_error).__name__,
          },
        )

    try:
      await redis_client.close()
    except Exception as close_error:
      logger.warning(f"Failed to close Redis client: {close_error}")
