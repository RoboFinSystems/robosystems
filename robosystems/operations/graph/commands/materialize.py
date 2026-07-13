"""Materialize command — business logic extracted from the old materialize router.

Called by the graph operations router's ``materialize_op`` handler.
Returns a dict with ``operation_id`` and ``status`` fields so the
caller can wrap the result in an ``OperationEnvelope``.
"""

from __future__ import annotations

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.models.api.graphs.operations import MaterializeOp
from robosystems.models.core import User


async def materialize_cmd(
  graph_id: str,
  body: MaterializeOp,
  current_user: User,
  db: Session,
) -> dict:
  """Run graph materialization (or dry-run) and return a result dict.

  Handles:
  - Billing enforcement / write-access check
  - Source auto-detection based on graph type
  - Dry-run limit validation (returns immediately, no lock acquired)
  - Distributed lock to prevent concurrent materialization
  - Content-limit gate (413 if tier limits exceeded)
  - Source routing: extensions (OLTP) vs staged (DuckDB) x direct vs Dagster

  Returns a dict with at least ``operation_id`` and ``status``.
  For dry-run returns ``{"status": "dry_run", "operation_id": "dry_run", ...}``.
  """
  from robosystems.config.constants import INGESTION_LOCK_TTL
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.logger import api_logger, logger
  from robosystems.middleware.auth.distributed_lock import DistributedLock
  from robosystems.middleware.billing.enforcement import require_graph_access
  from robosystems.middleware.graph.types import SHARED_REPO_WRITE_ERROR_MESSAGE
  from robosystems.middleware.robustness import CircuitBreakerManager

  circuit_breaker = CircuitBreakerManager()

  # Enforce subscription / write-access
  graph = require_graph_access(graph_id, db, require_write=True)

  # Auto-detect source from graph type when not specified
  source = _resolve_source(body.source, graph.graph_type)

  circuit_breaker.check_circuit(graph_id, "graph_materialization")

  if is_shared_repository_or_subgraph(graph_id.lower()):
    logger.warning(
      f"User {current_user.id} attempted materialization on shared repository {graph_id}"
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=SHARED_REPO_WRITE_ERROR_MESSAGE,
    )

  # Dry run: validate limits and return without executing
  if body.dry_run:
    from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

    graph_tier = graph.graph_tier or "ladybug-standard"
    limit_check = await IngestionLimitChecker.check_materialization_limits(
      db=db,
      graph_id=graph_id,
      tier=graph_tier,
    )
    api_logger.info(
      "Materialization dry run completed",
      extra={
        "component": "materialize_cmd",
        "action": "dry_run_checked",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "allowed": limit_check["allowed"],
        "tier": graph_tier,
      },
    )
    return {
      "status": "dry_run",
      "graph_id": graph_id,
      "operation_id": "dry_run",
      "message": "Dry run completed - no materialization executed",
      "limit_check": limit_check,
    }

  # Acquire distributed lock to prevent concurrent materialization
  lock = None
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
    lock = None  # Degraded mode — proceed without lock

  try:
    graph_tier = graph.graph_tier or "ladybug-standard"
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
          "error": "Materialization operation limit exceeded",
          "errors": limit_check["errors"],
          "current_usage": limit_check["current_usage"],
          "limits": limit_check["limits"],
          "tier": graph_tier,
        },
      )

    api_logger.info(
      "Materialization job queued",
      extra={
        "component": "materialize_cmd",
        "action": "job_queued",
        "user_id": str(current_user.id),
        "graph_id": graph_id,
        "source": source,
        "force": body.force,
        "rebuild": body.rebuild,
      },
    )

    if source == "extensions":
      from robosystems.worker.client import enqueue_task

      lock_key = f"graph_materialize:{graph_id}" if lock else None
      response = await enqueue_task(
        task_type="extensions_materialize",
        graph_id=graph_id,
        user_id=str(current_user.id),
        params={
          "rebuild": body.rebuild,
          "lock_key": lock_key,
        },
      )
      return {
        "status": "queued",
        "graph_id": graph_id,
        "operation_id": response["operation_id"],
        "message": "Extensions materialization queued.",
      }

    # Staged path: uploaded parquet files → DuckDB → LadybugDB
    use_direct = _should_use_direct_materialization(db, graph_id)
    lock_key = f"graph_materialize:{graph_id}" if lock else None

    if use_direct:
      from robosystems.worker.client import enqueue_task

      response = await enqueue_task(
        task_type="graph_materialization",
        graph_id=graph_id,
        user_id=str(current_user.id),
        params={
          "force": body.force,
          "rebuild": body.rebuild,
          "materialize_embeddings": body.materialize_embeddings,
          "lock_key": lock_key,
        },
      )
      return {
        "status": "queued",
        "graph_id": graph_id,
        "operation_id": response["operation_id"],
        "message": "Materialization started (direct).",
      }

    else:
      from robosystems.middleware.sse import build_graph_job_config
      from robosystems.worker.client import enqueue_task

      run_config = build_graph_job_config(
        "materialize_graph_job",
        graph_id=graph_id,
        user_id=str(current_user.id),
        force=body.force,
        rebuild=body.rebuild,
        materialize_embeddings=body.materialize_embeddings,
      )
      response = await enqueue_task(
        task_type="dagster_job_monitor",
        graph_id=graph_id,
        user_id=str(current_user.id),
        params={
          "job_name": "materialize_graph_job",
          "run_config": run_config,
          "lock_key": lock_key,
        },
      )
      return {
        "status": "queued",
        "graph_id": graph_id,
        "operation_id": response["operation_id"],
        "message": "Materialization queued (Dagster).",
      }

  except Exception:
    if lock is not None:
      lock.release()
    raise


def _resolve_source(source: str | None, graph_type: str) -> str:
  """Infer and validate materialization source from graph type."""
  if source is None:
    return "extensions" if graph_type == "entity" else "staged"

  if graph_type == "entity" and source == "staged":
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=(
        "Entity graphs cannot use source='staged'. "
        "Entity graph data comes from the extensions OLTP pipeline. "
        "Use source='extensions' or omit the source field."
      ),
    )
  if graph_type == "generic" and source == "extensions":
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=(
        "Generic graphs cannot use source='extensions'. "
        "Generic graphs use uploaded files as their data source. "
        "Use source='staged' or omit the source field."
      ),
    )
  return source


def _should_use_direct_materialization(db: Session, graph_id: str) -> bool:
  """Use direct fast path for small graphs; route large ones to Dagster."""
  from sqlalchemy import func

  from robosystems.config import env
  from robosystems.config.constants import GRAPH_MATERIALIZATION_THRESHOLD_MB
  from robosystems.logger import logger
  from robosystems.models.core import GraphFile, GraphTable

  if not env.DIRECT_GRAPH_MATERIALIZATION_ENABLED:
    return False

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
