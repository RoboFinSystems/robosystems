"""Business logic behind the ``materialize`` graph operation.

Returns ``operation_id`` and ``status`` for the router to wrap in an
``OperationEnvelope``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.models.api.graphs.operations import MaterializeOp
from robosystems.models.core import User

if TYPE_CHECKING:
  from robosystems.middleware.auth.distributed_lock import DistributedLock


async def materialize_cmd(
  graph_id: str,
  body: MaterializeOp,
  current_user: User,
  db: Session,
) -> dict:
  """Run graph materialization (or a dry run) and return a result dict.

  The per-graph lock fails closed: 409 when held, 503 + Retry-After when the
  lock service is down. A dry run checks limits and takes no lock.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.logger import api_logger, logger
  from robosystems.middleware.billing.enforcement import require_graph_access
  from robosystems.middleware.graph.types import SHARED_REPO_WRITE_ERROR_MESSAGE
  from robosystems.middleware.robustness import CircuitBreakerManager

  circuit_breaker = CircuitBreakerManager()

  graph = require_graph_access(graph_id, db, require_write=True)

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

  from robosystems.middleware.graph.write_pause import refuse_while_writes_paused

  await refuse_while_writes_paused()
  lock = acquire_materialize_lock(graph_id)

  # The worker releases the lock by lock_id (compare-and-delete), so a task
  # that outlives the TTL cannot strip a successor's lock. It is never
  # extended: the task timeout is half of INGESTION_LOCK_TTL.
  lock_key = f"graph_materialize:{graph_id}"
  lock_id = lock.lock_id

  try:
    graph_tier = graph.graph_tier or "ladybug-standard"
    from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

    limit_check = await IngestionLimitChecker.check_materialization_limits(
      db=db,
      graph_id=graph_id,
      tier=graph_tier,
    )

    if not limit_check["allowed"]:
      # Unverifiable storage is transient: 503 to retry, not 413 ("over cap").
      if limit_check.get("retryable"):
        raise HTTPException(
          status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
          detail={
            "error": "Storage usage could not be verified; retry shortly",
            "errors": limit_check["errors"],
            "tier": graph_tier,
          },
          headers={"Retry-After": "30"},
        )
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

      response = await enqueue_task(
        task_type="extensions_materialize",
        graph_id=graph_id,
        user_id=str(current_user.id),
        params={
          "rebuild": body.rebuild,
          "lock_key": lock_key,
          "lock_id": lock_id,
        },
      )
      return {
        "status": "queued",
        "graph_id": graph_id,
        "operation_id": response["operation_id"],
        "message": "Extensions materialization queued.",
      }

    # Staged path: uploaded parquet files → DuckDB → LadybugDB
    _require_rebuild_for_populated_graph(db, graph_id, rebuild=body.rebuild)
    use_direct = _should_use_direct_materialization(db, graph_id)

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
          "lock_id": lock_id,
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
          # dagster.yaml runs one materialize_db value at a time: a backstop
          # for any exit that frees the lock under a live run.
          "tags": {"materialize_db": graph_id},
          "pass_operation_id": True,
          "lock_key": lock_key,
          "lock_id": lock_id,
        },
      )
      return {
        "status": "queued",
        "graph_id": graph_id,
        "operation_id": response["operation_id"],
        "message": "Materialization queued (Dagster).",
      }

  except Exception:
    lock.release()
    raise


def acquire_materialize_lock(graph_id: str) -> DistributedLock:
  """Take the per-graph lock every writer into a graph database holds.

  Fails closed: 409 when held, 503 + Retry-After when the lock service is
  down. A retry is cheap, while an unlocked double-writer silently duplicates
  relationship edges.
  """
  from robosystems.config.constants import INGESTION_LOCK_TTL
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.logger import logger
  from robosystems.middleware.auth.distributed_lock import DistributedLock

  lock_unavailable = HTTPException(
    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    detail=(
      "Materialization lock service unavailable; retry shortly "
      "(materialization refuses to run unlocked)"
    ),
    headers={"Retry-After": "30"},
  )
  try:
    redis_client = create_redis_client(ValkeyDatabase.LOCKS)
    lock = DistributedLock(
      redis_client, f"graph_materialize:{graph_id}", ttl_seconds=INGESTION_LOCK_TTL
    )
    lock_result = lock.acquire(blocking=False)
  except Exception as e:
    logger.warning(f"Could not acquire distributed lock for {graph_id}: {e}")
    raise lock_unavailable from e

  if not lock_result.acquired:
    if lock_result.backend_error:
      logger.warning(
        f"Distributed lock backend error for {graph_id}: {lock_result.error_message}"
      )
      raise lock_unavailable
    raise HTTPException(
      status_code=status.HTTP_409_CONFLICT,
      detail="Materialization already in progress for this graph",
    )
  return lock


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


def _require_rebuild_for_populated_graph(
  db: Session, graph_id: str, *, rebuild: bool
) -> None:
  """Reject a non-rebuild materialize that would re-copy ingested rows.

  Staging tables rebuild from every uploaded file, so a non-rebuild run after
  a prior ingest replays them: duplicate nodes fail and duplicate edges load
  silently. Rejected only when there are both ingested and pending files.
  """
  if rebuild:
    return

  from robosystems.models.core import GraphFile, GraphTable

  def _file_exists(*filters) -> bool:
    return (
      db.query(GraphFile.id)
      .join(GraphTable, GraphFile.table_id == GraphTable.id)
      .filter(GraphTable.graph_id == graph_id, *filters)
      .first()
      is not None
    )

  previously_ingested = _file_exists(GraphFile.graph_status == "ingested")
  if not previously_ingested:
    return

  pending_files = _file_exists(
    GraphFile.duckdb_status == "staged",
    GraphFile.graph_status != "ingested",
  )
  if not pending_files:
    return

  raise HTTPException(
    status_code=status.HTTP_409_CONFLICT,
    detail={
      "error": (
        "This graph already contains materialized data; materializing new "
        "uploads without a rebuild would re-copy previously ingested rows."
      ),
      "resolution": ("Pass rebuild=true to rebuild the graph from all uploaded files."),
    },
  )


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
