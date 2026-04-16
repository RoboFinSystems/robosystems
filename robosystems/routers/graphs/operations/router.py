"""Graph lifecycle operations — CQRS command surface.

All handlers return ``OperationEnvelope`` with idempotency + audit.
Sync operations use ``execute_operation``; async operations use the
manual ``wrap_pending`` + ``log_operation_audit`` pattern (same as
``auto_map_elements_op`` in roboledger operations).

URL surface: ``POST /v1/graphs/{graph_id}/operations/{op_name}``
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  IdempotencyKeyConflictError,
  OperationContext,
  OperationEnvelope,
  execute_operation,
  fingerprint_body,
  generate_operation_id,
  get_idempotency_cache,
  log_operation_audit,
  wrap_pending,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.graphs.backups import BackupCreateRequest
from robosystems.models.api.graphs.operations import (
  DeleteSubgraphOp,
  RestoreBackupOp,
  UpgradeTierOp,
)
from robosystems.models.api.graphs.subgraphs import CreateSubgraphRequest
from robosystems.models.core import User

router = APIRouter(tags=["Graph Operations"])

_OP_TAG = "Graph Operations"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)
_GRAPH_OPS_PATH = "/v1/graphs/{graph_id}/operations"
_AUDIT_EVENT = "graph.operation"


def _ctx(
  *,
  graph_id: str,
  user_id: str,
  op: str,
  idempotency_key: str | None,
  body: object,
) -> OperationContext:
  return OperationContext(
    domain="graph",
    operation_name=op,
    graph_id=graph_id,
    user_id=user_id,
    idempotency_key=idempotency_key,
    body_fingerprint=fingerprint_body(body),
  )


async def _dispatch(ctx, runner, cache, on_fresh_success=None):
  try:
    return await execute_operation(
      ctx, runner, idempotency_cache=cache, on_fresh_success=on_fresh_success
    )
  except IdempotencyKeyConflictError as exc:
    raise HTTPException(status_code=409, detail=str(exc))


async def _idempotency_check(
  cache: IdempotencyCache,
  user_id: str,
  graph_id: str,
  op_name: str,
  idempotency_key: str | None,
  body_fingerprint: str,
) -> OperationEnvelope | None:
  """Manual idempotency lookup for async (pending) operations."""
  if idempotency_key is None:
    return None
  try:
    cached = await cache.get(
      user_id, graph_id, op_name, idempotency_key, body_fingerprint
    )
  except IdempotencyKeyConflictError as exc:
    log_operation_audit(
      operation_name=op_name,
      operation_id=generate_operation_id(),
      user_id=user_id,
      graph_id=graph_id,
      duration_ms=0.0,
      status="failed",
      idempotency_key=idempotency_key,
      error=str(exc),
      event=_AUDIT_EVENT,
    )
    raise HTTPException(status_code=409, detail=str(exc))
  if cached is not None:
    log_operation_audit(
      operation_name=op_name,
      operation_id=cached.operation_id,
      user_id=user_id,
      graph_id=graph_id,
      duration_ms=0.0,
      status=cached.status,
      idempotency_key=idempotency_key,
      idempotent_replay=True,
      event=_AUDIT_EVENT,
    )
    return cached.model_copy(update={"idempotent_replay": True})
  return None


# ═══════════════════════════════════════════════════════════════════════════
# create-subgraph
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-subgraph",
  response_model=OperationEnvelope,
  operation_id="opCreateSubgraph",
  summary="Create Subgraph",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/create-subgraph",
  method="POST",
  business_event_type="graph_create_subgraph",
)
async def create_subgraph_op(
  body: CreateSubgraphRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  """Create a new subgraph, optionally forking parent data."""
  from robosystems.routers.graphs.subgraphs.main import create_subgraph

  op_name = "create-subgraph"
  user_id = str(user.id)

  if body.fork_parent:
    # Async path — enqueue worker task, return pending envelope
    replay = await _idempotency_check(
      cache, user_id, graph_id, op_name, idempotency_key, fingerprint_body(body)
    )
    if replay is not None:
      return replay

    # Delegate to the existing handler's business logic
    result = await create_subgraph(
      request=body, graph_id=graph_id, current_user=user, db=db
    )
    operation_id = result.get("operation_id", generate_operation_id())

    envelope = wrap_pending(
      op_name,
      operation_id=operation_id,
      partial_result=result,
      created_by=user_id,
    )
    if idempotency_key is not None:
      await cache.put(
        user_id, graph_id, op_name, idempotency_key, envelope, fingerprint_body(body)
      )
    log_operation_audit(
      operation_name=op_name,
      operation_id=envelope.operation_id,
      user_id=user_id,
      graph_id=graph_id,
      duration_ms=0.0,
      status="pending",
      idempotency_key=idempotency_key,
      event=_AUDIT_EVENT,
    )
    return envelope

  # Sync path — create immediately
  ctx = _ctx(
    graph_id=graph_id,
    user_id=user_id,
    op=op_name,
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    result = await create_subgraph(
      request=body, graph_id=graph_id, current_user=user, db=db
    )
    return result

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# delete-subgraph
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/delete-subgraph",
  response_model=OperationEnvelope,
  operation_id="opDeleteSubgraph",
  summary="Delete Subgraph",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/delete-subgraph",
  method="POST",
  business_event_type="graph_delete_subgraph",
)
async def delete_subgraph_op(
  body: DeleteSubgraphOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  """Delete a subgraph database."""
  from robosystems.models.api.graphs.subgraphs import DeleteSubgraphRequest
  from robosystems.routers.graphs.subgraphs.delete import delete_subgraph

  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-subgraph",
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    # Adapt body to match existing handler's expected model
    delete_request = DeleteSubgraphRequest(
      force=body.force, backup_first=body.backup_first
    )
    result = await delete_subgraph(
      graph_id=graph_id,
      subgraph_name=body.subgraph_name,
      request=delete_request,
      current_user=user,
      session=db,
    )
    return result

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# create-backup
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-backup",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="opCreateBackup",
  summary="Create Backup",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/create-backup",
  method="POST",
  business_event_type="graph_create_backup",
)
async def create_backup_op(
  body: BackupCreateRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  """Create a backup of the graph database (async)."""
  from robosystems.config import env
  from robosystems.config.graph_tier import GraphTierConfig
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.middleware.sse import build_graph_job_config
  from robosystems.models.core import Graph
  from robosystems.routers.graphs.backups.utils import verify_admin_access
  from robosystems.worker.client import enqueue_task

  op_name = "create-backup"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await _idempotency_check(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  # Validation
  verify_admin_access(user, graph_id, db)

  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Creating backups is not allowed on shared repository '{graph_id}'.",
    )

  if not env.BACKUP_CREATION_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Backup creation is currently disabled.",
    )

  if body.backup_format != "full_dump":
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Only 'full_dump' backup format is currently supported",
    )

  # Cap retention to tier max
  graph_record = Graph.get_by_id(graph_id, db)
  if graph_record and graph_record.graph_tier:
    backup_limits = GraphTierConfig.get_backup_limits(graph_record.graph_tier)
    tier_max = backup_limits.get("backup_retention_days", 90)
    if body.retention_days > tier_max:
      body.retention_days = tier_max

  # Enqueue Dagster backup job
  run_config = build_graph_job_config(
    "backup_graph_job",
    graph_id=graph_id,
    user_id=user_id,
    backup_type="full",
    backup_format=body.backup_format,
    retention_days=body.retention_days,
    compression=True,
    encryption=body.encryption,
  )

  response = await enqueue_task(
    task_type="dagster_job_monitor",
    graph_id=graph_id,
    user_id=user_id,
    params={"job_name": "backup_graph_job", "run_config": run_config},
  )
  operation_id = response["operation_id"]

  envelope = wrap_pending(
    op_name,
    operation_id=operation_id,
    partial_result={
      "status": "accepted",
      "message": "Backup creation started",
      "monitoring": {
        "sse_endpoint": f"/v1/operations/{operation_id}/stream",
      },
    },
    created_by=user_id,
  )

  if idempotency_key is not None:
    await cache.put(user_id, graph_id, op_name, idempotency_key, envelope, body_fp)

  log_operation_audit(
    operation_name=op_name,
    operation_id=operation_id,
    user_id=user_id,
    graph_id=graph_id,
    duration_ms=0.0,
    status="pending",
    idempotency_key=idempotency_key,
    event=_AUDIT_EVENT,
  )
  return envelope


# ═══════════════════════════════════════════════════════════════════════════
# restore-backup
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/restore-backup",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="opRestoreBackup",
  summary="Restore Backup",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/restore-backup",
  method="POST",
  business_event_type="graph_restore_backup",
)
async def restore_backup_op(
  body: RestoreBackupOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  """Restore a graph database from an encrypted backup.

  Blocked for entity graphs — OLTP is the source of truth, use
  the materialize operation instead.
  """
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.middleware.sse import build_graph_job_config
  from robosystems.models.core import Graph, GraphBackup
  from robosystems.routers.graphs.backups.utils import verify_admin_access
  from robosystems.worker.client import enqueue_task

  op_name = "restore-backup"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await _idempotency_check(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  # Validation
  verify_admin_access(user, graph_id, db)

  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Restore operations are not allowed on shared repository '{graph_id}'.",
    )

  # Entity graph guard
  graph_record = Graph.get_by_id(graph_id, db)
  if graph_record and graph_record.graph_type == "entity":
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Cannot restore backups for entity graphs. The graph is automatically "
      "materialized from the extensions database. Use the materialize operation instead.",
    )

  # Verify backup exists, belongs to this graph, is encrypted
  backup_record = GraphBackup.get_by_id(body.backup_id, db)
  if not backup_record:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND, detail="Backup not found"
    )
  if backup_record.graph_id != graph_id:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Backup does not belong to this graph",
    )
  if not backup_record.encryption_enabled:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Only encrypted backups can be restored for security reasons",
    )

  # Enqueue Dagster restore job
  run_config = build_graph_job_config(
    "restore_graph_job",
    graph_id=graph_id,
    backup_id=body.backup_id,
    user_id=user_id,
    create_system_backup=body.create_system_backup,
    verify_after_restore=body.verify_after_restore,
  )

  response = await enqueue_task(
    task_type="dagster_job_monitor",
    graph_id=graph_id,
    user_id=user_id,
    params={"job_name": "restore_graph_job", "run_config": run_config},
  )
  operation_id = response["operation_id"]

  envelope = wrap_pending(
    op_name,
    operation_id=operation_id,
    partial_result={
      "status": "pending",
      "message": "Graph database restore scheduled from encrypted backup",
    },
    created_by=user_id,
  )

  if idempotency_key is not None:
    await cache.put(user_id, graph_id, op_name, idempotency_key, envelope, body_fp)

  log_operation_audit(
    operation_name=op_name,
    operation_id=operation_id,
    user_id=user_id,
    graph_id=graph_id,
    duration_ms=0.0,
    status="pending",
    idempotency_key=idempotency_key,
    event=_AUDIT_EVENT,
  )
  return envelope


# ═══════════════════════════════════════════════════════════════════════════
# upgrade-tier
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/upgrade-tier",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="opUpgradeTier",
  summary="Upgrade Tier",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/upgrade-tier",
  method="POST",
  business_event_type="graph_upgrade_tier",
)
async def upgrade_tier_op(
  body: UpgradeTierOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  """Change the infrastructure tier on a graph (async EBS migration)."""
  from robosystems.models.api.billing.subscription import UpgradeSubscriptionRequest
  from robosystems.routers.graphs.subscriptions import _change_graph_tier

  op_name = "upgrade-tier"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await _idempotency_check(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  # Delegate to existing tier change logic
  upgrade_request = UpgradeSubscriptionRequest(new_plan_name=body.new_tier)
  sub_response = await _change_graph_tier(graph_id, upgrade_request, user, db)

  # The existing function returns GraphSubscriptionResponse with operation_id
  operation_id = sub_response.operation_id or generate_operation_id()

  envelope = wrap_pending(
    op_name,
    operation_id=operation_id,
    partial_result={
      "old_tier": None,  # Available in sub_response context
      "new_tier": body.new_tier,
      "subscription_status": sub_response.status,
    },
    created_by=user_id,
  )

  if idempotency_key is not None:
    await cache.put(user_id, graph_id, op_name, idempotency_key, envelope, body_fp)

  log_operation_audit(
    operation_name=op_name,
    operation_id=operation_id,
    user_id=user_id,
    graph_id=graph_id,
    duration_ms=0.0,
    status="pending",
    idempotency_key=idempotency_key,
    event=_AUDIT_EVENT,
  )
  return envelope


# ═══════════════════════════════════════════════════════════════════════════
# materialize
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/materialize",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="opMaterialize",
  summary="Materialize Graph",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/materialize",
  method="POST",
  business_event_type="graph_materialize",
)
async def materialize_op(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
  force: bool = False,
  rebuild: bool = False,
  ignore_errors: bool = True,
  dry_run: bool = False,
  source: str | None = None,
  materialize_embeddings: bool = False,
) -> OperationEnvelope:
  """Materialize graph from staging tables or extensions OLTP.

  Delegates to the existing materialize_graph handler which handles
  distributed locking, source routing, and Dagster/direct dispatch.
  """
  from robosystems.routers.graphs.materialize import (
    MaterializeRequest,
    materialize_graph,
  )

  body = MaterializeRequest(
    force=force,
    rebuild=rebuild,
    ignore_errors=ignore_errors,
    dry_run=dry_run,
    source=source,
    materialize_embeddings=materialize_embeddings,
  )

  op_name = "materialize"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await _idempotency_check(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  # Delegate to existing materialize handler
  result = await materialize_graph(
    body=body, graph_id=graph_id, current_user=user, db=db
  )

  # The existing handler returns different shapes for dry_run vs normal
  if dry_run:
    # Dry run returns immediately — sync envelope
    envelope = OperationEnvelope(
      operation=op_name,
      operationId=generate_operation_id(),
      status="completed",
      result=result if isinstance(result, dict) else result.model_dump(mode="json"),
      at=__import__(
        "robosystems.middleware.operations.core", fromlist=["_utcnow_iso"]
      )._utcnow_iso(),
      createdBy=user_id,
    )
  else:
    operation_id = (
      result.operation_id
      if hasattr(result, "operation_id")
      else result.get("operation_id", generate_operation_id())
    )
    envelope = wrap_pending(
      op_name,
      operation_id=operation_id,
      partial_result=result
      if isinstance(result, dict)
      else result.model_dump(mode="json"),
      created_by=user_id,
    )

  if idempotency_key is not None:
    await cache.put(user_id, graph_id, op_name, idempotency_key, envelope, body_fp)

  log_operation_audit(
    operation_name=op_name,
    operation_id=envelope.operation_id,
    user_id=user_id,
    graph_id=graph_id,
    duration_ms=0.0,
    status=envelope.status,
    idempotency_key=idempotency_key,
    event=_AUDIT_EVENT,
  )
  return envelope
