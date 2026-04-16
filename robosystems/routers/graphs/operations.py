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
  check_idempotency,
  execute_operation,
  fingerprint_body,
  generate_operation_id,
  get_idempotency_cache,
  log_operation_audit,
  wrap_completed,
  wrap_pending,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.graphs.backups import BackupCreateRequest
from robosystems.models.api.graphs.operations import (
  ChangeTierOp,
  DeleteSubgraphOp,
  MaterializeOp,
  RestoreBackupOp,
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


# ═══════════════════════════════════════════════════════════════════════════
# create-subgraph
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-subgraph",
  response_model=OperationEnvelope,
  operation_id="opCreateSubgraph",
  summary="Create Subgraph",
  description="Set `fork_parent=true` to clone parent graph data into the new subgraph (async, returns a pending `OperationEnvelope`). Without `fork_parent`, creates an empty subgraph synchronously.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
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
  from robosystems.routers.graphs.subgraphs.main import create_subgraph

  op_name = "create-subgraph"
  user_id = str(user.id)

  if body.fork_parent:
    # Async path — enqueue worker task, return pending envelope
    replay = await check_idempotency(
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
  description="Set `backup_first=true` to create a safety backup before deletion. Requires admin role on the parent graph.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
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
  from robosystems.models.core.graph.graph_user import GraphUser
  from robosystems.routers.graphs.subgraphs.utils import (
    get_subgraph_by_name,
    get_subgraph_service,
  )
  from robosystems.security import SecurityAuditLogger, SecurityEventType

  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-subgraph",
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    subgraph = get_subgraph_by_name(graph_id, body.subgraph_name, db, user)

    user_graph = (
      db.query(GraphUser)
      .filter(
        GraphUser.user_id == user.id,
        GraphUser.graph_id == subgraph.parent_graph_id,
      )
      .first()
    )
    if not user_graph or user_graph.role != "admin":
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access to parent graph required to delete subgraphs",
      )

    subgraph_id = subgraph.graph_id
    subgraph_service = get_subgraph_service()
    deletion_result = await subgraph_service.delete_subgraph_database(
      subgraph_id=subgraph_id, force=body.force, create_backup=body.backup_first
    )
    backup_location = deletion_result.get("backup_location")

    subgraph.delete(db)

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUBGRAPH_DELETED,
      details={
        "action": "subgraph_deleted",
        "subgraph_id": subgraph_id,
        "parent_graph_id": subgraph.parent_graph_id,
        "user_id": user.id,
        "forced": body.force,
        "backup_created": body.backup_first,
      },
      risk_level="medium",
    )

    return {
      "graph_id": subgraph_id,
      "status": "deleted",
      "backup_location": backup_location,
    }

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
  description="Not allowed on shared repositories. Only `full_dump` format supported. Retention capped at tier maximum. Monitor progress via SSE at `/v1/operations/{operation_id}/stream`.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
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

  replay = await check_idempotency(
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
  description="Only encrypted backups can be restored. Not allowed on entity graphs (use `materialize` instead) or shared repositories.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
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
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.middleware.sse import build_graph_job_config
  from robosystems.models.core import Graph, GraphBackup
  from robosystems.routers.graphs.backups.utils import verify_admin_access
  from robosystems.worker.client import enqueue_task

  op_name = "restore-backup"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await check_idempotency(
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
# change-tier
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/change-tier",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="opChangeTier",
  summary="Change Tier",
  description="Async EBS migration (3-5 min). Valid tiers: `ladybug-standard`, `ladybug-large`, `ladybug-xlarge`.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/change-tier",
  method="POST",
  business_event_type="graph_change_tier",
)
async def change_tier_op(
  body: ChangeTierOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.operations.graph.commands.tier import change_graph_tier_cmd

  op_name = "change-tier"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await check_idempotency(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  operation_id = await change_graph_tier_cmd(graph_id, body.new_tier, user, db)

  envelope = wrap_pending(
    op_name,
    operation_id=operation_id,
    partial_result={
      "new_tier": body.new_tier,
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
  description="Rebuilds LadybugDB from staging tables (file uploads) or extensions OLTP data. Set `dry_run=true` to preview without changes.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/materialize",
  method="POST",
  business_event_type="graph_materialize",
)
async def materialize_op(
  body: MaterializeOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.operations.graph.commands.materialize import materialize_cmd

  op_name = "materialize"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await check_idempotency(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  result = await materialize_cmd(graph_id, body, user, db)

  if body.dry_run:
    envelope = wrap_completed(op_name, result=result, created_by=user_id)
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
