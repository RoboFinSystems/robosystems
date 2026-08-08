"""Graph lifecycle operations — CQRS command surface.

All handlers return ``OperationEnvelope`` with idempotency + audit.
Sync operations use ``execute_operation``; async operations use the
manual ``wrap_pending`` + ``log_operation_audit`` pattern (same as
``auto_map_elements_op`` in roboledger operations).

URL surface: ``POST /v1/graphs/{graph_id}/operations/{op_name}``
"""

from __future__ import annotations

from fastapi import (
  APIRouter,
  Depends,
  Header,
  HTTPException,
  Path,
  status,
)
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session
from robosystems.logger import get_logger
from robosystems.middleware.auth.dependencies import (
  get_current_user_with_graph,
  require_graph_write_role,
)
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
  DeleteGraphOp,
  DeleteSubgraphOp,
  GraphMetadataResult,
  MaterializeOp,
  RestoreBackupOp,
  UpdateGraphMetadataOp,
)
from robosystems.models.api.graphs.subgraphs import CreateSubgraphRequest
from robosystems.models.core import User

logger = get_logger(__name__)

router = APIRouter()

_OP_TAG = "Graph Operations"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)
_GRAPH_OPS_PATH = "/v1/graphs/{graph_id}/operations"
_AUDIT_EVENT = "graph.operation"

# ── Surfaces refused on subgraphs ──────────────────────────────────────────
#
# A subgraph exists to be written to *directly*: raw Cypher and schema
# extension, which the parent graph refuses outright (see
# `middleware/mcp/tools/subgraph_write_tools` — "the main graph is read-only to
# raw statements"). The staging pipeline is the parent's write path and works
# the opposite way round: it rebuilds the database from DuckDB into a `-wip`
# copy and file-renames it over the active one (`_materialize_blue_green`).
# Point both at one database and the swap silently discards every direct write
# made since staging began. They are not two ways to write — they are two
# owners of the same file, and only one can win.
_SUBGRAPH_NO_STAGING = (
  "The file staging pipeline is not available on subgraphs. Staging rebuilds "
  "the database and swaps it into place, which would discard the direct writes "
  "a subgraph exists for. Upload to the parent graph, or write to the subgraph "
  "directly through its own MCP connector."
)

# Semantic memory is per-database storage on the parent's instance. A subgraph
# is meant to be cheap to create and throw away, and its memory would be a
# second store to keep in step with the parent's for no gain.
_SUBGRAPH_NO_MEMORY = (
  "Semantic memory is not available on subgraphs. Store memories on the parent "
  "graph, or keep the data in the subgraph itself as nodes."
)


def _block_subgraph(graph_id: str, detail: str) -> None:
  """Refuse a surface that a subgraph must not have.

  Distinct from `_block_shared_repo`, which is about ownership — this is about
  two write models that cannot share one database.
  """
  from robosystems.middleware.graph.utils import is_subgraph

  if is_subgraph(graph_id):
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


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
  operation_id="createSubgraph",
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

  # `get_current_user_with_graph` proves graph membership only; a read-only
  # `viewer` would otherwise reach this write. Enforce member/admin, fail-closed.
  require_graph_write_role(user_id, graph_id)

  if body.fork_parent:
    # Async path — enqueue worker task, return pending envelope
    replay = await check_idempotency(
      cache, user_id, graph_id, op_name, idempotency_key, fingerprint_body(body)
    )
    if replay is not None:
      return replay

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
  operation_id="deleteSubgraph",
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
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.models.core.graph.graph_user import GraphUser
  from robosystems.routers.graphs.subgraphs.utils import (
    get_subgraph_by_name,
    get_subgraph_service,
  )
  from robosystems.security import SecurityAuditLogger, SecurityEventType

  # Shared-repo subgraphs (e.g. `sec_historical`) are platform-managed.
  # The admin-role check further down would implicitly block this too,
  # but keep the explicit guard so a future change to how repo access is
  # granted can't silently expose this path.
  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=(f"Deleting subgraphs is not allowed on shared repository '{graph_id}'."),
    )

  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-subgraph",
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    subgraph = get_subgraph_by_name(graph_id, body.subgraph_name, db, user)

    if not GraphUser.user_has_admin_access(user.id, subgraph.parent_graph_id, db):
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
# delete-graph
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/delete-graph",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="deleteGraph",
  summary="Delete Graph",
  description=(
    "Permanently destroys a user graph and cancels its subscription. Two "
    "modes via the `at_period_end` body flag: omit it (or pass `false`) to "
    "tear down immediately (~10 min); pass `true` to keep the graph usable "
    "through the current billing period and tear it down at the period "
    "boundary via the existing suspend → deprovision pipeline. Requires "
    "`confirm` to equal the URL `graph_id`. Caller must be both org owner "
    "(billing authority) and admin on the graph (operational authority). "
    "Not allowed on shared repositories."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/delete-graph",
  method="POST",
  business_event_type="graph_delete_graph",
)
async def delete_graph_op(
  body: DeleteGraphOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.models.core import OrgRole, OrgUser
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingEventType,
    BillingSubscription,
    SubscriptionStatus,
  )
  from robosystems.models.core.graph.graph_user import GraphUser
  from robosystems.operations.providers.payment_provider import get_payment_provider
  from robosystems.security import SecurityAuditLogger, SecurityEventType

  # Shared repos run on platform-managed infrastructure with no
  # user-cancelable subscription. Reject up-front with a clear error.
  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Deleting a shared repository '{graph_id}' is not allowed.",
    )

  if body.confirm != graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=(
        "`confirm` must equal the graph_id in the URL to authorize permanent deletion."
      ),
    )

  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-graph",
    idempotency_key=idempotency_key,
    body=body,
  )

  async def _runner():
    if not GraphUser.user_has_admin_access(user.id, graph_id, db):
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Admin access to the graph required to delete it.",
      )

    subscription = BillingSubscription.get_by_resource(
      resource_type="graph", resource_id=graph_id, session=db
    )
    if not subscription:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"No active subscription found for graph {graph_id}.",
      )

    # Deletion is a billing event — match the authorization on every other
    # cancel path (billing/subscriptions, repo cancel) by also requiring the
    # caller to be org owner. Graph admin alone is operational authority and
    # not enough to authorize a billing-side action.
    org_user = OrgUser.get_by_org_and_user(subscription.org_id, user.id, db)
    if not org_user or org_user.role != OrgRole.OWNER:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Only organization owners can delete a graph.",
      )

    if subscription.status in ("canceled", "canceling"):
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Subscription is already canceled — graph teardown is in progress.",
      )

    if subscription.status == "upgrading":
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          "Cannot delete graph during tier upgrade. Wait for upgrade to "
          "complete and retry."
        ),
      )

    # `at_period_end` only makes sense for active subscriptions: a pending /
    # provisioning / paused sub may not have `current_period_end` set, which
    # would leave `ends_at = None` and let the deprovision sensor skip it
    # forever. Force callers to use immediate teardown in that case.
    if body.at_period_end and subscription.status != SubscriptionStatus.ACTIVE.value:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          f"Cannot defer teardown to period end: subscription is "
          f"'{subscription.status}', not 'active'. Retry without "
          "`at_period_end=true` to delete immediately."
        ),
      )

    immediate = not body.at_period_end

    # Stripe-side cancel: full cancel for immediate, period-end for deferred.
    # Stripe's default cancel does NOT prorate or refund; period-end uses
    # the standard Subscription.modify(cancel_at_period_end=True) path.
    if subscription.stripe_subscription_id:
      try:
        provider = get_payment_provider("stripe")
        if immediate:
          provider.cancel_subscription(subscription.stripe_subscription_id)
        else:
          provider.stripe.Subscription.modify(
            subscription.stripe_subscription_id,
            cancel_at_period_end=True,
          )
      except Exception as exc:
        # Stripe side is best-effort; the local subscription state is the
        # source of truth for the deprovision pipeline. Log to the standard
        # logger (so it shows up in normal app logs / alerting) AND to the
        # security audit trail so a reconciliation job can replay later.
        logger.error(
          f"Failed to cancel Stripe subscription "
          f"{subscription.stripe_subscription_id} during delete-graph "
          f"for {graph_id}: {exc}",
          extra={
            "graph_id": graph_id,
            "subscription_id": subscription.id,
            "at_period_end": body.at_period_end,
          },
          exc_info=True,
        )
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.OPERATION_FAILED,
          details={
            "action": "stripe_cancel_failed_during_delete_graph",
            "graph_id": graph_id,
            "subscription_id": subscription.id,
            "at_period_end": body.at_period_end,
            "error": str(exc),
          },
          risk_level="low",
        )

    subscription.cancel(db, immediate=immediate)

    mode = "immediate" if immediate else "period_end"
    BillingAuditLog.log_event(
      session=db,
      event_type=BillingEventType.SUBSCRIPTION_CANCELED,
      description=(
        f"Subscription {subscription.id} canceled via delete-graph op "
        f"for {graph_id} ({mode})"
      ),
      actor_type="user",
      actor_user_id=str(user.id),
      org_id=subscription.org_id,
      subscription_id=subscription.id,
      event_data={
        "immediate": immediate,
        "via": "graph_ops.delete_graph",
        "resource_type": "graph",
        "resource_id": graph_id,
      },
    )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.GRAPH_DELETED,
      details={
        "action": "graph_delete_requested",
        "graph_id": graph_id,
        "user_id": str(user.id),
        "subscription_id": subscription.id,
        "at_period_end": body.at_period_end,
      },
      risk_level="medium",
    )

    if immediate:
      return {
        "graph_id": graph_id,
        "subscription_id": subscription.id,
        "status": "deprovisioning_queued",
        "message": (
          "Subscription canceled. Deprovisioning is queued and typically "
          "completes within ~10 minutes. Poll graph status to verify."
        ),
      }

    return {
      "graph_id": graph_id,
      "subscription_id": subscription.id,
      "status": "scheduled_for_deprovision",
      "ends_at": subscription.ends_at.isoformat() if subscription.ends_at else None,
      "message": (
        "Subscription canceled at period end. Graph remains usable until "
        "the period closes, then transitions through suspend → deprovision."
      ),
    }

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# create-backup
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-backup",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="createBackup",
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

  # Cap retention to the tier's maximum. Unconditional: a missing graph row
  # or tier falls back to the smallest tier's cap rather than skipping the
  # clamp — an uncapped value would be accepted here and then destroyed by
  # the 90-day S3 lifecycle rule, leaving a COMPLETED record pointing at a
  # deleted object.
  graph_record = Graph.get_by_id(graph_id, db)
  backup_tier = (
    str(graph_record.graph_tier)
    if graph_record and graph_record.graph_tier
    else "ladybug-standard"
  )
  tier_max = GraphTierConfig.get_backup_limits(backup_tier).get(
    "backup_retention_days", 7
  )
  effective_retention_days = min(body.retention_days, tier_max)

  run_config = build_graph_job_config(
    "backup_graph_job",
    graph_id=graph_id,
    user_id=user_id,
    backup_type="full",
    backup_format=body.backup_format,
    retention_days=effective_retention_days,
    compression=True,
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
      "message": (
        "Backup creation started"
        if effective_retention_days == body.retention_days
        else (
          f"Backup creation started (retention capped to "
          f"{effective_retention_days} days, the {backup_tier} maximum)"
        )
      ),
      "retention_days": effective_retention_days,
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
  operation_id="restoreBackup",
  summary="Restore Backup",
  description="Not allowed on entity graphs (use `materialize` instead) or shared repositories. Destructive: the existing database is snapshotted, then overwritten. Monitor progress via SSE at `/v1/operations/{operation_id}/stream`.",
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
  from robosystems.middleware.sse import build_graph_job_config
  from robosystems.models.core import GraphBackup
  from robosystems.routers.graphs.backups.utils import (
    restore_unsupported_reason,
    verify_admin_access,
  )
  from robosystems.worker.client import enqueue_task

  op_name = "restore-backup"
  user_id = str(user.id)
  body_fp = fingerprint_body(body)

  replay = await check_idempotency(
    cache, user_id, graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  verify_admin_access(user, graph_id, db)

  # Restore availability is a property of the graph, not of the backup.
  unsupported = restore_unsupported_reason(graph_id, db)
  if unsupported:
    unsupported_status, unsupported_detail = unsupported
    raise HTTPException(
      status_code=unsupported_status,
      detail=unsupported_detail,
    )

  # Verify the backup exists and belongs to this graph
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
      "message": "Graph database restore scheduled",
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
  operation_id="changeTier",
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
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
  from robosystems.operations.graph.commands.tier import change_graph_tier_cmd

  # Shared repositories (SEC, etc.) run on platform-managed infrastructure —
  # they have no user-owned subscription, so `change_graph_tier_cmd` would
  # fail at the subscription lookup anyway. Reject up-front so the error
  # message points at the real reason instead of "subscription not found".
  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=(f"Tier changes are not allowed on shared repository '{graph_id}'."),
    )

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
# update-graph-metadata
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/update-graph-metadata",
  response_model=OperationEnvelope[GraphMetadataResult],
  operation_id="updateGraphMetadata",
  summary="Update Graph Metadata",
  description=(
    "Edit the graph's platform-level label: display name, description, and "
    'tags. Partial — only supplied (non-null) fields change; pass `""` to '
    "clear the description and `[]` to clear the tags. Requires admin on the "
    "graph. This is not the entity name that appears on financial statements "
    "— change that through the roboledger `update-entity` operation."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  f"{_GRAPH_OPS_PATH}/update-graph-metadata",
  method="POST",
  business_event_type="graph_update_metadata",
)
async def update_graph_metadata_op(
  body: UpdateGraphMetadataOp,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.operations.graph.commands.metadata import update_graph_metadata_cmd

  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="update-graph-metadata",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    if body.graph_name is None and body.description is None and body.tags is None:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="No fields provided for update.",
      )
    return update_graph_metadata_cmd(
      graph_id,
      graph_name=body.graph_name,
      description=body.description,
      tags=body.tags,
      user_id=str(user.id),
      db=db,
    )

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# materialize
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/materialize",
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="materialize",
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

  _block_subgraph(graph_id, _SUBGRAPH_NO_STAGING)

  # `get_current_user_with_graph` proves graph membership only; a read-only
  # `viewer` would otherwise reach this write. Enforce member/admin, fail-closed.
  require_graph_write_role(user_id, graph_id)

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
