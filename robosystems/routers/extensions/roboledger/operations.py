"""RoboLedger operation routes.

Every route follows the pattern:

1. `POST /extensions/roboledger/{graph_id}/operations/{op_name}`
2. Typed request body (path params embedded in body for
   update/delete commands)
3. A `_runner()` closure that opens an extensions session, calls the
   ops layer, translates domain errors into HTTPExceptions
4. `execute_operation(ctx, runner, cache)` handles envelope +
   idempotency + audit

**Registered (23):**

- Entity: `update-entity`
- Fiscal calendar / periods: `initialize`, `set-close-target`,
  `close-period`, `reopen-period`
- Schedules: `create-schedule`, `truncate-schedule`,
  `create-closing-entry`, `create-manual-closing-entry`
- Taxonomies: `create-taxonomy`, `create-structure`,
  `create-mapping-association`, `delete-mapping-association`
- Mappings (async): `auto-map-elements` — the one
  Dagster-dispatched op, returns `status: "pending"` and streams
  through `/v1/operations/{operation_id}/stream`
- Reports: `create-report`, `regenerate-report`, `delete-report`,
  `share-report`
- Publish lists: `create-publish-list`, `update-publish-list`,
  `delete-publish-list`, `add-publish-list-members`,
  `remove-publish-list-member`

`build-fact-grid` is registered separately in the sibling `views.py`
file so it can be mounted independently of `ROBOLEDGER_ENABLED` (it
needs to work for SEC-only deployments without roboledger tenants).

**URL migration note for SDK consumers:** the legacy
`POST /v1/ledger/{graph_id}/mappings/{mapping_id}/auto-map`
endpoint has moved to
`POST /extensions/roboledger/{graph_id}/operations/auto-map-elements`
with `mapping_id` now in the request body instead of the path.
Same async semantics, same SSE stream URL for progress.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from pydantic import BaseModel
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.extensions import (
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
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.entity import UpdateEntityRequest
from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodRequest,
  InitializeLedgerRequest,
  ReopenPeriodRequest,
  SetCloseTargetRequest,
)
from robosystems.models.api.extensions.publish_lists import (
  AddMembersRequest,
  CreatePublishListRequest,
  UpdatePublishListRequest,
)
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  RegenerateReportRequest,
  ShareReportRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateClosingEntryRequest,
  CreateManualClosingEntryRequest,
  CreateScheduleRequest,
  TruncateScheduleRequest,
)
from robosystems.models.api.extensions.taxonomies import (
  CreateAssociationRequest,
  CreateStructureRequest,
  CreateTaxonomyRequest,
)
from robosystems.models.core import User
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.roboledger.commands.entity import update_parent_entity
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  PeriodNotClosedError,
  PeriodNotFoundInLedgerError,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  close_period as cmd_close_period,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  initialize_ledger as cmd_initialize_ledger,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  reopen_period as cmd_reopen_period,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  set_close_target as cmd_set_close_target,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  MembersAlreadyPresentError,
  PublishListNameConflictError,
  PublishListNotAuthorizedError,
  PublishListNotFoundError,
  SelfAddError,
  TargetGraphMissingExtensionError,
  TargetGraphsNotFoundError,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  add_publish_list_members as cmd_add_publish_list_members,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  create_publish_list as cmd_create_publish_list,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  delete_publish_list as cmd_delete_publish_list,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  remove_publish_list_member as cmd_remove_publish_list_member,
)
from robosystems.operations.roboledger.commands.publish_lists import (
  update_publish_list as cmd_update_publish_list,
)
from robosystems.operations.roboledger.commands.reports import (
  NoEntityError,
  NotAuthorizedError,
  PublishListEmptyError,
  ReportNotFoundError,
  ReportNotPublishedError,
  TaxonomyNotFoundError,
)
from robosystems.operations.roboledger.commands.reports import (
  PublishListNotFoundError as ReportPublishListNotFoundError,
)
from robosystems.operations.roboledger.commands.reports import (
  create_report as cmd_create_report,
)
from robosystems.operations.roboledger.commands.reports import (
  delete_report as cmd_delete_report,
)
from robosystems.operations.roboledger.commands.reports import (
  regenerate_report as cmd_regenerate_report,
)
from robosystems.operations.roboledger.commands.reports import (
  share_report as cmd_share_report,
)
from robosystems.operations.roboledger.commands.schedules import (
  create_closing_entry as cmd_create_closing_entry,
)
from robosystems.operations.roboledger.commands.schedules import (
  create_manual_closing_entry as cmd_create_manual_closing_entry,
)
from robosystems.operations.roboledger.commands.schedules import (
  create_schedule as cmd_create_schedule,
)
from robosystems.operations.roboledger.commands.schedules import (
  truncate_schedule as cmd_truncate_schedule,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  ElementNotFoundError,
  MappingStructureNotFoundError,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association as cmd_create_mapping_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_structure as cmd_create_structure,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_taxonomy as cmd_create_taxonomy,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  delete_mapping_association as cmd_delete_mapping_association,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  FiscalCalendarService,
  PeriodNotFoundError,
  UnbalancedLedgerError,
  parse_period,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  PeriodCloseService,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CalendarAlreadyInitializedError,
  InvalidCloseTargetError,
)
from robosystems.operations.roboledger.schedules import ScheduleService

router = APIRouter()

_OP_TAG = "Extensions: RoboLedger"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)

# Stateless service singletons reused across requests (same pattern
# as the old router-level `_svc = FiscalCalendarService()`).
_fiscal_svc = FiscalCalendarService()
_close_svc = PeriodCloseService(_fiscal_svc)
_schedule_svc = ScheduleService()


def _ctx(
  *,
  graph_id: str,
  user_id: str,
  op: str,
  idempotency_key: str | None,
  body: object,
) -> OperationContext:
  """Build the per-request operation context with body fingerprint."""
  return OperationContext(
    domain="roboledger",
    operation_name=op,
    graph_id=graph_id,
    user_id=user_id,
    idempotency_key=idempotency_key,
    body_fingerprint=fingerprint_body(body),
  )


async def _dispatch(
  ctx: OperationContext,
  runner,
  cache: IdempotencyCache,
  on_fresh_success=None,
) -> OperationEnvelope:
  """Run `execute_operation` and translate idempotency conflicts to 409."""
  try:
    return await execute_operation(
      ctx, runner, idempotency_cache=cache, on_fresh_success=on_fresh_success
    )
  except IdempotencyKeyConflictError as exc:
    raise HTTPException(status_code=409, detail=str(exc))


def _ledger_404() -> HTTPException:
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


def _is_schema_missing(exc: ProgrammingError) -> bool:
  msg = str(exc)
  return "does not exist" in msg and ("schema" in msg or "relation" in msg)


# ───────────────────────────────────────────────────────────────────────────
# Operation request bodies — wrap path-specific IDs into the request body.
# ───────────────────────────────────────────────────────────────────────────


class SetCloseTargetOperation(SetCloseTargetRequest):
  pass  # `period` already in body


class ClosePeriodOperation(ClosePeriodRequest):
  period: str


class ReopenPeriodOperation(ReopenPeriodRequest):
  period: str


class TruncateScheduleOperation(TruncateScheduleRequest):
  structure_id: str


class CreateClosingEntryOperation(CreateClosingEntryRequest):
  structure_id: str


class CreateMappingAssociationOperation(CreateAssociationRequest):
  mapping_id: str


class DeleteMappingAssociationOperation(BaseModel):
  mapping_id: str
  association_id: str


class RegenerateReportOperation(RegenerateReportRequest):
  report_id: str


class DeleteReportOperation(BaseModel):
  report_id: str


class ShareReportOperation(ShareReportRequest):
  report_id: str


class UpdatePublishListOperation(UpdatePublishListRequest):
  list_id: str


class DeletePublishListOperation(BaseModel):
  list_id: str


class AddPublishListMembersOperation(AddMembersRequest):
  list_id: str


class RemovePublishListMemberOperation(BaseModel):
  list_id: str
  member_id: str


class DeleteResult(BaseModel):
  """Return shape for delete operations."""

  deleted: bool


# ═══════════════════════════════════════════════════════════════════════════
# Entity operations
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/update-entity",
  response_model=OperationEnvelope,
  operation_id="opUpdateEntity",
  summary="Update Entity",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def update_entity_op(
  body: UpdateEntityRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Update entity details. Only provided (non-null) fields are updated."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="update-entity",
    idempotency_key=idempotency_key,
    body=body,
  )
  updates = body.model_dump(exclude_none=True)

  def _runner():
    if not updates:
      raise HTTPException(status_code=400, detail="No fields provided for update.")
    try:
      with extensions_session(graph_id) as session:
        result = update_parent_entity(session, updates)
    except (ValueError, ProgrammingError):
      raise _ledger_404()
    if result is None:
      raise HTTPException(
        status_code=404, detail="No entity found. Create an entity graph first."
      )
    return result

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# Fiscal calendar / periods operations
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/initialize",
  response_model=OperationEnvelope,
  operation_id="opInitializeLedger",
  summary="Initialize Ledger",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def initialize_op(
  body: InitializeLedgerRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  """One-time ledger initialization — create fiscal calendar + seed periods."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="initialize",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        response, _warnings = cmd_initialize_ledger(
          session,
          platform_db,
          graph_id,
          body,
          actor_id=str(user.id),
          service=_fiscal_svc,
        )
        return response
    except CalendarAlreadyInitializedError as e:
      raise HTTPException(status_code=409, detail=str(e))
    except InvalidCloseTargetError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/set-close-target",
  response_model=OperationEnvelope,
  operation_id="opSetCloseTarget",
  summary="Set Close Target",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def set_close_target_op(
  body: SetCloseTargetOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  """Set the user-controlled close target (YYYY-MM)."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="set-close-target",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_set_close_target(
          session,
          platform_db,
          graph_id,
          body.period,
          actor_id=str(user.id),
          note=body.note,
          service=_fiscal_svc,
        )
    except InvalidCloseTargetError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/close-period",
  response_model=OperationEnvelope,
  operation_id="opClosePeriod",
  summary="Close Fiscal Period",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def close_period_op(
  body: ClosePeriodOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  """Close a fiscal period — the final commit action."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="close-period",
    idempotency_key=idempotency_key,
    body=body,
  )

  try:
    parse_period(body.period)
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_close_period(
          session,
          platform_db,
          graph_id,
          body.period,
          actor_id=str(user.id),
          allow_stale_sync=body.allow_stale_sync,
          note=body.note,
          service=_fiscal_svc,
          close_service=_close_svc,
        )
    except CloseGateFailed as e:
      if e.no_calendar:
        raise HTTPException(
          status_code=404,
          detail=(
            "Fiscal calendar not initialized. Call /operations/initialize first."
          ),
        )
      raise HTTPException(
        status_code=422,
        detail={
          "message": f"Cannot close period {body.period!r}.",
          "blockers": e.blockers,
        },
      )
    except PeriodNotFoundError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except UnbalancedLedgerError as e:
      raise HTTPException(
        status_code=422,
        detail=(
          f"Balance sheet equation broken for this period: "
          f"total debits={e.total_debit} total credits={e.total_credit}. "
          f"Difference={e.total_debit - e.total_credit}. "
          f"Review the ledger before closing."
        ),
      )
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/reopen-period",
  response_model=OperationEnvelope,
  operation_id="opReopenPeriod",
  summary="Reopen Fiscal Period",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def reopen_period_op(
  body: ReopenPeriodOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
  """Reopen a closed fiscal period — decrements `closed_through`."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="reopen-period",
    idempotency_key=idempotency_key,
    body=body,
  )

  try:
    parse_period(body.period)
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_reopen_period(
          session,
          platform_db,
          graph_id,
          body.period,
          actor_id=str(user.id),
          reason=body.reason,
          note=body.note,
          service=_fiscal_svc,
        )
    except PeriodNotFoundInLedgerError:
      raise HTTPException(
        status_code=404, detail=f"Fiscal period {body.period!r} not found."
      )
    except PeriodNotClosedError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# Schedule operations
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-schedule",
  response_model=OperationEnvelope,
  operation_id="opCreateSchedule",
  summary="Create Schedule",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_schedule_op(
  body: CreateScheduleRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Create a schedule with pre-generated monthly facts."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-schedule",
    idempotency_key=idempotency_key,
    body=body,
  )

  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_create_schedule(
          session, body, created_by=str(user.id), service=_schedule_svc
        )
    except ValueError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "schedule_created"),
  )


@router.post(
  "/truncate-schedule",
  response_model=OperationEnvelope,
  operation_id="opTruncateSchedule",
  summary="Truncate Schedule (End Early)",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def truncate_schedule_op(
  body: TruncateScheduleOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """End a schedule early — delete forward facts + stale drafts."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="truncate-schedule",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_truncate_schedule(
          session,
          body.structure_id,
          body,
          updated_by=str(user.id),
          service=_schedule_svc,
        )
    except ValueError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "schedule_truncated"),
  )


@router.post(
  "/create-closing-entry",
  response_model=OperationEnvelope,
  operation_id="opCreateClosingEntry",
  summary="Create Closing Entry",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_closing_entry_op(
  body: CreateClosingEntryOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Create a draft closing entry from a schedule's facts for a period."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-closing-entry",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_create_closing_entry(
          session,
          body.structure_id,
          body,
          created_by=str(user.id),
          service=_schedule_svc,
        )
    except ValueError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "closing_entry_created"),
  )


@router.post(
  "/create-manual-closing-entry",
  response_model=OperationEnvelope,
  operation_id="opCreateManualClosingEntry",
  summary="Create Manual Closing Entry",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_manual_closing_entry_op(
  body: CreateManualClosingEntryRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Create a manual (non-schedule) draft closing entry."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-manual-closing-entry",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_create_manual_closing_entry(
          session, body, created_by=str(user.id), service=_schedule_svc
        )
    except ValueError as e:
      raise HTTPException(status_code=422, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "manual_entry_created"),
  )


# ═══════════════════════════════════════════════════════════════════════════
# Taxonomy operations
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-taxonomy",
  response_model=OperationEnvelope,
  operation_id="opCreateTaxonomy",
  summary="Create Taxonomy",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_taxonomy_op(
  body: CreateTaxonomyRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Create a new taxonomy definition."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-taxonomy",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_create_taxonomy(session, body, created_by=str(user.id))
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/create-structure",
  response_model=OperationEnvelope,
  operation_id="opCreateStructure",
  summary="Create Structure",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_structure_op(
  body: CreateStructureRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Create a new structure (statement, mapping, schedule, etc.)."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-structure",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_create_structure(session, body, created_by=str(user.id))
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/create-mapping-association",
  response_model=OperationEnvelope,
  operation_id="opCreateMappingAssociation",
  summary="Create Mapping Association",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_mapping_association_op(
  body: CreateMappingAssociationOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Add a mapping association (CoA element → reporting concept)."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-mapping-association",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_create_mapping_association(
            session,
            body.mapping_id,
            body,
            created_by=str(user.id),
          )
        except MappingStructureNotFoundError:
          raise HTTPException(status_code=404, detail="Mapping not found")
        except ElementNotFoundError as e:
          # Match the old router's 400 status for element-not-found
          raise HTTPException(
            status_code=400, detail=f"{e.side.capitalize()} element not found"
          )
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/delete-mapping-association",
  response_model=OperationEnvelope,
  operation_id="opDeleteMappingAssociation",
  summary="Delete Mapping Association",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def delete_mapping_association_op(
  body: DeleteMappingAssociationOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Remove a mapping association."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-mapping-association",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        deleted = cmd_delete_mapping_association(
          session, body.mapping_id, body.association_id
        )
    except (ValueError, ProgrammingError):
      raise _ledger_404()
    if not deleted:
      raise HTTPException(status_code=404, detail="Association not found")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


class AutoMapElementsOperation(BaseModel):
  """Request body for the auto-map-elements async operation."""

  mapping_id: str


@router.post(
  "/auto-map-elements",
  response_model=OperationEnvelope,
  status_code=202,
  operation_id="opAutoMapElements",
  summary="Auto-Map Elements via AI (async)",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def auto_map_elements_op(
  body: AutoMapElementsOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Trigger autonomous CoA → US GAAP mapping via background worker.

  This is the only async/Dagster-dispatched ledger operation. Instead
  of running synchronously, it enqueues a `MappingAgent` task and
  returns a `pending` envelope with the worker-issued operation_id —
  callers subscribe via `/v1/operations/{operation_id}/stream` for SSE
  progress events.

  Confidence thresholds (in the agent):
  - ≥0.90: auto-approved mapping
  - 0.70-0.89: flagged for review
  - <0.70: skipped (left unmapped)
  """
  from robosystems.worker.client import enqueue_task

  op_name = "auto-map-elements"
  user_id = str(user.id)
  body_fingerprint = fingerprint_body(body)

  # Idempotency cache lookup (manual — execute_operation handles it for
  # sync ops, but pending ops bypass that path because they don't
  # produce a normal `result` payload). Same Stripe-style semantics:
  # replay on (user, graph, key, body) match, conflict on body change.
  if idempotency_key is not None:
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
      )
      return cached

  task_response = await enqueue_task(
    task_type="agent",
    graph_id=graph_id,
    user_id=user_id,
    params={"agent_type": "mapping", "mapping_id": body.mapping_id},
  )

  envelope = wrap_pending(
    op_name,
    operation_id=task_response["operation_id"],
    partial_result={
      "operation_type": task_response.get("operation_type"),
      "links": task_response.get("_links"),
      "deduplicated": task_response.get("deduplicated", False),
    },
  )

  if idempotency_key is not None:
    await cache.put(
      user_id, graph_id, op_name, idempotency_key, envelope, body_fingerprint
    )

  log_operation_audit(
    operation_name=op_name,
    operation_id=envelope.operation_id,
    user_id=user_id,
    graph_id=graph_id,
    duration_ms=0.0,
    status="pending",
    idempotency_key=idempotency_key,
  )
  return envelope


# ═══════════════════════════════════════════════════════════════════════════
# Report operations
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-report",
  response_model=OperationEnvelope,
  operation_id="opCreateReport",
  summary="Create Report",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_report_op(
  body: CreateReportRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Create a report definition, generate facts, and mark as published."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_create_report(session, graph_id, body, created_by=str(user.id))
        except TaxonomyNotFoundError as e:
          raise HTTPException(status_code=422, detail=f"Taxonomy '{e}' not found.")
        except NoEntityError as e:
          raise HTTPException(status_code=422, detail=str(e))
    except (ValueError, ProgrammingError) as e:
      if isinstance(e, ProgrammingError) and not _is_schema_missing(e):
        raise
      raise _ledger_404()

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "report_generated"),
  )


@router.post(
  "/regenerate-report",
  response_model=OperationEnvelope,
  operation_id="opRegenerateReport",
  summary="Regenerate Report",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def regenerate_report_op(
  body: RegenerateReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Regenerate a report with new period dates."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="regenerate-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_regenerate_report(
            session, graph_id, body.report_id, body, acting_user_id=str(user.id)
          )
        except ReportNotFoundError:
          raise HTTPException(
            status_code=404, detail=f"Report '{body.report_id}' not found."
          )
        except NotAuthorizedError:
          raise HTTPException(
            status_code=403, detail="Not authorized to modify this report."
          )
        except ValueError as e:
          raise HTTPException(status_code=422, detail=str(e))
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "report_generated"),
  )


@router.post(
  "/delete-report",
  response_model=OperationEnvelope,
  operation_id="opDeleteReport",
  summary="Delete Report",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def delete_report_op(
  body: DeleteReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Delete a report definition and its facts."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          deleted = cmd_delete_report(session, body.report_id, str(user.id))
        except NotAuthorizedError:
          raise HTTPException(
            status_code=403, detail="Not authorized to delete this report."
          )
    except (ValueError, ProgrammingError):
      raise _ledger_404()
    if not deleted:
      raise HTTPException(
        status_code=404, detail=f"Report '{body.report_id}' not found."
      )
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/share-report",
  response_model=OperationEnvelope,
  operation_id="opShareReport",
  summary="Share Report",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def share_report_op(
  body: ShareReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Share a published report to a publish list's members."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="share-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      return cmd_share_report(
        graph_id, body.report_id, body, acting_user_id=str(user.id)
      )
    except ReportPublishListNotFoundError:
      raise HTTPException(status_code=404, detail="Publish list not found.")
    except PublishListEmptyError:
      raise HTTPException(status_code=422, detail="Publish list has no members.")
    except ReportNotFoundError:
      raise HTTPException(
        status_code=404, detail=f"Report '{body.report_id}' not found."
      )
    except NotAuthorizedError:
      raise HTTPException(
        status_code=403, detail="Not authorized to share this report."
      )
    except ReportNotPublishedError:
      raise HTTPException(
        status_code=422, detail="Only published reports can be shared."
      )
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# Publish list operations
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-publish-list",
  response_model=OperationEnvelope,
  operation_id="opCreatePublishList",
  summary="Create Publish List",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def create_publish_list_op(
  body: CreatePublishListRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Create a new publish list."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="create-publish-list",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_create_publish_list(session, body, created_by=str(user.id))
        except PublishListNameConflictError as e:
          raise HTTPException(status_code=409, detail=str(e))
    except ProgrammingError:
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/update-publish-list",
  response_model=OperationEnvelope,
  operation_id="opUpdatePublishList",
  summary="Update Publish List",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def update_publish_list_op(
  body: UpdatePublishListOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Update a publish list's name / description."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="update-publish-list",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_update_publish_list(
            session, body.list_id, body, acting_user_id=str(user.id)
          )
        except PublishListNotFoundError:
          raise HTTPException(status_code=404, detail="Publish list not found.")
        except PublishListNotAuthorizedError as e:
          raise HTTPException(status_code=403, detail=str(e))
        except PublishListNameConflictError as e:
          raise HTTPException(status_code=409, detail=str(e))
    except ProgrammingError:
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/delete-publish-list",
  response_model=OperationEnvelope,
  operation_id="opDeletePublishList",
  summary="Delete Publish List",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def delete_publish_list_op(
  body: DeletePublishListOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Delete a publish list."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="delete-publish-list",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          deleted = cmd_delete_publish_list(session, body.list_id, str(user.id))
        except PublishListNotAuthorizedError as e:
          raise HTTPException(status_code=403, detail=str(e))
    except ProgrammingError:
      raise _ledger_404()
    if not deleted:
      raise HTTPException(status_code=404, detail="Publish list not found.")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/add-publish-list-members",
  response_model=OperationEnvelope,
  operation_id="opAddPublishListMembers",
  summary="Add Members to Publish List",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def add_publish_list_members_op(
  body: AddPublishListMembersOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Add target graphs as members of a publish list."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="add-publish-list-members",
    idempotency_key=idempotency_key,
    body=body,
  )

  # The inherited AddMembersRequest is what the ops function takes; unwrap
  # our dispatch wrapper back to it so the ops layer sees the same shape.
  add_body = AddMembersRequest(target_graph_ids=body.target_graph_ids)

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_add_publish_list_members(
            session, body.list_id, graph_id, add_body, added_by=str(user.id)
          )
        except PublishListNotFoundError:
          raise HTTPException(status_code=404, detail="Publish list not found.")
        except TargetGraphsNotFoundError as e:
          raise HTTPException(status_code=404, detail=str(e))
        except TargetGraphMissingExtensionError as e:
          raise HTTPException(status_code=422, detail=str(e))
        except SelfAddError:
          raise HTTPException(
            status_code=422, detail="Cannot add your own graph to a publish list."
          )
        except MembersAlreadyPresentError as e:
          raise HTTPException(status_code=409, detail=str(e))
    except ProgrammingError:
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/remove-publish-list-member",
  response_model=OperationEnvelope,
  operation_id="opRemovePublishListMember",
  summary="Remove Member from Publish List",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
async def remove_publish_list_member_op(
  body: RemovePublishListMemberOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  """Remove a target graph from a publish list."""
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="remove-publish-list-member",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        deleted = cmd_remove_publish_list_member(session, body.list_id, body.member_id)
    except ProgrammingError:
      raise _ledger_404()
    if not deleted:
      raise HTTPException(status_code=404, detail="Member not found in this list.")
    return DeleteResult(deleted=True)

  return await _dispatch(ctx, _runner, cache)


# NOTE: `build-fact-grid` lives in the sibling `views.py` router.
# It's mounted at the same `/extensions/roboledger/{graph_id}/operations`
# prefix in `main.py`, but on its own router so it can be mounted
# independently of `ROBOLEDGER_ENABLED` — SEC-only deployments still need
# the fact-grid endpoint without enabling RoboLedger tenants.
