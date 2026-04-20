"""RoboLedger operation routes.

Every route follows the pattern:

1. `POST /extensions/roboledger/{graph_id}/operations/{op_name}`
2. Typed request body (path params embedded in body for
   update/delete commands)
3. A `_runner()` closure that opens an extensions session, calls the
   ops layer, translates domain errors into HTTPExceptions
4. `execute_operation(ctx, runner, cache)` handles envelope +
   idempotency + audit

**Registered (41):**

- Entity: `update-entity`
- Fiscal calendar / periods: `initialize`, `set-close-target`,
  `close-period`, `reopen-period`
- Schedules: `create-schedule`, `update-schedule`, `delete-schedule`,
  `truncate-schedule`, `create-closing-entry`, `create-manual-closing-entry`
- Taxonomies: `create-taxonomy`, `update-taxonomy`, `delete-taxonomy`,
  `create-structure`, `update-structure`, `delete-structure`,
  `create-mapping-association`, `delete-mapping-association`
- Elements (native CoA writes): `create-element`, `update-element`,
  `delete-element`
- Associations (bulk, generalized): `create-associations`,
  `update-association`, `delete-association`
- Transactions (standalone business events): `create-transaction`
- Journal entries (native accounting writes): `create-journal-entry`,
  `update-journal-entry`, `delete-journal-entry`, `reverse-journal-entry`
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
  GraphExtensionContext,
  OperationRegistrar,
  OperationSpec,
  require_graph_extension,
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
  get_idempotency_cache,
  log_operation_audit,
  wrap_pending,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.extensions.entity import UpdateEntityRequest
from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodRequest,
  InitializeLedgerRequest,
  ReopenPeriodRequest,
  SetCloseTargetRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  DeleteJournalEntryRequest,
  ReverseJournalEntryRequest,
  UpdateJournalEntryRequest,
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
  CreateClosingEntryOperation,
  CreateManualClosingEntryRequest,
  CreateScheduleRequest,
  DeleteScheduleRequest,
  TruncateScheduleOperation,
  UpdateScheduleRequest,
)
from robosystems.models.api.extensions.taxonomies import (
  BulkCreateAssociationsRequest,
  CreateElementRequest,
  CreateMappingAssociationOperation,
  CreateStructureRequest,
  CreateTaxonomyRequest,
  DeleteAssociationRequest,
  DeleteElementRequest,
  DeleteStructureRequest,
  DeleteTaxonomyRequest,
  LinkEntityTaxonomyRequest,
  UpdateAssociationRequest,
  UpdateElementRequest,
  UpdateStructureRequest,
  UpdateTaxonomyRequest,
)
from robosystems.models.api.extensions.transactions import CreateTransactionRequest
from robosystems.models.core import User
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  LibraryImmutableError,
)
from robosystems.operations.roboledger.commands.elements import (
  ElementCycleError,
)
from robosystems.operations.roboledger.commands.elements import (
  ElementNotFoundError as ElementMissingError,
)
from robosystems.operations.roboledger.commands.elements import (
  create_element as cmd_create_element,
)
from robosystems.operations.roboledger.commands.elements import (
  delete_element as cmd_delete_element,
)
from robosystems.operations.roboledger.commands.elements import (
  update_element as cmd_update_element,
)
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
from robosystems.operations.roboledger.commands.journal_entries import (
  JournalEntryNotDraftError,
  JournalEntryNotFoundError,
  JournalEntryNotPostedError,
  UnbalancedJournalEntryError,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry as cmd_create_journal_entry,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_transaction as cmd_create_transaction,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  delete_journal_entry as cmd_delete_journal_entry,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  reverse_journal_entry as cmd_reverse_journal_entry,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  update_journal_entry as cmd_update_journal_entry,
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
  ScheduleNotFoundError,
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
  delete_schedule as cmd_delete_schedule,
)
from robosystems.operations.roboledger.commands.schedules import (
  truncate_schedule as cmd_truncate_schedule,
)
from robosystems.operations.roboledger.commands.schedules import (
  update_schedule as cmd_update_schedule,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  AssociationNotFoundError,
  ElementNotFoundError,
  EntityNotFoundError,
  MappingStructureNotFoundError,
  StructureNotFoundError,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  TaxonomyNotFoundError as TaxonomyMissingError,  # alias: avoids collision with commands.reports.TaxonomyNotFoundError
)
from robosystems.operations.roboledger.commands.taxonomies import (
  bulk_create_associations as cmd_bulk_create_associations,
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
  delete_association as cmd_delete_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  delete_mapping_association as cmd_delete_mapping_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  delete_structure as cmd_delete_structure,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  delete_taxonomy as cmd_delete_taxonomy,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  link_entity_taxonomy as cmd_link_entity_taxonomy,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  update_association as cmd_update_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  update_structure as cmd_update_structure,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  update_taxonomy as cmd_update_taxonomy,
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

router = APIRouter()

_OP_TAG = "Extensions: RoboLedger"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)

# Stateless service singletons reused across requests (same pattern
# as the old router-level `_svc = FiscalCalendarService()`).
_fiscal_svc = FiscalCalendarService()
_close_svc = PeriodCloseService(_fiscal_svc)


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
# Operation registrar — declarative registration for simple operations.
#
# New write ops should land as `_registrar.register(OperationSpec(...))`
# calls instead of hand-written `@router.post` blocks. Operations with
# unusual needs (async Dagster dispatch, platform-DB dependencies,
# custom multi-stage error trees) still use the hand-written pattern
# further down in this file.
# ───────────────────────────────────────────────────────────────────────────

_registrar = OperationRegistrar(
  router=router,
  domain="roboledger",
  tag=_OP_TAG,
  rate_limit_dep=_RATE_LIMIT,
  ctx_builder=_ctx,
  dispatcher=_dispatch,
  session_factory=extensions_session,
  schema_missing_404=_ledger_404,
  user_dep=get_current_user_with_graph,
  graph_id_pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  extension="roboledger",
)

# Shared callable so every hand-written `@router.post` endpoint below
# participates in FastAPI's dependency-resolution cache (one factory
# call at module load, not per-endpoint).
_require_roboledger = require_graph_extension("roboledger")


# ───────────────────────────────────────────────────────────────────────────
# Operation request bodies — wrap path-specific IDs into the request body.
# ───────────────────────────────────────────────────────────────────────────


class SetCloseTargetOperation(SetCloseTargetRequest):
  pass  # `period` already in body


class ClosePeriodOperation(ClosePeriodRequest):
  period: str


class ReopenPeriodOperation(ReopenPeriodRequest):
  period: str


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
  description="Only provided (non-null) fields are updated.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/update-entity",
  method="POST",
  business_event_type="ledger_update_entity",
)
async def update_entity_op(
  body: UpdateEntityRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  description="One-time setup: creates the fiscal calendar and seeds periods. Returns 409 if already initialized.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/initialize",
  method="POST",
  business_event_type="ledger_initialize",
)
async def initialize_op(
  body: InitializeLedgerRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
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
  description="Period format: YYYY-MM. The close target is the user-controlled goal date, distinct from `closed_through` (what's actually closed).",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/set-close-target",
  method="POST",
  business_event_type="ledger_set_close_target",
)
async def set_close_target_op(
  body: SetCloseTargetOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
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
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/close-period",
  method="POST",
  business_event_type="ledger_close_period",
)
async def close_period_op(
  body: ClosePeriodOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
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
  description="Decrements `closed_through` by one — only the most recently closed period can be reopened.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/reopen-period",
  method="POST",
  business_event_type="ledger_reopen_period",
)
async def reopen_period_op(
  body: ReopenPeriodOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  platform_db: Session = Depends(get_db_session),
) -> OperationEnvelope:
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
#
# Registered on the registrar (see `update_schedule_op`/`delete_schedule_op`
# below). `create-schedule`, `truncate-schedule`, `create-closing-entry`,
# and `create-manual-closing-entry` are all declared there. Keeping them
# grouped with update/delete keeps schedule-surface specs together.
# ═══════════════════════════════════════════════════════════════════════════


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
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/create-taxonomy",
  method="POST",
  business_event_type="ledger_create_taxonomy",
)
async def create_taxonomy_op(
  body: CreateTaxonomyRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  description="Structures organize elements within a taxonomy. Types: `statement`, `mapping`, `schedule`, `presentation`, `calculation`.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/create-structure",
  method="POST",
  business_event_type="ledger_create_structure",
)
async def create_structure_op(
  body: CreateStructureRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
    except LibraryImmutableError as exc:
      raise HTTPException(status_code=403, detail=str(exc)) from exc
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


# `create-mapping-association` is registered on the registrar — see the
# "Taxonomy mapping write" block below.


@router.post(
  "/delete-mapping-association",
  response_model=OperationEnvelope,
  operation_id="opDeleteMappingAssociation",
  summary="Delete Mapping Association",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/delete-mapping-association",
  method="POST",
  business_event_type="ledger_delete_mapping_association",
)
async def delete_mapping_association_op(
  body: DeleteMappingAssociationOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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


# ═══════════════════════════════════════════════════════════════════════════
# Native-accounting CRUD surface — registered via the factory.
#
# Each operation below is a declarative OperationSpec. The registrar
# builds the FastAPI route, wraps it with metrics + idempotency, and
# translates domain exceptions via the error_map. Adding a new op is
# a single OperationSpec block — not 50 lines of route boilerplate.
#
# These module-level names (`update_taxonomy_op`, etc.) exist so tests
# can import the handler functions directly; the registrar returns the
# metrics-wrapped handler, matching what a hand-written decorator stack
# would leave at module scope.
# ═══════════════════════════════════════════════════════════════════════════

# ── Taxonomy update + delete ──────────────────────────────────────────────

update_taxonomy_op = _registrar.register(
  OperationSpec(
    name="update-taxonomy",
    summary="Update Taxonomy",
    description="Update mutable fields on a taxonomy. `taxonomy_type` is immutable.",
    command=cmd_update_taxonomy,
    request_model=UpdateTaxonomyRequest,
    error_map={TaxonomyMissingError: 404, LibraryImmutableError: 403},
    requires_created_by=False,
  )
)

delete_taxonomy_op = _registrar.register(
  OperationSpec(
    name="delete-taxonomy",
    summary="Delete Taxonomy",
    description=(
      "Soft-delete a taxonomy (sets `is_active=false`). Historical "
      "references remain valid."
    ),
    command=cmd_delete_taxonomy,
    request_model=DeleteTaxonomyRequest,
    error_map={TaxonomyMissingError: 404, LibraryImmutableError: 403},
    requires_created_by=False,
  )
)

link_entity_taxonomy_op = _registrar.register(
  OperationSpec(
    name="link-entity-taxonomy",
    summary="Link Entity to Taxonomy",
    description=(
      "Link the graph's entity to a taxonomy (creates the "
      "ENTITY_HAS_TAXONOMY edge). Idempotent — returns existing "
      "linkage if it already exists. Required after creating a CoA "
      "taxonomy so the platform knows which chart of accounts the "
      "entity reports under."
    ),
    command=cmd_link_entity_taxonomy,
    request_model=LinkEntityTaxonomyRequest,
    error_map={
      EntityNotFoundError: 404,
      TaxonomyMissingError: 404,
    },
    requires_created_by=False,
  )
)

# ── Structure update + delete ─────────────────────────────────────────────

update_structure_op = _registrar.register(
  OperationSpec(
    name="update-structure",
    summary="Update Structure",
    description=(
      "Update mutable fields on a structure. `structure_type` and "
      "`taxonomy_id` are immutable."
    ),
    command=cmd_update_structure,
    request_model=UpdateStructureRequest,
    error_map={StructureNotFoundError: 404, LibraryImmutableError: 403},
    requires_created_by=False,
  )
)

delete_structure_op = _registrar.register(
  OperationSpec(
    name="delete-structure",
    summary="Delete Structure",
    description=(
      "Soft-delete a structure (sets `is_active=false`). Associations "
      "referencing it are effectively orphaned."
    ),
    command=cmd_delete_structure,
    request_model=DeleteStructureRequest,
    error_map={StructureNotFoundError: 404, LibraryImmutableError: 403},
    requires_created_by=False,
  )
)

# ── Element CRUD ──────────────────────────────────────────────────────────

create_element_op = _registrar.register(
  OperationSpec(
    name="create-element",
    summary="Create Element",
    description=(
      "Create an element within a taxonomy. For chart-of-accounts "
      "taxonomies this is how native accounts are added."
    ),
    command=cmd_create_element,
    request_model=CreateElementRequest,
    error_map={
      TaxonomyMissingError: 404,
      LibraryImmutableError: 403,
      ElementMissingError: (
        400,
        lambda e: f"Parent element not found: {e.element_id}",  # type: ignore[attr-defined]
      ),
    },
  )
)

update_element_op = _registrar.register(
  OperationSpec(
    name="update-element",
    summary="Update Element",
    description=(
      "Update mutable fields on an element. `taxonomy_id` and `source` "
      "are immutable. Reparenting cascades path/depth to descendants."
    ),
    command=cmd_update_element,
    request_model=UpdateElementRequest,
    error_map={
      ElementMissingError: 404,
      ElementCycleError: 422,
      LibraryImmutableError: 403,
    },
    requires_created_by=False,
  )
)

delete_element_op = _registrar.register(
  OperationSpec(
    name="delete-element",
    summary="Delete Element",
    description=(
      "Soft-delete an element (sets `is_active=false`). Historical "
      "line items referencing it remain valid."
    ),
    command=cmd_delete_element,
    request_model=DeleteElementRequest,
    error_map={ElementMissingError: 404, LibraryImmutableError: 403},
    requires_created_by=False,
  )
)

# ── Association bulk + update + delete ───────────────────────────────────

create_associations_op = _registrar.register(
  OperationSpec(
    name="create-associations",
    summary="Create Associations (Bulk)",
    description=(
      "Create N associations in a single structure, atomically. Handles "
      "50+ presentation arcs, 25+ calculation arcs, or a full table "
      "linkbase in one call. Any failed row rolls back the batch."
    ),
    command=cmd_bulk_create_associations,
    request_model=BulkCreateAssociationsRequest,
    error_map={
      StructureNotFoundError: 404,
      ElementNotFoundError: (
        400,
        lambda e: (  # type: ignore[attr-defined]
          f"{e.side.capitalize()} element not found: {e.element_id}"
        ),
      ),
    },
  )
)

update_association_op = _registrar.register(
  OperationSpec(
    name="update-association",
    summary="Update Association",
    description=(
      "Update mutable fields on an association. `from_element_id`, "
      "`to_element_id`, `association_type`, and `structure_id` are "
      "immutable — delete and recreate instead."
    ),
    command=cmd_update_association,
    request_model=UpdateAssociationRequest,
    error_map={AssociationNotFoundError: 404, LibraryImmutableError: 403},
    requires_created_by=False,
  )
)

delete_association_op = _registrar.register(
  OperationSpec(
    name="delete-association",
    summary="Delete Association",
    description=(
      "Hard-delete an association. Generalizes delete-mapping-association "
      "to all association types (presentation, calculation, mapping)."
    ),
    command=cmd_delete_association,
    request_model=DeleteAssociationRequest,
    error_map={AssociationNotFoundError: 404, LibraryImmutableError: 403},
    requires_created_by=False,
  )
)

# ── Transaction + Journal entry CRUD ────────────────────────────────────

create_transaction_op = _registrar.register(
  OperationSpec(
    name="create-transaction",
    summary="Create Transaction",
    description=(
      "Create a standalone business-event Transaction without entries. "
      "Returns a transaction_id that can be passed to create-journal-entry "
      "to attach one or more journal entries to this event. Use this when "
      "a single event (invoice, payment, deposit) produces multiple entries "
      "over its lifecycle."
    ),
    command=cmd_create_transaction,
    request_model=CreateTransactionRequest,
    error_map={ValueError: 422},
  )
)

create_journal_entry_op = _registrar.register(
  OperationSpec(
    name="create-journal-entry",
    summary="Create Journal Entry",
    description=(
      "Create a new draft journal entry with balanced line items. "
      "Enforces DR=CR at the validation layer. Entries are always "
      "created as drafts; posting happens via close-period or a "
      "future per-entry post op."
    ),
    command=cmd_create_journal_entry,
    request_model=CreateJournalEntryRequest,
    error_map={
      ClosedPeriodError: 422,
      UnbalancedJournalEntryError: 422,
      ValueError: 422,
    },
  )
)

update_journal_entry_op = _registrar.register(
  OperationSpec(
    name="update-journal-entry",
    summary="Update Journal Entry",
    description=(
      "Update a draft journal entry. Posted entries are immutable and "
      "must be corrected via reverse-journal-entry. If line_items is "
      "provided, existing line items are replaced atomically and the "
      "new set must balance."
    ),
    command=cmd_update_journal_entry,
    request_model=UpdateJournalEntryRequest,
    error_map={
      JournalEntryNotFoundError: 404,
      JournalEntryNotDraftError: 422,
      ClosedPeriodError: 422,
      UnbalancedJournalEntryError: 422,
      ValueError: 422,
    },
    requires_created_by=False,
  )
)

delete_journal_entry_op = _registrar.register(
  OperationSpec(
    name="delete-journal-entry",
    summary="Delete Journal Entry",
    description=(
      "Hard-delete a draft journal entry. Posted entries are immutable "
      "and must be reversed instead."
    ),
    command=cmd_delete_journal_entry,
    request_model=DeleteJournalEntryRequest,
    error_map={
      JournalEntryNotFoundError: 404,
      JournalEntryNotDraftError: 422,
    },
    requires_created_by=False,
  )
)

reverse_journal_entry_op = _registrar.register(
  OperationSpec(
    name="reverse-journal-entry",
    summary="Reverse Journal Entry",
    description=(
      "Reverse a posted journal entry by creating a new offsetting "
      "entry (debits ↔ credits) and marking the original as "
      "status='reversed'. Both entries stay in the ledger — the audit "
      "trail shows original + reversal side by side."
    ),
    command=cmd_reverse_journal_entry,
    request_model=ReverseJournalEntryRequest,
    error_map={
      JournalEntryNotFoundError: 404,
      JournalEntryNotPostedError: 422,
      ClosedPeriodError: 422,
      ValueError: 422,
    },
  )
)

# ── Schedule create / update / delete / truncate + closing entries ───────


def _validate_schedule_period(body: CreateScheduleRequest) -> None:
  """Pre-DB guard for `create-schedule`."""
  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")


create_schedule_op = _registrar.register(
  OperationSpec(
    name="create-schedule",
    summary="Create Schedule",
    description=(
      "Create a schedule and pre-generate monthly amortization facts "
      "spanning the period range. `entry_template` defines the debit/credit "
      "elements used by `create-closing-entry` each period."
    ),
    command=cmd_create_schedule,
    request_model=CreateScheduleRequest,
    error_map={ValueError: 422},
    pre_validate=_validate_schedule_period,
    mark_stale_reason="schedule_created",
  )
)

truncate_schedule_op = _registrar.register(
  OperationSpec(
    name="truncate-schedule",
    summary="Truncate Schedule (End Early)",
    description=(
      "End a schedule early by deleting forward facts and any stale draft "
      "closing entries past the cutoff. Historical facts and posted entries "
      "are preserved. Use this when a business event (asset disposal, "
      "contract cancellation) shortens the schedule's lifespan."
    ),
    command=cmd_truncate_schedule,
    request_model=TruncateScheduleOperation,
    error_map={ValueError: 422, ScheduleNotFoundError: 404},
    mark_stale_reason="schedule_truncated",
  )
)

create_closing_entry_op = _registrar.register(
  OperationSpec(
    name="create-closing-entry",
    summary="Create Closing Entry",
    description=(
      "Create a draft closing entry pre-populated from a schedule's facts "
      "for the given period. Idempotent — safe to call repeatedly; the "
      "`outcome` field describes what happened "
      "(`created`, `unchanged`, `regenerated`, `removed`, `skipped`)."
    ),
    command=cmd_create_closing_entry,
    request_model=CreateClosingEntryOperation,
    error_map={ValueError: 422},
    mark_stale_reason="closing_entry_created",
  )
)

create_manual_closing_entry_op = _registrar.register(
  OperationSpec(
    name="create-manual-closing-entry",
    summary="Create Manual Closing Entry",
    description=(
      "Create a draft closing entry with manually specified line items — "
      "not tied to a schedule. Use for one-off business events (asset "
      "disposal, correcting entry, impairment). Total debits must equal "
      "total credits."
    ),
    command=cmd_create_manual_closing_entry,
    request_model=CreateManualClosingEntryRequest,
    error_map={ValueError: 422},
    mark_stale_reason="manual_entry_created",
  )
)

update_schedule_op = _registrar.register(
  OperationSpec(
    name="update-schedule",
    summary="Update Schedule",
    description=(
      "Update mutable fields on a schedule: name, entry_template, "
      "schedule_metadata. Period range and monthly_amount are NOT "
      "editable — use truncate-schedule + create-schedule instead."
    ),
    command=cmd_update_schedule,
    request_model=UpdateScheduleRequest,
    error_map={ScheduleNotFoundError: 404},
    requires_created_by=False,
  )
)

delete_schedule_op = _registrar.register(
  OperationSpec(
    name="delete-schedule",
    summary="Delete Schedule",
    description=(
      "Permanently delete a schedule, cascading through facts and "
      "associations. For ending a schedule early without removing "
      "history, use truncate-schedule instead."
    ),
    command=cmd_delete_schedule,
    request_model=DeleteScheduleRequest,
    error_map={ScheduleNotFoundError: 404},
    requires_created_by=False,
  )
)

# ── Taxonomy mapping write ────────────────────────────────────────────────

create_mapping_association_op = _registrar.register(
  OperationSpec(
    name="create-mapping-association",
    summary="Create Mapping Association",
    description=(
      "Link a chart-of-accounts element to a US GAAP reporting concept. "
      "For bulk associations (presentation/calculation linkbases, 50+ "
      "arcs at once) use `create-associations` instead."
    ),
    command=cmd_create_mapping_association,
    request_model=CreateMappingAssociationOperation,
    error_map={
      MappingStructureNotFoundError: (404, lambda _e: "Mapping not found"),
      LibraryImmutableError: 403,
      ElementNotFoundError: (
        400,
        lambda e: f"{e.side.capitalize()} element not found",  # type: ignore[attr-defined]
      ),
    },
  )
)


class AutoMapElementsOperation(BaseModel):
  """Request body for the auto-map-elements async operation."""

  mapping_id: str


@router.post(
  "/auto-map-elements",
  response_model=OperationEnvelope,
  status_code=202,
  operation_id="opAutoMapElements",
  summary="Auto-Map Elements via AI (async)",
  description="Dispatches to the background worker — returns a `pending` envelope immediately. Monitor via SSE at `/v1/operations/{operation_id}/stream`. Confidence thresholds: ≥0.90 auto-approved, 0.70–0.89 flagged for review, <0.70 skipped.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/auto-map-elements",
  method="POST",
  business_event_type="ledger_auto_map_elements",
)
async def auto_map_elements_op(
  body: AutoMapElementsOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  from robosystems.worker.client import enqueue_task

  op_name = "auto-map-elements"
  user_id = str(user.id)
  body_fingerprint = fingerprint_body(body)

  replay = await check_idempotency(
    cache,
    user_id,
    graph_id,
    op_name,
    idempotency_key,
    body_fingerprint,
    event="extensions.operation",
  )
  if replay is not None:
    return replay

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
    created_by=user_id,
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
  description="Generates report facts from the ledger and marks the report as published.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/create-report",
  method="POST",
  business_event_type="ledger_create_report",
)
async def create_report_op(
  body: CreateReportRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/regenerate-report",
  method="POST",
  business_event_type="ledger_regenerate_report",
)
async def regenerate_report_op(
  body: RegenerateReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  description="Deletes the report definition and all generated facts.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/delete-report",
  method="POST",
  business_event_type="ledger_delete_report",
)
async def delete_report_op(
  body: DeleteReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  description="Only published reports can be shared. Sends the report to all members of the target publish list.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/share-report",
  method="POST",
  business_event_type="ledger_share_report",
)
async def share_report_op(
  body: ShareReportOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/create-publish-list",
  method="POST",
  business_event_type="ledger_create_publish_list",
)
async def create_publish_list_op(
  body: CreatePublishListRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  description="Updates the publish list's `name` and/or `description`. Membership is managed via add/remove-member operations.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/update-publish-list",
  method="POST",
  business_event_type="ledger_update_publish_list",
)
async def update_publish_list_op(
  body: UpdatePublishListOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/delete-publish-list",
  method="POST",
  business_event_type="ledger_delete_publish_list",
)
async def delete_publish_list_op(
  body: DeletePublishListOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/add-publish-list-members",
  method="POST",
  business_event_type="ledger_add_publish_list_members",
)
async def add_publish_list_members_op(
  body: AddPublishListMembersOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/remove-publish-list-member",
  method="POST",
  business_event_type="ledger_remove_publish_list_member",
)
async def remove_publish_list_member_op(
  body: RemovePublishListMemberOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
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
