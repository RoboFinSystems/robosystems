"""RoboLedger operation routes.

Every route follows the pattern:

1. `POST /extensions/roboledger/{graph_id}/operations/{op_name}`
2. Typed request body (path params embedded in body for
   update/delete commands)
3. A `_runner()` closure that opens an extensions session, calls the
   ops layer, translates domain errors into HTTPExceptions
4. `execute_operation(ctx, runner, cache)` handles envelope +
   idempotency + audit

**Registered (38) — in logical workflow order:**

- Setup: `initialize`, `update-entity`, `change-reporting-style`
- Ontology / Taxonomy Blocks: `create-taxonomy-block`,
  `update-taxonomy-block`, `delete-taxonomy-block`,
  `link-entity-taxonomy`
- Mapping (craft, not curation): `create-mapping-association`,
  `delete-mapping-association`, `auto-map-elements`
- Information Blocks: `create-information-block`,
  `update-information-block`, `delete-information-block`,
  `evaluate-rules`
- Agents: `create-agent`, `update-agent`
- Event Blocks: `create-event-block`, `update-event-block`,
  `preview-event-block`. `create-event-block` is the single write surface
  for business events that cause GL consequences. Four event types are
  dispatched via the Python handler registry: `journal_entry_recorded`
  (manual journal entry — any type, draft or posted),
  `journal_entry_reversed` (offsetting reversal of a posted entry),
  `schedule_entry_due` (a schedule period matured), and `asset_disposed`
  (atomic disposal + schedule termination, including the schedule
  truncate that previously needed a separate call).
- Journal Entries: `update-journal-entry`, `delete-journal-entry`. All
  GL-write origination flows through
  `create-event-block(event_type='journal_entry_recorded')`; reversal
  through `event_type='journal_entry_reversed'`. The draft-correction
  path (update / delete on a draft entry) is a CRUD surface.
- Close Workflow: `set-close-target`, `close-period`, `reopen-period`.
  Closing-entry drafting goes through
  `create-event-block(event_type='schedule_entry_due')` (schedule-derived)
  or `event_type='journal_entry_recorded'` (manual). Asset disposal via
  `event_type='asset_disposed'` (handles the schedule truncate
  internally).
- Reports: `create-report`, `regenerate-report`, `delete-report`,
  `share-report`
- Publish Lists: `create-publish-list`, `update-publish-list`,
  `delete-publish-list`, `add-publish-list-members`,
  `remove-publish-list-member`

Raw ontology CRUD (taxonomies, structures, elements, non-mapping
associations) is not exposed publicly — the Taxonomy Block envelope is
the only tenant-facing construction path. Mapping associations stay
direct (craft, not curation).

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
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from robosystems.adapters.quickbooks.client.api import QBAuthFailedError
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
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES, DeleteResult
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  EventBlockEnvelope,
  ExecuteEventBlockRequest,
  ExecuteEventBlockResponse,
  UpdateEventBlockRequest,
)
from robosystems.models.api.event_handler import (
  CreateEventHandlerRequest,
  EventHandlerResponse,
  PreviewEventBlockResponse,
  UpdateEventHandlerRequest,
)
from robosystems.models.api.extensions.agent import (
  CreateAgentRequest,
  LedgerAgentResponse,
  UpdateAgentRequest,
)
from robosystems.models.api.extensions.entity import (
  ChangeReportingStyleRequest,
  ChangeReportingStyleResponse,
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  BackfillPlanHistoryRequest,
  BackfillPlanHistoryResponse,
  ClosePeriodRequest,
  ClosePeriodResponse,
  FiscalCalendarResponse,
  InitializeLedgerRequest,
  InitializeLedgerResponse,
  PendingObligationDetailResponse,
  ReopenPeriodRequest,
  SetCloseTargetRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  DeleteJournalEntryRequest,
  JournalEntryResponse,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.publish_lists import (
  AddMembersRequest,
  CreatePublishListRequest,
  PublishListMemberResponse,
  PublishListResponse,
  UpdatePublishListRequest,
)
from robosystems.models.api.extensions.report_package import (
  FileReportRequest,
  TransitionFilingStatusRequest,
)
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  RegenerateReportRequest,
  ReportResponse,
  ShareReportRequest,
  ShareReportResponse,
)
from robosystems.models.api.extensions.schedules import (
  PromoteObligationsRequest,
  PromoteObligationsResponse,
  RebuildScheduleRequest,
  ScheduleCreatedResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse,
  CreateMappingAssociationOperation,
  DeleteMappingAssociationOperation,
  EntityTaxonomyResponse,
  LinkEntityTaxonomyRequest,
)
from robosystems.models.api.extensions.text_blocks import (
  BindTextBlockRequest,
  BindTextBlockResponse,
)
from robosystems.models.api.information_block import (
  AssertMetricsRequest,
  AssertMetricsResponse,
  ComputeForecastRequest,
  ComputeForecastResponse,
  ComputeMetricsRequest,
  ComputeMetricsResponse,
  CreateInformationBlockRequest,
  DeleteInformationBlockRequest,
  DeleteInformationBlockResponse,
  EvaluateRulesRequest,
  EvaluateRulesResponse,
  InformationBlockEnvelope,
  UpdateInformationBlockRequest,
)
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  DeleteTaxonomyBlockResponse,
  TaxonomyBlockEnvelope,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.core import User
from robosystems.operations.event_block import (
  EventNotFoundError,
  InvalidEventTransitionError,
)
from robosystems.operations.event_block import (
  create_event_block as cmd_create_event_block,
)
from robosystems.operations.event_block import (
  execute_event_block as cmd_execute_event_block,
)
from robosystems.operations.event_block import (
  preview_event_block as cmd_preview_event_block,
)
from robosystems.operations.event_block import (
  update_event_block as cmd_update_event_block,
)
from robosystems.operations.event_block.engine import (
  EngineValidationError,
)
from robosystems.operations.event_block.python_handlers._disposal_plan import (
  ScheduleNotFoundError as DisposalScheduleNotFoundError,
)
from robosystems.operations.event_block.python_handlers.journal_entry_recorded import (
  ElementResolutionError,
)
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.event_block.registry import (
  HandlerAmbiguousError,
  HandlerNotFoundError,
)
from robosystems.operations.event_block.template import (
  TemplateInterpolationError,
)
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.information_block.commands import (
  create_information_block as cmd_create_information_block,
)
from robosystems.operations.information_block.commands import (
  delete_information_block as cmd_delete_information_block,
)
from robosystems.operations.information_block.commands import (
  update_information_block as cmd_update_information_block,
)
from robosystems.operations.information_block.forecast_compute import (
  cmd_compute_forecast,
)
from robosystems.operations.information_block.metrics import (
  cmd_assert_metrics,
  cmd_compute_metrics,
)
from robosystems.operations.information_block.rules.commands import (
  cmd_evaluate_rules,
)
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  LibraryImmutableError,
)
from robosystems.operations.roboledger.commands.agent import (
  AgentNotFoundError,
  DuplicateExternalIdError,
)
from robosystems.operations.roboledger.commands.agent import (
  create_agent as cmd_create_agent,
)
from robosystems.operations.roboledger.commands.agent import (
  update_agent as cmd_update_agent,
)
from robosystems.operations.roboledger.commands.entity import update_parent_entity
from robosystems.operations.roboledger.commands.event_handler import (
  EventHandlerNotFoundError,
  TemplateValidationError,
)
from robosystems.operations.roboledger.commands.event_handler import (
  create_event_handler as cmd_create_event_handler,
)
from robosystems.operations.roboledger.commands.event_handler import (
  update_event_handler as cmd_update_event_handler,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  BackfillPreconditionError,
  PeriodNotClosedError,
  PeriodNotFoundInLedgerError,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  backfill_plan_history as cmd_backfill_plan_history,
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
  delete_journal_entry as cmd_delete_journal_entry,
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
from robosystems.operations.roboledger.commands.reporting_style import (
  EntityNotFoundError as ReportingStyleEntityNotFoundError,
)
from robosystems.operations.roboledger.commands.reporting_style import (
  ReportingStyleInvalidError,
  change_reporting_style,
)
from robosystems.operations.roboledger.commands.reports import (
  BundleUploadError,
  InvalidFilingTransitionError,
  NoEntityError,
  NotAuthorizedError,
  PublishListEmptyError,
  ReportNotFiledError,
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
  file_report as cmd_file_report,
)
from robosystems.operations.roboledger.commands.reports import (
  regenerate_report as cmd_regenerate_report,
)
from robosystems.operations.roboledger.commands.reports import (
  share_report as cmd_share_report,
)
from robosystems.operations.roboledger.commands.reports import (
  transition_filing_status as cmd_transition_filing_status,
)
from robosystems.operations.roboledger.commands.schedules import (
  ScheduleNotFoundError,
)
from robosystems.operations.roboledger.commands.schedules import (
  promote_obligations as cmd_promote_obligations,
)
from robosystems.operations.roboledger.commands.schedules import (
  rebuild_schedule as cmd_rebuild_schedule,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  AssociationNotFoundError,
  ElementNotFoundError,
  EntityNotFoundError,
  MappingStructureNotFoundError,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  TaxonomyNotFoundError as TaxonomyMissingError,  # alias: avoids collision with commands.reports.TaxonomyNotFoundError
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association as cmd_create_mapping_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  delete_mapping_association as cmd_delete_mapping_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  link_entity_taxonomy as cmd_link_entity_taxonomy,
)
from robosystems.operations.roboledger.commands.text_blocks import (
  DocumentNotFoundError as TextBlockDocumentNotFoundError,
)
from robosystems.operations.roboledger.commands.text_blocks import (
  bind_text_block as cmd_bind_text_block,
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
  WritebackFailed,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CalendarAlreadyInitializedError,
  InvalidCloseTargetError,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampError,
)
from robosystems.operations.taxonomy_block.commands import (
  TaxonomyAuthoringDisabledError,
)
from robosystems.operations.taxonomy_block.commands import (
  create_taxonomy_block as cmd_create_taxonomy_block,
)
from robosystems.operations.taxonomy_block.commands import (
  delete_taxonomy_block as cmd_delete_taxonomy_block,
)
from robosystems.operations.taxonomy_block.commands import (
  update_taxonomy_block as cmd_update_taxonomy_block,
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
  """Close a single fiscal period. Carries the YYYY-MM `period` in the
  request body alongside the close-time options inherited from
  :class:`ClosePeriodRequest`.
  """

  period: str = Field(
    ...,
    pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
    description=(
      "Period to close, in YYYY-MM. Must be exactly `closed_through + 1` "
      "— close runs sequentially."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"period": "2026-03"},
        {
          "period": "2026-03",
          "allow_stale_sync": True,
          "note": "QB sync down; data manually verified.",
        },
      ]
    },
  )


class ReopenPeriodOperation(ReopenPeriodRequest):
  """Reopen a closed fiscal period."""

  period: str = Field(
    ...,
    pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
    description=(
      "Period to reopen, in YYYY-MM. Any closed period may be reopened. "
      "Reopening the current `closed_through` retreats it by one month; "
      "reopening an earlier period leaves `closed_through` where it is "
      "(a prior-period adjustment), and its re-close restores the period "
      "without moving the pointer."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "period": "2026-03",
          "reason": "Discovered late vendor invoice — needs to land in March.",
        },
      ]
    },
  )


class BackfillPlanHistoryOperation(BackfillPlanHistoryRequest):
  """Compile monthly statement history behind the close boundary."""

  pass  # range and chunking already in body


class RegenerateReportOperation(RegenerateReportRequest):
  """Regenerate facts for an existing Report. Carries `report_id` from
  the path-style RPC body; period overrides are inherited from
  :class:`RegenerateReportRequest`.
  """

  report_id: str = Field(..., description="The Report to regenerate.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0"},
        {
          "report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0",
          "period_start": "2026-04-01",
          "period_end": "2026-06-30",
        },
      ]
    },
  )


class DeleteReportOperation(BaseModel):
  """Delete a Report definition and all its facts."""

  report_id: str = Field(..., description="The Report to delete.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    },
  )


class ShareReportOperation(ShareReportRequest):
  """Share a published Report to every member of a publish list."""

  report_id: str = Field(..., description="The published Report to share.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0",
          "publish_list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0",
        },
      ]
    },
  )


class UpdatePublishListOperation(UpdatePublishListRequest):
  """Update a publish list's metadata. Carries `list_id`."""

  list_id: str = Field(..., description="The publish list to update.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0", "name": "Senior Lenders"},
      ]
    },
  )


class DeletePublishListOperation(BaseModel):
  """Delete a publish list. All membership rows are removed; reports
  previously shared via this list are not affected (each share is an
  independent copy in the recipient's graph).
  """

  list_id: str = Field(..., description="The publish list to delete.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    },
  )


class AddPublishListMembersOperation(AddMembersRequest):
  """Add recipient graphs to a publish list."""

  list_id: str = Field(..., description="The publish list to add members to.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0",
          "target_graph_ids": ["kg_abc12345", "kg_def67890"],
        },
      ]
    },
  )


class RemovePublishListMemberOperation(BaseModel):
  """Remove a single recipient from a publish list."""

  list_id: str = Field(..., description="The publish list.")
  member_id: str = Field(..., description="The membership row to remove.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "list_id": "pl_01HVF8T0M2YTAY3BBNRH0V0",
          "member_id": "plm_01HVF8T0M2YTAY3BBNRH0V0",
        },
      ]
    },
  )


# ═══════════════════════════════════════════════════════════════════════════
# Setup
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/initialize",
  response_model=OperationEnvelope[InitializeLedgerResponse],
  operation_id="initializeLedger",
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
  "/update-entity",
  response_model=OperationEnvelope[LedgerEntityResponse],
  operation_id="updateEntity",
  summary="Update Entity",
  description=(
    "Update the graph's primary entity. Only provided (non-null) fields "
    "are updated. The graph is implicit in the URL — the operation "
    "always targets the graph's primary entity."
  ),
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


change_reporting_style_op = _registrar.register(
  OperationSpec(
    name="change-reporting-style",
    summary="Change Reporting Style",
    description=(
      "Switch the reporting entity's Reporting Style — how its statements "
      "are laid out (equity-form, close-target concept, per-statement "
      "Networks). Validates that the target Style has a complete "
      "composition in the tenant schema, then flips "
      "`entities.reporting_style_id`. Omit `entity_id` to target the "
      "graph's primary entity. Filed Reports are unaffected (their "
      "FactSet rows pin their structures at create-time); new reports use "
      "the new Style. Idempotent on the same id."
    ),
    command=change_reporting_style,
    request_model=ChangeReportingStyleRequest,
    result_type=ChangeReportingStyleResponse,
    error_map={
      ReportingStyleEntityNotFoundError: 404,
      ReportingStyleInvalidError: 422,
    },
    requires_created_by=False,
  )
)


# ═══════════════════════════════════════════════════════════════════════════
# Ontology / Taxonomy Blocks
#
# Taxonomy Block is the only tenant-facing path for ontology curation.
# Raw CRUD (create-taxonomy, create-structure, create/update/delete-element,
# create-associations) is not exposed on the public surface.
# ═══════════════════════════════════════════════════════════════════════════

create_taxonomy_block_op = _registrar.register(
  OperationSpec(
    name="create-taxonomy-block",
    summary="Create Taxonomy Block",
    description=(
      "Create a taxonomy block atomically: one envelope carrying the "
      "taxonomy row plus its structures, elements, associations, and "
      "rules. Dispatches by `taxonomy_type` — `chart_of_accounts` "
      "(declarative tenant CoA), `reporting_extension`, and "
      "`custom_ontology` are supported; `reporting_standard` is "
      "library-origin (501). `reporting_extension` / `custom_ontology` "
      "authoring may be disabled per environment "
      "(TAXONOMY_AUTHORING_ENABLED) — disabled surfaces 403. "
      "NOT the path for a functional close schedule: a structure with "
      "block_type='schedule' here is a bare ontology row with none of the "
      "schedule machinery (per-period facts, schedule_entry_due "
      "obligations, closing-entry generator). To create a working "
      "schedule use create-information-block(block_type='schedule')."
    ),
    command=cmd_create_taxonomy_block,
    request_model=CreateTaxonomyBlockRequest,
    result_type=TaxonomyBlockEnvelope,
    error_map={
      TaxonomyAuthoringDisabledError: 403,
      ValueError: 422,
      NotImplementedError: 501,
    },
    mark_stale_reason="taxonomy_block_created",
  )
)

update_taxonomy_block_op = _registrar.register(
  OperationSpec(
    name="update-taxonomy-block",
    summary="Update Taxonomy Block",
    description=(
      "Incrementally mutate a taxonomy block via typed delta lists "
      "(elements/structures/associations/rules to add, update, remove). "
      "Dispatches by the target taxonomy's stored `taxonomy_type`. "
      "Library-origin block types (`reporting_standard`) surface 501. "
      "`reporting_extension` / `custom_ontology` authoring may be "
      "disabled per environment (TAXONOMY_AUTHORING_ENABLED) — "
      "disabled surfaces 403."
    ),
    command=cmd_update_taxonomy_block,
    request_model=UpdateTaxonomyBlockRequest,
    result_type=TaxonomyBlockEnvelope,
    error_map={
      TaxonomyAuthoringDisabledError: 403,
      ValueError: 422,
      NotImplementedError: 501,
    },
    mark_stale_reason="taxonomy_block_updated",
  )
)

delete_taxonomy_block_op = _registrar.register(
  OperationSpec(
    name="delete-taxonomy-block",
    summary="Delete Taxonomy Block",
    description=(
      "Delete a taxonomy block and return a thin confirmation. "
      "`cascade_facts=True` also deletes Fact rows that reference the "
      "taxonomy's elements; default False fails the delete if such "
      "facts exist. Library-origin block types surface 501."
    ),
    command=cmd_delete_taxonomy_block,
    request_model=DeleteTaxonomyBlockRequest,
    result_type=DeleteTaxonomyBlockResponse,
    error_map={
      ValueError: 422,
      NotImplementedError: 501,
    },
    mark_stale_reason="taxonomy_block_deleted",
  )
)

link_entity_taxonomy_op = _registrar.register(
  OperationSpec(
    name="link-entity-taxonomy",
    summary="Link Entity to Taxonomy",
    description=(
      "Link the graph's entity to a taxonomy. Idempotent — returns "
      "existing linkage if it already exists. CoA blocks auto-link "
      "at create time; use this only to switch the primary CoA or "
      "link a reporting extension / custom ontology explicitly."
    ),
    command=cmd_link_entity_taxonomy,
    request_model=LinkEntityTaxonomyRequest,
    result_type=EntityTaxonomyResponse,
    error_map={
      EntityNotFoundError: 404,
      TaxonomyMissingError: 404,
    },
    requires_created_by=False,
  )
)


# ═══════════════════════════════════════════════════════════════════════════
# Mapping (craft, not curation)
#
# Mapping associations stay direct — mapping is iterative AI-assisted craft,
# not a curation envelope. `auto-map-elements` dispatches to the background
# worker and returns a `pending` envelope immediately.
# ═══════════════════════════════════════════════════════════════════════════

create_mapping_association_op = _registrar.register(
  OperationSpec(
    name="create-mapping-association",
    summary="Create Mapping Association",
    description=(
      "Link a chart-of-accounts element to a US GAAP reporting concept. "
      "One mapping edge per call — use `auto-map-elements` for bulk "
      "AI-assisted mapping. Duplicate (from, to, type) tuples return 409."
    ),
    command=cmd_create_mapping_association,
    request_model=CreateMappingAssociationOperation,
    result_type=AssociationResponse,
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


delete_mapping_association_op = _registrar.register(
  OperationSpec(
    name="delete-mapping-association",
    summary="Delete Mapping Association",
    description=(
      "Remove a single CoA → reporting-concept mapping edge by id. The "
      "mapping structure itself remains; only the association row is "
      "dropped. Use this to correct a wrong mapping — delete the bad edge, "
      "then `create-mapping-association` the right one. Find the "
      "association id via the `library_element_arcs` GraphQL field. "
      "Library-seeded rows cannot be deleted (403)."
    ),
    command=cmd_delete_mapping_association,
    request_model=DeleteMappingAssociationOperation,
    result_type=DeleteResult,
    requires_created_by=False,
    error_map={
      AssociationNotFoundError: (404, lambda _e: "Association not found"),
      LibraryImmutableError: 403,
    },
  )
)


class AutoMapElementsOperation(BaseModel):
  """Run the MappingOperator over a mapping structure (async).

  The MappingOperator walks every unmapped CoA element and proposes
  associations to reporting concepts. Confidence thresholds: ≥0.90
  auto-approved (association created), 0.70-0.89 flagged for review
  (created with `confidence` set; surface it in your UI), <0.70 skipped.
  Returns a `pending` envelope immediately; subscribe to the SSE stream
  for progress.
  """

  mapping_id: str = Field(..., description="The mapping structure to populate.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    },
  )


@router.post(
  "/auto-map-elements",
  response_model=OperationEnvelope,
  status_code=202,
  operation_id="autoMapElements",
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
    task_type="operator",
    graph_id=graph_id,
    user_id=user_id,
    params={"operator_type": "mapping", "mapping_id": body.mapping_id},
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
# Information Blocks
#
# Generic construction envelope for schedules, statement blocks, metrics,
# and future block types. `evaluate-rules` runs the rule engine against a
# block's materialized facts (decoding mode, 6 patterns).
# ═══════════════════════════════════════════════════════════════════════════

create_information_block_op = _registrar.register(
  OperationSpec(
    name="create-information-block",
    summary="Create Information Block",
    description=(
      "Generic Information Block construction entry. `block_type` selects "
      "the registered block type; `payload` is validated against that "
      "type's creation schema at dispatch. Schedule dispatches to the "
      "existing Schedule machinery; statement block types raise 501 "
      "(use create-report instead). Authoring schedules for a close? Call "
      "`get-close-playbook` (mode='initiate') first — one schedule is a "
      "single debit/credit element pair, so multi-line entries become "
      "multiple schedules, and element ids must be real (discover via "
      "get-graph-schema / get-unmapped-elements / suggest-mapping)."
    ),
    command=cmd_create_information_block,
    request_model=CreateInformationBlockRequest,
    result_type=InformationBlockEnvelope,
    error_map={
      ValueError: 422,
      NotImplementedError: 501,
      ScheduleNotFoundError: 404,
    },
    mark_stale_reason="information_block_created",
  )
)

update_information_block_op = _registrar.register(
  OperationSpec(
    name="update-information-block",
    summary="Update Information Block",
    description=(
      "Generic Information Block update entry. Dispatches by `block_type` "
      "to the registered mutation handler. Block types whose Structures "
      "are library-seeded and immutable (statement family) surface 501."
    ),
    command=cmd_update_information_block,
    request_model=UpdateInformationBlockRequest,
    result_type=InformationBlockEnvelope,
    error_map={
      ValueError: 422,
      NotImplementedError: 501,
      ScheduleNotFoundError: 404,
    },
    mark_stale_reason="information_block_updated",
  )
)

delete_information_block_op = _registrar.register(
  OperationSpec(
    name="delete-information-block",
    summary="Delete Information Block",
    description=(
      "Generic Information Block deletion entry. Returns a thin "
      "confirmation (deleted / structure_id / block_type / name). "
      "Block types whose Structures are library-seeded cannot be "
      "deleted per tenant and surface 501."
    ),
    command=cmd_delete_information_block,
    request_model=DeleteInformationBlockRequest,
    result_type=DeleteInformationBlockResponse,
    error_map={
      ValueError: 422,
      NotImplementedError: 501,
      ScheduleNotFoundError: 404,
    },
    mark_stale_reason="information_block_deleted",
  )
)

evaluate_rules_op = _registrar.register(
  OperationSpec(
    name="evaluate-rules",
    summary="Evaluate Rules for an Information Block",
    description=(
      "Runs every rule targeting the given structure (plus element- and "
      "association-scoped rules for the structure's atoms), binds "
      "$Variable references to in-scope facts via qname lookup, writes "
      "one VerificationResult row per rule, and returns the results plus "
      "a status-keyed summary. Decoding mode, 6 patterns "
      "(EqualTo, RollUp, RollForward, SumEquals, Exists, CoExists)."
    ),
    command=cmd_evaluate_rules,
    request_model=EvaluateRulesRequest,
    result_type=EvaluateRulesResponse,
    error_map={ValueError: 422},
    requires_created_by=True,
  )
)

compute_metrics_op = _registrar.register(
  OperationSpec(
    name="compute-metrics",
    summary="Compute Metrics for a Metric Block",
    description=(
      "Resolves the Derive rules scoped to a metric block "
      "(block_type='metric'), binds each rule's operands to the entity's "
      "most recent persisted report facts at period_end, evaluates, and "
      "upserts the period's standing factset_type='metric' FactSet — one "
      "per (structure, entity, period_end), so successive runs accumulate "
      "the time series and re-running a period replaces its values. "
      "Metrics with missing operands or undefined ratios are skipped "
      "with a reason, never errored."
    ),
    command=cmd_compute_metrics,
    request_model=ComputeMetricsRequest,
    result_type=ComputeMetricsResponse,
    error_map={ValueError: 422},
    mark_stale_reason="metrics_computed",
    requires_created_by=True,
  )
)

assert_metrics_op = _registrar.register(
  OperationSpec(
    name="assert-metrics",
    summary="Assert Metrics for a Metric Block",
    description=(
      "Writes externally-observed metric values (usage counts, marketing "
      "numbers, hand-carried figures) into the period's standing "
      "factset_type='metric' FactSet with AssertedProvenance — the "
      "observation sibling of compute-metrics. One standing FactSet per "
      "(structure, entity, period_end); re-asserting a period replaces "
      "its facts. Structures carrying Derive rules are compute-owned and "
      "rejected: asserted and derived metric series keep disjoint "
      "structures. Observations must resolve to concepts on the "
      "structure's presentation catalog. Deterministic and non-AI — no "
      "credits consumed."
    ),
    command=cmd_assert_metrics,
    request_model=AssertMetricsRequest,
    result_type=AssertMetricsResponse,
    error_map={ValueError: 422},
    mark_stale_reason="metrics_asserted",
    requires_created_by=True,
  )
)

compute_forecast_op = _registrar.register(
  OperationSpec(
    name="compute-forecast",
    summary="Compute Forecast for a Forecast Block",
    description=(
      "Walks a forecast block's driver cascade month-by-month forward "
      "from its base period: lever-driven rs-driver Derive rules in "
      "dependency order, carry-forward for unmodeled income-statement "
      "lines, calc-DAG subtotals — upserting one scenario "
      "income-statement FactSet (plus a working-capital balance-sheet "
      "set) per forward month, all keyed by the block's scenario_id "
      "(NULL = actuals; scenario reads pass it as a filter). Re-running "
      "replaces each month's values. Rules with missing lever months or "
      "unbound operands are skipped with a reason (their targets fall "
      "back to carry-forward), never errored. Deterministic and non-AI "
      "— no credits consumed."
    ),
    command=cmd_compute_forecast,
    request_model=ComputeForecastRequest,
    result_type=ComputeForecastResponse,
    error_map={ValueError: 422},
    mark_stale_reason="forecast_computed",
    requires_created_by=True,
  )
)


# Hand-written (not registrar): needs the platform DB session for the
# Document lookup plus the trusted-path graph_id — the OperationSpec
# runner passes neither.
@router.post(
  "/bind-text-block",
  response_model=OperationEnvelope[BindTextBlockResponse],
  operation_id="bindTextBlock",
  summary="Bind Text Block",
  description=(
    "Bind a platform Document (markdown) — or one of its sections — to a "
    "disclosure element as a Nonnumeric text-block fact. The document "
    "stays the editable source of truth; the fact snapshots its text into "
    "a standing 'disclosure' FactSet with document provenance "
    "(document_id + section + content_hash). Re-binding the same element "
    "and period replaces the fact and refreshes the hash. Reports "
    "generated afterward snapshot the standing set, so filed reports are "
    "immutable against later document edits."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/bind-text-block",
  method="POST",
  business_event_type="ledger_bind_text_block",
)
async def bind_text_block_op(
  body: BindTextBlockRequest,
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
    op="bind-text-block",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_bind_text_block(
          session,
          platform_db,
          graph_id,
          body,
          created_by=str(user.id),
        )
    except TextBlockDocumentNotFoundError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
      # SectionNotFound / TextBlockStructure / TextBlockElement / size cap
      raise HTTPException(status_code=422, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(
    ctx,
    _runner,
    cache,
    on_fresh_success=lambda _env: mark_graph_stale(graph_id, "text_block_bound"),
  )


# ═══════════════════════════════════════════════════════════════════════════
# Agents
#
# Counterparty records (customers, vendors, employees, etc.).
# events.agent_id references this table.
# ═══════════════════════════════════════════════════════════════════════════

create_agent_op = _registrar.register(
  OperationSpec(
    name="create-agent",
    summary="Create Agent",
    description=(
      "Create a counterparty record (customer, vendor, employee, etc.). "
      "The (source, external_id) pair is a dedup key — a second insert with "
      "the same pair returns 409. Use update-agent to patch fields."
    ),
    command=cmd_create_agent,
    request_model=CreateAgentRequest,
    result_type=LedgerAgentResponse,
    error_map={DuplicateExternalIdError: 409, ValueError: 422},
  )
)

update_agent_op = _registrar.register(
  OperationSpec(
    name="update-agent",
    summary="Update Agent",
    description=(
      "Patch counterparty fields. Only supplied fields are updated. "
      "Set is_active=false to deactivate (agents are never deleted — they are "
      "reference data referenced by events and transactions)."
    ),
    command=cmd_update_agent,
    request_model=UpdateAgentRequest,
    result_type=LedgerAgentResponse,
    error_map={AgentNotFoundError: 404, ValueError: 422},
  )
)


# ═══════════════════════════════════════════════════════════════════════════
# Event Blocks
#
# Real-world business event layer. Two write modes:
# apply_handlers=False captures the event without firing GL postings;
# apply_handlers=True resolves an event_handler and fires its
# transaction template atomically with the event row.
# ═══════════════════════════════════════════════════════════════════════════

create_event_block_op = _registrar.register(
  OperationSpec(
    name="create-event-block",
    summary="Create Event Block",
    description=(
      "Persist a real-world business event. "
      "apply_handlers=False (default): capture-only, status='captured'. "
      "apply_handlers=True: resolves an event_handler, fires the template, "
      "creates GL entries atomically, status='classified'. "
      "Use preview-event-block to dry-run before committing."
    ),
    command=cmd_create_event_block,
    request_model=CreateEventBlockRequest,
    result_type=EventBlockEnvelope,
    error_map={
      HandlerNotFoundError: 404,
      HandlerAmbiguousError: 409,
      TemplateInterpolationError: 422,
      EngineValidationError: 422,
      HandlerMetadataValidationError: 422,
      DisposalScheduleNotFoundError: 404,
      JournalEntryNotFoundError: 404,
      JournalEntryNotPostedError: 422,
      ClosedPeriodError: 422,
      UnbalancedJournalEntryError: 422,
      ValueError: 422,
    },
  )
)

update_event_block_op = _registrar.register(
  OperationSpec(
    name="update-event-block",
    summary="Update Event Block",
    description=(
      "Apply a status transition (captured → committed | voided) and/or "
      "field corrections (description, effective_at, metadata_patch) to an "
      "existing event block. Only supplied fields are updated. When the "
      "transition is captured/classified → committed, the registered "
      "Python handler fires against the captured metadata to produce the "
      "GL rows; errors from the handler (validation, element resolution, "
      "closed period, unbalanced lines) surface as 422 here so the inbox "
      "UI can display the failure reason without retry."
    ),
    command=cmd_update_event_block,
    request_model=UpdateEventBlockRequest,
    result_type=EventBlockEnvelope,
    # Error map covers both update-only failures (top two) and the
    # handler-firing path that runs on captured/classified → committed.
    error_map={
      EventNotFoundError: 404,
      InvalidEventTransitionError: 422,
      HandlerMetadataValidationError: 422,
      ElementResolutionError: 422,
      ClosedPeriodError: 422,
      UnbalancedJournalEntryError: 422,
    },
  )
)


# `execute-event-block` publishes an event to the source-of-truth
# system (QuickBooks). For events on a connection with
# `write_policy='qb_authoritative'` / `'hybrid'`, this posts a JE to QB
# via the QB API with `request_id=event.id` for idempotency, captures
# the returned `qb_txn_id` on `event.metadata.qb_external_id`,
# transitions status to `'fulfilled'` (or `'pending'` on QB rejection),
# and promotes linked draft GL rows to `'posted'`. The cross-source
# matcher in the loader recognises the round-tripped entry on the next
# sync.
execute_event_block_op = _registrar.register(
  OperationSpec(
    name="execute-event-block",
    summary="Execute Event Block (publish to source-of-truth system)",
    description=(
      "For events on a connection with write_policy='qb_authoritative' "
      "or 'hybrid', publish the captured GL plan to the source-of-truth "
      "system (QuickBooks). Captures qb_txn_id on "
      "event.metadata.qb_external_id, transitions status to 'fulfilled' "
      "(or 'pending' on rejection), and promotes draft GL rows to "
      "'posted'. Native-policy events fast-path through with no QB "
      "write — RoboSystems is the system of record."
    ),
    command=cmd_execute_event_block,
    request_model=ExecuteEventBlockRequest,
    result_type=ExecuteEventBlockResponse,
    error_map={
      EventNotFoundError: 404,
      # AuthClientError from Intuit (revoked / scope-insufficient /
      # rotated past grace) surfaces as QBAuthFailedError. The
      # connection has already been flipped to needs_reauth by the
      # QBClient itself; 401 signals to the UI that the operator must
      # reconnect via OAuth.
      QBAuthFailedError: 401,
      ValueError: 422,
    },
  )
)


# ═══════════════════════════════════════════════════════════════════════════
# Event Handlers
#
# Dynamic rule registry that drives event → GL transformation.
# ═══════════════════════════════════════════════════════════════════════════

create_event_handler_op = _registrar.register(
  OperationSpec(
    name="create-event-handler",
    summary="Create Event Handler",
    description=(
      "Define a rule that fires GL transactions when a matching event block "
      "is created with apply_handlers=True. Match criteria (event_type, "
      "event_category, match_source, match_agent_type, etc.) act as AND-joined "
      "filters — null fields match anything. The highest-priority matching handler "
      "wins. AI-suggested handlers (suggested_by='ai') require approval before "
      "they are eligible for matching."
    ),
    command=cmd_create_event_handler,
    request_model=CreateEventHandlerRequest,
    result_type=EventHandlerResponse,
    error_map={TemplateValidationError: 422, ValueError: 422},
  )
)

update_event_handler_op = _registrar.register(
  OperationSpec(
    name="update-event-handler",
    summary="Update Event Handler",
    description=(
      "Patch an event handler's match criteria, template, priority, or active "
      "state. Pass approve=true to approve an AI-suggested handler; "
      "approve=false to revoke approval. Only supplied fields are updated."
    ),
    command=cmd_update_event_handler,
    request_model=UpdateEventHandlerRequest,
    result_type=EventHandlerResponse,
    error_map={
      EventHandlerNotFoundError: 404,
      TemplateValidationError: 422,
      ValueError: 422,
    },
  )
)

preview_event_block_op = _registrar.register(
  OperationSpec(
    name="preview-event-block",
    summary="Preview Event Block",
    description=(
      "Dry-run: resolve the matching handler and evaluate the transaction "
      "template without writing any rows. Returns the matched handler + planned "
      "debit/credit lines + any validation errors. Use this before "
      "create-event-block(apply_handlers=True) to confirm the GL plan."
    ),
    command=cmd_preview_event_block,
    request_model=CreateEventBlockRequest,
    result_type=PreviewEventBlockResponse,
    error_map={ValueError: 422},
  )
)

# ═══════════════════════════════════════════════════════════════════════════
# Journal Entries
#
# Update / delete / reverse operations on existing journal entries. All
# creation (manual entries, closing entries, adjusting entries, posted
# imports) is handled through
# `create-event-block(event_type='journal_entry_recorded')` — the single
# write surface for GL entries the user originates. See the Python handler
# registry.
# ═══════════════════════════════════════════════════════════════════════════

update_journal_entry_op = _registrar.register(
  OperationSpec(
    name="update-journal-entry",
    summary="Update Journal Entry",
    description=(
      "Update a draft journal entry. Posted entries are immutable and "
      "must be corrected via "
      "`create-event-block(event_type='journal_entry_reversed')`. If "
      "line_items is provided, existing line items are replaced "
      "atomically and the new set must balance."
    ),
    command=cmd_update_journal_entry,
    request_model=UpdateJournalEntryRequest,
    result_type=JournalEntryResponse,
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
    result_type=DeleteResult,
    error_map={
      JournalEntryNotFoundError: 404,
      JournalEntryNotDraftError: 422,
    },
    requires_created_by=False,
  )
)

# On-demand obligation promotion. Normally the `scheduled_obligation_promoter`
# Dagster sensor runs this every few minutes; this operation exposes the same
# `promote_pending_obligations` sweep so an interactive caller or an MCP close
# co-pilot can promote matured schedule obligations now instead of waiting for
# the background cadence — required to drive a schedule-driven close to
# completion in a single session. Idempotent (skips already-classified rows,
# reconciles to existing drafts).
promote_obligations_op = _registrar.register(
  OperationSpec(
    name="promote-obligations",
    summary="Promote Due Schedule Obligations",
    description=(
      "Promote matured pending schedule obligations (schedule_entry_due "
      "events whose period boundary has passed) to 'classified', and — when "
      "dispatch_handlers=true (default) — draft their closing entries in the "
      "same transaction. This is the on-demand form of the background "
      "obligation-promotion sweep; run it before close-period when a schedule "
      "was just created or when you can't wait for the Dagster sensor. "
      "Idempotent: re-running skips already-classified obligations and "
      "reconciles to existing drafts."
    ),
    command=cmd_promote_obligations,
    request_model=PromoteObligationsRequest,
    result_type=PromoteObligationsResponse,
    error_map={ValueError: 422},
  )
)


# Re-run the schedule generator in place on an existing schedule. Atomic
# alternative to delete-then-recreate (which orphans the obligation chain):
# preserves the structure id + element associations + taxonomy, voids the
# old pending obligation chain, deletes the old facts + rules, and
# regenerates fresh forward facts + a fresh obligation chain from the
# schedule's stored definition. Re-scopes historical-vs-in-scope from the
# CURRENT fiscal calendar `closed_through`. This is the redo-after-a-
# generator-fix path (e.g. the roll-forward direction fix).
rebuild_schedule_op = _registrar.register(
  OperationSpec(
    name="rebuild-schedule",
    summary="Rebuild Schedule In Place",
    description=(
      "Re-run the schedule generator in place on an existing schedule. "
      "Atomic alternative to delete-then-recreate (which orphans pending "
      "obligations): preserves the structure id + element associations + "
      "taxonomy, voids the old pending obligation chain, deletes the old "
      "facts and SumEquals rules, and regenerates fresh forward facts + a "
      "fresh obligation chain from the schedule's stored definition "
      "(entry_template / schedule_metadata / monthly_amount / period "
      "bounds). The historical-vs-in-scope split is re-derived from the "
      "CURRENT fiscal calendar closed_through. Use this to pick up a fixed "
      "generator (e.g. the roll-forward direction fix) without orphaning "
      "obligations."
    ),
    command=cmd_rebuild_schedule,
    request_model=RebuildScheduleRequest,
    result_type=ScheduleCreatedResponse,
    error_map={
      ScheduleNotFoundError: 404,
      ValueError: 422,
    },
    mark_stale_reason="schedule_rebuilt",
  )
)


# ═══════════════════════════════════════════════════════════════════════════
# Close Workflow
#
# Month-end processing: configure the close target, generate schedule
# entries, handle asset disposals, then lock the period.
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/set-close-target",
  response_model=OperationEnvelope[FiscalCalendarResponse],
  operation_id="setCloseTarget",
  summary="Set Close Target",
  description=(
    "Set the user-controlled goal period for closing (`close_target`). "
    "Format: YYYY-MM. Distinct from `closed_through` (what's actually "
    "locked) — setting a target doesn't close anything; call "
    "`close-period` for that. The catch-up sequence between "
    "`closed_through` and this target appears on the response's "
    "`fiscal_calendar.catch_up_sequence`."
  ),
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


# All ledger writes — closing entries, journal entries, schedule
# truncation, asset disposal, journal entry reversal — are handled via
# create-event-block with Python-registered event types. See
# operations/event_block/python_handlers/ for the handler modules:
# journal_entry_recorded (manual GL writes), journal_entry_reversed
# (offsetting reversal of a posted entry), schedule_entry_due (schedule
# period matured), asset_disposed (atomic disposal + schedule termination
# — `truncate-schedule` retired as a public op since this is its only
# real consumer).


@router.post(
  "/close-period",
  response_model=OperationEnvelope[ClosePeriodResponse],
  operation_id="closePeriod",
  summary="Close Fiscal Period",
  description=(
    "Lock a single fiscal period. Posts draft entries, runs the "
    "balance-sheet equation check, advances `closed_through` by one, "
    "auto-advances `close_target` if this close caught up to it, and "
    "stamps the period's canonical statement FactSets from the posted "
    "ledger (`statements_stamped` / `stamped_statement_sets` in the "
    "response; soft-skipped with `statement_stamp_note` when reporting "
    "isn't set up). Period must be exactly `closed_through + 1` — "
    "sequence violations return 422 with structured `blockers`. Common "
    "blockers: `sync_stale` (override with `allow_stale_sync=true` "
    "after manual verification), `period_incomplete` (draft entries "
    "unbalanced), `sequence_violation` (out-of-order)."
  ),
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
      detail: dict = {
        "message": f"Cannot close period {body.period!r}.",
        "blockers": e.blockers,
      }
      if e.gate.pending_obligation_count:
        detail["pending_obligation_count"] = e.gate.pending_obligation_count
        # Route through the Pydantic response model so the error-detail
        # shape stays aligned with the calendar-read shape; if the model
        # gains a field, this serialization picks it up automatically.
        detail["pending_obligation_sample"] = [
          PendingObligationDetailResponse(
            event_id=d.event_id,
            schedule_id=d.schedule_id,
            schedule_name=d.schedule_name,
            period=d.period,
          ).model_dump()
          for d in e.gate.pending_obligation_sample
        ]
        detail["earliest_pending_period"] = e.gate.earliest_pending_period
      if e.gate.sync_stale_days is not None:
        detail["sync_stale_days"] = e.gate.sync_stale_days
      raise HTTPException(status_code=422, detail=detail)
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
    except WritebackFailed as e:
      # Close-period pre-publish failure. Surface the structured
      # failed-events payload so operators can see exactly which drafts
      # QB rejected (mapping issue, balance error, closed-in-QB period,
      # etc.) and retry the close after fixing.
      raise HTTPException(
        status_code=422,
        detail={
          "message": str(e),
          "failed_events": e.failed_events,
          "code": "WRITE_BACK_FAILED",
        },
      )
    except StatementStampError as e:
      # The close rolled back: reporting is configured but the pivot
      # couldn't stamp the period's canonical statement sets. Fix the
      # cause (mapping/style) and re-run the close — nothing was
      # committed.
      raise HTTPException(
        status_code=422,
        detail={
          "message": str(e),
          "code": "STATEMENT_STAMP_FAILED",
        },
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
  response_model=OperationEnvelope[FiscalCalendarResponse],
  operation_id="reopenPeriod",
  summary="Reopen Fiscal Period",
  description=(
    "Reopen a closed period for adjustment. Reopening the current "
    "`closed_through` decrements it by one; reopening an earlier period "
    "is a prior-period adjustment and leaves `closed_through` unchanged "
    "— re-closing it restores the period without advancing the pointer. "
    "Either way the period's entries become writable again. Retracts "
    "the month's canonical statement FactSets (a reopened month is no "
    "longer a closed assertion; re-closing restamps them). The required "
    "`reason` is captured in the audit log. Use sparingly — reopen "
    "invalidates downstream artifacts that trusted the closed state "
    "(reports, shared filings)."
  ),
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
        # Wire shape unchanged: the envelope still carries the refreshed
        # calendar; the retraction count rides the MCP surface only.
        return cmd_reopen_period(
          session,
          platform_db,
          graph_id,
          body.period,
          actor_id=str(user.id),
          reason=body.reason,
          note=body.note,
          service=_fiscal_svc,
        ).fiscal_calendar
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


@router.post(
  "/backfill-plan-history",
  response_model=OperationEnvelope[BackfillPlanHistoryResponse],
  operation_id="backfillPlanHistory",
  summary="Backfill Plan History",
  description=(
    "Compile monthly statement history behind the close boundary — the "
    "plan's historical columns. Seeds any missing FiscalPeriod rows "
    "(baseline-closed) back to the clamped `start_period`, then "
    "restamps each month lacking canonical statement FactSets by "
    "running the real reopen → reclose cycle (balance validation, "
    "statement rules, and audit events per month). Chunked: at most "
    "`max_periods` months per call, oldest first — loop until "
    "`remaining_periods` comes back empty. Idempotent: already-stamped "
    "months are never touched. Months holding draft entries are "
    "skipped, never posted. `start_period` is clamped to the earliest "
    "month with ledger data, so deep-history tenants only backfill "
    "what actually exists."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/backfill-plan-history",
  method="POST",
  business_event_type="ledger_backfill_plan_history",
)
async def backfill_plan_history_op(
  body: BackfillPlanHistoryOperation,
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
    op="backfill-plan-history",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        return cmd_backfill_plan_history(
          session,
          platform_db,
          graph_id,
          body,
          actor_id=str(user.id),
          service=_fiscal_svc,
          close_service=_close_svc,
        )
    except BackfillPreconditionError as e:
      raise HTTPException(
        status_code=422,
        detail={"message": str(e), "code": e.code},
      )
    except FiscalCalendarError as e:
      raise HTTPException(status_code=404, detail=str(e))
    except ProgrammingError as e:
      if _is_schema_missing(e):
        raise _ledger_404()
      raise

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# Reports
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-report",
  response_model=OperationEnvelope[ReportResponse],
  operation_id="createReport",
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
        except BundleUploadError as e:
          # S3 unavailable during the publish-time bundle stamp. Publish
          # aborted by ``_stamp_report_bundle`` to keep the invariant
          # "every published Report has a stored bundle artifact"; surface
          # as 502 (upstream failure) so the client can retry.
          raise HTTPException(status_code=502, detail=str(e))
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
  response_model=OperationEnvelope[ReportResponse],
  operation_id="regenerateReport",
  summary="Regenerate Report",
  description=(
    "Re-runs fact generation for an existing Report against the latest "
    "ledger state. Pass `period_start`/`period_end`/`periods` only if "
    "you want to change the reporting window."
  ),
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
        except InvalidFilingTransitionError as e:
          raise HTTPException(status_code=422, detail=str(e))
        except BundleUploadError as e:
          # Same fail-loud semantics as create-report: a regenerate
          # without a stamped bundle would publish a Report whose
          # ``bundle_url`` lags the regenerated facts.
          raise HTTPException(status_code=502, detail=str(e))
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
  response_model=OperationEnvelope[DeleteResult],
  operation_id="deleteReport",
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
        except ReportNotFiledError as e:
          raise HTTPException(status_code=422, detail=str(e))
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
  response_model=OperationEnvelope[ShareReportResponse],
  operation_id="shareReport",
  summary="Share Report",
  description=(
    "Pushes a published report to every member of the target publish "
    "list. Each share is an independent copy: the report row + all its "
    "facts are cloned into the recipient's tenant schema with "
    "`source_graph_id` / `source_report_id` provenance fields populated. "
    "Per-target outcomes (success or error) surface in the response — "
    "share does not fail-fast across targets."
  ),
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


@router.post(
  "/file-report",
  response_model=OperationEnvelope[ReportResponse],
  operation_id="fileReport",
  summary="File Report",
  description=(
    "Transitions the Report's filing_status to 'filed' — locks the package. "
    "Allowed from 'draft' or 'under_review'. Stamps filed_at + filed_by."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/file-report",
  method="POST",
  business_event_type="ledger_file_report",
)
async def file_report_op(
  body: FileReportRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="file-report",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_file_report(session, body.report_id, filed_by=str(user.id))
        except ReportNotFoundError:
          raise HTTPException(
            status_code=404, detail=f"Report '{body.report_id}' not found."
          )
        except InvalidFilingTransitionError as e:
          raise HTTPException(status_code=422, detail=str(e))
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/transition-filing-status",
  response_model=OperationEnvelope[ReportResponse],
  operation_id="transitionFilingStatus",
  summary="Transition Filing Status",
  description=(
    "Move a Report along the non-file legs of the filing lifecycle "
    "(draft ↔ under_review, filed → archived). Use 'file-report' to "
    "reach 'filed' so audit fields land cleanly."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/transition-filing-status",
  method="POST",
  business_event_type="ledger_transition_filing_status",
)
async def transition_filing_status_op(
  body: TransitionFilingStatusRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="transition-filing-status",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return cmd_transition_filing_status(
            session, body.report_id, body.target_status
          )
        except ReportNotFoundError:
          raise HTTPException(
            status_code=404, detail=f"Report '{body.report_id}' not found."
          )
        except InvalidFilingTransitionError as e:
          raise HTTPException(status_code=422, detail=str(e))
    except (ValueError, ProgrammingError):
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


# ═══════════════════════════════════════════════════════════════════════════
# Publish Lists
# ═══════════════════════════════════════════════════════════════════════════


@router.post(
  "/create-publish-list",
  response_model=OperationEnvelope[PublishListResponse],
  operation_id="createPublishList",
  summary="Create Publish List",
  description=(
    "Create a publish list (a saved set of recipient graphs). Members "
    "are managed separately via add/remove-member operations."
  ),
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
  response_model=OperationEnvelope[PublishListResponse],
  operation_id="updatePublishList",
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
  response_model=OperationEnvelope[DeleteResult],
  operation_id="deletePublishList",
  summary="Delete Publish List",
  description=(
    "Delete a publish list and its membership rows. Reports previously "
    "shared via this list are not affected — each share is an independent "
    "copy in the recipient's graph."
  ),
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
  response_model=OperationEnvelope[list[PublishListMemberResponse]],
  operation_id="addPublishListMembers",
  summary="Add Members to Publish List",
  description=(
    "Add one or more recipient graphs to a publish list. Targets must "
    "exist and have the same extension enabled (e.g. roboledger). "
    "Self-graph rejected (422); already-member rejected (409)."
  ),
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
  response_model=OperationEnvelope[DeleteResult],
  operation_id="removePublishListMember",
  summary="Remove Member from Publish List",
  description="Remove a single recipient from a publish list.",
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
