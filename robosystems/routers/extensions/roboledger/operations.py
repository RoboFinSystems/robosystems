"""RoboLedger operation routes.

Every route follows the pattern:

1. `POST /extensions/roboledger/{graph_id}/operations/{op_name}`
2. Typed request body (path params embedded in body for
   update/delete commands)
3. A `_runner()` closure that opens an extensions session, calls the
   ops layer, translates domain errors into HTTPExceptions
4. `execute_operation(ctx, runner, cache)` handles envelope +
   idempotency + audit

**Registered (36) — in logical workflow order:**

- Setup: `initialize`, `update-entity`
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
  path (update / delete on a draft entry) stays as a CRUD surface for
  now — it migrates to the event lifecycle when correction handlers
  ship in a later phase.
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
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  EventBlockEnvelope,
  UpdateEventBlockRequest,
)
from robosystems.models.api.event_handler import (
  CreateEventHandlerRequest,
  UpdateEventHandlerRequest,
)
from robosystems.models.api.extensions.agent import (
  CreateAgentRequest,
  UpdateAgentRequest,
)
from robosystems.models.api.extensions.entity import UpdateEntityRequest
from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodRequest,
  InitializeLedgerRequest,
  ReopenPeriodRequest,
  SetCloseTargetRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  DeleteJournalEntryRequest,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.publish_lists import (
  AddMembersRequest,
  CreatePublishListRequest,
  UpdatePublishListRequest,
)
from robosystems.models.api.extensions.report_package import (
  FileReportRequest,
  TransitionFilingStatusRequest,
)
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  RegenerateReportRequest,
  ShareReportRequest,
)
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
  LinkEntityTaxonomyRequest,
)
from robosystems.models.api.information_block import (
  CreateInformationBlockRequest,
  DeleteInformationBlockRequest,
  DeleteInformationBlockResponse,
  EvaluateRulesRequest,
  InformationBlockEnvelope,
  UpdateInformationBlockRequest,
)
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
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
from robosystems.operations.roboledger.commands.reports import (
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
from robosystems.operations.roboledger.commands.taxonomies import (
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
# Setup
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
      "(declarative tenant CoA) is live; `reporting_extension` / "
      "`custom_ontology` / `reporting_standard` land in later sub-phases."
    ),
    command=cmd_create_taxonomy_block,
    request_model=CreateTaxonomyBlockRequest,
    error_map={
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
      "Library-origin block types (`reporting_standard`) surface 501."
    ),
    command=cmd_update_taxonomy_block,
    request_model=UpdateTaxonomyBlockRequest,
    error_map={
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
    description=("Link a chart-of-accounts element to a US GAAP reporting concept."),
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
# Information Blocks
#
# Generic construction envelope for schedules, statement blocks, metrics,
# and future block types. `evaluate-rules` runs the rule engine against a
# block's materialized facts (decoding mode, 5 patterns).
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
      "(use create-report instead)."
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
      "a status-keyed summary. Phase delta.3 — decoding mode, 5 patterns "
      "(EqualTo, RollUp, RollForward, Exists, CoExists)."
    ),
    command=cmd_evaluate_rules,
    request_model=EvaluateRulesRequest,
    error_map={ValueError: 422},
    requires_created_by=True,
  )
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
    error_map={AgentNotFoundError: 404, ValueError: 422},
  )
)


# ═══════════════════════════════════════════════════════════════════════════
# Event Blocks
#
# Real-world business event layer (event-driven-ledger.md). Two write
# modes: apply_handlers=False captures the event without firing GL
# postings; apply_handlers=True resolves an event_handler and fires its
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

# ═══════════════════════════════════════════════════════════════════════════
# Close Workflow
#
# Month-end processing: configure the close target, generate schedule
# entries, handle asset disposals, then lock the period.
# ═══════════════════════════════════════════════════════════════════════════


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
# Reports
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


@router.post(
  "/file-report",
  response_model=OperationEnvelope,
  operation_id="opFileReport",
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
  response_model=OperationEnvelope,
  operation_id="opTransitionFilingStatus",
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
