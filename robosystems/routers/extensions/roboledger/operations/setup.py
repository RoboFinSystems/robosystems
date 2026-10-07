"""Ledger setup: provision the tenant schema, seed the chart of accounts, and
edit the reporting entity. One-time or rare writes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.db.extensions import extensions_session
from robosystems.middleware.extensions import OperationSpec
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
  InitializeChartOfAccountsResponse,
)
from robosystems.models.api.extensions.entity import (
  ChangeReportingStyleRequest,
  ChangeReportingStyleResponse,
  CreateEntityRequest,
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  InitializeLedgerRequest,
  InitializeLedgerResponse,
)
from robosystems.models.core import User
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  ChartAlreadyExistsError,
  ChartTemplateNotFoundError,
)
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts as cmd_initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.entity import (
  EntityHierarchyError,
  EntityTickerTakenError,
  ParentEntityNotFoundError,
)
from robosystems.operations.roboledger.commands.entity import (
  create_entity as cmd_create_entity,
)
from robosystems.operations.roboledger.commands.entity import (
  update_entity as cmd_update_entity,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  initialize_ledger as cmd_initialize_ledger,
)
from robosystems.operations.roboledger.commands.reporting_style import (
  EntityNotFoundError as ReportingStyleEntityNotFoundError,
)
from robosystems.operations.roboledger.commands.reporting_style import (
  ReportingStyleInvalidError,
  change_reporting_style,
)
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CalendarAlreadyInitializedError,
  InvalidCloseTargetError,
)
from robosystems.routers.extensions.roboledger._common import (
  _RATE_LIMIT,
  _ctx,
  _dispatch,
  _fiscal_svc,
  _require_roboledger_write,
  make_registrar,
)

router = APIRouter()

_OP_TAG = "RoboLedger: Setup"
_registrar = make_registrar(router, _OP_TAG)


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
  user: User = Depends(_require_roboledger_write),
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

  return await _dispatch(ctx, _runner, cache)


initialize_chart_of_accounts_op = _registrar.register(
  OperationSpec(
    name="initialize-chart-of-accounts",
    summary="Initialize Chart of Accounts",
    description=(
      "Create the graph's chart of accounts from a shipped template — the "
      "fresh-company path to native books. Use when the graph has NO chart "
      "(a QuickBooks-synced tenant never needs this: its chart arrives with "
      "the sync and stays after a sever) and before connecting a bank feed, "
      "which needs a chart to resolve against. Templates: `saas` "
      "(subscription software), `services` (professional services), "
      "`product` (inventory and COGS) — the `chartTemplates` GraphQL field "
      "lists them with names and account counts. Creates the chart, its "
      "`coa_mapping` structure and the template's CoA → rs-gaap mapping "
      "associations in one transaction, with the equity rows mapped by the "
      "entity's legal form (`entity_type`, defaulting to the graph's primary "
      "entity). One-time: 409 once a chart exists — a chart is never "
      "replaced. Customize afterwards with update-taxonomy-block; accounts "
      "that carry activity are never deleted."
    ),
    command=cmd_initialize_chart_of_accounts,
    request_model=InitializeChartOfAccountsRequest,
    result_type=InitializeChartOfAccountsResponse,
    business_event_type="ledger_initialize_chart_of_accounts",
    error_map={
      ChartAlreadyExistsError: 409,
      ChartTemplateNotFoundError: 422,
    },
    mark_stale_reason="chart_of_accounts_initialized",
  )
)


# A new entity is an Entity node and an ENTITY_OWNS_ENTITY edge in the graph.
create_entity_op = _registrar.register(
  OperationSpec(
    name="create-entity",
    summary="Create Entity",
    description=(
      "Add an entity to the graph's reporting group: a subsidiary under "
      "`parent_entity_id` (default the group parent) that keeps its own "
      "books, chart of accounts and close, on the group's fiscal cadence. "
      "A graph created without an entity gets this one as its group parent. "
      "Creates the entity row only — give it a chart next "
      "(initialize-chart-of-accounts with `entity_id`) and a calendar "
      "(initialize with `entity_id`); from then on every ledger operation "
      "takes `entity_id` to act in its books, and omitting it means the "
      "group parent. The Reporting Style follows `entity_type` unless "
      "`reporting_style_id` names one. `ticker` prefixes the entity's "
      "account names and must be unique in the graph (409). There is no "
      "cap on entities: a graph is one reporting group, and everyone with "
      "access to it sees every entity."
    ),
    command=cmd_create_entity,
    request_model=CreateEntityRequest,
    result_type=LedgerEntityResponse,
    business_event_type="ledger_create_entity",
    error_map={
      EntityNotInGraphError: 404,
      EntityHierarchyError: 422,
      EntityTickerTakenError: 409,
      ReportingStyleInvalidError: 422,
    },
    mark_stale_reason="entity_created",
  )
)


def _require_entity_updates(body: UpdateEntityRequest) -> None:
  if not body.model_dump(exclude_none=True, exclude={"entity_id"}):
    raise HTTPException(status_code=400, detail="No fields provided for update.")


# The updatable fields are columns of the materialized Entity node, so an edit
# must mark the graph stale.
update_entity_op = _registrar.register(
  OperationSpec(
    name="update-entity",
    summary="Update Entity",
    description=(
      "Update an entity of the graph's reporting group. Only provided "
      "(non-null) fields are updated. Omit `entity_id` to target the group "
      "parent; name a subsidiary's id to edit it. `ownership_pct` is "
      "refused on the group parent (422)."
    ),
    command=cmd_update_entity,
    request_model=UpdateEntityRequest,
    result_type=LedgerEntityResponse,
    business_event_type="ledger_update_entity",
    # Keeps the empty-body 400 without mapping ValueError to 400, which would
    # swallow the command's genuine 422 validation failures.
    pre_validate=_require_entity_updates,
    error_map={
      ParentEntityNotFoundError: 404,
      EntityNotInGraphError: 404,
      EntityHierarchyError: 422,
    },
    mark_stale_reason="entity_updated",
  )
)


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
