"""RoboLedger OLTP-backed analytical reads (``live-financial-statement``,
``consolidation-worksheet``, ``preview-consolidation-entries``).

Sibling to the graph-backed `views.py`, but gated by ``ROBOLEDGER_ENABLED``
rather than staying mounted for SEC-only deployments.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.extensions import (
  GraphExtensionContext,
  require_graph_extension,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.extensions.consolidation import (
  PreviewConsolidationEntriesRequest,
  PreviewConsolidationEntriesResponse,
)
from robosystems.models.api.extensions.reports import (
  ConsolidationWorksheetRequest,
  ConsolidationWorksheetResponse,
  LiveFinancialStatementRequest,
  LiveFinancialStatementResponse,
)
from robosystems.models.core import User
from robosystems.operations.roboledger.reads.consolidation import (
  NoConsolidationEntityError,
  preview_consolidation_entries,
)
from robosystems.operations.roboledger.reads.reports import (
  LIVE_STATEMENT_TYPES,
  CoaMappingNotFoundError,
  CombinedViewOnParentOnlyError,
  get_consolidation_worksheet,
  get_live_financial_statement,
  resolve_reporting_window,
)
from robosystems.routers.extensions.roboledger._common import (
  _ctx,
  _dispatch,
  _ledger_404,
)

router = APIRouter()

_OP_TAG = "RoboLedger: Analytical Views"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)

_require_roboledger = require_graph_extension("roboledger")


@router.post(
  "/live-financial-statement",
  response_model=OperationEnvelope[LiveFinancialStatementResponse],
  operation_id="liveFinancialStatement",
  summary="Live Financial Statement",
  description=(
    "Generate an ad-hoc financial statement directly from the tenant's "
    "OLTP ledger data using the active CoA→GAAP mapping. This is the "
    "authoritative source for RoboLedger entity graphs — no graph "
    "materialization required. One entity's books: `entity_id` names a "
    "subsidiary, omitted means the group parent. On the parent, `scope: "
    "combined` sums every member consolidated in full at the rs-gaap "
    "concepts for the months each was a member, nothing eliminated, and "
    "`scope: consolidated` adds the consolidation entity's entries — the "
    "eliminations and noncontrolling interest. Rejected on "
    "shared-repository graphs; those should use "
    "`financial-statement-analysis` instead."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/live-financial-statement",
  method="POST",
  business_event_type="ledger_live_financial_statement",
)
async def live_financial_statement_op(
  body: LiveFinancialStatementRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="live-financial-statement",
    idempotency_key=idempotency_key,
    body=body,
  )

  if body.statement_type not in LIVE_STATEMENT_TYPES:
    raise HTTPException(
      status_code=400,
      detail=(
        f"Unknown statement_type '{body.statement_type}'. "
        f"Valid types: {', '.join(LIVE_STATEMENT_TYPES)}."
      ),
    )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        start, end = resolve_reporting_window(
          session,
          period_start=body.period_start,
          period_end=body.period_end,
          period_type=body.period_type,
          fiscal_year=body.fiscal_year,
        )
        try:
          return get_live_financial_statement(
            session,
            graph_id=graph_id,
            statement_type=body.statement_type,
            period_start=start,
            period_end=end,
            limit=body.limit,
            entity_id=body.entity_id,
            consolidated=body.consolidated,
            scope=body.scope,
            # Reporting Style resolves from the entity on this same session.
          )
        except (CoaMappingNotFoundError, CombinedViewOnParentOnlyError) as exc:
          raise HTTPException(status_code=422, detail=str(exc))
    except ProgrammingError:
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/consolidation-worksheet",
  response_model=OperationEnvelope[ConsolidationWorksheetResponse],
  operation_id="consolidationWorksheet",
  summary="Consolidation Worksheet",
  description=(
    "The group's statement for one window with the entity as the column, "
    "read live from the tenant's OLTP ledger: the parent and each member "
    "consolidated in full, their combined sum, the consolidation entity's "
    "adjustments, the consolidated total, and each equity-method or cost "
    "member beside the sum, never in it. Every column renders under the "
    "group parent's Reporting Style. Rejected on shared-repository graphs."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/consolidation-worksheet",
  method="POST",
  business_event_type="ledger_consolidation_worksheet",
)
async def consolidation_worksheet_op(
  body: ConsolidationWorksheetRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="consolidation-worksheet",
    idempotency_key=idempotency_key,
    body=body,
  )

  if body.statement_type not in LIVE_STATEMENT_TYPES:
    raise HTTPException(
      status_code=400,
      detail=(
        f"Unknown statement_type '{body.statement_type}'. "
        f"Valid types: {', '.join(LIVE_STATEMENT_TYPES)}."
      ),
    )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        start, end = resolve_reporting_window(
          session,
          period_start=body.period_start,
          period_end=body.period_end,
          period_type=body.period_type,
          fiscal_year=body.fiscal_year,
        )
        try:
          return get_consolidation_worksheet(
            session,
            graph_id=graph_id,
            statement_type=body.statement_type,
            period_start=start,
            period_end=end,
            limit=body.limit,
          )
        except CoaMappingNotFoundError as exc:
          raise HTTPException(status_code=422, detail=str(exc))
    except ProgrammingError:
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/preview-consolidation-entries",
  response_model=OperationEnvelope[PreviewConsolidationEntriesResponse],
  operation_id="previewConsolidationEntries",
  summary="Preview Consolidation Entries",
  description=(
    "The consolidating entries a group's month needs, computed from the "
    "books and posting nothing. For each member consolidated in full: the "
    "parent's investment eliminated against the member's capital (its "
    "contributions net of distributions), the outside owners' share of that "
    "capital as noncontrolling interest, the intercompany balances, the "
    "month's intercompany income against the matching expense, and the "
    "outside owners' share of the month's result; for each member on the "
    "equity method, the parent's share of its result. Accounts are found by "
    "the rs-gaap concept they map to, and an intercompany line by the group "
    "entity its event names as counterparty. Each entry is the difference "
    "between the balance its kind should hold for the member at the month "
    "end and what the consolidation entity already holds, so the entries a "
    "person posted with the same kind count. A difference between the "
    "parent's investment and its share of a member's capital is returned as "
    "a residual and its entry withheld, never plugged; intercompany figures "
    "the two sides do not agree on are returned as reconciling items. "
    "Rejected on shared-repository graphs."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/preview-consolidation-entries",
  method="POST",
  business_event_type="ledger_preview_consolidation_entries",
)
async def preview_consolidation_entries_op(
  body: PreviewConsolidationEntriesRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = _ctx(
    graph_id=graph_id,
    user_id=str(user.id),
    op="preview-consolidation-entries",
    idempotency_key=idempotency_key,
    body=body,
  )

  def _runner():
    try:
      with extensions_session(graph_id) as session:
        try:
          return preview_consolidation_entries(session, period=body.period)
        except NoConsolidationEntityError as exc:
          raise HTTPException(status_code=422, detail=str(exc))
    except ProgrammingError:
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)
