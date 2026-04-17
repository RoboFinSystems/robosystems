"""RoboLedger OLTP-backed analytical reads.

Sibling to `views.py` (graph-backed). Both surfaces live under
`/extensions/roboledger/{graph_id}/operations/*`; this file hosts
reads whose source of truth is the extensions OLTP database rather
than the materialized graph.

The first (and currently only) op here is ``live-financial-statement``
— the entity-graph companion to ``financial-statement-analysis``.
Gated by ``ROBOLEDGER_ENABLED`` (unlike `views.py` which stays
mounted for SEC-only deployments).
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
from robosystems.models.api.extensions.reports import (
  LiveFinancialStatementRequest,
)
from robosystems.models.core import User
from robosystems.operations.roboledger.reads.reports import (
  CoaMappingNotFoundError,
  get_live_financial_statement,
  resolve_reporting_window,
)
from robosystems.routers.extensions.roboledger.operations import (
  _ctx,
  _dispatch,
  _ledger_404,
)

router = APIRouter()

_OP_TAG = "Extensions: RoboLedger"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)

_VALID_STATEMENT_TYPES = (
  "income_statement",
  "balance_sheet",
  "equity_statement",
)

_require_roboledger = require_graph_extension("roboledger")


@router.post(
  "/live-financial-statement",
  response_model=OperationEnvelope,
  operation_id="opLiveFinancialStatement",
  summary="Live Financial Statement",
  description=(
    "Generate an ad-hoc financial statement directly from the tenant's "
    "OLTP ledger data using the active CoA→GAAP mapping. This is the "
    "authoritative source for RoboLedger entity graphs — no graph "
    "materialization required. Rejected on shared-repository graphs; "
    "those should use `financial-statement-analysis` instead."
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

  if body.statement_type not in _VALID_STATEMENT_TYPES:
    raise HTTPException(
      status_code=400,
      detail=(
        f"Unknown statement_type '{body.statement_type}'. "
        f"Valid types: {', '.join(_VALID_STATEMENT_TYPES)}. "
        "Note: cash_flow_statement is not yet supported for OLTP ledgers; "
        "use financial-statement-analysis against SEC for cash flow data."
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
          )
        except CoaMappingNotFoundError as exc:
          raise HTTPException(status_code=422, detail=str(exc))
    except ProgrammingError:
      raise _ledger_404()

  return await _dispatch(ctx, _runner, cache)
