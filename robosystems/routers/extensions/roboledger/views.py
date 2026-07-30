"""RoboLedger graph-backed analytical views (fact-grid operation).

Hosts `POST /extensions/roboledger/{graph_id}/operations/build-fact-grid`
— the one read-shaped operation in the dispatcher. Lives in its own
router file (separate from `operations.py`) so it can be mounted
independently of `ROBOLEDGER_ENABLED`.

**Why a separate router?**
Fact-grid queries the LadybugDB graph schema (XBRL hypercube) — the
same schema the SEC shared repository uses. A deployment that hosts
SEC research without enabling RoboLedger tenants still wants this
endpoint. Bundling it into `operations.py` (which is gated by
`ROBOLEDGER_ENABLED`) would lose the feature for SEC-only deployments,
which is a regression vs. the legacy `/v1/graphs/{g}/views` endpoint
that gated only on `FACT_GRID_ENABLED`.

**Why still under `/extensions/roboledger/`?**
Because the fact-grid is roboledger-schema-specific. It doesn't fit on
the platform graph surface (which is schema-agnostic) and it doesn't
fit on GraphQL (which can't represent multi-dimensional pivot tables
naturally). The operations dispatcher is the right home — we just
gate the mount on `FACT_GRID_ENABLED` independently.

**Idempotency + audit:**
Because this is a real operation in the dispatcher, it gets the
`OperationEnvelope`, idempotency-key cache (genuinely useful as a
deterministic cache for expensive analytical queries), and audit
logging for free.
"""

from __future__ import annotations

import time
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Path

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationContext,
  OperationEnvelope,
  fingerprint_body,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
from robosystems.models.api.extensions.reports import (
  AnalyticalStatementFactRow,
  FinancialStatementAnalysisRequest,
  FinancialStatementAnalysisResponse,
  ResolvedReportInfo,
)
from robosystems.models.api.views import (
  CreateViewRequest,
  ElementSummary,
  FactRecord,
  ViewMetadata,
  ViewResponse,
)
from robosystems.models.core import User
from robosystems.operations.roboledger.reads.reports import (
  ANALYSIS_STATEMENT_TYPES,
)
from robosystems.operations.roboledger.views import (
  FactGridBuilder,
  deduplicate_facts,
  query_fact_grid,
  query_financial_statement,
  summarize_by_element,
)

# Import _dispatch from the sibling operations module so error
# translation (idempotency conflict → 409, etc.) stays centralized.
from robosystems.routers.extensions.roboledger.operations import _dispatch

router = APIRouter()

_OP_TAG = "Extensions: RoboLedger"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)


@router.post(
  "/build-fact-grid",
  response_model=OperationEnvelope[ViewResponse],
  operation_id="buildFactGrid",
  summary="Build Fact Grid",
  description="Queries LadybugDB `Fact` nodes by element qnames or canonical concepts, with filters for periods, entities, form, and fiscal context. Returns deduplicated facts plus the aspects they span — arranging them into a table is the consumer's job, since collapsing cells safely requires the full aspect signature. Works on both roboledger tenant graphs (post-materialization) and the SEC shared repository.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/build-fact-grid",
  method="POST",
  business_event_type="ledger_build_fact_grid",
)
async def build_fact_grid_op(
  body: CreateViewRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = OperationContext(
    domain="roboledger",
    operation_name="build-fact-grid",
    graph_id=graph_id,
    user_id=str(user.id),
    idempotency_key=idempotency_key,
    body_fingerprint=fingerprint_body(body),
  )

  if not body.elements and not body.canonical_concepts:
    raise HTTPException(
      status_code=400,
      detail="Provide elements (qnames) and/or canonical_concepts",
    )
  if not body.periods and not body.period_type and body.fiscal_year is None:
    raise HTTPException(
      status_code=400,
      detail="Provide periods, period_type, or fiscal_year to scope the query",
    )

  async def _runner():
    start_time = time.time()
    fact_data = await query_fact_grid(
      graph_id=graph_id,
      elements=body.elements or None,
      canonical_concepts=body.canonical_concepts or None,
      periods=body.periods or None,
      entity=body.entity,
      entities=body.entities or None,
      form=body.form,
      fiscal_year=body.fiscal_year,
      fiscal_period=body.fiscal_period,
      period_type=body.period_type,
    )

    builder = FactGridBuilder()
    fact_grid = builder.build(
      fact_data=fact_data, view_config=body.view_config, source="fact_grid"
    )

    construction_time_ms = (time.time() - start_time) * 1000
    metadata = ViewMetadata(
      view_id=str(uuid.uuid4()),
      facts_processed=fact_grid.metadata.fact_count,
      construction_time_ms=construction_time_ms,
      source="fact_grid",
    )

    summary = None
    if body.include_summary and fact_grid.facts:
      summary = {
        element: ElementSummary(**stats)
        for element, stats in summarize_by_element(fact_grid.facts).items()
      }

    return ViewResponse(
      metadata=metadata,
      dimensions=fact_grid.dimensions,
      facts=[FactRecord(**fact) for fact in fact_grid.facts],
      summary=summary,
    )

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/financial-statement-analysis",
  response_model=OperationEnvelope[FinancialStatementAnalysisResponse],
  operation_id="financialStatementAnalysis",
  summary="Financial Statement Analysis",
  description=(
    "Query a rendered financial statement from the graph-backed XBRL "
    "hypercube (Structure → FactSet → Fact). Works on the SEC shared "
    "repository today and on any RoboLedger tenant graph whose ledger "
    "has been materialized to LadybugDB. For shared-repo graphs, "
    "provide `ticker` to auto-resolve the latest filing; for tenant "
    "graphs, provide `report_id` explicitly."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/financial-statement-analysis",
  method="POST",
  business_event_type="ledger_financial_statement_analysis",
)
async def financial_statement_analysis_op(
  body: FinancialStatementAnalysisRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = OperationContext(
    domain="roboledger",
    operation_name="financial-statement-analysis",
    graph_id=graph_id,
    user_id=str(user.id),
    idempotency_key=idempotency_key,
    body_fingerprint=fingerprint_body(body),
  )

  if body.statement_type not in ANALYSIS_STATEMENT_TYPES:
    raise HTTPException(
      status_code=400,
      detail=(
        f"Unknown statement_type '{body.statement_type}'. "
        f"Valid types: {', '.join(ANALYSIS_STATEMENT_TYPES)}"
      ),
    )

  is_shared = is_shared_repository_or_subgraph(graph_id)

  if is_shared and not body.ticker and not body.report_id:
    raise HTTPException(
      status_code=400,
      detail="ticker is required on shared-repository graphs (e.g. SEC).",
    )

  if not is_shared and not body.report_id:
    raise HTTPException(
      status_code=400,
      detail="report_id is required for tenant graphs.",
    )

  async def _runner():
    report_id = body.report_id
    resolved: dict | None = None

    if is_shared and not report_id and body.ticker:
      from robosystems.adapters.sec.mcp import resolve_sec_report

      resolved = await resolve_sec_report(
        graph_id,
        ticker=body.ticker,
        period_type=body.period_type,
        fiscal_year=body.fiscal_year,
      )
      report_id = resolved.get("identifier") if resolved else None

    rows: list[dict] = []
    if report_id or body.ticker:
      rows = await query_financial_statement(
        graph_id,
        statement_type=body.statement_type,
        report_id=report_id,
        ticker=body.ticker,
        period_type=body.period_type,
        limit=body.limit,
      )

    deduped = deduplicate_facts(rows)[: body.limit]
    facts = [
      AnalyticalStatementFactRow(
        canonical_concept=row.get("canonical_concept"),
        qname=row.get("qname", ""),
        name=row.get("name", ""),
        value=row.get("value"),
        end_date=row.get("end_date"),
        period_type=row.get("period_type"),
        duration_type=row.get("duration_type"),
      )
      for row in deduped
    ]

    resolved_info: ResolvedReportInfo | None = None
    if resolved:
      resolved_info = ResolvedReportInfo(
        report_id=resolved.get("identifier", ""),
        form=resolved.get("form"),
        filing_date=resolved.get("filing_date"),
        fiscal_year=resolved.get("fiscal_year"),
        fiscal_period=resolved.get("fiscal_period"),
      )

    return FinancialStatementAnalysisResponse(
      graph_id=graph_id,
      statement_type=body.statement_type,
      ticker=body.ticker,
      report_id=report_id,
      resolved_report=resolved_info,
      facts=facts,
      fact_count=len(facts),
    )

  return await _dispatch(ctx, _runner, cache)
