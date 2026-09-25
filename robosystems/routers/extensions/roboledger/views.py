"""RoboLedger analytical views: `build-fact-grid`, `financial-statement-analysis`,
and the `disclosures` / `information-block` pair.

The first two read the LadybugDB graph; the pair runs xbrlkit's tools over the
report held whole (the published filing on a shared repository, the ledger's
own report on a tenant). A separate router from the operations package so the
mount gates on `FACT_GRID_ENABLED`: a deployment serving SEC research without
RoboLedger tenants still gets these endpoints.
"""

from __future__ import annotations

import time
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from sqlalchemy.orm import Session

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.billing.enforcement import require_graph_access
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
  DisclosuresRequest,
  DisclosuresResponse,
  FinancialStatementAnalysisRequest,
  FinancialStatementAnalysisResponse,
  InformationBlockRequest,
  InformationBlockResponse,
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
from robosystems.operations.roboledger.reads.reports import ANALYSIS_STATEMENT_TYPES
from robosystems.operations.roboledger.views import (
  BlockNotFoundError,
  FactGridBuilder,
  ReportNotFoundError,
  ReportSelectorError,
  ReportTooLargeError,
  deduplicate_facts,
  query_disclosures,
  query_fact_grid,
  query_financial_statement,
  query_information_block,
  resolve_report,
  resolved_report_info,
  summarize_by_element,
)

# From `_common`, not the operations package, so a FACT_GRID_ENABLED-only
# deployment doesn't import ledger commands it never mounts.
from robosystems.routers.extensions.roboledger._common import _dispatch

router = APIRouter()

_OP_TAG = "RoboLedger: Analytical Views"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)


async def _require_readable_graph(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  session: Session = Depends(get_db_session),
) -> None:
  """Lifecycle/subscription gate (read strength) for the analytical views,
  then the shared repositories' per-plan read limits.

  Not `require_graph_extension`, which refuses shared repos; this is the same
  `require_graph_access` check, and shared repos pass (their access is
  per-user). A view read counts as a query, like the REST Cypher route.
  Depends on the auth dependency so graph state is never observable before
  authentication.

  Views accept an Idempotency-Key and do not use it: a read has nothing to
  deduplicate, and a replay record would hold the whole result in the shared
  cache for a day.
  """
  from robosystems.routers.graphs.query.execute import (
    _check_shared_repository_limits,
  )

  require_graph_access(graph_id, session, require_write=False)
  await _check_shared_repository_limits(
    graph_id, user, session, endpoint="query", operation="query"
  )


_READABLE_GRAPH = Depends(_require_readable_graph)


@router.post(
  "/build-fact-grid",
  response_model=OperationEnvelope[ViewResponse],
  operation_id="buildFactGrid",
  summary="Build Fact Grid",
  description="Queries LadybugDB `Fact` nodes by element qnames or canonical concepts, with filters for periods, entities, form, and fiscal context. Returns deduplicated facts plus the aspects they span — arranging them into a table is the consumer's job, since collapsing cells safely requires the full aspect signature. Works on both roboledger tenant graphs (post-materialization) and the SEC shared repository.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT, _READABLE_GRAPH],
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
    idempotency_key=None,  # see _require_readable_graph: reads keep no replay
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

  # Only on shared repos, where an entity-less query returns an arbitrary
  # slice across thousands of filers. A tenant graph is already scoped to its
  # entity by the URL (often a private company with no ticker or CIK).
  if (
    is_shared_repository_or_subgraph(graph_id) and not body.entity and not body.entities
  ):
    raise HTTPException(
      status_code=400,
      detail=("entity or entities is required on shared-repository graphs (e.g. SEC)."),
    )

  async def _runner():
    start_time = time.time()
    fact_data, truncated = await query_fact_grid(
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
      limit=body.limit,
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
      truncated=truncated,
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
  dependencies=[_RATE_LIMIT, _READABLE_GRAPH],
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
    idempotency_key=None,  # see _require_readable_graph: reads keep no replay
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

      # An unresolved fiscal_year is a 404: the ticker path below never
      # receives fiscal_year and would silently return the newest filing.
      if body.fiscal_year is not None and not report_id:
        raise HTTPException(
          status_code=404,
          detail=(
            f"No {body.period_type or 'annual'} filing found for "
            f"{body.ticker} in fiscal year {body.fiscal_year}."
          ),
        )

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
        start_date=row.get("start_date"),
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


# ── Information blocks: the map and the block ──────────────────────────────
# xbrlkit's own ``disclosures`` / ``information_block`` over the report held
# whole, so hosted and local tools answer identically. The graph is not in the path.


def _report_selector_errors(exc: ValueError) -> HTTPException:
  """Selector errors → 400, missing report/block → 404, too large → 422;
  anything else falls through to the dispatcher's policy."""
  if isinstance(exc, ReportSelectorError):
    return HTTPException(status_code=400, detail=str(exc))
  if isinstance(exc, (ReportNotFoundError, BlockNotFoundError)):
    return HTTPException(status_code=404, detail=str(exc))
  if isinstance(exc, ReportTooLargeError):
    return HTTPException(status_code=422, detail=str(exc))
  raise exc


@router.post(
  "/disclosures",
  response_model=OperationEnvelope[DisclosuresResponse],
  operation_id="disclosures",
  summary="Disclosures",
  description=(
    "The map of a report's sections: one row per disclosure family — a note "
    "with its policies, tables and details, a statement with its parenthetical, "
    "the cover page — with block counts by level, fact and text-block counts, in "
    "filing order; with `topic`, one family's blocks with the ids "
    "`information-block` takes. Reads the report whole — the published filing "
    "on the SEC shared repository, the ledger's own report on a tenant graph — "
    "into xbrlkit's model and runs xbrlkit's own `disclosures`, so both answer "
    "as `xbrlkit serve` does. Cheap: call it before `information-block`. "
    "Shared-repo graphs take `ticker` (auto-resolving the latest filing) or "
    "`report_id`; tenant graphs take `report_id`. A filing processed before its "
    "artifacts existed answers 404 until the repository is reprocessed."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT, _READABLE_GRAPH],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/disclosures",
  method="POST",
  business_event_type="ledger_disclosures",
)
async def disclosures_op(
  body: DisclosuresRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = OperationContext(
    domain="roboledger",
    operation_name="disclosures",
    graph_id=graph_id,
    user_id=str(user.id),
    idempotency_key=None,  # see _require_readable_graph: reads keep no replay
    body_fingerprint=fingerprint_body(body),
  )

  async def _runner():
    try:
      report_id, resolved = await resolve_report(
        graph_id,
        report_id=body.report_id,
        ticker=body.ticker,
        fiscal_year=body.fiscal_year,
        period_type=body.period_type,
      )
      result = await query_disclosures(graph_id, report_id, topic=body.topic)
    except ValueError as exc:
      raise _report_selector_errors(exc) from exc
    return DisclosuresResponse(**result, resolved_report=resolved_report_info(resolved))

  return await _dispatch(ctx, _runner, cache)


@router.post(
  "/information-block",
  response_model=OperationEnvelope[InformationBlockResponse],
  operation_id="informationBlock",
  summary="Information Block",
  description=(
    "One section of a report read whole — the expensive call: rows in "
    "presentation order with the consolidated value per period column, the "
    "same rows broken out by the section's own axes, the axes with the members "
    "that carry facts, every total's calculation children with a footing check, "
    "and the section's text blocks. Member breakdowns and period columns are "
    "kept most-reported / most-recent first up to a response budget; a row is "
    "never left blank by a cut, and `members_omitted` / `periods_omitted` say "
    "what was. A block longer than `max_rows` is `truncated`; pass its "
    "`next_offset` as `offset` for the next page. Take `block` from "
    "`disclosures`. Same resolution as "
    "`disclosures`: `ticker` or `report_id` on shared-repo graphs, `report_id` "
    "on tenant graphs. On a tenant graph this reads the section as the "
    "ledger's report holds it; `get-information-block` returns one authored "
    "block's envelope with its rules and verification."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT, _READABLE_GRAPH],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/information-block",
  method="POST",
  business_event_type="ledger_information_block",
)
async def information_block_op(
  body: InformationBlockRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(get_current_user_with_graph),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  ctx = OperationContext(
    domain="roboledger",
    operation_name="information-block",
    graph_id=graph_id,
    user_id=str(user.id),
    idempotency_key=None,  # see _require_readable_graph: reads keep no replay
    body_fingerprint=fingerprint_body(body),
  )

  async def _runner():
    try:
      report_id, resolved = await resolve_report(
        graph_id,
        report_id=body.report_id,
        ticker=body.ticker,
        fiscal_year=body.fiscal_year,
        period_type=body.period_type,
      )
      result = await query_information_block(
        graph_id,
        report_id,
        body.block,
        periods=body.periods,
        member=body.member,
        max_rows=body.max_rows,
        max_members=body.max_members,
        offset=body.offset,
      )
    except ValueError as exc:
      raise _report_selector_errors(exc) from exc
    return InformationBlockResponse(
      **result, resolved_report=resolved_report_info(resolved)
    )

  return await _dispatch(ctx, _runner, cache)
