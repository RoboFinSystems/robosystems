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
from robosystems.models.api.views import (
  CreateViewRequest,
  ViewMetadata,
  ViewResponse,
)
from robosystems.models.core import User
from robosystems.operations.roboledger.views import (
  FactGridBuilder,
  query_fact_grid,
)

# Import _dispatch from the sibling operations module so error
# translation (idempotency conflict → 409, etc.) stays centralized.
from robosystems.routers.extensions.roboledger.operations import _dispatch

router = APIRouter()

_OP_TAG = "Extensions: RoboLedger"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)


@router.post(
  "/build-fact-grid",
  response_model=OperationEnvelope,
  operation_id="opBuildFactGrid",
  summary="Build Fact Grid",
  description="Queries LadybugDB `Fact` nodes by element qnames or canonical concepts, with filters for periods, entities, form, and fiscal context. Returns a deduplicated pivot table. Works on both roboledger tenant graphs (post-materialization) and the SEC shared repository.",
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
    pivot_table = builder.generate_pivot_table(fact_grid, body.view_config)

    construction_time_ms = (time.time() - start_time) * 1000
    metadata = ViewMetadata(
      view_id=str(uuid.uuid4()),
      facts_processed=fact_grid.metadata.fact_count,
      construction_time_ms=construction_time_ms,
      source="fact_grid",
    )

    presentations: dict[str, object] = {"pivot_table": pivot_table}

    # Optional inline summary stats — element-keyed aggregates, only
    # computed when the caller asks for them.
    if (
      body.include_summary
      and fact_grid.facts_df is not None
      and not fact_grid.facts_df.empty
    ):
      df = fact_grid.facts_df
      if "element_name" in df.columns and "value" in df.columns:
        summary: dict[str, dict[str, float]] = {}
        for element_name in df["element_name"].unique():
          element_data = df[df["element_name"] == element_name]
          summary[element_name] = {
            "count": len(element_data),
            "total": float(element_data["value"].sum()),
            "average": float(element_data["value"].mean()),
            "min": float(element_data["value"].min()),
            "max": float(element_data["value"].max()),
          }
        presentations["summary"] = summary

    return ViewResponse(metadata=metadata, presentations=presentations)

  return await _dispatch(ctx, _runner, cache)
