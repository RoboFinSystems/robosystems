import time
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.iam import User
from robosystems.models.api.views import (
  CreateViewRequest,
  ViewMetadata,
  ViewResponse,
  ViewSourceType,
)
from robosystems.operations.views import (
  FactGridBuilder,
  aggregate_trial_balance,
  query_facts_with_aspects,
)

router = APIRouter(prefix="/views", tags=["views"])


@router.post("")
async def create_view(
  graph_id: str,
  request: CreateViewRequest,
  req: Request,
  current_user: User = Depends(get_current_user),
):
  """
  Generate financial report view from data source (dual-mode support).

  **Mode 1: Transaction Aggregation (generate_from_transactions)**
  - Aggregates raw transaction data to trial balance
  - Creates facts on-demand
  - Shows real-time reporting from source of truth

  **Mode 2: Existing Facts (pivot_existing_facts)**
  - Queries existing Fact nodes
  - Supports multi-dimensional analysis
  - Works with SEC filings and pre-computed facts

  Both modes:
  - Build FactGrid from data
  - Generate pivot table presentation
  - Return consistent response format
  """
  start_time = time.time()

  try:
    if request.source.type == ViewSourceType.TRANSACTIONS:
      fact_data = await aggregate_trial_balance(
        graph_id=graph_id,
        period_start=request.source.period_start,
        period_end=request.source.period_end,
        entity_id=request.source.entity_id,
      )
      source = "trial_balance_aggregation"
      period_start = request.source.period_start
      period_end = request.source.period_end

    elif request.source.type == ViewSourceType.FACT_SET:
      fact_data = await query_facts_with_aspects(
        graph_id=graph_id,
        fact_set_id=request.source.fact_set_id,
        period_start=request.source.period_start,
        period_end=request.source.period_end,
        entity_id=request.source.entity_id,
      )
      source = "fact_set_query"
      period_start = request.source.period_start
      period_end = request.source.period_end

    else:
      raise HTTPException(
        status_code=400,
        detail=f"Unsupported source type: {request.source.type}",
      )

    builder = FactGridBuilder()
    fact_grid = builder.build(
      fact_data=fact_data,
      view_config=request.view_config,
      source=source,
    )

    pivot_table = builder.generate_pivot_table(fact_grid)

    construction_time_ms = (time.time() - start_time) * 1000

    metadata = ViewMetadata(
      view_id=str(uuid.uuid4()),
      facts_processed=fact_grid.metadata.fact_count,
      construction_time_ms=construction_time_ms,
      source=source,
      period_start=period_start,
      period_end=period_end,
    )

    return ViewResponse(
      metadata=metadata,
      presentations={"pivot_table": pivot_table},
    )

  except Exception as e:
    raise HTTPException(
      status_code=500,
      detail=f"Failed to create view: {str(e)}",
    ) from e
