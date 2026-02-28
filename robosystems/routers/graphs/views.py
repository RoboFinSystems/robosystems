import time
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request

from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.api.views import (
  CreateViewRequest,
  ViewMetadata,
)
from robosystems.models.iam import User
from robosystems.operations.views import (
  FactGridBuilder,
  query_fact_grid,
)

router = APIRouter(prefix="/views", tags=["Views"])


@router.post("", operation_id="create_view")
async def create_view(
  graph_id: str,
  request: CreateViewRequest,
  req: Request,
  current_user: User = Depends(get_current_user),
):
  """
  Build a fact grid from existing facts in the graph.

  Queries Fact nodes by element qnames or canonical concepts, with optional
  filters for periods, entities, filing form, fiscal context, and period type.
  Returns deduplicated consolidated facts as a pivot table.
  """
  start_time = time.time()

  if not request.elements and not request.canonical_concepts:
    raise HTTPException(
      status_code=400,
      detail="Provide elements (qnames) and/or canonical_concepts",
    )

  if not request.periods and not request.period_type and request.fiscal_year is None:
    raise HTTPException(
      status_code=400,
      detail="Provide periods, period_type, or fiscal_year to scope the query",
    )

  try:
    fact_data = await query_fact_grid(
      graph_id=graph_id,
      elements=request.elements or None,
      canonical_concepts=request.canonical_concepts or None,
      periods=request.periods or None,
      entity=request.entity,
      entities=request.entities or None,
      form=request.form,
      fiscal_year=request.fiscal_year,
      fiscal_period=request.fiscal_period,
      period_type=request.period_type,
    )

    builder = FactGridBuilder()
    fact_grid = builder.build(
      fact_data=fact_data,
      view_config=request.view_config,
      source="fact_grid",
    )

    pivot_table = builder.generate_pivot_table(fact_grid, request.view_config)

    construction_time_ms = (time.time() - start_time) * 1000

    metadata = ViewMetadata(
      view_id=str(uuid.uuid4()),
      facts_processed=fact_grid.metadata.fact_count,
      construction_time_ms=construction_time_ms,
      source="fact_grid",
    )

    response_data = {
      "metadata": metadata,
      "presentations": {"pivot_table": pivot_table},
    }

    if (
      request.include_summary
      and fact_grid.facts_df is not None
      and not fact_grid.facts_df.empty
    ):
      df = fact_grid.facts_df
      if "element_name" in df.columns and "value" in df.columns:
        summary = {}
        for element_name in df["element_name"].unique():
          element_data = df[df["element_name"] == element_name]
          summary[element_name] = {
            "count": len(element_data),
            "total": float(element_data["value"].sum()),
            "average": float(element_data["value"].mean()),
            "min": float(element_data["value"].min()),
            "max": float(element_data["value"].max()),
          }
        response_data["summary"] = summary

    return response_data

  except HTTPException:
    raise
  except Exception as e:
    raise HTTPException(
      status_code=500,
      detail=f"Failed to create view: {e!s}",
    ) from e
