import time
from typing import Any, Dict, List

import pandas as pd
from robosystems.models.api.views import (
  Dimension,
  DimensionType,
  FactGrid,
  FactGridMetadata,
  ViewConfig,
)


class FactGridBuilder:
  """
  Build FactGrid from fact data (unified for both modes).

  Works with:
  - Mode 1: Trial balance data from transaction aggregation
  - Mode 2: Existing facts queried from graph
  """

  def build(
    self,
    fact_data: pd.DataFrame,
    view_config: ViewConfig,
    source: str,
  ) -> FactGrid:
    """
    Build FactGrid from fact data.

    Args:
        fact_data: DataFrame with fact data (from either mode)
        view_config: User-specified view configuration
        source: Source identifier for metadata

    Returns:
        FactGrid ready for presentation generation
    """
    start_time = time.time()

    if fact_data.empty:
      return self._build_empty_grid(source)

    dimensions = self._extract_dimensions(fact_data)

    metadata = FactGridMetadata(
      fact_count=len(fact_data),
      dimension_count=len(dimensions),
      construction_time_ms=(time.time() - start_time) * 1000,
      source=source,
      lineage={
        "fact_count": len(fact_data),
        "columns": list(fact_data.columns),
      },
    )

    return FactGrid(
      dimensions=dimensions,
      facts_df=fact_data,
      metadata=metadata,
    )

  def _extract_dimensions(self, fact_data: pd.DataFrame) -> List[Dimension]:
    """Extract dimensions from fact data."""
    dimensions = []

    if "element_name" in fact_data.columns:
      unique_elements = fact_data["element_name"].dropna().unique().tolist()
      dimensions.append(
        Dimension(
          name="Element",
          type=DimensionType.ELEMENT,
          members=unique_elements,
        )
      )

    if "period_start" in fact_data.columns or "period_end" in fact_data.columns:
      period_members = []
      if "period_start" in fact_data.columns:
        period_members.extend(fact_data["period_start"].dropna().unique().tolist())
      if "period_end" in fact_data.columns:
        period_members.extend(fact_data["period_end"].dropna().unique().tolist())
      unique_periods = sorted(set(period_members))

      dimensions.append(
        Dimension(
          name="Period",
          type=DimensionType.PERIOD,
          members=unique_periods,
        )
      )

    if "entity_id" in fact_data.columns:
      unique_entities = fact_data["entity_id"].dropna().unique().tolist()
      if unique_entities:
        dimensions.append(
          Dimension(
            name="Entity",
            type=DimensionType.ENTITY,
            members=unique_entities,
          )
        )

    if "dimension_axis" in fact_data.columns:
      unique_axes = fact_data["dimension_axis"].dropna().unique().tolist()
      if unique_axes:
        dimensions.append(
          Dimension(
            name="DimensionAxis",
            type=DimensionType.DIMENSION_AXIS,
            members=unique_axes,
          )
        )

    return dimensions

  def _build_empty_grid(self, source: str) -> FactGrid:
    """Build empty FactGrid when no facts found."""
    return FactGrid(
      dimensions=[],
      facts_df=pd.DataFrame(),
      metadata=FactGridMetadata(
        fact_count=0,
        dimension_count=0,
        construction_time_ms=0.0,
        source=source,
        lineage=None,
      ),
    )

  def generate_pivot_table(self, fact_grid: FactGrid) -> Dict[str, Any]:
    """
    Generate pivot table presentation from FactGrid.

    Returns simple pivot table structure for now.
    Future: Full multi-dimensional pivot with hierarchies.
    """
    if fact_grid.facts_df is None or fact_grid.facts_df.empty:
      return {
        "index": [],
        "columns": [],
        "data": [],
        "metadata": {"row_count": 0, "column_count": 0},
      }

    df = fact_grid.facts_df

    element_col = "element_name" if "element_name" in df.columns else None
    value_col = "numeric_value" if "numeric_value" in df.columns else "net_balance"

    if not element_col or value_col not in df.columns:
      return {
        "index": list(df.index),
        "columns": list(df.columns),
        "data": df.values.tolist(),
        "metadata": {
          "row_count": len(df),
          "column_count": len(df.columns),
        },
      }

    period_col = None
    for col in ["period_start", "period_end"]:
      if col in df.columns and not df[col].isna().all():
        period_col = col
        break

    if period_col:
      pivot = df.pivot_table(
        index=[element_col],
        columns=[period_col],
        values=value_col,
        aggfunc="sum",
        fill_value=0.0,
      )
    else:
      pivot = df[[element_col, value_col]].copy()
      pivot = pivot.groupby(element_col)[value_col].sum()

    index_values = (
      [[idx] for idx in pivot.index]
      if isinstance(pivot.index, pd.Index)
      else pivot.index.tolist()
    )

    if isinstance(pivot, pd.DataFrame):
      column_values = (
        [[col] for col in pivot.columns]
        if isinstance(pivot.columns, pd.Index)
        else pivot.columns.tolist()
      )
      data_values = pivot.values.tolist()
    else:
      column_values = [["Total"]]
      data_values = [[val] for val in pivot.values.tolist()]

    return {
      "index": index_values,
      "columns": column_values,
      "data": data_values,
      "metadata": {
        "row_count": len(index_values),
        "column_count": len(column_values),
        "has_periods": period_col is not None,
      },
    }
