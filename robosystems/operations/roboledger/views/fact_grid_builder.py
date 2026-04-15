import time
from typing import Any

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

    df = fact_data.copy()

    if view_config.rows or view_config.columns:
      df = self._apply_aspect_filtering(df, view_config)

    dimensions = self._extract_dimensions(df)

    metadata = FactGridMetadata(
      fact_count=len(df),
      dimension_count=len(dimensions),
      construction_time_ms=(time.time() - start_time) * 1000,
      source=source,
      lineage={
        "original_fact_count": len(fact_data),
        "filtered_fact_count": len(df),
        "columns": list(df.columns),
      },
    )

    return FactGrid(
      dimensions=dimensions,
      facts_df=df,
      metadata=metadata,
    )

  def _apply_aspect_filtering(
    self, df: pd.DataFrame, view_config: ViewConfig
  ) -> pd.DataFrame:
    """
    Apply aspect filtering based on ViewAxisConfig.selected_members.

    Filters DataFrame to only include rows where axis values match selected members.
    """
    result = df.copy()

    all_axes = (view_config.rows or []) + (view_config.columns or [])

    for axis in all_axes:
      if not axis.selected_members:
        continue

      column_map = {
        "element": "element_id",
        "period": "period_end",
        "entity": "entity_id",
        "dimension": "dimension_member",
      }

      column_name = column_map.get(axis.type)
      if not column_name or column_name not in result.columns:
        continue

      mask = result[column_name].isin(axis.selected_members)
      if not axis.include_null_dimension:
        mask = mask | result[column_name].isna()

      result = result[mask]

    return result

  def _extract_dimensions(self, fact_data: pd.DataFrame) -> list[Dimension]:
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

  def generate_pivot_table(
    self, fact_grid: FactGrid, view_config: ViewConfig | None = None
  ) -> dict[str, Any]:
    """
    Generate pivot table presentation from FactGrid.

    Supports:
    - Element hierarchies with subtotals
    - Custom member ordering and labels
    - ViewConfig-driven axis configuration

    Memory Warning:
    Creates DataFrame copies for pivot operations. For datasets > 100k rows,
    consider streaming or batching approaches to avoid memory pressure.
    Memory usage scales approximately O(rows * columns) for the pivot table.
    """
    if fact_grid.facts_df is None or fact_grid.facts_df.empty:
      return {
        "index": [],
        "columns": [],
        "data": [],
        "metadata": {"row_count": 0, "column_count": 0},
      }

    df = fact_grid.facts_df.copy()

    element_col = "element_label" if "element_label" in df.columns else "element_name"
    if element_col not in df.columns:
      element_col = None

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
    for col in ["period_end", "period_start"]:
      if col in df.columns and not df[col].isna().all():
        period_col = col
        break

    period_axis = None
    if view_config:
      for axis in (view_config.rows or []) + (view_config.columns or []):
        if axis.type == "period":
          period_axis = axis
          break

    if period_col:
      pivot = df.pivot_table(
        index=[element_col],
        columns=[period_col],
        values=value_col,
        aggfunc="sum",
        fill_value=0.0,
      )

      if period_axis and period_axis.member_order:
        available_cols = [c for c in period_axis.member_order if c in pivot.columns]
        pivot = pivot[available_cols]

      if period_axis and period_axis.member_labels:
        pivot = pivot.rename(columns=period_axis.member_labels)
    else:
      pivot = df[[element_col, value_col]].copy()
      pivot = pivot.groupby(element_col)[value_col].sum()

    element_axis = None
    if view_config:
      for axis in (view_config.rows or []) + (view_config.columns or []):
        if axis.type == "element":
          element_axis = axis
          break

    if element_axis and element_axis.element_order and isinstance(pivot, pd.DataFrame):
      element_id_to_name = {}
      if "element_id" in df.columns and element_col in df.columns:
        mapping_df = df[["element_id", element_col]].drop_duplicates()
        element_id_to_name = dict(
          zip(mapping_df["element_id"], mapping_df[element_col], strict=False)
        )

      if element_id_to_name:
        ordered_names = [
          element_id_to_name.get(eid, eid)
          for eid in element_axis.element_order
          if element_id_to_name.get(eid, eid) in pivot.index
        ]
        if ordered_names:
          pivot = pivot.reindex(ordered_names)
      else:
        available_elements = [e for e in element_axis.element_order if e in pivot.index]
        if available_elements:
          pivot = pivot.reindex(available_elements)

    if element_axis and element_axis.element_labels and isinstance(pivot, pd.DataFrame):
      pivot = pivot.rename(index=element_axis.element_labels)

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
        "has_hierarchy": "element_depth" in df.columns,
      },
    }
