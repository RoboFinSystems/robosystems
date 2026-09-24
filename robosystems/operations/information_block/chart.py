"""Chart View projection — panel/series config over a rendering.

Mixed units can't share a y-axis, so rows group into one panel per
``item_type`` family, in first-appearance order. The projection carries
structure only; values stay in ``rendering.rows`` and the x-axis is
``rendering.periods``. Subtotal rows are excluded.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.models.api.information_block import (
  ChartLite,
  ChartPanelLite,
  ChartSeriesLite,
  RenderingLite,
)

if TYPE_CHECKING:
  from robosystems.models.extensions import Element

_FAMILY_LABELS = {
  "monetary": "Monetary",
  "ratio": "Ratios",
  "percent": "Percentages",
  "multiple": "Multiples",
  "days": "Days",
}


def _family_for(element: Element | None) -> tuple[str | None, str | None]:
  """(item_type family, panel label) for one row's element; NULL
  ``item_type`` falls back to ``is_monetary``."""
  if element is None:
    return None, None
  if element.item_type is not None:
    return element.item_type, _FAMILY_LABELS.get(element.item_type, element.item_type)
  if element.is_monetary:
    return "monetary", _FAMILY_LABELS["monetary"]
  return None, None


def build_chart_projection(
  rendering: RenderingLite | None,
  elements_by_id: dict[str, Element],
) -> ChartLite | None:
  """Group a rendering's plottable rows into format-family chart panels;
  ``None`` when there is nothing to plot."""
  if rendering is None or not rendering.rows:
    return None

  panels: dict[str | None, ChartPanelLite] = {}
  order: list[str | None] = []
  for row in rendering.rows:
    if row.is_subtotal or row.text_value is not None:
      continue
    family, label = _family_for(elements_by_id.get(row.element_id))
    if family not in panels:
      panels[family] = ChartPanelLite(
        label=label, item_type=family, kind="line", series=[]
      )
      order.append(family)
    panels[family].series.append(
      ChartSeriesLite(
        key=row.element_id,
        element_id=row.element_id,
        label=row.element_name,
      )
    )

  if not order:
    return None
  return ChartLite(panels=[panels[family] for family in order])


__all__ = ["build_chart_projection"]
