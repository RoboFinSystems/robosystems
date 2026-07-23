"""Chart View projection — panel/series config over a rendering.

The 7th ``type-of View`` arm (A-2), and the second real server-shaped
one after ``rendering``. The server's value-add is the grouping logic:
mixed-unit catalogs are unplottable on one y-axis, so rows group into
one panel per ``item_type`` format family (NULL falls back to the
``is_monetary`` binary), panel order following first appearance in
presentation-arc order. The projection carries structure only — series
values live in ``rendering.rows`` (joined by ``element_id``) and the
x-axis is ``rendering.periods`` — so the chart arm never duplicates the
value matrix.

Rendering-generic by design: A-2 invokes it from the metric envelope
builder only, but any builder whose rendering rows carry plottable
per-period values (statements, schedules) can adopt it without
redesign. Subtotal rows are excluded — statement charts need subtotal
curation first, which is the adopting builder's problem, not this
helper's.
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
  """(item_type family, panel label) for one row's element.

  NULL ``item_type`` falls back to the ``is_monetary`` binary — the
  same fallback the unit mapping uses — so untyped catalogs still get
  coherent monetary/pure panels.
  """
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
  """Group a rendering's plottable rows into format-family chart panels.

  Returns ``None`` when there is nothing to plot (no rendering, no
  rows) — the envelope simply omits the arm and the frontend's chart
  mode renders its empty state.
  """
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
