"""Tests for the chart View projection — panel grouping over a rendering."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from robosystems.models.api.information_block import (
  RenderingLite,
  RenderingPeriodLite,
  RenderingRowLite,
)
from robosystems.operations.information_block.chart import build_chart_projection


def _element(
  element_id: str,
  *,
  item_type: str | None = None,
  is_monetary: bool = False,
) -> SimpleNamespace:
  return SimpleNamespace(id=element_id, item_type=item_type, is_monetary=is_monetary)


def _row(
  element_id: str,
  name: str,
  *,
  values: list[float | None] | None = None,
  is_subtotal: bool = False,
  text_value: str | None = None,
) -> RenderingRowLite:
  return RenderingRowLite(
    element_id=element_id,
    element_name=name,
    values=values or [1.0, 2.0],
    is_subtotal=is_subtotal,
    text_value=text_value,
  )


def _rendering(rows: list[RenderingRowLite]) -> RenderingLite:
  return RenderingLite(
    rows=rows,
    periods=[
      RenderingPeriodLite(start=date(2026, 5, 1), end=date(2026, 5, 31)),
      RenderingPeriodLite(start=date(2026, 6, 1), end=date(2026, 6, 30)),
    ],
  )


class TestBuildChartProjection:
  def test_groups_rows_into_item_type_panels_in_arc_order(self) -> None:
    rendering = _rendering(
      [
        _row("el_wc", "Working Capital"),
        _row("el_cr", "Current Ratio"),
        _row("el_qr", "Quick Ratio"),
        _row("el_roe", "Return on Equity"),
      ]
    )
    elements = {
      "el_wc": _element("el_wc", item_type="monetary"),
      "el_cr": _element("el_cr", item_type="ratio"),
      "el_qr": _element("el_qr", item_type="ratio"),
      "el_roe": _element("el_roe", item_type="percent"),
    }

    chart = build_chart_projection(rendering, elements)

    assert chart is not None
    assert [p.item_type for p in chart.panels] == ["monetary", "ratio", "percent"]
    assert [p.label for p in chart.panels] == [
      "Monetary",
      "Ratios",
      "Percentages",
    ]
    ratio_panel = chart.panels[1]
    assert [s.label for s in ratio_panel.series] == ["Current Ratio", "Quick Ratio"]
    for panel in chart.panels:
      assert panel.kind == "line"
      for series in panel.series:
        assert series.key == series.element_id

  def test_null_item_type_falls_back_to_is_monetary(self) -> None:
    rendering = _rendering(
      [_row("el_a", "Untyped Monetary"), _row("el_b", "Untyped Pure")]
    )
    elements = {
      "el_a": _element("el_a", is_monetary=True),
      "el_b": _element("el_b", is_monetary=False),
    }

    chart = build_chart_projection(rendering, elements)

    assert chart is not None
    assert [p.item_type for p in chart.panels] == ["monetary", None]
    assert chart.panels[0].label == "Monetary"
    assert chart.panels[1].label is None

  def test_excludes_subtotal_and_text_rows(self) -> None:
    rendering = _rendering(
      [
        _row("el_a", "Line"),
        _row("el_sub", "Subtotal", is_subtotal=True),
        _row("el_txt", "Policy", text_value="# markdown"),
      ]
    )
    elements = {
      "el_a": _element("el_a", item_type="ratio"),
      "el_sub": _element("el_sub", item_type="ratio"),
      "el_txt": _element("el_txt"),
    }

    chart = build_chart_projection(rendering, elements)

    assert chart is not None
    assert len(chart.panels) == 1
    assert [s.element_id for s in chart.panels[0].series] == ["el_a"]

  def test_none_for_empty_rendering(self) -> None:
    assert build_chart_projection(None, {}) is None
    assert build_chart_projection(_rendering([]), {}) is None

  def test_none_when_every_row_is_excluded(self) -> None:
    rendering = _rendering([_row("el_sub", "Subtotal", is_subtotal=True)])
    assert build_chart_projection(rendering, {}) is None

  def test_unknown_element_lands_in_untyped_panel(self) -> None:
    rendering = _rendering([_row("el_ghost", "Ghost")])
    chart = build_chart_projection(rendering, {})
    assert chart is not None
    assert chart.panels[0].item_type is None
    assert chart.panels[0].series[0].label == "Ghost"

  def test_unknown_family_uses_raw_item_type_as_label(self) -> None:
    rendering = _rendering([_row("el_x", "Exotic")])
    chart = build_chart_projection(
      rendering, {"el_x": _element("el_x", item_type="shares")}
    )
    assert chart is not None
    assert chart.panels[0].item_type == "shares"
    assert chart.panels[0].label == "shares"
