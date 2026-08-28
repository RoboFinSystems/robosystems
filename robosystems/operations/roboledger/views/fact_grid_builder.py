"""Assemble queried facts into a `FactGrid`.

Scoping and aspect extraction only — **no pivoting**. Collapsing facts into
cells is a rendering decision that depends on the full aspect signature
(element · period · entity · unit), and getting it wrong is silent: summing
across entities or across two taxonomies whose elements share a local name
produces a number that looks authoritative and is meaningless. That
arrangement belongs to the consumer — `@robosystems/report-components` keys
cells on the whole signature — so this layer hands back the facts as queried.
"""

import time
from typing import Any

from robosystems.models.api.views import (
  Dimension,
  DimensionType,
  FactGrid,
  FactGridMetadata,
  ViewConfig,
)

# Aspect name (ViewAxisConfig.type) → the key `query_fact_grid` returns it under.
_AXIS_COLUMNS = {
  "element": "element_id",
  "period": "period_end",
  "entity": "entity_ticker",
}


def summarize_by_element(facts: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
  """Per-element aggregates over the returned facts.

  Shared by the REST and MCP surfaces so both report the same numbers.
  `total` and `average` span every returned period, so they are emitted for
  duration facts only. A balance is a point-in-time measure: summed or
  averaged across periods it yields a figure that looks authoritative and
  means nothing, and a model reading the field quotes it. Instants (no
  `period_start`, or an explicit `period_type` of `instant`) carry
  `count` / `min` / `max` only.

  Overlapping duration windows that share a `period_end` (a 10-Q reports the
  same element for both the quarter and the year-to-date window ending the
  same day) contribute only their narrowest window per (entity, period_end):
  summing a quarter and the YTD window that contains it double-counts the
  quarter. The window survives in the `facts` list either way — this is an
  aggregation rule, not a filter.
  """
  summary: dict[str, dict[str, float]] = {}

  # (element, entity, period_end) → (period_start, value); the latest start
  # is the narrowest window ending that day. Instants have no period_start
  # and arrive one-per-end after dedup, so they pass through unchanged.
  narrowest: dict[tuple[str, Any, Any], tuple[str, float]] = {}
  instant_elements: set[str] = set()
  for fact in facts:
    element = fact.get("element_id")
    value = fact.get("value")
    if element is None or value is None:
      continue
    key = (str(element), fact.get("entity_ticker"), fact.get("period_end"))
    start = str(fact.get("period_start") or "")
    if not start or fact.get("period_type") == "instant":
      instant_elements.add(str(element))
    prev = narrowest.get(key)
    if prev is None or start > prev[0]:
      narrowest[key] = (start, float(value))

  by_element: dict[str, list[float]] = {}
  for (element, _entity, _period_end), (_start, value) in narrowest.items():
    by_element.setdefault(element, []).append(value)

  for element, values in by_element.items():
    stats: dict[str, float] = {"count": len(values)}
    if element not in instant_elements:
      stats["total"] = sum(values)
      stats["average"] = sum(values) / len(values)
    stats["min"] = min(values)
    stats["max"] = max(values)
    summary[element] = stats

  return summary


class FactGridBuilder:
  """Build a FactGrid from queried fact records."""

  def build(
    self,
    fact_data: list[dict[str, Any]],
    view_config: ViewConfig,
    source: str,
  ) -> FactGrid:
    """Scope `query_fact_grid` records per `view_config` into a FactGrid.

    The grid carries the scoped facts plus the aspects they span; `source`
    is recorded on the metadata.
    """
    start_time = time.time()

    if not fact_data:
      return self._build_empty_grid(source)

    facts = fact_data
    if view_config.rows or view_config.columns:
      facts = self._apply_aspect_filtering(facts, view_config)

    dimensions = self._extract_dimensions(facts)

    metadata = FactGridMetadata(
      fact_count=len(facts),
      dimension_count=len(dimensions),
      construction_time_ms=(time.time() - start_time) * 1000,
      source=source,
      lineage={
        "original_fact_count": len(fact_data),
        "filtered_fact_count": len(facts),
      },
    )

    return FactGrid(
      dimensions=dimensions,
      facts=facts,
      metadata=metadata,
    )

  def _apply_aspect_filtering(
    self, facts: list[dict[str, Any]], view_config: ViewConfig
  ) -> list[dict[str, Any]]:
    """Restrict facts to each axis's `selected_members`.

    A fact whose aspect value is absent is dropped unless the axis sets
    `include_null_dimension`.
    """
    result = facts

    for axis in (view_config.rows or []) + (view_config.columns or []):
      if not axis.selected_members:
        continue

      key = _AXIS_COLUMNS.get(axis.type)
      if not key:
        continue

      selected = set(axis.selected_members)
      keep_null = axis.include_null_dimension
      result = [
        fact
        for fact in result
        if (fact.get(key) in selected) or (keep_null and fact.get(key) in (None, ""))
      ]

    return result

  def _extract_dimensions(self, facts: list[dict[str, Any]]) -> list[Dimension]:
    """Extract the aspects the facts span, in a stable order."""
    dimensions = []

    elements = self._unique(facts, "element_id")
    if elements:
      dimensions.append(
        Dimension(
          name="Element",
          type=DimensionType.ELEMENT,
          members=elements,
        )
      )

    periods = self._unique(facts, "period_end")
    if periods:
      dimensions.append(
        Dimension(
          name="Period",
          type=DimensionType.PERIOD,
          members=sorted(periods),
        )
      )

    # entity_ticker/entity_name are only returned when an entity filter was
    # applied; a ticker can be null for CIK- or name-matched entities.
    entities = self._unique(facts, "entity_ticker") or self._unique(
      facts, "entity_name"
    )
    if entities:
      dimensions.append(
        Dimension(
          name="Entity",
          type=DimensionType.ENTITY,
          members=entities,
        )
      )

    return dimensions

  @staticmethod
  def _unique(facts: list[dict[str, Any]], key: str) -> list[str]:
    """Distinct non-null values for `key`, in first-seen order."""
    seen: dict[str, None] = {}
    for fact in facts:
      value = fact.get(key)
      if value is not None and value != "":
        seen.setdefault(str(value), None)
    return list(seen)

  def _build_empty_grid(self, source: str) -> FactGrid:
    """Build empty FactGrid when no facts found."""
    return FactGrid(
      dimensions=[],
      facts=[],
      metadata=FactGridMetadata(
        fact_count=0,
        dimension_count=0,
        construction_time_ms=0.0,
        source=source,
        lineage=None,
      ),
    )
