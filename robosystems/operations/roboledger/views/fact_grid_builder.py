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
  `total` sums across every returned period — meaningful for duration facts,
  not for instants (summing a balance across time is not a balance).
  """
  summary: dict[str, dict[str, float]] = {}

  by_element: dict[str, list[float]] = {}
  for fact in facts:
    element = fact.get("element_id")
    value = fact.get("value")
    if element is None or value is None:
      continue
    by_element.setdefault(str(element), []).append(float(value))

  for element, values in by_element.items():
    summary[element] = {
      "count": len(values),
      "total": sum(values),
      "average": sum(values) / len(values),
      "min": min(values),
      "max": max(values),
    }

  return summary


class FactGridBuilder:
  """Build a FactGrid from queried fact records."""

  def build(
    self,
    fact_data: list[dict[str, Any]],
    view_config: ViewConfig,
    source: str,
  ) -> FactGrid:
    """Build FactGrid from fact records.

    Args:
        fact_data: Fact records from `query_fact_grid`.
        view_config: Caller-specified aspect scoping.
        source: Source identifier for metadata.

    Returns:
        FactGrid carrying the scoped facts and the aspects they span.
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
