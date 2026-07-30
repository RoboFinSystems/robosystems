from pydantic import BaseModel, Field

from robosystems.models.api.views.fact_grid import Dimension


class FactRecord(BaseModel):
  element_id: str = Field(..., description="Element qname (e.g., 'us-gaap:Assets')")
  element_name: str | None = Field(None, description="Element local name")
  period_end: str | None = Field(None, description="Period end date (YYYY-MM-DD)")
  value: float | None = Field(None, description="Numeric fact value")
  unit: str | None = Field(None, description="Unit of measure (e.g., 'USD')")
  entity_ticker: str | None = Field(
    None, description="Entity ticker; present only when an entity filter was applied"
  )
  entity_name: str | None = Field(
    None, description="Entity name; present only when an entity filter was applied"
  )


class ElementSummary(BaseModel):
  count: int = Field(..., description="Number of facts for this element")
  total: float = Field(..., description="Sum of values across the returned facts")
  average: float = Field(..., description="Mean value across the returned facts")
  min: float = Field(..., description="Minimum value across the returned facts")
  max: float = Field(..., description="Maximum value across the returned facts")


class ViewMetadata(BaseModel):
  view_id: str = Field(..., description="Unique view identifier")
  facts_processed: int = Field(..., description="Number of facts processed")
  construction_time_ms: float = Field(
    ..., description="Time to build view in milliseconds"
  )
  source: str = Field(..., description="Data source type")
  period_start: str | None = Field(None, description="Period start date")
  period_end: str | None = Field(None, description="Period end date")
  truncated: bool = Field(
    default=False,
    description=(
      "True when more facts matched than `limit` allowed. The returned facts "
      "are the most recent by period; narrow the filters or raise `limit` to "
      "see the rest."
    ),
  )


class ViewResponse(BaseModel):
  """Flat, deduplicated facts plus the aspects they span.

  No server-side pivot: each fact is returned as its own record so the
  consumer can arrange cells on the full aspect signature. Summing across
  entities, units, or elements that merely share a local name is a
  presentation decision this endpoint refuses to make on the caller's behalf.
  """

  metadata: ViewMetadata = Field(..., description="View metadata")
  dimensions: list[Dimension] = Field(
    default_factory=list, description="Aspects spanned by the returned facts"
  )
  facts: list[FactRecord] = Field(
    default_factory=list, description="Deduplicated fact records"
  )
  summary: dict[str, ElementSummary] | None = Field(
    None,
    description=(
      "Per-element aggregates, only when include_summary=true. Note that "
      "`total` sums across every returned period, which is meaningful for "
      "duration facts and not for instants."
    ),
  )
