from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DimensionType(str, Enum):
  ELEMENT = "element"
  PERIOD = "period"
  ENTITY = "entity"


class Dimension(BaseModel):
  name: str = Field(..., description="Dimension name (e.g., 'Element', 'Period')")
  type: DimensionType = Field(..., description="Dimension type")
  members: list[str] = Field(
    default_factory=list, description="List of dimension members"
  )

  class Config:
    use_enum_values = True


class FactGridMetadata(BaseModel):
  fact_count: int = Field(..., description="Number of facts in grid")
  dimension_count: int = Field(..., description="Number of dimensions")
  construction_time_ms: float = Field(
    ..., description="Time to build FactGrid in milliseconds"
  )
  source: str = Field(
    ...,
    description="Source of facts (e.g., 'trial_balance_aggregation', 'fact_set_query')",
  )
  lineage: dict[str, Any] | None = Field(
    None, description="Lineage information for traceability"
  )


class FactGrid(BaseModel):
  """A scoped, deduplicated set of facts plus the aspects they span.

  Deliberately *not* a pivot: cells are not collapsed and nothing is
  aggregated. Arranging facts into a table is the consumer's job — see
  ``@robosystems/report-components``, whose pivot engine keys cells on a
  fact's full aspect signature rather than on ``(element, period)``.
  """

  dimensions: list[Dimension] = Field(..., description="Dimensions in the grid")
  facts: list[dict[str, Any]] = Field(
    default_factory=list, description="Deduplicated fact records"
  )
  metadata: FactGridMetadata = Field(..., description="Metadata about the grid")
