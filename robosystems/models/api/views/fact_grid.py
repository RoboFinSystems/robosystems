from enum import Enum
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field


class DimensionType(str, Enum):
  ELEMENT = "element"
  PERIOD = "period"
  ENTITY = "entity"
  DIMENSION_AXIS = "dimension_axis"


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
  dimensions: list[Dimension] = Field(..., description="Dimensions in the grid")
  facts_df: Any | None = Field(
    None,
    description="Pandas DataFrame with fact data (not serialized, internal use only)",
    exclude=True,
  )
  metadata: FactGridMetadata = Field(..., description="Metadata about the grid")

  class Config:
    arbitrary_types_allowed = True

  def as_pivot_table(self, config: dict[str, Any] | None = None) -> pd.DataFrame:
    if self.facts_df is None:
      return pd.DataFrame()

    if config is None:
      return self.facts_df

    return self.facts_df
