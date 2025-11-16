from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class ViewSourceType(str, Enum):
  TRANSACTIONS = "transactions"
  FACT_SET = "fact_set"


class ViewSource(BaseModel):
  type: ViewSourceType = Field(..., description="Type of data source")
  period_start: Optional[str] = Field(
    None, description="Start date for transaction aggregation (YYYY-MM-DD)"
  )
  period_end: Optional[str] = Field(
    None, description="End date for transaction aggregation (YYYY-MM-DD)"
  )
  fact_set_id: Optional[str] = Field(
    None, description="FactSet ID for existing facts mode"
  )
  entity_id: Optional[str] = Field(None, description="Filter by entity (optional)")

  class Config:
    use_enum_values = True


class ViewAxisConfig(BaseModel):
  type: str = Field(
    ..., description="Axis type: 'element', 'period', 'dimension', 'entity'"
  )
  hierarchy_root: Optional[str] = Field(
    None, description="Root element for hierarchy (e.g., 'us-gaap:Assets')"
  )
  include_subtotals: bool = Field(False, description="Include subtotals in hierarchy")
  dimension_axis: Optional[str] = Field(
    None, description="Dimension axis name for dimension-type axes"
  )


class ViewConfig(BaseModel):
  rows: List[ViewAxisConfig] = Field(
    default_factory=list, description="Row axis configuration"
  )
  columns: List[ViewAxisConfig] = Field(
    default_factory=list, description="Column axis configuration"
  )
  values: str = Field(
    "numeric_value", description="Field to use for values (default: numeric_value)"
  )
  aggregation_function: str = Field(
    "sum", description="Aggregation function: sum, average, count"
  )
  fill_value: float = Field(0.0, description="Value to use for missing data")


class CreateViewRequest(BaseModel):
  name: Optional[str] = Field(None, description="Optional name for the view")
  source: ViewSource = Field(..., description="Data source configuration")
  view_config: ViewConfig = Field(
    default_factory=ViewConfig, description="View configuration"
  )
  presentation_formats: List[str] = Field(
    default=["pivot_table"], description="Presentation formats to generate"
  )
