from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


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

  dimension_axis: Optional[str] = Field(
    default=None, description="Dimension axis name for dimension-type axes"
  )
  include_null_dimension: bool = Field(
    default=False,
    description="Include facts where this dimension is NULL (default: false)",
  )

  selected_members: Optional[List[str]] = Field(
    default=None,
    description="Specific members to include (e.g., ['2024-12-31', '2023-12-31'])",
  )
  member_order: Optional[List[str]] = Field(
    default=None, description="Explicit ordering of members (overrides default sort)"
  )
  member_labels: Optional[Dict[str, str]] = Field(
    default=None,
    description="Custom labels for members (e.g., {'2024-12-31': 'Current Year'})",
  )

  element_order: Optional[List[str]] = Field(
    default=None,
    description="Element ordering for hierarchy display (e.g., ['us-gaap:Assets', 'us-gaap:Cash', ...])",
  )
  element_labels: Optional[Dict[str, str]] = Field(
    default=None,
    description="Custom labels for elements (e.g., {'us-gaap:Cash': 'Cash and Cash Equivalents'})",
  )

  @field_validator("type")
  @classmethod
  def validate_axis_type(cls, v: str) -> str:
    allowed = ["element", "period", "dimension", "entity"]
    if v not in allowed:
      raise ValueError(f"Axis type must be one of {allowed}, got: {v}")
    return v


class ViewConfig(BaseModel):
  rows: List[ViewAxisConfig] = Field(
    default_factory=list, description="Row axis configuration"
  )
  columns: List[ViewAxisConfig] = Field(
    default_factory=list, description="Column axis configuration"
  )
  values: str = Field(
    default="numeric_value",
    description="Field to use for values (default: numeric_value)",
  )
  aggregation_function: str = Field(
    default="sum", description="Aggregation function: sum, average, count"
  )
  fill_value: float = Field(default=0.0, description="Value to use for missing data")


class CreateViewRequest(BaseModel):
  name: Optional[str] = Field(None, description="Optional name for the view")
  source: ViewSource = Field(..., description="Data source configuration")
  view_config: ViewConfig = Field(
    default_factory=ViewConfig, description="View configuration"
  )
  presentation_formats: List[str] = Field(
    default=["pivot_table"], description="Presentation formats to generate"
  )
  mapping_structure_id: Optional[str] = Field(
    default=None,
    description="Optional mapping structure ID to aggregate Chart of Accounts elements into reporting taxonomy elements",
  )
