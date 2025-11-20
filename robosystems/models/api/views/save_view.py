from typing import List, Optional
from pydantic import BaseModel, Field, field_validator


class SaveViewRequest(BaseModel):
  report_id: Optional[str] = Field(
    default=None,
    description="Existing report ID to update (if provided, deletes existing facts/structures and creates new ones)",
    max_length=100,
    pattern=r"^[a-zA-Z0-9_-]+$",
  )
  report_type: str = Field(
    description="Type of report (e.g., 'Annual Report', 'Quarterly Report', '10-K')",
    max_length=200,
  )
  period_start: str = Field(
    description="Period start date (YYYY-MM-DD)",
    pattern=r"^\d{4}-\d{2}-\d{2}$",
  )
  period_end: str = Field(
    description="Period end date (YYYY-MM-DD)",
    pattern=r"^\d{4}-\d{2}-\d{2}$",
  )
  entity_id: Optional[str] = Field(
    default=None,
    description="Entity identifier (defaults to primary entity)",
    max_length=100,
    pattern=r"^[a-zA-Z0-9_-]+$",
  )
  include_presentation: bool = Field(
    default=True, description="Create presentation structures"
  )
  include_calculation: bool = Field(
    default=True, description="Create calculation structures"
  )

  @field_validator("report_type")
  @classmethod
  def validate_report_type(cls, v: str) -> str:
    if "\n" in v or "\r" in v:
      raise ValueError("Report type cannot contain newline characters")
    if len(v.strip()) == 0:
      raise ValueError("Report type cannot be empty or whitespace only")
    return v.strip()

  @field_validator("period_start", "period_end")
  @classmethod
  def validate_date_format(cls, v: str) -> str:
    try:
      from datetime import datetime

      datetime.strptime(v, "%Y-%m-%d")
    except ValueError as e:
      raise ValueError(f"Date must be in YYYY-MM-DD format: {e}")
    return v


class FactDetail(BaseModel):
  fact_id: str
  element_uri: str
  element_name: str
  numeric_value: float
  unit: str
  period_start: str
  period_end: str


class StructureDetail(BaseModel):
  structure_id: str
  structure_type: str
  name: str
  element_count: int


class SaveViewResponse(BaseModel):
  report_id: str = Field(
    ..., description="Unique report identifier (used as parquet export prefix)"
  )
  report_type: str
  entity_id: str
  entity_name: str
  period_start: str
  period_end: str
  fact_count: int
  presentation_count: int
  calculation_count: int
  facts: List[FactDetail]
  structures: List[StructureDetail]
  created_at: str
  parquet_export_prefix: str = Field(..., description="Prefix for parquet file exports")
