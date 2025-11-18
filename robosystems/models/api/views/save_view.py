from typing import List, Optional
from pydantic import BaseModel, Field


class SaveViewRequest(BaseModel):
  report_id: Optional[str] = Field(
    None,
    description="Existing report ID to update (if provided, deletes existing facts/structures and creates new ones)",
  )
  report_type: str = Field(
    ...,
    description="Type of report (e.g., 'Annual Report', 'Quarterly Report', '10-K')",
  )
  period_start: str = Field(..., description="Period start date (YYYY-MM-DD)")
  period_end: str = Field(..., description="Period end date (YYYY-MM-DD)")
  entity_id: Optional[str] = Field(
    None, description="Entity identifier (defaults to primary entity)"
  )
  include_presentation: bool = Field(True, description="Create presentation structures")
  include_calculation: bool = Field(True, description="Create calculation structures")


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
