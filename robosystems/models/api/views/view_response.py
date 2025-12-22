from typing import Any

from pydantic import BaseModel, Field


class PivotTablePresentation(BaseModel):
  index: list[list[Any]] = Field(..., description="Row index (hierarchical)")
  columns: list[list[Any]] = Field(..., description="Column headers (hierarchical)")
  data: list[list[Any]] = Field(..., description="Data values")
  metadata: dict[str, Any] = Field(
    default_factory=dict, description="Additional metadata"
  )


class ViewMetadata(BaseModel):
  view_id: str = Field(..., description="Unique view identifier")
  facts_processed: int = Field(..., description="Number of facts processed")
  construction_time_ms: float = Field(
    ..., description="Time to build view in milliseconds"
  )
  source: str = Field(..., description="Data source type")
  period_start: str | None = Field(None, description="Period start date")
  period_end: str | None = Field(None, description="Period end date")


class ViewResponse(BaseModel):
  metadata: ViewMetadata = Field(..., description="View metadata")
  presentations: dict[str, Any] = Field(
    ..., description="Presentation formats (pivot_table, narrative, etc.)"
  )
