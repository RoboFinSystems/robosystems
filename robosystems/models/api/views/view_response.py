from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PivotTablePresentation(BaseModel):
  index: List[List[Any]] = Field(..., description="Row index (hierarchical)")
  columns: List[List[Any]] = Field(..., description="Column headers (hierarchical)")
  data: List[List[Any]] = Field(..., description="Data values")
  metadata: Dict[str, Any] = Field(
    default_factory=dict, description="Additional metadata"
  )


class ViewMetadata(BaseModel):
  view_id: str = Field(..., description="Unique view identifier")
  facts_processed: int = Field(..., description="Number of facts processed")
  construction_time_ms: float = Field(
    ..., description="Time to build view in milliseconds"
  )
  source: str = Field(..., description="Data source type")
  period_start: Optional[str] = Field(None, description="Period start date")
  period_end: Optional[str] = Field(None, description="Period end date")


class ViewResponse(BaseModel):
  metadata: ViewMetadata = Field(..., description="View metadata")
  presentations: Dict[str, Any] = Field(
    ..., description="Presentation formats (pivot_table, narrative, etc.)"
  )
