"""Report request and response models."""

from datetime import date, datetime

from pydantic import BaseModel, Field

# ── Requests ───────────────────────────────────────────────────────────────


class CreateReportRequest(BaseModel):
  name: str = Field(..., description="Report name")
  report_type: str = Field(
    ...,
    description="Report type: income_statement, balance_sheet, cash_flow",
  )
  mapping_id: str = Field(..., description="Mapping structure ID for CoA→GAAP rollup")
  period_start: date = Field(..., description="Period start date (inclusive)")
  period_end: date = Field(..., description="Period end date (inclusive)")
  period_type: str = Field(
    "quarterly", description="Period type: monthly, quarterly, annual"
  )
  comparative: bool = Field(True, description="Include prior period comparison")


class RegenerateReportRequest(BaseModel):
  period_start: date = Field(..., description="New period start date")
  period_end: date = Field(..., description="New period end date")


# ── Responses ──────────────────────────────────────────────────────────────


class FactRowResponse(BaseModel):
  element_id: str
  element_qname: str
  element_name: str
  classification: str
  current_value: float
  prior_value: float | None = None
  is_subtotal: bool = False
  depth: int = 0


class ValidationCheckResponse(BaseModel):
  passed: bool
  checks: list[str]
  failures: list[str]
  warnings: list[str]


class ReportResponse(BaseModel):
  """Report definition summary (for list endpoints)."""

  id: str
  name: str
  report_type: str
  generation_status: str
  period_type: str
  comparative: bool
  mapping_id: str | None = None
  period_start: date | None = None
  period_end: date | None = None
  ai_generated: bool = False
  created_at: datetime
  last_generated: datetime | None = None


class ReportWithDataResponse(ReportResponse):
  """Report definition + generated fact grid data."""

  structure_id: str | None = None
  structure_name: str | None = None
  comparative_period_start: date | None = None
  comparative_period_end: date | None = None
  rows: list[FactRowResponse] = Field(default_factory=list)
  validation: ValidationCheckResponse | None = None
  unmapped_count: int = 0


class ReportListResponse(BaseModel):
  reports: list[ReportResponse]
