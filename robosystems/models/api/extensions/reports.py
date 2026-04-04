"""Report request and response models."""

from datetime import date, datetime

from pydantic import BaseModel, Field

# ── Period specification ───────────────────────────────────��──────────────


class PeriodSpec(BaseModel):
  """A reporting period column."""

  start: date
  end: date
  label: str


# ── Requests ───────────────────────────────────────────────────────────────


class CreateReportRequest(BaseModel):
  name: str = Field(..., description="Report name")
  taxonomy_id: str = Field(
    "tax_usgaap_reporting",
    description="Taxonomy ID — determines which structures are available",
  )
  mapping_id: str = Field(..., description="Mapping structure ID for CoA→GAAP rollup")
  period_start: date = Field(..., description="Period start date (inclusive)")
  period_end: date = Field(..., description="Period end date (inclusive)")
  period_type: str = Field(
    "quarterly", description="Period type: monthly, quarterly, annual"
  )
  comparative: bool = Field(True, description="Include prior period comparison")
  periods: list[PeriodSpec] | None = Field(
    None,
    description="Multi-period columns. Overrides period_start/period_end/comparative when set.",
  )


class RegenerateReportRequest(BaseModel):
  period_start: date | None = Field(None, description="New period start date")
  period_end: date | None = Field(None, description="New period end date")
  periods: list[PeriodSpec] | None = Field(
    None, description="New period columns. Overrides period_start/period_end."
  )


class ShareReportRequest(BaseModel):
  publish_list_id: str = Field(..., description="Publish list to share the report to")


# ── Responses ──────────────────────────────────────────────────────────────


class FactRowResponse(BaseModel):
  element_id: str
  element_qname: str
  element_name: str
  classification: str
  values: list[float | None] = Field(default_factory=list)
  is_subtotal: bool = False
  depth: int = 0


class ValidationCheckResponse(BaseModel):
  passed: bool
  checks: list[str]
  failures: list[str]
  warnings: list[str]


class StructureSummary(BaseModel):
  """A structure available within this report's taxonomy."""

  id: str
  name: str
  structure_type: str


class ReportResponse(BaseModel):
  """Report definition summary."""

  id: str
  name: str
  taxonomy_id: str
  generation_status: str
  period_type: str
  period_start: date | None = None
  period_end: date | None = None
  comparative: bool
  periods: list[PeriodSpec] | None = None
  mapping_id: str | None = None
  ai_generated: bool = False
  created_at: datetime
  last_generated: datetime | None = None
  structures: list[StructureSummary] = Field(default_factory=list)
  # Entity context
  entity_name: str | None = None
  # Sharing provenance (populated for received reports)
  source_graph_id: str | None = None
  source_report_id: str | None = None
  shared_at: datetime | None = None


class ShareResultItem(BaseModel):
  target_graph_id: str
  status: str  # "shared" or "error"
  error: str | None = None
  fact_count: int = 0


class ShareReportResponse(BaseModel):
  report_id: str
  results: list[ShareResultItem]


class StatementResponse(BaseModel):
  """Rendered financial statement — facts viewed through a structure."""

  report_id: str
  structure_id: str
  structure_name: str
  structure_type: str
  periods: list[PeriodSpec] = Field(default_factory=list)
  rows: list[FactRowResponse] = Field(default_factory=list)
  validation: ValidationCheckResponse | None = None
  unmapped_count: int = 0


class ReportListResponse(BaseModel):
  reports: list[ReportResponse]
