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
  classification: str | None = None
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
  rule_summary: dict[str, int] | None = None


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


# ── Live financial statement (OLTP) ────────────────────────────────────────


class LiveFinancialStatementRequest(BaseModel):
  """Request for live-financial-statement (OLTP, entity graphs only)."""

  statement_type: str = Field(
    ..., description="income_statement | balance_sheet | equity_statement"
  )
  period_start: date | None = Field(
    None, description="Explicit window start. Overrides period_type/fiscal_year."
  )
  period_end: date | None = Field(
    None, description="Explicit window end. Overrides period_type/fiscal_year."
  )
  period_type: str | None = Field(
    None, description="annual | quarterly | instant (ignored when dates supplied)"
  )
  fiscal_year: int | None = Field(
    None, description="Fiscal year for annual window (anchored on FiscalCalendar)"
  )
  limit: int = Field(50, ge=1, le=1000, description="Max fact rows returned")


class LiveStatementFactRow(BaseModel):
  """A single row of an OLTP-backed ad-hoc statement."""

  qname: str
  name: str
  classification: str | None = None
  values: list[float | None]
  depth: int = 0
  is_subtotal: bool = False


class LiveFinancialStatementResponse(BaseModel):
  """Rendered OLTP-backed ad-hoc statement."""

  graph_id: str
  statement_type: str
  periods: list[PeriodSpec]
  facts: list[LiveStatementFactRow]
  fact_count: int
  unmapped_count: int
  truncated: bool = False


# ── Financial statement analysis (graph Cypher) ───────────────────────────


class FinancialStatementAnalysisRequest(BaseModel):
  """Request for financial-statement-analysis (graph-backed Cypher)."""

  statement_type: str = Field(
    ...,
    description="income_statement | balance_sheet | cash_flow_statement | equity_statement",
  )
  ticker: str | None = Field(
    None,
    description="Company ticker (required on shared-repo graphs, ignored otherwise)",
  )
  report_id: str | None = Field(
    None,
    description="Specific report identifier. If omitted, auto-resolves latest by ticker + filters.",
  )
  fiscal_year: int | None = Field(
    None, description="Filter by fiscal year focus when auto-resolving the report"
  )
  period_type: str | None = Field(None, description="annual | quarterly | instant")
  limit: int = Field(50, ge=1, le=1000)


class ResolvedReportInfo(BaseModel):
  """Information about the auto-resolved report."""

  report_id: str
  form: str | None = None
  filing_date: str | None = None
  fiscal_year: int | None = None
  fiscal_period: str | None = None


class AnalyticalStatementFactRow(BaseModel):
  """A single fact row from the graph-backed statement analysis."""

  canonical_concept: str | None = None
  qname: str
  name: str
  value: float | None = None
  end_date: str | None = None
  period_type: str | None = None
  duration_type: str | None = None


class FinancialStatementAnalysisResponse(BaseModel):
  """Results of the financial-statement-analysis view op."""

  graph_id: str
  statement_type: str
  ticker: str | None = None
  report_id: str | None = None
  resolved_report: ResolvedReportInfo | None = None
  facts: list[AnalyticalStatementFactRow]
  fact_count: int
