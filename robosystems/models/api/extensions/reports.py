"""Report request and response models."""

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

# ── Period specification ─────────────────────────────────────────────────────


class PeriodSpec(BaseModel):
  """A single reporting period column.

  Reports render facts in N period columns side-by-side. Each
  ``PeriodSpec`` is one column — its ``start``/``end`` define the
  window the report's facts roll up into; ``label`` is what the renderer
  prints in the column header. For year-over-year statements, supply two
  PeriodSpecs (current + comparative); for YTD by quarter, supply four.
  """

  start: date = Field(
    ..., description="Period start date (inclusive). Window the column rolls up."
  )
  end: date = Field(
    ..., description="Period end date (inclusive). Window the column rolls up."
  )
  label: str = Field(
    ...,
    description="Column header label (e.g. 'FY2025 Q3', '2024', 'YTD').",
  )


# ── Requests ─────────────────────────────────────────────────────────────────


class CreateReportRequest(BaseModel):
  """Generate report facts from the ledger and publish a Report definition.

  The report is materialized synchronously: we resolve the taxonomy +
  CoA mapping, roll up GL facts into reportable concepts, attach them
  to a fresh ``Report`` row, evaluate any reporting-rule structures
  (cell-level checks), and stamp ``generation_status='published'``.
  Subsequent ``regenerate-report`` calls re-run the same pipeline against
  the latest ledger state without creating a new Report row.

  ``period_start``/``period_end``/``comparative`` is the simple path
  (auto-derives current + prior period). For multi-column reports
  (YTD-by-quarter, multi-year) supply ``periods`` explicitly — when
  set, ``period_start``/``period_end``/``comparative`` are ignored as
  inputs to period generation.
  """

  name: str = Field(
    ...,
    description="Human-readable report name shown in lists and headers.",
  )
  taxonomy_id: str = Field(
    "rs-gaap",
    description=(
      "Taxonomy that defines the structures (BS / IS / CF / Equity / "
      "Schedules) this report can render. Accepts either an exact "
      "tenant-specific taxonomy UUID or a standard name (e.g. 'rs-gaap'). "
      "Standard names resolve to the latest reporting_standard taxonomy "
      "with that name. Defaults to 'rs-gaap', the canonical reporting "
      "vocabulary."
    ),
  )
  mapping_id: str = Field(
    ...,
    description=(
      "Mapping structure that rolls up the tenant's chart of accounts "
      "to the taxonomy's reporting concepts. Created via "
      "`create-mapping-association` / `auto-map-elements`."
    ),
  )
  period_start: date = Field(
    ...,
    description=(
      "Current-period start (inclusive). Ignored when `periods` is supplied."
    ),
  )
  period_end: date = Field(
    ...,
    description=(
      "Current-period end (inclusive). Must be >= `period_start`. "
      "Ignored when `periods` is supplied."
    ),
  )
  period_type: str = Field(
    "quarterly",
    description="Period cadence: `monthly`, `quarterly`, or `annual`.",
  )
  comparative: bool = Field(
    True,
    description=(
      "When true, automatically generates a prior-period column "
      "(same length as the current period). Ignored when `periods` is "
      "supplied."
    ),
  )
  periods: list[PeriodSpec] | None = Field(
    None,
    description=(
      "Explicit period columns. When set, overrides "
      "`period_start`/`period_end`/`comparative`. Use for multi-period "
      "layouts (YTD-by-quarter, multi-year, custom rolling windows)."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "name": "Q3 2026 Financials",
          "taxonomy_id": "tax_usgaap_reporting",
          "mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0",
          "period_start": "2026-07-01",
          "period_end": "2026-09-30",
          "period_type": "quarterly",
          "comparative": True,
        },
        {
          "name": "FY2025 Annual Report",
          "taxonomy_id": "tax_usgaap_reporting",
          "mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0",
          "period_start": "2025-01-01",
          "period_end": "2025-12-31",
          "period_type": "annual",
          "comparative": True,
        },
        {
          "name": "FY2026 YTD by Quarter",
          "taxonomy_id": "tax_usgaap_reporting",
          "mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0",
          "period_start": "2026-01-01",
          "period_end": "2026-09-30",
          "period_type": "quarterly",
          "comparative": False,
          "periods": [
            {"start": "2026-01-01", "end": "2026-03-31", "label": "Q1 2026"},
            {"start": "2026-04-01", "end": "2026-06-30", "label": "Q2 2026"},
            {"start": "2026-07-01", "end": "2026-09-30", "label": "Q3 2026"},
          ],
        },
      ]
    }
  )


class RegenerateReportRequest(BaseModel):
  """Regenerate facts for an existing Report against the latest ledger.

  Same pipeline as `create-report` but re-uses the existing Report row.
  All previously persisted facts for the report are dropped and replaced
  — rule evaluation re-runs and ``last_generated`` is stamped. Useful
  after correcting GL data or adjusting the mapping. To change the
  reporting window, pass any of ``period_start`` / ``period_end`` /
  ``periods``; omit them to regenerate against the same window stored on
  the report.
  """

  period_start: date | None = Field(
    None,
    description=(
      "New current-period start. Optional — omit to keep the existing window."
    ),
  )
  period_end: date | None = Field(
    None,
    description=(
      "New current-period end. Optional — omit to keep the existing window."
    ),
  )
  periods: list[PeriodSpec] | None = Field(
    None,
    description=(
      "New explicit period columns. Overrides "
      "`period_start`/`period_end` when supplied."
    ),
  )


class ShareReportRequest(BaseModel):
  """Push a published report to every member of a publish list.

  Only reports with ``generation_status='published'`` are shareable.
  Each share is an independent copy: the report row + all its facts are
  cloned into the recipient's tenant schema with ``source_graph_id`` /
  ``source_report_id`` provenance fields populated. Per-target results
  surface in the response (``ShareReportResponse``).
  """

  publish_list_id: str = Field(
    ...,
    description=(
      "Publish list whose members will receive the report. Created "
      "via `create-publish-list`."
    ),
  )


class RevokeReportShareRequest(BaseModel):
  """Withdraw a report previously shared to one recipient graph.

  The sender's half of the share controls: deletes the copy from that
  recipient's tenant schema and stamps the share record ``revoked_at``. Scoped
  to a single target — revoking a distribution to a whole publish list means
  one call per member, so a withdrawal is always a deliberate act.
  """

  target_graph_id: str = Field(
    ...,
    description="Recipient graph whose copy should be withdrawn.",
  )


# ── Responses ────────────────────────────────────────────────────────────────


class FactRowResponse(BaseModel):
  """A single fact row inside a rendered statement.

  One row per concept, with one value per period column. Subtotals and
  hierarchy depth come from the structure being projected.
  """

  element_id: str = Field(..., description="Internal element identifier.")
  element_qname: str = Field(
    ...,
    description="QName of the reporting concept (e.g. 'us-gaap:Revenues').",
  )
  element_name: str = Field(..., description="Human-readable concept label.")
  trait: str | None = Field(
    None,
    description=(
      "Concept trait flag from the structure (e.g. 'total', "
      "'subtotal', 'header'). Drives presentation."
    ),
  )
  values: list[float | None] = Field(
    default_factory=list,
    description=(
      "One value per period column, in the same order as `periods`. "
      "Null when the concept had no facts in that window."
    ),
  )
  is_subtotal: bool = Field(
    False, description="True when the row should render as a subtotal line."
  )
  depth: int = Field(
    0,
    description="Indentation depth in the structure hierarchy (0 = root).",
  )


class ValidationCheckResponse(BaseModel):
  """Aggregate result of running reporting rules over a structure."""

  passed: bool = Field(..., description="True iff every rule produced zero failures.")
  checks: list[str] = Field(
    ...,
    description="Names of rules that were evaluated.",
  )
  failures: list[str] = Field(
    ...,
    description="Human-readable descriptions of rule failures.",
  )
  warnings: list[str] = Field(
    ...,
    description="Non-blocking advisories from rule evaluation.",
  )


class StructureSummary(BaseModel):
  """A structure available within this report's taxonomy.

  Each structure is a renderable section (Balance Sheet, Income
  Statement, Cash Flow Statement, Equity, or a Schedule). The Report
  row owns the facts; structures are the lenses that project them.
  """

  id: str = Field(..., description="Structure identifier.")
  name: str = Field(..., description="Human-readable structure name.")
  block_type: str = Field(
    ...,
    description=(
      "Structure category: `balance_sheet`, `income_statement`, "
      "`cash_flow_statement`, `equity_statement`, `schedule`."
    ),
  )


class ReportResponse(BaseModel):
  """Report definition summary — header metadata, no facts.

  Returned by ``create-report``, ``regenerate-report``,
  ``file-report``, and ``transition-filing-status``. Use the package
  read endpoint to retrieve a Report rehydrated with its rendered
  ``InformationBlockEnvelope`` items.
  """

  id: str = Field(..., description="Report identifier (ULID).")
  name: str = Field(..., description="Human-readable report name.")
  taxonomy_id: str = Field(..., description="Taxonomy this report renders against.")
  generation_status: str = Field(
    ...,
    description=(
      "Computation lifecycle: `generating`, `published`, `failed`. "
      "Orthogonal to `filing_status`."
    ),
  )
  period_type: str = Field(
    ..., description="Period cadence: `monthly`, `quarterly`, `annual`."
  )
  period_start: date | None = Field(None, description="Current-period start.")
  period_end: date | None = Field(None, description="Current-period end.")
  comparative: bool = Field(
    ..., description="True when an auto-generated prior-period column is included."
  )
  periods: list[PeriodSpec] | None = Field(
    None,
    description=(
      "Explicit period columns when the report was created with a multi-period layout."
    ),
  )
  mapping_id: str | None = Field(
    None, description="CoA → taxonomy mapping the facts were rolled up through."
  )
  ai_generated: bool = Field(
    False,
    description="True when the report was created by an AI agent rather than a user.",
  )
  created_at: datetime = Field(..., description="When the report row was created.")
  last_generated: datetime | None = Field(
    None, description="When the facts were last (re)generated."
  )
  structures: list[StructureSummary] = Field(
    default_factory=list,
    description=(
      "Structures available for this report's taxonomy — renderable "
      "sections (BS / IS / CF / Equity / Schedules)."
    ),
  )
  entity_name: str | None = Field(
    None,
    description="Display name of the primary entity the report is tagged to.",
  )
  filing_status: str = Field(
    "draft",
    description=(
      "Filing lifecycle (orthogonal to `generation_status`): `draft`, "
      "`under_review`, `filed`, `archived`."
    ),
  )
  filed_at: datetime | None = Field(
    None, description="When the report was transitioned to `filed`."
  )
  filed_by: str | None = Field(
    None, description="User ID that transitioned the report to `filed`."
  )
  supersedes_id: str | None = Field(
    None,
    description=(
      "When this report restates an earlier filing, the predecessor's report ID."
    ),
  )
  superseded_by_id: str | None = Field(
    None,
    description=("When this report has been restated, the successor's report ID."),
  )
  source_graph_id: str | None = Field(
    None,
    description=(
      "Origin graph for received (shared) reports — populated only on "
      "the recipient's copy."
    ),
  )
  source_report_id: str | None = Field(
    None,
    description=(
      "Origin report ID for received (shared) reports — populated "
      "only on the recipient's copy."
    ),
  )
  shared_at: datetime | None = Field(
    None, description="When the report was shared into this graph (recipient side)."
  )
  rule_summary: dict[str, int] | None = Field(
    None,
    description=(
      "Counts by rule outcome (e.g. `{'passed': 12, 'failed': 1}`) "
      "from the most recent evaluation. Null until rules run."
    ),
  )


class ShareResultItem(BaseModel):
  """Per-target outcome of a `share-report` call."""

  target_graph_id: str = Field(
    ..., description="Recipient graph ID (a publish-list member)."
  )
  status: str = Field(
    ...,
    description=(
      "`shared` when the copy succeeded; `error` when this target "
      "failed (other targets may still have succeeded)."
    ),
  )
  error: str | None = Field(
    None, description="Error message when `status='error'`, else null."
  )
  fact_count: int = Field(
    0, description="Number of facts copied into the target on success."
  )


class ShareReportResponse(BaseModel):
  """Result of sharing a report to a publish list — one row per target."""

  report_id: str = Field(..., description="The report that was shared.")
  results: list[ShareResultItem] = Field(
    ...,
    description=(
      "Per-recipient outcomes. Inspect to find partial failures — the "
      "share operation does not fail-fast across targets."
    ),
  )


class RevokeReportShareResponse(BaseModel):
  """Outcome of withdrawing a shared report from one recipient."""

  report_id: str = Field(..., description="The report whose share was revoked.")
  target_graph_id: str = Field(..., description="Recipient the copy was pulled from.")
  revoked_at: datetime = Field(..., description="When the share was revoked.")
  copy_deleted: bool = Field(
    ...,
    description=(
      "True when a copy was found and deleted in the recipient's schema. "
      "False when the recipient had already deleted it themselves — the "
      "share is still marked revoked."
    ),
  )


class StatementResponse(BaseModel):
  """Rendered financial statement — facts viewed through a structure.

  Returned by report read endpoints when a single statement is
  requested. The package mode endpoint returns a list of these
  rehydrated as ``InformationBlockEnvelope`` items instead.
  """

  report_id: str = Field(
    ..., description="The Report this statement was rendered from."
  )
  structure_id: str = Field(..., description="Structure projected for this statement.")
  structure_name: str = Field(..., description="Human-readable structure name.")
  block_type: str = Field(
    ...,
    description=(
      "Structure category: `balance_sheet`, `income_statement`, "
      "`cash_flow_statement`, `equity_statement`, `schedule`."
    ),
  )
  periods: list[PeriodSpec] = Field(
    default_factory=list,
    description="Period columns rendered in this statement, in display order.",
  )
  rows: list[FactRowResponse] = Field(
    default_factory=list,
    description="One row per concept in the structure, in tree order.",
  )
  validation: ValidationCheckResponse | None = Field(
    None,
    description=(
      "Outcome of running reporting rules over this structure. Null "
      "when the structure has no rules attached."
    ),
  )
  unmapped_count: int = Field(
    0,
    description=(
      "Number of GL elements that fell through the mapping with no "
      "destination concept. Indicates mapping gaps."
    ),
  )


class ReportListResponse(BaseModel):
  """List of report header summaries (used by report list reads)."""

  reports: list[ReportResponse] = Field(
    ..., description="Report definitions, newest first."
  )


# ── Serialization bundle download ────────────────────────────────────────────


class ReportBundleDownloadResponse(BaseModel):
  """Presigned-URL response for a published Report's serialization bundle.

  Every flavor resolves to a short-lived presigned URL pointing at the
  bundle in S3 — JSON-LD is stamped at publish time, XBRL is
  materialized on first download and cached by ``generation_count``.
  The client follows ``download_url`` to fetch the artifact directly
  from S3 (the API never streams the bytes). Mirrors the
  backup-download shape the frontend already consumes.
  """

  download_url: str = Field(
    ..., description="Presigned URL that streams the bundle directly from S3."
  )
  expires_at: datetime = Field(
    ..., description="UTC timestamp at which the presigned URL stops working."
  )
  content_type: str = Field(
    ..., description="MIME type of the artifact behind the URL."
  )
  format: str = Field(
    ...,
    description=(
      "Serialization flavor delivered by this URL — one of the "
      "``RdfFlavor`` / ``XbrlFlavor`` values (e.g. ``jsonld``, ``xbrl-2.1``)."
    ),
  )
  generation_count: int = Field(
    ..., description="Bundle generation number stamped on the Report."
  )


# ── Live financial statement (OLTP) ──────────────────────────────────────────


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
  trait: str | None = None
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
  unmapped_count: int = 0
  truncated: bool = False


# ── Financial statement analysis (graph Cypher) ─────────────────────────────


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
