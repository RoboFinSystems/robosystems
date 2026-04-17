"""Read operations for report definitions and rendered statements.

Ported from `routers/ledger/reports.py` with the helper functions
(`_build_periods`, `_load_structures`, `_resolve_entity_name`,
`_report_to_response`) made module-level so both the REST router and
the GraphQL resolver can call them.
"""

from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.reports import (
  FactRowResponse,
  LiveFinancialStatementResponse,
  LiveStatementFactRow,
  PeriodSpec,
  ReportListResponse,
  ReportResponse,
  StatementResponse,
  StructureSummary,
  ValidationCheckResponse,
)
from robosystems.models.extensions import Report
from robosystems.models.extensions.roboledger import Structure
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  get_fiscal_year_start_month,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec as FactPeriodSpec,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  ReportFact as ReportFactData,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  _compute_prior_period,
  generate_report_facts,
  render_structure_view,
)
from robosystems.operations.roboledger.reports.guard_rails import validate_report

VALID_STRUCTURE_TYPES = {
  "income_statement",
  "balance_sheet",
  "equity_statement",
  "custom",
}

# Statement types accepted by the live (OLTP) path — cash_flow_statement is
# not yet supported on OLTP (no generator). Shared across REST router and
# MCP tool so they stay in sync.
LIVE_STATEMENT_TYPES: tuple[str, ...] = (
  "income_statement",
  "balance_sheet",
  "equity_statement",
)

# Statement types accepted by the graph-backed analysis path. The graph
# hypercube carries cash-flow facts from XBRL filings, so it's valid here.
ANALYSIS_STATEMENT_TYPES: tuple[str, ...] = (
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
)


class StatementStructureNotFoundError(LookupError):
  """Raised when a statement's structure_type isn't in the report's taxonomy."""


class CoaMappingNotFoundError(LookupError):
  """Raised when no CoA→GAAP mapping exists for ad-hoc statement generation."""


def generate_adhoc_private_statement(
  session: Session,
  *,
  statement_type: str,
  periods: list[FactPeriodSpec],
  taxonomy_id: str = "tax_usgaap_reporting",
):
  """Generate an ad-hoc private-company statement directly from OLTP data.

  Unlike `get_statement`, which renders a previously-saved Report, this
  helper builds a one-shot statement from the current ledger using the
  active CoA→GAAP mapping. Used by the `live-financial-statement`
  operation (both REST and MCP) — no saved Report needed.

  Returns the rendered structure grid plus an `unmapped_count` counter.
  Raises `CoaMappingNotFoundError` if the tenant hasn't completed the
  mapping workflow yet — the caller translates to a user-facing tip.
  """
  mapping = (
    session.query(Structure).filter(Structure.structure_type == "coa_mapping").first()
  )
  if mapping is None:
    raise CoaMappingNotFoundError(
      "No CoA→GAAP mapping found. Run the mapping workflow first."
    )

  facts = generate_report_facts(
    session=session,
    taxonomy_id=taxonomy_id,
    mapping_id=mapping.id,
    periods=periods,
  )

  grid = render_structure_view(
    session=session,
    facts=facts.facts,
    structure_type=statement_type,
    periods=periods,
  )

  return grid, facts.unmapped_count


def build_periods(
  period_start: date | None,
  period_end: date | None,
  comparative: bool,
  periods_json: list | None = None,
) -> list[FactPeriodSpec]:
  """Build period specs from request data.

  If `periods_json` is provided (multi-period mode), use it directly.
  Otherwise, build from period_start/period_end/comparative (legacy mode).
  """
  if periods_json:
    result = []
    for p in periods_json:
      if isinstance(p, dict):
        # JSONB returns dates as strings — parse them
        start = (
          date.fromisoformat(p["start"]) if isinstance(p["start"], str) else p["start"]
        )
        end = date.fromisoformat(p["end"]) if isinstance(p["end"], str) else p["end"]
        result.append(FactPeriodSpec(start=start, end=end, label=p["label"]))
      else:
        result.append(FactPeriodSpec(start=p.start, end=p.end, label=p.label))
    return result

  if period_start is None or period_end is None:
    return []

  specs = [FactPeriodSpec(start=period_start, end=period_end, label="Current")]
  if comparative:
    prior_start, prior_end = _compute_prior_period(period_start, period_end)
    specs.append(FactPeriodSpec(start=prior_start, end=prior_end, label="Prior"))
  return specs


def periods_to_json(periods: list[FactPeriodSpec]) -> list[dict]:
  """Serialize period specs for JSONB storage."""
  return [{"start": str(p.start), "end": str(p.end), "label": p.label} for p in periods]


def load_structures(session: Session, taxonomy_id: str) -> list[StructureSummary]:
  """Load available (statement-ish) structures for a taxonomy."""
  result = session.execute(
    text("""
      SELECT id, name, structure_type FROM structures
      WHERE taxonomy_id = :taxonomy_id
        AND structure_type NOT IN ('chart_of_accounts', 'coa_mapping')
        AND is_active = true
      ORDER BY structure_type
    """),
    {"taxonomy_id": taxonomy_id},
  )
  return [
    StructureSummary(id=r.id, name=r.name, structure_type=r.structure_type)
    for r in result
  ]


def resolve_entity_name(session: Session, report_def: Report) -> str | None:
  """Resolve the entity name for a report.

  For shared reports: look up the linked entity by source_graph_id.
  For native reports: look up the parent entity.
  """
  if report_def.source_graph_id:
    row = session.execute(
      text(
        "SELECT name FROM entities WHERE metadata->>'source_graph_id' = :sgid LIMIT 1"
      ),
      {"sgid": report_def.source_graph_id},
    ).first()
  else:
    row = session.execute(
      text("SELECT name FROM entities WHERE is_parent = true LIMIT 1")
    ).first()
  return row.name if row else None


def report_to_response(
  report_def: Report,
  structures: list[StructureSummary],
  entity_name: str | None = None,
) -> ReportResponse:
  """Map a Report row + structures + entity_name to the wire response."""
  periods = None
  if report_def.periods:
    periods = [
      PeriodSpec(start=p["start"], end=p["end"], label=p["label"])
      for p in report_def.periods
    ]

  return ReportResponse(
    id=report_def.id,
    name=report_def.name,
    taxonomy_id=report_def.taxonomy_id,
    generation_status=report_def.generation_status,
    period_type=report_def.period_type,
    period_start=report_def.period_start,
    period_end=report_def.period_end,
    comparative=report_def.comparative,
    periods=periods,
    mapping_id=report_def.mapping_id,
    ai_generated=report_def.ai_generated,
    created_at=report_def.created_at,
    last_generated=report_def.last_generated,
    structures=structures,
    entity_name=entity_name,
    source_graph_id=report_def.source_graph_id,
    source_report_id=report_def.source_report_id,
    shared_at=report_def.shared_at,
  )


def list_reports(session: Session) -> ReportListResponse:
  """List all report definitions, most recent first."""
  rows = (
    session.execute(select(Report).order_by(Report.created_at.desc())).scalars().all()
  )

  structure_cache: dict[str, list[StructureSummary]] = {}
  reports = []
  for r in rows:
    if r.taxonomy_id not in structure_cache:
      structure_cache[r.taxonomy_id] = load_structures(session, r.taxonomy_id)
    entity_name = resolve_entity_name(session, r)
    reports.append(report_to_response(r, structure_cache[r.taxonomy_id], entity_name))

  return ReportListResponse(reports=reports)


def get_report(session: Session, report_id: str) -> ReportResponse | None:
  """Return a report definition with its structures, or None if not found."""
  report_def = session.get(Report, report_id)
  if report_def is None:
    return None
  structures = load_structures(session, report_def.taxonomy_id)
  entity_name = resolve_entity_name(session, report_def)
  return report_to_response(report_def, structures, entity_name)


def get_statement(
  session: Session, report_id: str, structure_type: str
) -> StatementResponse | None:
  """Render a financial statement for a report + structure_type.

  Returns `None` when the report itself doesn't exist. Raises
  `StatementStructureNotFoundError` when the structure_type isn't in
  the report's taxonomy. The caller translates both into HTTP 404s.
  """
  if structure_type not in VALID_STRUCTURE_TYPES:
    raise ValueError(
      f"Invalid structure_type '{structure_type}'. "
      f"Must be one of: {', '.join(sorted(VALID_STRUCTURE_TYPES))}"
    )

  report_def = session.get(Report, report_id)
  if report_def is None:
    return None

  periods = build_periods(
    report_def.period_start,
    report_def.period_end,
    report_def.comparative,
    report_def.periods,
  )

  if not periods:
    return StatementResponse(
      report_id=report_def.id,
      structure_id="",
      structure_name="",
      structure_type=structure_type,
    )

  fact_rows = session.execute(
    text("""
      SELECT rf.element_id, rf.value, rf.period_start, rf.period_end,
             rf.period_type, e.qname, e.name, e.classification, e.balance_type
      FROM facts rf
      JOIN elements e ON e.id = rf.element_id
      WHERE rf.report_id = :report_id
    """),
    {"report_id": report_id},
  )

  facts = [
    ReportFactData(
      element_id=r.element_id,
      element_qname=r.qname,
      element_name=r.name,
      classification=r.classification,
      balance_type=r.balance_type or "debit",
      value=r.value,
      period_start=r.period_start,
      period_end=r.period_end,
      period_type=r.period_type,
    )
    for r in fact_rows
  ]

  if not facts:
    return StatementResponse(
      report_id=report_def.id,
      structure_id="",
      structure_name="",
      structure_type=structure_type,
      periods=[PeriodSpec(start=p.start, end=p.end, label=p.label) for p in periods],
    )

  grid = render_structure_view(
    session=session,
    facts=facts,
    structure_type=structure_type,
    periods=periods,
  )

  if not grid.structure_id:
    raise StatementStructureNotFoundError(structure_type)

  validation = validate_report(structure_type, grid.rows)

  rows = [
    FactRowResponse(
      element_id=r.element_id,
      element_qname=r.element_qname,
      element_name=r.element_name,
      classification=r.classification,
      values=r.values,
      is_subtotal=r.is_subtotal,
      depth=r.depth,
    )
    for r in grid.rows
  ]

  validation_resp = (
    ValidationCheckResponse(
      passed=validation.passed,
      checks=validation.checks,
      failures=validation.failures,
      warnings=validation.warnings,
    )
    if validation
    else None
  )

  return StatementResponse(
    report_id=report_def.id,
    structure_id=grid.structure_id,
    structure_name=grid.structure_name,
    structure_type=structure_type,
    periods=[PeriodSpec(start=p.start, end=p.end, label=p.label) for p in grid.periods],
    rows=rows,
    validation=validation_resp,
    unmapped_count=grid.unmapped_count,
  )


# ── Live (ad-hoc OLTP) financial statement ────────────────────────────────


def _last_day_of_month(year: int, month: int) -> date:
  """Return the last calendar day of the given year/month."""
  if month == 12:
    return date(year, 12, 31)
  return date(year, month + 1, 1) - timedelta(days=1)


def resolve_reporting_window(
  session: Session,
  *,
  period_start: date | None,
  period_end: date | None,
  period_type: str | None,
  fiscal_year: int | None,
) -> tuple[date, date]:
  """Pick a reporting window from fuzzy inputs.

  Explicit `period_start` / `period_end` always win. Otherwise the
  window is derived from `period_type`:

  - ``annual`` — fiscal year aligned on the graph's
    ``fiscal_year_start_month`` (January when no calendar exists).
    ``fiscal_year`` selects which year; defaults to the current year.
  - ``quarterly`` — current calendar quarter (3 months).
  - anything else (``instant`` or unset) — current calendar month.

  Extracted from the old MCP financial-statement tool so the REST and
  MCP surfaces share identical behavior.
  """
  today = date.today()

  if period_end or period_start:
    end = period_end or _last_day_of_month(today.year, today.month)
    start = period_start or end.replace(day=1)
    return start, end

  if period_type == "annual":
    fy_start_month = get_fiscal_year_start_month(session)
    year = fiscal_year if fiscal_year is not None else today.year
    start = date(year, fy_start_month, 1)
    end_year = year if fy_start_month == 1 else year + 1
    end_month = fy_start_month - 1 if fy_start_month > 1 else 12
    end = _last_day_of_month(end_year, end_month)
    return start, end

  if period_type == "quarterly":
    quarter = (today.month - 1) // 3
    q_start_month = quarter * 3 + 1
    start = date(today.year, q_start_month, 1)
    end = _last_day_of_month(today.year, q_start_month + 2)
    return start, end

  # instant / None → current calendar month
  end = _last_day_of_month(today.year, today.month)
  start = end.replace(day=1)
  return start, end


def build_current_and_prior_periods(start: date, end: date) -> list[FactPeriodSpec]:
  """Return [current, prior] period specs of matching duration."""
  duration = (end - start).days + 1
  prior_end = start - timedelta(days=1)
  prior_start = prior_end - timedelta(days=duration - 1)
  return [
    FactPeriodSpec(start=start, end=end, label="Current"),
    FactPeriodSpec(start=prior_start, end=prior_end, label="Prior"),
  ]


def get_live_financial_statement(
  session: Session,
  *,
  graph_id: str,
  statement_type: str,
  period_start: date,
  period_end: date,
  limit: int = 50,
) -> LiveFinancialStatementResponse:
  """Generate an OLTP-backed ad-hoc statement and format the response.

  Thin wrapper around ``generate_adhoc_private_statement`` that:
  - builds current+prior periods of matching duration
  - filters subtotal rows and all-zero rows
  - caps at ``limit`` rows (marking ``truncated=True`` when capped)

  Raises ``CoaMappingNotFoundError`` when no CoA→GAAP mapping exists;
  the caller translates to a user-facing tip (400/422).
  """
  periods = build_current_and_prior_periods(period_start, period_end)
  grid, unmapped_count = generate_adhoc_private_statement(
    session,
    statement_type=statement_type,
    periods=periods,
  )

  facts: list[LiveStatementFactRow] = []
  for row in grid.rows:
    if row.is_subtotal:
      continue
    if not any(v != 0.0 for v in row.values):
      continue
    facts.append(
      LiveStatementFactRow(
        qname=row.element_qname,
        name=row.element_name,
        classification=row.classification,
        values=row.values,
        depth=row.depth,
        is_subtotal=row.is_subtotal,
      )
    )

  truncated = len(facts) > limit
  if truncated:
    facts = facts[:limit]

  return LiveFinancialStatementResponse(
    graph_id=graph_id,
    statement_type=statement_type,
    periods=[PeriodSpec(start=p.start, end=p.end, label=p.label) for p in periods],
    facts=facts,
    fact_count=len(facts),
    unmapped_count=unmapped_count,
    truncated=truncated,
  )
