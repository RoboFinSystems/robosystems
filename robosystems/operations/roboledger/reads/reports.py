"""Read operations for report definitions and rendered statements.

Ported from `routers/ledger/reports.py` with the helper functions
(`_build_periods`, `_load_structures`, `_resolve_entity_name`,
`_report_to_response`) made module-level so both the REST router and
the GraphQL resolver can call them.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import select, text
from sqlalchemy.orm import Session

if TYPE_CHECKING:
  from robosystems.models.api.extensions.report_package import (
    ReportPackageEnvelope,
  )

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
from robosystems.operations.roboledger.reports.network_picker import (
  load_close_target_concept,
)

VALID_BLOCK_TYPES = {
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
  "custom",
}

# Statement types accepted by the live (OLTP) path. cash_flow_statement
# is supported via the rs-gaap-presentation CashFlow-indirect Disclosure;
# the renderer compiles it like any other arithmetic CAP, but for OLTP
# data without explicit operating/investing/financing tagging, leaf rows
# typically render zero (filtered out as empty) — the structure is in
# place for sources that DO post directly to those leaves (e.g. SEC
# filings, manual close adjustments). Shared across REST router and MCP
# tool so they stay in sync.
LIVE_STATEMENT_TYPES: tuple[str, ...] = (
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
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
  """Raised when a statement's block_type isn't in the report's taxonomy."""


class CoaMappingNotFoundError(LookupError):
  """Raised when no CoA→GAAP mapping exists for ad-hoc statement generation."""


def generate_adhoc_private_statement(
  session: Session,
  *,
  statement_type: str,
  periods: list[FactPeriodSpec],
  reporting_style_id: str,
):
  """Generate an ad-hoc private-company statement directly from OLTP data.

  Unlike `get_statement`, which renders a previously-saved Report, this
  helper builds a one-shot statement from the current ledger using the
  active CoA→GAAP mapping. Used by the `live-financial-statement`
  operation (both REST and MCP) — no saved Report needed.

  The Network is resolved via the graph's Reporting Style composition
  (§3.2 Phase 1) — the renderer doesn't pick among same-typed structures
  by recency anymore. Pass ``Graph.reporting_style_id`` from the platform
  DB.

  ``generate_report_facts`` still wants a ``taxonomy_id`` to scope its
  CoA→GAAP arc walk; rs-gaap-presentation remains the canonical target
  taxonomy (every Default Style Network lives in it). Other Styles that
  cite Networks in a different taxonomy will need this resolved per
  picked Network in a future commit.

  Returns the rendered structure grid plus an `unmapped_count` counter.
  Raises `CoaMappingNotFoundError` if the tenant hasn't completed the
  mapping workflow yet — the caller translates to a user-facing tip.
  """
  mapping = (
    session.query(Structure).filter(Structure.block_type == "coa_mapping").first()
  )
  if mapping is None:
    raise CoaMappingNotFoundError(
      "No CoA→GAAP mapping found. Run the mapping workflow first."
    )

  taxonomy_row = session.execute(
    text(
      "SELECT id FROM taxonomies WHERE standard = 'rs-gaap-presentation' "
      "ORDER BY version DESC LIMIT 1"
    )
  ).fetchone()
  resolved_taxonomy_id = taxonomy_row.id if taxonomy_row else ""

  facts = generate_report_facts(
    session=session,
    taxonomy_id=resolved_taxonomy_id,
    mapping_id=mapping.id,
    periods=periods,
    close_target_qname=load_close_target_concept(session, reporting_style_id),
  )

  grid = render_structure_view(
    session=session,
    facts=facts.facts,
    block_type=statement_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
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
      SELECT id, name, block_type FROM structures
      WHERE taxonomy_id = :taxonomy_id
        AND block_type NOT IN ('chart_of_accounts', 'coa_mapping')
        AND is_active = true
      ORDER BY block_type
    """),
    {"taxonomy_id": taxonomy_id},
  )
  return [
    StructureSummary(id=r.id, name=r.name, block_type=r.block_type) for r in result
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
    filing_status=report_def.filing_status,
    filed_at=report_def.filed_at,
    filed_by=report_def.filed_by,
    supersedes_id=report_def.supersedes_id,
    superseded_by_id=report_def.superseded_by_id,
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


# Display order for the package mode — drives ``ReportPackageItem.display_order``
# when a Structure has no explicit ordering metadata of its own. New
# statement-family block types should be added here as they're seeded.
_BLOCK_TYPE_DISPLAY_ORDER: dict[str, int] = {
  "balance_sheet": 1,
  "income_statement": 2,
  "cash_flow_statement": 3,
  "equity_statement": 4,
  "schedule": 100,
}


def get_report_package(
  session: Session, report_id: str
) -> ReportPackageEnvelope | None:
  """Rehydrate a Report as a package — metadata + N rendered items.

  Loads the Report row, then queries its FactSets via
  ``fact_sets.report_id`` (the Report owns its FactSets — see
  ``models/extensions/roboledger/fact_set.py``). Each FactSet is
  rehydrated into a full ``InformationBlockEnvelope`` via
  :func:`get_information_block_for_fact_set` so the frontend renders
  the package without per-section refetches.

  Returns ``None`` when the Report doesn't exist. Items are ordered by
  ``Structure.block_type`` (BS → IS → CF → Equity → Schedule) with
  ties broken by ``fact_set.created_at``.
  """
  from robosystems.models.api.extensions.report_package import (
    ReportPackageEnvelope,
    ReportPackageItem,
  )
  from robosystems.models.extensions.roboledger import FactSet
  from robosystems.operations.information_block.reads import (
    get_information_block_for_fact_set,
  )

  report_def = session.get(Report, report_id)
  if report_def is None:
    return None

  entity_name = resolve_entity_name(session, report_def)

  # Load FactSets (with their Structures) attached to this Report.
  # Inner join is correct: a FactSet without a Structure can't drive a
  # registered handler (``get_information_block_for_fact_set`` returns
  # None for a null ``structure_id``), so the orphan is dropped here
  # rather than dragged through the rehydration step.
  rows = session.execute(
    select(FactSet, Structure)
    .join(Structure, Structure.id == FactSet.structure_id)
    .where(FactSet.report_id == report_id)
    .order_by(FactSet.created_at)
  ).all()

  items: list[ReportPackageItem] = []
  for fs, structure in rows:
    envelope = get_information_block_for_fact_set(session, fs.id)
    if envelope is None:
      # FactSets whose Structure isn't a registered block type are
      # skipped — they're a real data row but the package mode has no
      # way to render them.
      continue
    block_type = structure.block_type if structure is not None else None
    items.append(
      ReportPackageItem(
        fact_set_id=fs.id,
        structure_id=fs.structure_id,
        display_order=_BLOCK_TYPE_DISPLAY_ORDER.get(block_type or "", 50),
        block=envelope,
      )
    )

  items.sort(key=lambda it: (it.display_order, it.fact_set_id))

  return ReportPackageEnvelope(
    id=report_def.id,
    name=report_def.name,
    description=report_def.description,
    taxonomy_id=report_def.taxonomy_id,
    period_type=report_def.period_type,
    period_start=report_def.period_start,
    period_end=report_def.period_end,
    generation_status=report_def.generation_status,
    last_generated=report_def.last_generated,
    filing_status=report_def.filing_status,
    filed_at=report_def.filed_at,
    filed_by=report_def.filed_by,
    supersedes_id=report_def.supersedes_id,
    superseded_by_id=report_def.superseded_by_id,
    source_graph_id=report_def.source_graph_id,
    source_report_id=report_def.source_report_id,
    shared_at=report_def.shared_at,
    entity_name=entity_name,
    ai_generated=report_def.ai_generated,
    created_at=report_def.created_at,
    created_by=report_def.created_by,
    items=items,
  )


def get_statement(
  session: Session,
  graph_id: str,
  report_id: str,
  block_type: str,
  reporting_style_id: str | None = None,
) -> StatementResponse | None:
  """Render a financial statement for a report + block_type.

  Returns `None` when the report itself doesn't exist. Raises
  `StatementStructureNotFoundError` when the block_type isn't in
  the report's taxonomy. The caller translates both into HTTP 404s.

  ``reporting_style_id`` resolves the graph's active Style (§3.2 Phase 1).
  Callers with the value already in scope (REST routes via
  ``GraphExtensionContext``, GraphQL resolvers via ``info.context``)
  should pass it explicitly to avoid the per-render platform-DB
  round-trip; if omitted, falls back to ``load_graph_reporting_style``
  which opens its own platform session.
  """
  if block_type not in VALID_BLOCK_TYPES:
    raise ValueError(
      f"Invalid block_type '{block_type}'. "
      f"Must be one of: {', '.join(sorted(VALID_BLOCK_TYPES))}"
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
      block_type=block_type,
    )

  # Classification (FASB elementsOfFinancialStatements trait) is resolved via
  # the element_traits junction table. The trait join is wrapped in a
  # subquery to prevent fact-row duplication when an element has
  # multiple ``is_primary=TRUE`` traits across different categories
  # (e.g. an asset element marked primary in both EFS and liquidity
  # axes). Without the subquery, a single fact would emit one row per
  # primary trait and double-count downstream.
  # Filter to FactSets whose owning structure matches the block_type
  # being rendered. Without this filter, elements that the report
  # persisted into multiple FactSets (e.g. ``rs-gaap:NetIncomeLoss``
  # lives in IS + CF + SE per ``_persist_report_facts`` design) would
  # all flow into ``_facts_to_balance_dict`` and get summed (its line-
  # 505 ``net_balance += fact.value`` is intentional for ancestor
  # rollup, but cross-structure duplicates inflate the value 2x/3x).
  # Filtering on the FactSet's owning structure block_type isolates the
  # render to the one statement's worth of facts.
  fact_rows = session.execute(
    text("""
      SELECT rf.element_id, rf.value, rf.period_start, rf.period_end,
             rf.period_type, e.qname, e.name,
             trait_info.identifier AS trait, e.balance_type
      FROM facts rf
      JOIN fact_sets fs ON fs.id = rf.fact_set_id
      JOIN structures s ON s.id = fs.structure_id
      JOIN elements e ON e.id = rf.element_id
      LEFT JOIN (
        SELECT et.element_id, t.identifier
        FROM element_traits et
        JOIN traits t ON t.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND t.category = 'elementsOfFinancialStatements'
      ) trait_info ON trait_info.element_id = e.id
      WHERE fs.report_id = :report_id
        AND s.block_type = :block_type
    """),
    {"report_id": report_id, "block_type": block_type},
  )

  facts = [
    ReportFactData(
      element_id=r.element_id,
      element_qname=r.qname,
      element_name=r.name,
      classification=r.trait,
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
      block_type=block_type,
      periods=[PeriodSpec(start=p.start, end=p.end, label=p.label) for p in periods],
    )

  if reporting_style_id is None:
    from robosystems.operations.roboledger.reports.network_picker import (
      load_graph_reporting_style,
    )

    reporting_style_id = load_graph_reporting_style(graph_id)
  grid = render_structure_view(
    session=session,
    facts=facts,
    block_type=block_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
  )

  if not grid.structure_id:
    raise StatementStructureNotFoundError(block_type)

  validation = validate_report(block_type, grid.rows)

  # Drop XBRL is_abstract rows (presentation scaffolding — *Abstract,
  # *Table, *LineItems, *RollUp wrappers that aren't themselves
  # reportable concepts). Mirrors the live-financial-statement filter
  # (operations/roboledger/reads/reports.py::get_live_financial_statement
  # line ~670). Without this, the IS root abstract surfaces as a row
  # whose value is the sum of every descendant — confusing and
  # double-counting visually.
  rows = [
    FactRowResponse(
      element_id=r.element_id,
      element_qname=r.element_qname,
      element_name=r.element_name,
      trait=r.classification,
      values=r.values,
      is_subtotal=r.is_subtotal,
      depth=r.depth,
    )
    for r in grid.rows
    if not r.is_abstract
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
    block_type=block_type,
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
  reporting_style_id: str | None = None,
) -> LiveFinancialStatementResponse:
  """Generate an OLTP-backed ad-hoc statement and format the response.

  Thin wrapper around ``generate_adhoc_private_statement`` that:
  - builds current+prior periods of matching duration
  - filters subtotal rows and all-zero rows
  - caps at ``limit`` rows (marking ``truncated=True`` when capped)

  ``reporting_style_id`` resolves the graph's active Style (§3.2 Phase 1).
  Routes already loading ``GraphExtensionContext`` (which carries it)
  should pass the value explicitly to skip the platform-DB lookup;
  callers without it in scope fall back to ``load_graph_reporting_style``.

  Raises ``CoaMappingNotFoundError`` when no CoA→GAAP mapping exists;
  the caller translates to a user-facing tip (400/422).
  """
  if reporting_style_id is None:
    from robosystems.operations.roboledger.reports.network_picker import (
      load_graph_reporting_style,
    )

    reporting_style_id = load_graph_reporting_style(graph_id)
  periods = build_current_and_prior_periods(period_start, period_end)
  grid, unmapped_count = generate_adhoc_private_statement(
    session,
    statement_type=statement_type,
    periods=periods,
    reporting_style_id=reporting_style_id,
  )

  facts: list[LiveStatementFactRow] = []
  for row in grid.rows:
    # Drop XBRL `is_abstract` rows (`*Abstract`, `*Table`, `*LineItems`,
    # `*RollUp`). They're presentation scaffolding — section headers and
    # calc-cluster wrappers — that carry the rolled-up value of their
    # children in the disclosure DAG but aren't themselves reportable
    # concepts. Their value is duplicated by the concrete subtotal
    # underneath them (e.g., `fac:CostRevenueRollUp` mirrors
    # `fac:CostOfRevenue`).
    if row.is_abstract:
      continue
    # Show every concrete row that carries a value, including subtotals.
    # FAC anchor rows (fac:GrossProfit, fac:OperatingIncomeLoss, …) are
    # `is_subtotal=True` because they have child summands in the
    # Disclosure DAG, but their value is the calc-DAG result the reader
    # most wants to see. The UI distinguishes them via the `is_subtotal`
    # flag on the response.
    if not any(v != 0.0 for v in row.values):
      continue
    facts.append(
      LiveStatementFactRow(
        qname=row.element_qname,
        name=row.element_name,
        trait=row.classification,
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
