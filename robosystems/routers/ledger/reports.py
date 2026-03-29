"""Report CRUD and generation endpoints."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import select
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  FactRowResponse,
  RegenerateReportRequest,
  ReportListResponse,
  ReportResponse,
  ReportWithDataResponse,
  ValidationCheckResponse,
)
from robosystems.models.core import User
from robosystems.models.extensions import ReportDefinition
from robosystems.operations.reports.fact_grid import build_fact_grid
from robosystems.operations.reports.guard_rails import validate_report

router = APIRouter()

VALID_REPORT_TYPES = {
  "income_statement",
  "balance_sheet",
  "cash_flow",
  "equity",
  "custom",
}


def _ledger_404():
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


def _report_404(report_id: str):
  return HTTPException(
    status_code=404,
    detail=f"Report '{report_id}' not found.",
  )


def _fact_grid_to_response(
  grid, report_def: ReportDefinition
) -> ReportWithDataResponse:
  """Convert a FactGrid + ReportDefinition to the API response."""
  rows = [
    FactRowResponse(
      element_id=r.element_id,
      element_qname=r.element_qname,
      element_name=r.element_name,
      classification=r.classification,
      current_value=r.current_value,
      prior_value=r.prior_value,
      is_subtotal=r.is_subtotal,
      depth=r.depth,
    )
    for r in grid.rows
  ]

  validation = (
    ValidationCheckResponse(
      passed=grid.validation.passed,
      checks=grid.validation.checks,
      failures=grid.validation.failures,
      warnings=grid.validation.warnings,
    )
    if hasattr(grid, "validation") and grid.validation
    else None
  )

  return ReportWithDataResponse(
    id=report_def.id,
    name=report_def.name,
    report_type=report_def.report_type,
    generation_status=report_def.generation_status,
    period_type=report_def.period_type,
    comparative=report_def.comparative,
    mapping_id=report_def.mapping_id,
    period_start=report_def.period_start,
    period_end=report_def.period_end,
    ai_generated=report_def.ai_generated,
    created_at=report_def.created_at,
    last_generated=report_def.last_generated,
    structure_id=grid.structure_id,
    structure_name=grid.structure_name,
    comparative_period_start=grid.comparative_period_start,
    comparative_period_end=grid.comparative_period_end,
    rows=rows,
    validation=validation,
    unmapped_count=grid.unmapped_count,
  )


# ── Endpoints ──────────────────────────────────────────────────────────────


@router.post(
  "/reports",
  response_model=ReportWithDataResponse,
  status_code=201,
  operation_id="createReport",
  summary="Create Report",
  tags=["Ledger"],
)
async def create_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  body: CreateReportRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Create a report definition and generate the fact grid synchronously."""
  if body.report_type not in VALID_REPORT_TYPES:
    raise HTTPException(
      status_code=422,
      detail=f"Invalid report_type '{body.report_type}'. Must be one of: {', '.join(sorted(VALID_REPORT_TYPES))}",
    )

  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")

  try:
    with extensions_session(graph_id) as session:
      # Create the report definition
      report_def = ReportDefinition(
        name=body.name,
        report_type=body.report_type,
        mapping_id=body.mapping_id,
        period_type=body.period_type,
        period_start=body.period_start,
        period_end=body.period_end,
        comparative=body.comparative,
        generation_status="generating",
        created_by=current_user.id,
      )
      session.add(report_def)
      session.flush()

      # Build the fact grid (synchronous — fast SQL + tree traversal)
      grid = build_fact_grid(
        session=session,
        report_type=body.report_type,
        mapping_id=body.mapping_id,
        period_start=body.period_start,
        period_end=body.period_end,
        comparative=body.comparative,
      )

      # Validate
      validation = validate_report(body.report_type, grid.rows)
      grid.validation = validation

      # Update status
      report_def.generation_status = "published"
      report_def.last_generated = datetime.now(UTC)
      session.commit()

      return _fact_grid_to_response(grid, report_def)

  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.get(
  "/reports",
  response_model=ReportListResponse,
  operation_id="listReports",
  summary="List Reports",
  tags=["Ledger"],
)
async def list_reports(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """List all report definitions for this graph."""
  try:
    with extensions_session(graph_id) as session:
      rows = (
        session.execute(
          select(ReportDefinition).order_by(ReportDefinition.created_at.desc())
        )
        .scalars()
        .all()
      )

      return ReportListResponse(
        reports=[
          ReportResponse(
            id=r.id,
            name=r.name,
            report_type=r.report_type,
            generation_status=r.generation_status,
            period_type=r.period_type,
            comparative=r.comparative,
            mapping_id=r.mapping_id,
            period_start=r.period_start,
            period_end=r.period_end,
            ai_generated=r.ai_generated,
            created_at=r.created_at,
            last_generated=r.last_generated,
          )
          for r in rows
        ]
      )
  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.get(
  "/reports/{report_id}",
  response_model=ReportWithDataResponse,
  operation_id="getReport",
  summary="Get Report",
  tags=["Ledger"],
)
async def get_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Get a report definition with its generated fact grid data.

  The fact grid is recomputed from the current mapped trial balance data
  each time — this ensures the report always reflects the latest data.
  """
  try:
    with extensions_session(graph_id) as session:
      report_def = session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)

      if not report_def.period_start or not report_def.period_end:
        return ReportWithDataResponse(
          id=report_def.id,
          name=report_def.name,
          report_type=report_def.report_type,
          generation_status=report_def.generation_status,
          period_type=report_def.period_type,
          comparative=report_def.comparative,
          mapping_id=report_def.mapping_id,
          period_start=report_def.period_start,
          period_end=report_def.period_end,
          ai_generated=report_def.ai_generated,
          created_at=report_def.created_at,
          last_generated=report_def.last_generated,
        )

      # Recompute fact grid from current data
      grid = build_fact_grid(
        session=session,
        report_type=report_def.report_type,
        mapping_id=report_def.mapping_id or "",
        period_start=report_def.period_start,
        period_end=report_def.period_end,
        comparative=report_def.comparative,
      )

      validation = validate_report(report_def.report_type, grid.rows)
      grid.validation = validation

      return _fact_grid_to_response(grid, report_def)

  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.post(
  "/reports/{report_id}/regenerate",
  response_model=ReportWithDataResponse,
  operation_id="regenerateReport",
  summary="Regenerate Report",
  tags=["Ledger"],
)
async def regenerate_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  body: RegenerateReportRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Regenerate a report with new period dates.

  Same report configuration (type, mapping, settings), new dates.
  """
  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")

  try:
    with extensions_session(graph_id) as session:
      report_def = session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)

      # Update period
      report_def.period_start = body.period_start
      report_def.period_end = body.period_end
      report_def.generation_status = "generating"
      session.flush()

      # Regenerate
      grid = build_fact_grid(
        session=session,
        report_type=report_def.report_type,
        mapping_id=report_def.mapping_id or "",
        period_start=body.period_start,
        period_end=body.period_end,
        comparative=report_def.comparative,
      )

      validation = validate_report(report_def.report_type, grid.rows)
      grid.validation = validation

      report_def.generation_status = "published"
      report_def.last_generated = datetime.now(UTC)
      session.commit()

      return _fact_grid_to_response(grid, report_def)

  except (ValueError, ProgrammingError):
    raise _ledger_404()


@router.delete(
  "/reports/{report_id}",
  status_code=204,
  operation_id="deleteReport",
  summary="Delete Report",
  tags=["Ledger"],
)
async def delete_report(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report definition ID"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Delete a report definition."""
  try:
    with extensions_session(graph_id) as session:
      report_def = session.get(ReportDefinition, report_id)
      if not report_def:
        raise _report_404(report_id)

      session.delete(report_def)
      session.commit()

  except (ValueError, ProgrammingError):
    raise _ledger_404()
