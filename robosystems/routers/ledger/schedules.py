"""Schedule CRUD and closing entry endpoints."""

import asyncio
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.schedules import (
  ClosingEntryResponse,
  CreateClosingEntryRequest,
  CreateScheduleRequest,
  PeriodCloseItemResponse,
  PeriodCloseStatusResponse,
  ScheduleCreatedResponse,
  ScheduleFactResponse,
  ScheduleFactsResponse,
  ScheduleListResponse,
  ScheduleSummaryResponse,
)
from robosystems.models.core import User
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.schedules import ScheduleService

router = APIRouter()
_svc = ScheduleService()


def _ledger_404():
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


# ── Endpoints ──────────────────────────────────────────────────────────────


@router.post(
  "/schedules",
  response_model=ScheduleCreatedResponse,
  status_code=201,
  operation_id="createSchedule",
  summary="Create Schedule",
  tags=["Ledger"],
)
async def create_schedule(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  body: CreateScheduleRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Create a schedule with pre-generated facts for each monthly period."""
  if body.period_end < body.period_start:
    raise HTTPException(status_code=422, detail="period_end must be >= period_start")

  try:
    with extensions_session(graph_id) as session:
      from robosystems.operations.schedules.service import (
        EntryTemplate,
        ScheduleMetadata,
      )

      et = EntryTemplate(
        debit_element_id=body.entry_template.debit_element_id,
        credit_element_id=body.entry_template.credit_element_id,
        entry_type=body.entry_template.entry_type,
        memo_template=body.entry_template.memo_template,
      )

      sm = None
      if body.schedule_metadata:
        sm = ScheduleMetadata(
          method=body.schedule_metadata.method,
          original_amount=body.schedule_metadata.original_amount,
          residual_value=body.schedule_metadata.residual_value,
          useful_life_months=body.schedule_metadata.useful_life_months,
          asset_element_id=body.schedule_metadata.asset_element_id,
        )

      structure = _svc.create_schedule(
        session,
        name=body.name,
        taxonomy_id=body.taxonomy_id,
        element_ids=body.element_ids,
        period_start=body.period_start,
        period_end=body.period_end,
        monthly_amount=body.monthly_amount,
        entry_template=et,
        schedule_metadata=sm,
        created_by=current_user.id,
      )

      # Count generated facts and periods
      from sqlalchemy import text

      count_row = session.execute(
        text("SELECT COUNT(*) AS cnt FROM facts WHERE structure_id = :sid"),
        {"sid": structure.id},
      ).fetchone()

      period_row = session.execute(
        text(
          "SELECT COUNT(DISTINCT (period_start, period_end)) AS cnt "
          "FROM facts WHERE structure_id = :sid"
        ),
        {"sid": structure.id},
      ).fetchone()

      session.commit()

      # Mark graph stale (non-blocking — runs in thread to avoid blocking event loop)
      asyncio.get_running_loop().run_in_executor(
        None, mark_graph_stale, graph_id, "schedule_created"
      )

      return ScheduleCreatedResponse(
        structure_id=structure.id,
        name=structure.name,
        taxonomy_id=structure.taxonomy_id,
        total_periods=period_row.cnt if period_row else 0,
        total_facts=count_row.cnt if count_row else 0,
      )

  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Schedule creation failed: {e}")
    raise


@router.get(
  "/schedules",
  response_model=ScheduleListResponse,
  operation_id="listSchedules",
  summary="List Schedules",
  tags=["Ledger"],
)
async def list_schedules(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """List all active schedules for this graph."""
  try:
    with extensions_session(graph_id) as session:
      summaries = _svc.list_schedules(session)
      return ScheduleListResponse(
        schedules=[
          ScheduleSummaryResponse(
            structure_id=s.structure_id,
            name=s.name,
            taxonomy_name=s.taxonomy_name,
            entry_template=s.entry_template,
            schedule_metadata=s.schedule_metadata,
            total_periods=s.total_periods,
            periods_with_entries=s.periods_with_entries,
          )
          for s in summaries
        ]
      )

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Schedule listing failed: {e}")
    raise


@router.get(
  "/schedules/{structure_id}/facts",
  response_model=ScheduleFactsResponse,
  operation_id="getScheduleFacts",
  summary="Get Schedule Facts",
  tags=["Ledger"],
)
async def get_schedule_facts(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  structure_id: str = Path(..., description="Schedule structure ID"),
  period_start: date | None = Query(None, description="Filter: period start"),
  period_end: date | None = Query(None, description="Filter: period end"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Get facts for a schedule, optionally filtered by period."""
  try:
    with extensions_session(graph_id) as session:
      facts = _svc.get_schedule_facts(session, structure_id, period_start, period_end)
      return ScheduleFactsResponse(
        structure_id=structure_id,
        facts=[
          ScheduleFactResponse(
            element_id=f.element_id,
            element_name=f.element_name,
            value=f.value,
            period_start=f.period_start,
            period_end=f.period_end,
          )
          for f in facts
        ],
      )

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Schedule facts retrieval failed: {e}")
    raise


@router.get(
  "/schedules/close-status",
  response_model=PeriodCloseStatusResponse,
  operation_id="getPeriodCloseStatus",
  summary="Get Period Close Status",
  tags=["Ledger"],
)
async def get_period_close_status(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  period_start: date = Query(..., description="Fiscal period start"),
  period_end: date = Query(..., description="Fiscal period end"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Get close status for all schedules in a fiscal period."""
  try:
    with extensions_session(graph_id) as session:
      status = _svc.get_period_close_status(session, period_start, period_end)
      return PeriodCloseStatusResponse(
        fiscal_period_start=status.fiscal_period_start,
        fiscal_period_end=status.fiscal_period_end,
        period_status=status.period_status,
        schedules=[
          PeriodCloseItemResponse(
            structure_id=s.structure_id,
            structure_name=s.structure_name,
            amount=s.amount,
            status=s.status,
            entry_id=s.entry_id,
          )
          for s in status.schedules
        ],
        total_draft=status.total_draft,
        total_posted=status.total_posted,
      )

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Period close status failed: {e}")
    raise


@router.post(
  "/schedules/{structure_id}/closing-entry",
  response_model=ClosingEntryResponse,
  status_code=201,
  operation_id="createClosingEntry",
  summary="Create Closing Entry",
  tags=["Ledger"],
)
async def create_closing_entry(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  structure_id: str = Path(..., description="Schedule structure ID"),
  body: CreateClosingEntryRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Create a draft closing entry from a schedule's facts for a period."""
  try:
    with extensions_session(graph_id) as session:
      result = _svc.create_closing_entry(
        session,
        structure_id=structure_id,
        posting_date=body.posting_date,
        period_start=body.period_start,
        period_end=body.period_end,
        created_by=current_user.id,
        memo=body.memo,
      )
      session.commit()

      # Mark graph stale (non-blocking — runs in thread to avoid blocking event loop)
      asyncio.get_running_loop().run_in_executor(
        None, mark_graph_stale, graph_id, "closing_entry_created"
      )

      return ClosingEntryResponse(
        entry_id=result.entry_id,
        status=result.status,
        posting_date=result.posting_date,
        memo=result.memo,
        debit_element_id=result.debit_element_id,
        credit_element_id=result.credit_element_id,
        amount=result.amount,
      )

  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Closing entry creation failed: {e}")
    raise
