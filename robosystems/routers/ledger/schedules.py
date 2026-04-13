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
  CreateManualClosingEntryRequest,
  CreateScheduleRequest,
  PeriodCloseItemResponse,
  PeriodCloseStatusResponse,
  ScheduleCreatedResponse,
  ScheduleFactResponse,
  ScheduleFactsResponse,
  ScheduleListResponse,
  ScheduleSummaryResponse,
  TruncateScheduleRequest,
  TruncateScheduleResponse,
)
from robosystems.models.core import User
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.roboledger.schedules import ScheduleService
from robosystems.routers.ledger._common import ledger_404 as _ledger_404

router = APIRouter()
_svc = ScheduleService()


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
      from robosystems.operations.roboledger.schedules.service import (
        EntryTemplate,
        ScheduleMetadata,
      )

      et = EntryTemplate(
        debit_element_id=body.entry_template.debit_element_id,
        credit_element_id=body.entry_template.credit_element_id,
        entry_type=body.entry_template.entry_type,
        memo_template=body.entry_template.memo_template,
        auto_reverse=body.entry_template.auto_reverse,
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
        closed_through=body.closed_through,
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
            reversal_entry_id=s.reversal_entry_id,
            reversal_status=s.reversal_status,
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

      reversal_resp = None
      if result.reversal:
        reversal_resp = ClosingEntryResponse(
          outcome=result.reversal.outcome,
          entry_id=result.reversal.entry_id,
          status=result.reversal.status,
          posting_date=result.reversal.posting_date,
          memo=result.reversal.memo,
          debit_element_id=result.reversal.debit_element_id,
          credit_element_id=result.reversal.credit_element_id,
          amount=result.reversal.amount,
          reason=result.reversal.reason,
        )

      return ClosingEntryResponse(
        outcome=result.outcome,
        entry_id=result.entry_id,
        status=result.status,
        posting_date=result.posting_date,
        memo=result.memo,
        debit_element_id=result.debit_element_id,
        credit_element_id=result.credit_element_id,
        amount=result.amount,
        reason=result.reason,
        reversal=reversal_resp,
      )

  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Closing entry creation failed: {e}")
    raise


@router.patch(
  "/schedules/{structure_id}/truncate",
  response_model=TruncateScheduleResponse,
  operation_id="truncateSchedule",
  summary="Truncate Schedule (End Early)",
  tags=["Ledger"],
)
async def truncate_schedule(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  structure_id: str = Path(..., description="Schedule structure ID"),
  body: TruncateScheduleRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """End a schedule early.

  Used for events that cut a schedule's lifespan short — an asset is sold,
  a prepaid is cancelled, a contract is terminated. Deletes all facts with
  `period_start > new_end_date` and any stale draft entries that were
  produced from them.

  Posted entries are preserved — if any period after `new_end_date` has a
  posted closing entry, the truncate fails with 422 and the caller must
  reopen that period first.

  The truncation is logged to the schedule's metadata for audit.
  """
  try:
    with extensions_session(graph_id) as session:
      result = _svc.truncate_schedule(
        session,
        structure_id=structure_id,
        new_end_date=body.new_end_date,
        reason=body.reason,
        updated_by=current_user.id,
      )
      session.commit()

      asyncio.get_running_loop().run_in_executor(
        None, mark_graph_stale, graph_id, "schedule_truncated"
      )

      return TruncateScheduleResponse(
        structure_id=result["structure_id"],
        new_end_date=result["new_end_date"],
        facts_deleted=result["facts_deleted"],
        reason=result["reason"],
      )
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Schedule truncation failed: {e}")
    raise


@router.post(
  "/manual-closing-entry",
  response_model=ClosingEntryResponse,
  status_code=201,
  operation_id="createManualClosingEntry",
  summary="Create Manual Closing Entry",
  tags=["Ledger"],
)
async def create_manual_closing_entry(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  body: CreateManualClosingEntryRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Create a manual (non-schedule) draft closing entry.

  Used for one-off adjustments that aren't derived from a schedule: asset
  disposals, impairments, reclassifications, correcting entries.

  The entry is drafted like any schedule-derived entry and flows through
  the same review and close pipeline — `list-period-drafts` shows it,
  `close-period` commits it along with the rest.

  Line items can be any count (not just 2 like schedule entries). Total
  debits must equal total credits. `provenance` is set to 'manual_entry'
  and `source_structure_id` is null.
  """
  try:
    with extensions_session(graph_id) as session:
      result = _svc.create_manual_closing_entry(
        session,
        posting_date=body.posting_date,
        line_items=[
          {
            "element_id": li.element_id,
            "debit_amount": li.debit_amount,
            "credit_amount": li.credit_amount,
            "description": li.description,
          }
          for li in body.line_items
        ],
        memo=body.memo,
        created_by=current_user.id,
        entry_type=body.entry_type,
      )
      session.commit()

      asyncio.get_running_loop().run_in_executor(
        None, mark_graph_stale, graph_id, "manual_entry_created"
      )

      return ClosingEntryResponse(
        outcome=result.outcome,
        entry_id=result.entry_id,
        status=result.status,
        posting_date=result.posting_date,
        memo=result.memo,
        debit_element_id=result.debit_element_id,
        credit_element_id=result.credit_element_id,
        amount=result.amount,
        reason=result.reason,
      )
  except ValueError as e:
    raise HTTPException(status_code=422, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Manual closing entry creation failed: {e}")
    raise
