"""Fiscal calendar endpoints — close cadence state management.

All endpoints are synchronous in v1. When QB writeback lands, `POST /close`
will become async; this module stays sync (it only touches calendar state,
not external systems).
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.fiscal_calendar import (
  FiscalCalendarResponse,
  FiscalPeriodSummary,
  InitializeLedgerRequest,
  InitializeLedgerResponse,
  SetCloseTargetRequest,
)
from robosystems.models.core import User
from robosystems.models.core.connection.connection import Connection
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.fiscal_calendar import (
  FiscalCalendarService,
  add_months,
  current_month_period,
)
from robosystems.operations.fiscal_calendar.service import (
  CalendarAlreadyInitializedError,
  FiscalCalendarError,
  InvalidCloseTargetError,
)
from robosystems.routers.ledger._common import ledger_404 as _ledger_404

router = APIRouter()
_svc = FiscalCalendarService()


# ── Helpers ────────────────────────────────────────────────────────────────


def _get_qb_sync_state(
  platform_db: Session, graph_id: str
) -> tuple[bool, datetime | None]:
  """Look up the QB connection state for a graph.

  Returns (has_connection, last_sync_at) so the close gate can distinguish:

  - **No connection**: (False, None) — gate passes unconditionally
  - **Connection exists, never synced**: (True, None) — gate blocks as stale
  - **Connection exists, has synced**: (True, timestamp) — gate compares
    timestamp against period_end
  """
  # A graph can have multiple QB connection rows (disconnected/old/new).
  # Prefer a currently-connected one; fall back to the most recently updated.
  # `.first()` (not `.one_or_none()`) avoids a MultipleResultsFound crash.
  connection = (
    platform_db.query(Connection)
    .filter(Connection.graph_id == graph_id, Connection.provider == "quickbooks")
    .order_by(
      (Connection.status == "connected").desc(),
      Connection.updated_at.desc(),
    )
    .first()
  )
  if connection is None:
    return (False, None)
  return (True, connection.last_sync)


def _build_response(
  session,
  graph_id: str,
  calendar: FiscalCalendar,
  has_sync_connection: bool,
  last_sync_at: datetime | None,
) -> FiscalCalendarResponse:
  """Assemble the FiscalCalendarResponse from a calendar + derived state."""
  # Periods
  periods = (
    session.query(FiscalPeriod)
    .filter(FiscalPeriod.graph_id == graph_id)
    .order_by(FiscalPeriod.start_date)
    .all()
  )

  # Closeable check for the next period in the catch-up sequence.
  # Pass session+graph_id so fresh-tenant catch-up (no closed_through yet)
  # walks from the earliest open FiscalPeriod up to the target instead of
  # returning just `[close_target]`.
  catch_up = _svc.catch_up_sequence(calendar, session=session, graph_id=graph_id)
  next_period_to_close = catch_up[0] if catch_up else None
  gate = None
  if next_period_to_close is not None:
    gate = _svc.closeable_gate(
      session,
      graph_id,
      next_period_to_close,
      has_sync_connection=has_sync_connection,
      last_sync_at=last_sync_at,
    )

  return FiscalCalendarResponse(
    graph_id=graph_id,
    fiscal_year_start_month=calendar.fiscal_year_start_month,
    closed_through=calendar.closed_through_period,
    close_target=calendar.close_target_period,
    gap_periods=len(catch_up),
    catch_up_sequence=catch_up,
    closeable_now=gate.is_closeable if gate else False,
    blockers=gate.blockers if gate else [],
    last_close_at=calendar.last_close_at,
    initialized_at=calendar.initialized_at,
    last_sync_at=last_sync_at,
    periods=[
      FiscalPeriodSummary(
        name=p.name,
        start_date=p.start_date,
        end_date=p.end_date,
        status=p.status,
        closed_at=p.closed_at,
      )
      for p in periods
    ],
  )


# ── Endpoints ──────────────────────────────────────────────────────────────


@router.post(
  "/initialize",
  response_model=InitializeLedgerResponse,
  status_code=201,
  operation_id="initializeLedger",
  summary="Initialize Ledger",
  tags=["Ledger"],
)
async def initialize_ledger(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  body: InitializeLedgerRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  platform_db: Session = Depends(get_db_session),
):
  """One-time ledger initialization.

  Creates the fiscal calendar, seeds `FiscalPeriod` rows for the data window,
  and sets `closed_through` / `close_target`. Fails if the calendar already
  exists — use the reopen flow to undo prior closes instead of re-initializing.

  `auto_seed_schedules=true` is accepted but is a no-op in v1; schedule
  creation is deferred to the SchedulerAgent (Phase 5).
  """
  warnings: list[str] = []
  if body.auto_seed_schedules:
    warnings.append(
      "auto_seed_schedules=true is not yet implemented. "
      "Schedules must be created manually via /schedules. "
      "SchedulerAgent support will be added in a follow-up."
    )

  try:
    with extensions_session(graph_id) as session:
      # Initialize the calendar
      calendar = _svc.initialize(
        session,
        graph_id,
        closed_through=body.closed_through,
        fiscal_year_start_month=body.fiscal_year_start_month,
        actor_id=current_user.id,
        actor_type="user",
        note=body.note,
      )

      # Seed FiscalPeriod rows. Default window is 24 months before current
      # month through current month (inclusive), unless caller specifies an
      # earlier start or the closed_through anchor requires a wider range.
      current = current_month_period()
      default_start = add_months(current, -23)
      start_period = body.earliest_data_period or default_start
      if body.closed_through and body.closed_through < start_period:
        start_period = body.closed_through

      periods_created = _svc.ensure_fiscal_periods(
        session,
        graph_id,
        start_period=start_period,
        end_period=current,
        closed_through=body.closed_through,
      )

      session.commit()

      has_sync, last_sync_at = _get_qb_sync_state(platform_db, graph_id)
      return InitializeLedgerResponse(
        fiscal_calendar=_build_response(
          session, graph_id, calendar, has_sync, last_sync_at
        ),
        periods_created=periods_created,
        warnings=warnings,
      )

  except CalendarAlreadyInitializedError as e:
    raise HTTPException(status_code=409, detail=str(e))
  except InvalidCloseTargetError as e:
    raise HTTPException(status_code=422, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Ledger initialization failed: {e}")
    raise


@router.get(
  "/fiscal-calendar",
  response_model=FiscalCalendarResponse,
  operation_id="getFiscalCalendar",
  summary="Get Fiscal Calendar",
  tags=["Ledger"],
)
async def get_fiscal_calendar(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  platform_db: Session = Depends(get_db_session),
):
  """Return the current fiscal calendar state — pointers, gap, closeable status."""
  try:
    with extensions_session(graph_id) as session:
      calendar = _svc.get(session, graph_id)
      if calendar is None:
        raise HTTPException(
          status_code=404,
          detail=(
            "Fiscal calendar not initialized for this graph. "
            "Call POST /v1/ledger/{graph_id}/initialize first."
          ),
        )
      has_sync, last_sync_at = _get_qb_sync_state(platform_db, graph_id)
      return _build_response(session, graph_id, calendar, has_sync, last_sync_at)

  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Fiscal calendar read failed: {e}")
    raise


@router.post(
  "/fiscal-calendar/close-target",
  response_model=FiscalCalendarResponse,
  operation_id="setCloseTarget",
  summary="Set Close Target",
  tags=["Ledger"],
)
async def set_close_target(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  body: SetCloseTargetRequest = ...,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  platform_db: Session = Depends(get_db_session),
):
  """Set the close target for a graph.

  Validates that the target is a real YYYY-MM period, is not in the future,
  and is not before the current `closed_through`. Emits a `target_changed`
  audit event. Returns the updated calendar state.
  """
  try:
    with extensions_session(graph_id) as session:
      calendar = _svc.set_close_target(
        session,
        graph_id,
        body.period,
        actor_id=current_user.id,
        actor_type="user",
        note=body.note,
      )
      session.commit()
      has_sync, last_sync_at = _get_qb_sync_state(platform_db, graph_id)
      return _build_response(session, graph_id, calendar, has_sync, last_sync_at)

  except InvalidCloseTargetError as e:
    raise HTTPException(status_code=422, detail=str(e))
  except FiscalCalendarError as e:
    raise HTTPException(status_code=404, detail=str(e))
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"set_close_target failed: {e}")
    raise
