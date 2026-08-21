"""Fiscal calendar read operations.

Pure readers and response assemblers over `FiscalCalendarService`, shared by
the REST router and the GraphQL resolvers so the response shape is defined
once.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.fiscal_calendar import (
  FiscalCalendarResponse,
  FiscalPeriodSummary,
  PendingObligationDetailResponse,
)
from robosystems.models.core.connection.connection import Connection
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod


def get_fiscal_year_start_month(session: Session) -> int:
  """Return the graph's configured fiscal year start month, defaulting to 1.

  Pure helper used by report-window resolvers (e.g. the MCP financial
  statement tool) when the caller wants to align an annual reporting
  window to the tenant's fiscal year. Reads the first FiscalCalendar row
  in the extensions session — there is at most one per graph because
  the calendar is graph-singleton.
  """
  cal = session.query(FiscalCalendar).first()
  if cal and cal.fiscal_year_start_month:
    return int(cal.fiscal_year_start_month)
  return 1


def qb_sync_state(platform_db: Session, graph_id: str) -> tuple[bool, datetime | None]:
  """Look up the QB connection state for a graph.

  Returns `(has_connection, last_sync_at)` so the close gate can distinguish:

  - **No connection**: `(False, None)` — gate passes unconditionally
  - **Connection exists, never synced**: `(True, None)` — gate blocks as stale
  - **Connection exists, has synced**: `(True, timestamp)` — gate compares
    timestamp against period_end

  A graph can have multiple QB connection rows (disconnected/old/new).
  Prefer a currently-connected one; fall back to the most recently
  updated. `.first()` (not `.one_or_none()`) avoids a `MultipleResultsFound`
  crash.
  """
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


def build_fiscal_calendar_response(
  session: Session,
  graph_id: str,
  calendar: FiscalCalendar,
  has_sync_connection: bool,
  last_sync_at: datetime | None,
  service,
) -> FiscalCalendarResponse:
  """Assemble the FiscalCalendarResponse from a calendar + derived state.

  `service` is a `FiscalCalendarService` instance — passed explicitly
  so tests can patch the router-level `_svc` and the patched value
  flows in unchanged.
  """
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
  catch_up = service.catch_up_sequence(calendar, session=session, graph_id=graph_id)
  next_period_to_close = catch_up[0] if catch_up else None
  gate = None
  if next_period_to_close is not None:
    gate = service.closeable_gate(
      session,
      graph_id,
      next_period_to_close,
      has_sync_connection=has_sync_connection,
      last_sync_at=last_sync_at,
    )

  pending_obligation_sample = (
    [
      PendingObligationDetailResponse(
        event_id=d.event_id,
        schedule_id=d.schedule_id,
        schedule_name=d.schedule_name,
        period=d.period,
      )
      for d in gate.pending_obligation_sample
    ]
    if gate
    else []
  )
  stranded_obligation_sample = (
    [
      PendingObligationDetailResponse(
        event_id=d.event_id,
        schedule_id=d.schedule_id,
        schedule_name=d.schedule_name,
        period=d.period,
      )
      for d in gate.stranded_obligation_sample
    ]
    if gate
    else []
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
    pending_obligation_count=gate.pending_obligation_count if gate else 0,
    pending_obligation_sample=pending_obligation_sample,
    earliest_pending_period=gate.earliest_pending_period if gate else None,
    sync_stale_days=gate.sync_stale_days if gate else None,
    stranded_obligation_count=gate.stranded_obligation_count if gate else 0,
    stranded_obligation_sample=stranded_obligation_sample,
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
        has_close_receipt=p.close_receipt is not None,
      )
      for p in periods
    ],
  )
