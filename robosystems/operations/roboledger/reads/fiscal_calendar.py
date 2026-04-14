"""Fiscal calendar read operations.

The FiscalCalendarService itself stays as-is — these helpers are pure
readers and response assemblers that both the REST router and the
future GraphQL resolver call to avoid duplication.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.fiscal_calendar import (
  FiscalCalendarResponse,
  FiscalPeriodSummary,
)
from robosystems.models.core.connection.connection import Connection
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod


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
