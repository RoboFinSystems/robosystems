"""Write operations for the fiscal calendar and period close workflow.

Thin wrappers over the existing `FiscalCalendarService` and
`PeriodCloseService`. They take both an extensions session (for the
calendar/period tables) and a platform DB session (for QB sync state
lookup), plus a service instance so tests can swap it out.

The existing old REST router (`routers/ledger/fiscal_calendar.py`,
`routers/ledger/periods.py`) owns the module-level `_svc = FiscalCalendarService()`
singleton; after cutover, the command routes will construct their
own singletons at the route-layer.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodResponse,
  FiscalCalendarResponse,
  InitializeLedgerRequest,
  InitializeLedgerResponse,
)
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
  add_months,
  current_month_period,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  build_fiscal_calendar_response,
  qb_sync_state,
)


@dataclass
class ReopenPeriodResult:
  """Return value for `reopen_period` — wraps the refreshed calendar."""

  fiscal_calendar: FiscalCalendarResponse


class PeriodNotFoundInLedgerError(LookupError):
  """Raised when reopening a period that has no `FiscalPeriod` row."""


class PeriodNotClosedError(Exception):
  """Raised when reopening a period whose status is not `"closed"`."""

  def __init__(self, period: str, status: str) -> None:
    super().__init__(f"Period {period!r} is not closed (status={status!r}).")
    self.period = period
    self.status = status


def initialize_ledger(
  session: Session,
  platform_db: Session,
  graph_id: str,
  body: InitializeLedgerRequest,
  actor_id: str,
  service: FiscalCalendarService,
) -> tuple[InitializeLedgerResponse, list[str]]:
  """Initialize a fiscal calendar and seed fiscal periods.

  Returns the response plus a list of warnings (e.g., from
  `auto_seed_schedules=True` which is not implemented in v1). The
  caller has already validated the request body via Pydantic.

  Raises `CalendarAlreadyInitializedError` / `InvalidCloseTargetError`
  from the service layer — the caller translates to HTTP 409 / 422.
  """
  warnings: list[str] = []
  if body.auto_seed_schedules:
    warnings.append(
      "auto_seed_schedules=true is not yet implemented. "
      "Schedules must be created manually via create-schedule. "
      "SchedulerAgent support will be added in a follow-up."
    )

  calendar = service.initialize(
    session,
    graph_id,
    closed_through=body.closed_through,
    fiscal_year_start_month=body.fiscal_year_start_month,
    actor_id=actor_id,
    actor_type="user",
    note=body.note,
  )

  # Seed FiscalPeriod rows. Matches the old router's logic exactly.
  current = current_month_period()
  default_start = add_months(current, -23)
  start_period = body.earliest_data_period or default_start
  if body.closed_through and body.closed_through < start_period:
    start_period = body.closed_through

  periods_created = service.ensure_fiscal_periods(
    session,
    graph_id,
    start_period=start_period,
    end_period=current,
    closed_through=body.closed_through,
  )

  session.commit()

  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  fc_response = build_fiscal_calendar_response(
    session, graph_id, calendar, has_sync, last_sync_at, service
  )
  response = InitializeLedgerResponse(
    fiscal_calendar=fc_response,
    periods_created=periods_created,
    warnings=warnings,
  )
  return response, warnings


def set_close_target(
  session: Session,
  platform_db: Session,
  graph_id: str,
  period: str,
  actor_id: str,
  note: str | None,
  service: FiscalCalendarService,
) -> FiscalCalendarResponse:
  """Set the close target for a graph. Raises service-level exceptions."""
  calendar = service.set_close_target(
    session,
    graph_id,
    period,
    actor_id=actor_id,
    actor_type="user",
    note=note,
  )
  session.commit()
  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  return build_fiscal_calendar_response(
    session, graph_id, calendar, has_sync, last_sync_at, service
  )


def close_period(
  session: Session,
  platform_db: Session,
  graph_id: str,
  period: str,
  actor_id: str,
  allow_stale_sync: bool,
  note: str | None,
  service: FiscalCalendarService,
  close_service: PeriodCloseService,
) -> ClosePeriodResponse:
  """Close a fiscal period — the final commit action.

  Raises `CloseGateFailed`, `PeriodNotFoundError`,
  `UnbalancedLedgerError`, `FiscalCalendarError` — caller translates
  to appropriate HTTP status codes.
  """
  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  result = close_service.close(
    session,
    graph_id,
    period,
    actor_id=actor_id,
    actor_type="user",
    has_sync_connection=has_sync,
    last_sync_at=last_sync_at,
    allow_stale_sync=allow_stale_sync,
    note=note,
  )
  session.commit()

  fc_response = build_fiscal_calendar_response(
    session, graph_id, result.calendar, has_sync, last_sync_at, service
  )
  return ClosePeriodResponse(
    fiscal_calendar=fc_response,
    period=result.period,
    entries_posted=result.entries_posted,
    target_auto_advanced=result.target_auto_advanced,
  )


def reopen_period(
  session: Session,
  platform_db: Session,
  graph_id: str,
  period: str,
  actor_id: str,
  reason: str,
  note: str | None,
  service: FiscalCalendarService,
) -> FiscalCalendarResponse:
  """Reopen a closed fiscal period.

  Raises `PeriodNotFoundInLedgerError` if the `FiscalPeriod` row
  doesn't exist, `PeriodNotClosedError` if it's not actually closed,
  or service-level `FiscalCalendarError` for calendar issues.
  """
  fp = (
    session.query(FiscalPeriod)
    .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
    .one_or_none()
  )
  if fp is None:
    raise PeriodNotFoundInLedgerError(period)
  if fp.status != "closed":
    raise PeriodNotClosedError(period, fp.status)

  fp.status = "closing"
  fp.closed_at = None
  fp.closed_by = None
  session.flush()

  calendar = service.retreat_closed_through(
    session,
    graph_id,
    period,
    reason=reason,
    actor_id=actor_id,
    actor_type="user",
    note=note,
  )
  session.commit()

  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  return build_fiscal_calendar_response(
    session, graph_id, calendar, has_sync, last_sync_at, service
  )
