"""Write operations for the fiscal calendar and period close workflow.

Thin wrappers over `FiscalCalendarService` and `PeriodCloseService`. They
take an extensions session (calendar / period tables), a platform DB session
(QB sync state), and a service instance so tests can swap it out.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.fiscal_calendar import (
  BackfillPeriodOutcome,
  BackfillPlanHistoryRequest,
  BackfillPlanHistoryResponse,
  ClosePeriodResponse,
  FiscalCalendarResponse,
  InitializeLedgerRequest,
  InitializeLedgerResponse,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.locking import bounded_lock_wait
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  FiscalCalendarService,
  PeriodCloseService,
  PeriodNotFoundError,
  UnbalancedLedgerError,
  add_months,
  current_month_period,
  next_period,
  period_date_range,
  period_from_date,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  WritebackFailed,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  build_fiscal_calendar_response,
  qb_sync_state,
)


@dataclass
class ReopenPeriodResult:
  """Return value for `reopen_period` — wraps the refreshed calendar.

  ``statement_sets_retracted`` counts the reopened month's canonical
  statement FactSets deleted by the reopen — 0 when the close soft-skipped
  stamping. The REST router returns only ``fiscal_calendar``; the MCP tool
  surfaces the count.
  """

  fiscal_calendar: FiscalCalendarResponse
  statement_sets_retracted: int = 0


class PeriodNotFoundInLedgerError(LookupError):
  """Raised when reopening a period that has no `FiscalPeriod` row."""


class PeriodNotClosedError(Exception):
  """Raised when reopening a period whose status is not `"closed"`."""

  def __init__(self, period: str, status: str) -> None:
    super().__init__(f"Period {period!r} is not closed (status={status!r}).")
    self.period = period
    self.status = status


class BackfillPreconditionError(Exception):
  """Raised when a plan-history backfill can't start.

  ``code`` is machine-readable: ``nothing_closed`` (no close boundary to
  backfill behind — run close-period first), ``no_ledger_data`` (the
  graph has no entries to compile), or ``start_after_boundary`` (the
  requested start is past `closed_through`).
  """

  def __init__(self, code: str, message: str) -> None:
    super().__init__(message)
    self.code = code


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
      "Schedules must be created manually via create-information-block "
      "(block_type='schedule'). Automated seeding will be added in a follow-up."
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
  actor_type: str = "user",
  allow_stranded_obligations: bool = False,
) -> ClosePeriodResponse:
  """Close a fiscal period — the final commit action.

  `actor_type` defaults to `"user"` for REST callers; MCP tools pass
  `"agent"` so the audit log distinguishes Claude-driven closes from
  human-driven ones.

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
    actor_type=actor_type,
    has_sync_connection=has_sync,
    last_sync_at=last_sync_at,
    allow_stale_sync=allow_stale_sync,
    allow_stranded_obligations=allow_stranded_obligations,
    note=note,
  )
  session.commit()

  # The close's writes (entries posted, statement sets stamped) change
  # graph-materialized state, so the blue/green pipeline must rebuild —
  # without this marker the graph keeps serving pre-close data while
  # reporting fresh. Placed here (not the routers) so the REST and MCP
  # surfaces both get it. Non-fatal by design.
  from robosystems.operations.extensions.staleness import mark_graph_stale

  mark_graph_stale(graph_id, "period_closed")

  fc_response = build_fiscal_calendar_response(
    session, graph_id, result.calendar, has_sync, last_sync_at, service
  )
  return ClosePeriodResponse(
    fiscal_calendar=fc_response,
    period=result.period,
    entries_posted=result.entries_posted,
    entries_published_to_qb=result.entries_published_to_qb,
    entries_posted_locally=result.entries_posted_locally,
    target_auto_advanced=result.target_auto_advanced,
    rule_summary=result.rule_summary,
    evaluated_structure_ids=list(result.evaluated_structure_ids),
    statements_stamped=result.statements_stamped,
    statement_stamp_note=result.statement_stamp_note,
    stamped_statement_sets=dict(result.stamped_statement_sets),
    statement_rule_summary=result.statement_rule_summary,
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
  actor_type: str = "user",
) -> ReopenPeriodResult:
  """Reopen a closed fiscal period.

  Retracts the month's canonical statement FactSets (close-time
  stamping's inverse): a reopened month is no longer a closed assertion,
  so its persisted statements — and their verification results — go
  with it. The re-close restamps fresh sets.

  Raises `PeriodNotFoundInLedgerError` if the `FiscalPeriod` row
  doesn't exist, `PeriodNotClosedError` if it's not actually closed,
  or service-level `FiscalCalendarError` for calendar issues.
  """
  # Locked: this decides from `fp.status` and then writes it, so two concurrent
  # reopens cannot both retract the same month's statements.
  #
  # It does **not** serialize against `close_period`, which cannot take a
  # transaction-scoped lock at all (its QB pre-publish commits mid-close — see
  # that function's comment). A reopen interleaved with a close can still leave
  # a period marked closed whose statements were retracted; closing that needs
  # the session-scoped lock the close is waiting on.
  session.flush()
  with bounded_lock_wait(
    session,
    f"Period {period} is being closed or reopened by another process. "
    "Retry in a moment.",
  ):
    fp = (
      session.query(FiscalPeriod)
      .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
      .populate_existing()
      .with_for_update()
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
    actor_type=actor_type,
    note=note,
  )
  # Re-stamp schedule facts the retreated boundary has re-opened: the reopened
  # window's facts were tagged 'historical' at generation and must return to
  # 'in_scope' so the roll-forward carry-in and the re-close see the movement.
  # Function-level import — commands.schedules ↔ information_block.schedule form
  # a module-load cycle that a top-level import here would trip.
  from robosystems.operations.roboledger.commands.schedules import (
    reinstate_reopened_schedule_scopes,
  )

  reinstate_reopened_schedule_scopes(session)

  # Retract the reopened month's canonical statement sets. Function-level
  # import — statement_sets pulls in information-block machinery this
  # module otherwise never loads (same posture as the schedules import
  # above).
  from robosystems.operations.roboledger.reports.statement_sets import (
    retract_canonical_statement_sets,
  )

  ps, pe = period_date_range(period)
  retracted = retract_canonical_statement_sets(session, period_start=ps, period_end=pe)
  session.commit()

  # The reopen retracts the month's canonical statement sets — graph
  # state changed; mirror close_period's marker.
  from robosystems.operations.extensions.staleness import mark_graph_stale

  mark_graph_stale(graph_id, "period_reopened")

  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  return ReopenPeriodResult(
    fiscal_calendar=build_fiscal_calendar_response(
      session, graph_id, calendar, has_sync, last_sync_at, service
    ),
    statement_sets_retracted=len(retracted),
  )


def backfill_plan_history(
  session: Session,
  platform_db: Session,
  graph_id: str,
  body: BackfillPlanHistoryRequest,
  actor_id: str,
  service: FiscalCalendarService,
  close_service: PeriodCloseService,
  actor_type: str = "user",
) -> BackfillPlanHistoryResponse:
  """Compile monthly statement history behind the close boundary.

  Seeds missing `FiscalPeriod` rows (baseline-closed) back to the
  clamped start, then walks every month in the range that lacks
  canonical statement FactSets oldest-first (``body.restamp`` widens
  the walk to every month in range — the healing pass after an engine
  improvement changes what a stamp produces), running the real
  `reopen_period` → `close_period` cycle on each — identical semantics
  to a manual restamp, so balance validation, statement rules, QB
  writeback guards, and audit events all apply per month.

  Chunked and resumable: at most ``body.max_periods`` months are
  attempted per call; each month commits individually (via the reused
  commands), so an interrupted run resumes where it left off — a month
  left in status='closing' by a failed reclose skips straight to the
  close on retry. Months with draft entries are skipped, never posted:
  the backfill refuses to commit ledger changes nobody reviewed.

  A failed reclose halts processing (continuing would hole the series);
  the failure rides the month's outcome and everything untried lands in
  ``remaining_periods``.

  Raises `FiscalCalendarError` when the calendar isn't initialized and
  `BackfillPreconditionError` for nothing-closed / no-data / bad-range.
  """
  calendar = service.require(session, graph_id)
  closed_through = calendar.closed_through_period
  if closed_through is None:
    raise BackfillPreconditionError(
      "nothing_closed",
      "Nothing is closed yet — the backfill fills history behind the "
      "close boundary. Close the first period with close-period, or "
      "catch up normally.",
    )

  earliest_date = session.query(sa_func.min(Entry.posting_date)).scalar()
  if earliest_date is None:
    raise BackfillPreconditionError(
      "no_ledger_data",
      "The ledger has no entries — nothing to compile. Sync or import data first.",
    )
  earliest_available = period_from_date(earliest_date)

  start = body.start_period or earliest_available
  if start < earliest_available:
    start = earliest_available
  if start > closed_through:
    raise BackfillPreconditionError(
      "start_after_boundary",
      f"start_period {body.start_period!r} is after closed_through "
      f"{closed_through!r} — the backfill only compiles closed history.",
    )

  rows_created = service.ensure_fiscal_periods(
    session,
    graph_id,
    start_period=start,
    end_period=closed_through,
    closed_through=closed_through,
  )
  if rows_created:
    session.commit()

  # Function-level import — statement_sets pulls in information-block
  # machinery (same posture as reopen_period's imports above).
  from robosystems.operations.roboledger.reports.statement_sets import (
    StatementStampError,
    has_canonical_statement_sets,
  )

  candidates: list[str] = []
  current = start
  while current <= closed_through:
    ps, pe = period_date_range(current)
    if body.restamp or not has_canonical_statement_sets(
      session, period_start=ps, period_end=pe
    ):
      candidates.append(current)
    current = next_period(current)

  processed: list[BackfillPeriodOutcome] = []
  for period in candidates[: body.max_periods]:
    ps, pe = period_date_range(period)
    draft_count = (
      session.query(Entry)
      .filter(
        Entry.posting_date >= ps,
        Entry.posting_date <= pe,
        Entry.status == "draft",
      )
      .count()
    )
    if draft_count:
      processed.append(
        BackfillPeriodOutcome(
          period=period,
          status="skipped_drafts",
          detail=(
            f"{draft_count} draft entries in the window — review via "
            "list-period-drafts, then close-period or re-run the backfill."
          ),
        )
      )
      continue

    fp = (
      session.query(FiscalPeriod)
      .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
      .one()
    )
    try:
      if fp.status == "closed":
        reopen_period(
          session,
          platform_db,
          graph_id,
          period,
          actor_id=actor_id,
          reason="plan history backfill",
          note=body.note,
          service=service,
          actor_type=actor_type,
        )
      close_result = close_period(
        session,
        platform_db,
        graph_id,
        period,
        actor_id=actor_id,
        allow_stale_sync=body.allow_stale_sync,
        note=body.note,
        service=service,
        close_service=close_service,
        actor_type=actor_type,
        allow_stranded_obligations=body.allow_stranded_obligations,
      )
      processed.append(
        BackfillPeriodOutcome(
          period=period,
          status="stamped",
          statements_stamped=close_result.statements_stamped,
          statement_stamp_note=close_result.statement_stamp_note,
          statement_rule_summary=close_result.statement_rule_summary,
        )
      )
    except (
      CloseGateFailed,
      PeriodNotFoundError,
      UnbalancedLedgerError,
      WritebackFailed,
      StatementStampError,
      PeriodNotClosedError,
      FiscalCalendarError,
    ) as exc:
      session.rollback()
      processed.append(
        BackfillPeriodOutcome(period=period, status="failed", detail=str(exc))
      )
      break

  attempted = {outcome.period for outcome in processed}
  remaining = [p for p in candidates if p not in attempted]

  refreshed = service.require(session, graph_id)
  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  return BackfillPlanHistoryResponse(
    fiscal_calendar=build_fiscal_calendar_response(
      session, graph_id, refreshed, has_sync, last_sync_at, service
    ),
    earliest_available_period=earliest_available,
    effective_start_period=start,
    closed_through=closed_through,
    period_rows_created=rows_created,
    processed=processed,
    remaining_periods=remaining,
  )
