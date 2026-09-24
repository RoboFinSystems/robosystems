"""Fiscal calendar and period-close commands over `FiscalCalendarService` and
`PeriodCloseService`. Each takes an extensions session plus a platform
session (for QB sync state).
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
from robosystems.operations.locking import (
  RowLockedError,
  bounded_lock_wait,
  exclusive_period_fence,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  FiscalCalendarService,
  PeriodAlreadyClosedError,
  PeriodCloseService,
  PeriodNotFoundError,
  UnbalancedLedgerError,
  add_months,
  current_month_period,
  next_period,
  period_date_range,
  period_from_date,
  previous_period,
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
  """``statement_sets_retracted`` is 0 when the close skipped stamping."""

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


class ReopenOrderError(Exception):
  """Only the latest closed period can be reopened.

  ``reopen_order`` lists the months to reopen, latest first, down to the
  one requested.
  """

  def __init__(self, period: str, closed_through: str | None) -> None:
    self.period = period
    self.closed_through = closed_through
    order: list[str] = []
    if closed_through is not None and closed_through > period:
      current = closed_through
      while current >= period:
        order.append(current)
        current = previous_period(current)
    self.reopen_order = order
    boundary = (
      f"closed_through is {closed_through!r}"
      if closed_through is not None
      else "nothing is recorded as closed through"
    )
    steps = ", ".join(order) if order else period
    super().__init__(
      f"Cannot reopen {period!r}: only the latest closed period can be "
      f"reopened ({boundary}). Every later closed month carries statements "
      f"stamped from this month's numbers. Reopen latest-first — {steps} — "
      "then close forward month by month."
    )


class BackfillPreconditionError(Exception):
  """A plan-history backfill can't start.

  ``code``: ``nothing_closed``, ``no_ledger_data``, or ``start_after_boundary``.
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

  Raises `CalendarAlreadyInitializedError` / `InvalidCloseTargetError`.
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
  allow_reconciling_items: bool = False,
  fence_wait_ms: int | None = None,
) -> ClosePeriodResponse:
  """Close a fiscal period and commit.

  `fence_wait_ms=None` uses the request default (no wait); the worker
  passes its own budget.

  Raises `CloseGateFailed`, `PeriodNotFoundError`,
  `PeriodAlreadyClosedError`, `RowLockedError`, `UnbalancedLedgerError`,
  `WritebackFailed`, `StatementStampError`, `FiscalCalendarError`.
  """
  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  # The fence spans the QB publish commit and this commit, so no writer can
  # slip a draft into the month between stamping and commit.
  with exclusive_period_fence(
    graph_id,
    period,
    detail=(
      f"Period {period} is being closed or reopened by another process. "
      "Retry in a moment."
    ),
    wait_ms=fence_wait_ms,
  ):
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
      allow_reconciling_items=allow_reconciling_items,
      note=note,
    )
    session.commit()

  # Posted entries and stamped statements change materialized graph state.
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
  """Reopen the latest closed period, retracting its canonical statements.

  Raises `PeriodNotFoundInLedgerError`, `PeriodNotClosedError`,
  `ReopenOrderError` (a later month is still closed), or
  `FiscalCalendarError`.
  """
  # Same fence as close_period, so a reopen can't interleave with a close
  # or another reopen. Lock order: fence, then the FiscalPeriod row.
  with exclusive_period_fence(graph_id, period, detail=_fence_detail(period)):
    calendar, retracted = _reopen_under_fence(
      session,
      graph_id,
      period,
      actor_id=actor_id,
      reason=reason,
      note=note,
      service=service,
      actor_type=actor_type,
    )
    session.commit()

  from robosystems.operations.extensions.staleness import mark_graph_stale

  mark_graph_stale(graph_id, "period_reopened")

  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  return ReopenPeriodResult(
    fiscal_calendar=build_fiscal_calendar_response(
      session, graph_id, calendar, has_sync, last_sync_at, service
    ),
    statement_sets_retracted=len(retracted),
  )


def _fence_detail(period: str) -> str:
  return (
    f"Period {period} is being closed or reopened by another process. "
    "Retry in a moment."
  )


def _reopen_under_fence(
  session: Session,
  graph_id: str,
  period: str,
  *,
  actor_id: str,
  reason: str,
  note: str | None,
  service: FiscalCalendarService,
  actor_type: str,
  enforce_latest: bool = True,
):
  """The reopen's writes, flushed but not committed.

  Caller holds the exclusive period fence and owns the commit, so the
  backfill can reopen and re-close in one transaction.

  ``enforce_latest`` refuses any period but ``closed_through``: later closed
  months carry statements stamped from this month's numbers. The backfill
  restamp opts out because it recloses in the same transaction and walks
  forward.
  """
  session.flush()
  with bounded_lock_wait(session, _fence_detail(period)):
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
  if enforce_latest:
    # Under the calendar lock (period row first, then calendar, as close's
    # advance takes them): an unlocked read races the next month's close.
    closed_through = service.require_locked(session, graph_id).closed_through_period
    if closed_through != period:
      raise ReopenOrderError(period, closed_through)

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
  # The reopened window's schedule facts go back from 'historical' to
  # 'in_scope' so the re-close sees the movement. Local import: module cycle.
  from robosystems.operations.roboledger.commands.schedules import (
    reinstate_reopened_schedule_scopes,
  )

  reinstate_reopened_schedule_scopes(session)

  from robosystems.operations.roboledger.reports.statement_sets import (
    retract_canonical_statement_sets,
  )

  ps, pe = period_date_range(period)
  retracted = retract_canonical_statement_sets(session, period_start=ps, period_end=pe)
  return calendar, retracted


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

  Walks months lacking canonical statements (every month with
  ``body.restamp``) oldest-first through a real reopen → close, so every
  close check applies. Resumable: at most ``body.max_periods`` per call,
  each month committing on its own. Months with drafts are skipped, never
  posted. The first failure halts the walk, since continuing would hole
  the series.

  Raises `FiscalCalendarError` or `BackfillPreconditionError`.
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
        # No drafts in the window, so the close's QB marker commit has
        # nothing to publish: reopen and re-close commit or roll back as one.
        close_result = _restamp_closed_period(
          session,
          platform_db,
          graph_id,
          period,
          actor_id=actor_id,
          note=body.note,
          service=service,
          close_service=close_service,
          actor_type=actor_type,
          allow_stale_sync=body.allow_stale_sync,
          allow_stranded_obligations=body.allow_stranded_obligations,
          allow_reconciling_items=body.allow_reconciling_items,
        )
      else:
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
          allow_reconciling_items=body.allow_reconciling_items,
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
      PeriodAlreadyClosedError,
      PeriodNotFoundError,
      UnbalancedLedgerError,
      WritebackFailed,
      StatementStampError,
      PeriodNotClosedError,
      FiscalCalendarError,
      RowLockedError,
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


def _restamp_closed_period(
  session: Session,
  platform_db: Session,
  graph_id: str,
  period: str,
  *,
  actor_id: str,
  note: str | None,
  service: FiscalCalendarService,
  close_service: PeriodCloseService,
  actor_type: str,
  allow_stale_sync: bool,
  allow_stranded_obligations: bool,
  allow_reconciling_items: bool,
) -> ClosePeriodResponse:
  """Reopen and re-close a period in one transaction under one fence.

  Any failure rolls the reopen back with it.
  """
  has_sync, last_sync_at = qb_sync_state(platform_db, graph_id)
  with exclusive_period_fence(graph_id, period, detail=_fence_detail(period)):
    _reopen_under_fence(
      session,
      graph_id,
      period,
      actor_id=actor_id,
      reason="plan history backfill",
      note=note,
      service=service,
      actor_type=actor_type,
      enforce_latest=False,
    )
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
      allow_reconciling_items=allow_reconciling_items,
      note=note,
    )
    session.commit()

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
