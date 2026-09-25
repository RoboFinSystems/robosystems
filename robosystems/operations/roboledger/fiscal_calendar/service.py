"""Rolling close state for the fiscal calendar.

Owns the `closed_through_period` / `close_target_period` pointers, the
closeable gate, and the `fiscal_calendar_events` audit trail. Operates
within the caller's extensions-DB transaction; close orchestration lives in
the close-period operation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.db.integrity import violates
from robosystems.logger import logger
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_calendar import (
  FiscalCalendar,
  FiscalCalendarEvent,
)
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

from .periods import (
  last_completed_period,
  next_period,
  parse_period,
  period_date_range,
  previous_period,
)


@dataclass
class PendingObligationDetail:
  """One schedule-derived obligation blocking close, named for the caller."""

  event_id: str
  schedule_id: str | None
  schedule_name: str | None
  period: str  # YYYY-MM


@dataclass
class CloseableGateResult:
  """Outcome of the closeable gate for one period.

  `blockers` holds the reason codes below. The detail fields default to
  None / 0 / [] when their blocker isn't active.
  """

  is_closeable: bool
  blockers: list[str] = field(default_factory=list)

  pending_obligation_count: int = 0
  pending_obligation_sample: list[PendingObligationDetail] = field(default_factory=list)
  earliest_pending_period: str | None = None

  # None when not stale, or when the connection has never synced.
  sync_stale_days: int | None = None

  # Stranded and reconciling details populate even when their blocker is
  # bypassed, so the close audit trail records what was knowingly closed over.
  stranded_obligation_count: int = 0
  stranded_obligation_sample: list[PendingObligationDetail] = field(
    default_factory=list
  )

  reconciling_item_count: int = 0
  reconciling_item_sample: list[str] = field(default_factory=list)

  unposted_source_event_count: int = 0
  unposted_source_event_sample: list[str] = field(default_factory=list)

  SEQUENCE = "sequence_violation"
  PERIOD_INCOMPLETE = "period_incomplete"
  SYNC_STALE = "sync_stale"
  NO_CALENDAR = "calendar_not_initialized"
  ALREADY_CLOSED = "period_already_closed"
  PENDING_OBLIGATIONS = "pending_obligations"
  STRANDED_OBLIGATIONS = "stranded_obligations"
  RECONCILING_ITEMS = "reconciling_items"
  UNPOSTED_SOURCE_EVENTS = "unposted_source_events"


class FiscalCalendarError(ValueError):
  """Base class for fiscal calendar validation errors."""


class CalendarAlreadyInitializedError(FiscalCalendarError):
  """Raised when initialize() is called on a graph that already has a calendar."""


class InvalidCloseTargetError(FiscalCalendarError):
  """Raised when set_close_target() receives a value that fails validation."""


class AdvanceSequenceError(FiscalCalendarError):
  """Raised when advance_closed_through() receives a non-sequential period."""


class FiscalCalendarService:
  """Fiscal calendar state for a graph. Sessions arrive tenant-scoped."""

  def get(self, session: Session, graph_id: str) -> FiscalCalendar | None:
    return (
      session.query(FiscalCalendar)
      .filter(FiscalCalendar.graph_id == graph_id)
      .one_or_none()
    )

  def require(self, session: Session, graph_id: str) -> FiscalCalendar:
    calendar = self.get(session, graph_id)
    if calendar is None:
      raise FiscalCalendarError(
        f"Fiscal calendar not initialized for graph {graph_id}. "
        "Call POST /extensions/roboledger/{graph_id}/operations/initialize first."
      )
    return calendar

  def require_locked(self, session: Session, graph_id: str) -> FiscalCalendar:
    """`require`, with the calendar row locked for the write that follows.

    Serializes the pointer writers (set-close-target, close's advance,
    reopen's retreat); set-close-target holds no period fence, so without
    this it races a close's auto-advance. Lock order: FiscalPeriod row
    first, then this. The wait is bounded because callers are request-facing.
    """
    from robosystems.operations.locking import bounded_lock_wait

    session.flush()
    with bounded_lock_wait(
      session,
      f"The fiscal calendar for graph {graph_id} is being written by another "
      "process. Retry in a moment.",
    ):
      calendar = (
        session.query(FiscalCalendar)
        .filter(FiscalCalendar.graph_id == graph_id)
        .populate_existing()
        .with_for_update()
        .one_or_none()
      )
    if calendar is None:
      raise FiscalCalendarError(
        f"Fiscal calendar not initialized for graph {graph_id}. "
        "Call POST /extensions/roboledger/{graph_id}/operations/initialize first."
      )
    return calendar

  def get_or_create(
    self,
    session: Session,
    graph_id: str,
    *,
    fiscal_year_start_month: int = 1,
    created_by: str | None = None,
  ) -> FiscalCalendar:
    """Idempotent. Leaves both pointers unset; `initialize` does full setup."""
    calendar = self.get(session, graph_id)
    if calendar is not None:
      return calendar

    calendar = FiscalCalendar(
      graph_id=graph_id,
      fiscal_year_start_month=fiscal_year_start_month,
      created_by=created_by,
      updated_by=created_by,
    )
    session.add(calendar)
    try:
      session.flush()
    except IntegrityError as exc:
      # Two initializations raced past the `get` above.
      if not violates(exc, "uq_fiscal_calendar_graph"):
        raise
      raise CalendarAlreadyInitializedError(
        f"Fiscal calendar for graph {graph_id} was initialized concurrently."
      ) from exc
    logger.info(
      f"Created fiscal calendar for graph {graph_id} "
      f"(fiscal_year_start_month={fiscal_year_start_month})"
    )
    return calendar

  def initialize(
    self,
    session: Session,
    graph_id: str,
    *,
    closed_through: str | None = None,
    fiscal_year_start_month: int = 1,
    actor_id: str | None = None,
    actor_type: str = "user",
    note: str | None = None,
  ) -> FiscalCalendar:
    """One-time ledger initialization.

    `closed_through=None` means a fresh business that has never closed; the
    target is then left for the caller to set. Otherwise the target becomes
    the month after `closed_through`, which must be a completed month.

    Raises `CalendarAlreadyInitializedError` if already initialized, and
    `InvalidCloseTargetError` if `closed_through` is malformed or future.
    """
    existing = self.get(session, graph_id)
    if existing is not None and existing.initialized_at is not None:
      raise CalendarAlreadyInitializedError(
        f"Fiscal calendar for graph {graph_id} is already initialized "
        f"(initialized_at={existing.initialized_at.isoformat()})."
      )

    if closed_through is not None:
      try:
        parse_period(closed_through)
      except ValueError as e:
        raise InvalidCloseTargetError(str(e)) from e
      last_valid = last_completed_period()
      if closed_through > last_valid:
        raise InvalidCloseTargetError(
          f"closed_through={closed_through!r} is in the future. "
          f"Maximum allowed: {last_valid} (last completed calendar month)."
        )

    calendar = existing or self.get_or_create(
      session,
      graph_id,
      fiscal_year_start_month=fiscal_year_start_month,
      created_by=actor_id,
    )
    calendar.fiscal_year_start_month = fiscal_year_start_month
    calendar.closed_through_period = closed_through
    calendar.close_target_period = (
      next_period(closed_through) if closed_through else None
    )
    calendar.initialized_at = datetime.now(UTC)
    calendar.updated_by = actor_id
    session.flush()

    self.record_event(
      session,
      calendar,
      event_type="initialized",
      to_value=closed_through,
      actor_id=actor_id,
      actor_type=actor_type,
      note=note,
    )
    logger.info(
      f"Initialized fiscal calendar for graph {graph_id} "
      f"closed_through={closed_through} target={calendar.close_target_period}"
    )
    return calendar

  def set_close_target(
    self,
    session: Session,
    graph_id: str,
    period: str,
    *,
    actor_id: str | None = None,
    actor_type: str = "user",
    note: str | None = None,
  ) -> FiscalCalendar:
    """Set the close target: a completed month at or after `closed_through`.

    Moving it before `closed_through` is the reopen flow's job. Setting the
    current value still records a `target_changed` event.

    Raises ``InvalidCloseTargetError`` on validation failure, and
    ``FiscalCalendarError`` if the calendar has not been initialized.
    """
    try:
      parse_period(period)
    except ValueError as e:
      raise InvalidCloseTargetError(str(e)) from e

    calendar = self.require_locked(session, graph_id)

    last_valid = last_completed_period()
    if period > last_valid:
      raise InvalidCloseTargetError(
        f"close_target={period!r} is in the future. "
        f"Maximum allowed: {last_valid} (last completed calendar month)."
      )

    if calendar.closed_through_period and period < calendar.closed_through_period:
      raise InvalidCloseTargetError(
        f"close_target={period!r} is before closed_through="
        f"{calendar.closed_through_period!r}. "
        "Use POST /periods/{period}/reopen to undo a prior close."
      )

    previous_value = calendar.close_target_period
    calendar.close_target_period = period
    calendar.updated_by = actor_id
    session.flush()

    self.record_event(
      session,
      calendar,
      event_type="target_changed",
      from_value=previous_value,
      to_value=period,
      actor_id=actor_id,
      actor_type=actor_type,
      note=note,
    )
    logger.info(f"Fiscal calendar {graph_id} close_target {previous_value} → {period}")
    return calendar

  def _earliest_open_period(self, session: Session, graph_id: str) -> str | None:
    """Earliest non-closed FiscalPeriod name (YYYY-MM), or None if none exist.

    The first close must be this period: closing a later one first would move
    `closed_through` past the earlier months and strand them for good.
    """
    row = (
      session.query(FiscalPeriod.name)
      .filter(FiscalPeriod.graph_id == graph_id)
      .filter(FiscalPeriod.status != "closed")
      .order_by(FiscalPeriod.start_date.asc())
      .first()
    )
    return row[0] if row else None

  def is_latest_sequential_close(
    self,
    session: Session,
    graph_id: str,
    calendar: FiscalCalendar,
    period: str,
  ) -> bool:
    """Whether closing `period` should advance `closed_through`.

    False routes a reopened period to `record_reclose` instead.
    """
    if calendar is None:
      return False
    if calendar.closed_through_period is not None:
      return next_period(calendar.closed_through_period) == period
    return self._earliest_open_period(session, graph_id) == period

  def advance_closed_through(
    self,
    session: Session,
    graph_id: str,
    period: str,
    *,
    actor_id: str | None = None,
    actor_type: str = "user",
    note: str | None = None,
  ) -> FiscalCalendar:
    """Advance `closed_through_period` to `period` after a close.

    `period` must be `closed_through + 1`, or the earliest open FiscalPeriod
    when `closed_through` is None. Reaching the target auto-advances it to
    the next month (a `target_advanced_auto` event).

    Raises ``AdvanceSequenceError`` if `period` is out of sequence.
    """
    calendar = self.require_locked(session, graph_id)

    if calendar.closed_through_period:
      expected: str | None = next_period(calendar.closed_through_period)
    else:
      expected = self._earliest_open_period(session, graph_id) or period

    if period != expected:
      raise AdvanceSequenceError(
        f"Cannot advance closed_through from "
        f"{calendar.closed_through_period!r} to {period!r}. "
        f"Next period must be {expected!r}."
      )

    previous_closed_through = calendar.closed_through_period
    calendar.closed_through_period = period
    calendar.last_close_at = datetime.now(UTC)
    calendar.updated_by = actor_id
    session.flush()

    self.record_event(
      session,
      calendar,
      event_type="period_closed",
      period=period,
      from_value=previous_closed_through,
      to_value=period,
      actor_id=actor_id,
      actor_type=actor_type,
      note=note,
    )

    if (
      calendar.close_target_period is not None
      and calendar.closed_through_period >= calendar.close_target_period
    ):
      previous_target = calendar.close_target_period
      new_target = next_period(period)
      calendar.close_target_period = new_target
      session.flush()
      self.record_event(
        session,
        calendar,
        event_type="target_advanced_auto",
        from_value=previous_target,
        to_value=new_target,
        actor_id=actor_id,
        actor_type="system",
        note="Auto-advanced after close_target reached",
      )
      logger.info(
        f"Fiscal calendar {graph_id} close_target auto-advanced "
        f"{previous_target} → {new_target}"
      )

    logger.info(
      f"Fiscal calendar {graph_id} closed_through {previous_closed_through} → {period}"
    )
    return calendar

  def record_reclose(
    self,
    session: Session,
    graph_id: str,
    period: str,
    *,
    actor_id: str | None = None,
    actor_type: str = "user",
    note: str | None = None,
  ) -> FiscalCalendar:
    """Re-close a reopened period without moving `closed_through_period`.

    A reopen of a non-latest period never retreated the pointer, so its
    re-close must not advance it.
    """
    calendar = self.require_locked(session, graph_id)
    calendar.last_close_at = datetime.now(UTC)
    calendar.updated_by = actor_id
    session.flush()

    self.record_event(
      session,
      calendar,
      event_type="period_closed",
      period=period,
      from_value=calendar.closed_through_period,
      to_value=calendar.closed_through_period,
      actor_id=actor_id,
      actor_type=actor_type,
      note=note or "Re-close of previously reopened period",
    )
    logger.info(
      f"Fiscal calendar {graph_id} re-closed {period} "
      f"(closed_through unchanged at {calendar.closed_through_period})"
    )
    return calendar

  def retreat_closed_through(
    self,
    session: Session,
    graph_id: str,
    reopened_period: str,
    *,
    reason: str,
    actor_id: str | None = None,
    actor_type: str = "user",
    note: str | None = None,
  ) -> FiscalCalendar:
    """Retreat `closed_through_period` by one if `reopened_period` is it.

    Otherwise only the event is recorded: the public reopen refuses any
    other period, and the backfill restamp reopens an interior month and
    recloses forward within one transaction. `close_target_period` is left
    alone.
    """
    if not reason:
      raise FiscalCalendarError("reopen requires a non-empty reason")

    calendar = self.require_locked(session, graph_id)
    previous_closed_through = calendar.closed_through_period

    if calendar.closed_through_period == reopened_period:
      calendar.closed_through_period = previous_period(reopened_period)
      calendar.updated_by = actor_id
      session.flush()

    self.record_event(
      session,
      calendar,
      event_type="period_reopened",
      period=reopened_period,
      from_value=previous_closed_through,
      to_value=calendar.closed_through_period,
      actor_id=actor_id,
      actor_type=actor_type,
      reason=reason,
      note=note,
    )
    logger.info(
      f"Fiscal calendar {graph_id} reopened {reopened_period} "
      f"(closed_through {previous_closed_through} → {calendar.closed_through_period})"
    )
    return calendar

  def closeable_gate(
    self,
    session: Session,
    graph_id: str,
    period: str,
    *,
    today: date | None = None,
    has_sync_connection: bool = False,
    last_sync_at: datetime | None = None,
    allow_stale_sync: bool = False,
    allow_stranded_obligations: bool = False,
    allow_reconciling_items: bool = False,
    allow_unposted_source_events: bool = False,
  ) -> CloseableGateResult:
    """Check whether `period` can be closed now. Read-only; every blocker is
    returned, not just the first.

    Gates: sequence; period complete; sync current; no pending obligations;
    no stranded obligations (bypass: `allow_stranded_obligations`); no
    unresolved reconciling items (bypass: `allow_reconciling_items`); no
    source event dated in the period left uncommitted (bypass:
    `allow_unposted_source_events`).

    `has_sync_connection` and `last_sync_at` come from the platform DB and
    are distinct: no connection passes the sync gate, but a connection that
    has never synced (`last_sync_at=None`) blocks.
    """
    today = today or date.today()
    blockers: list[str] = []

    calendar = self.get(session, graph_id)
    if calendar is None:
      return CloseableGateResult(
        is_closeable=False,
        blockers=[CloseableGateResult.NO_CALENDAR],
      )

    # A reopened period ('closing') can sit anywhere in the closed range;
    # re-closing it fills a gap, so the sequence gates don't apply.
    fp_row = (
      session.query(FiscalPeriod)
      .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
      .first()
    )
    is_reclose = fp_row is not None and fp_row.status == "closing"

    if not is_reclose:
      if calendar.closed_through_period:
        expected: str | None = next_period(calendar.closed_through_period)
      else:
        expected = self._earliest_open_period(session, graph_id)
      if expected is not None and period != expected:
        blockers.append(CloseableGateResult.SEQUENCE)

    if (
      not is_reclose
      and calendar.closed_through_period is not None
      and period <= calendar.closed_through_period
    ):
      blockers.append(CloseableGateResult.ALREADY_CLOSED)

    # Not closeable until the day after month end: transactions can still
    # post during the last day.
    period_start, period_end = period_date_range(period)
    if period_end >= today:
      blockers.append(CloseableGateResult.PERIOD_INCOMPLETE)

    sync_stale_days: int | None = None
    if not allow_stale_sync and has_sync_connection:
      if last_sync_at is None:
        blockers.append(CloseableGateResult.SYNC_STALE)
      else:
        last_sync_date = (
          last_sync_at.date() if hasattr(last_sync_at, "date") else last_sync_at
        )
        if last_sync_date < period_end:
          blockers.append(CloseableGateResult.SYNC_STALE)
          sync_stale_days = (period_end - last_sync_date).days

    # Matured schedule obligations still pending: promote or void them.
    period_end_dt = datetime.combine(period_end, time(23, 59, 59), tzinfo=UTC)
    pending_count = (
      session.query(Event)
      .filter(
        Event.event_type == "schedule_entry_due",
        Event.status == "pending",
        Event.occurred_at <= period_end_dt,
      )
      .count()
    )
    pending_sample: list[PendingObligationDetail] = []
    earliest_pending_period: str | None = None
    if pending_count > 0:
      blockers.append(CloseableGateResult.PENDING_OBLIGATIONS)
      pending_events = (
        session.query(Event)
        .filter(
          Event.event_type == "schedule_entry_due",
          Event.status == "pending",
          Event.occurred_at <= period_end_dt,
        )
        .order_by(Event.occurred_at.asc())
        .limit(5)
        .all()
      )
      pending_sample = self._obligation_sample(session, pending_events)
      if pending_sample:
        earliest_pending_period = pending_sample[0].period

    # A promotion sweep that doesn't dispatch handlers leaves obligations
    # `classified` with no drafted entry: invisible to the pending count, and
    # an adjusting entry the close would silently omit.
    from robosystems.operations.event_block.promotion import (
      find_stranded_obligations,
    )

    stranded_events = find_stranded_obligations(session, as_of=period_end_dt)
    stranded_count = len(stranded_events)
    stranded_sample: list[PendingObligationDetail] = []
    if stranded_count > 0:
      if not allow_stranded_obligations:
        blockers.append(CloseableGateResult.STRANDED_OBLIGATIONS)
      stranded_sample = self._obligation_sample(session, stranded_events[:5])

    # Posted events whose source payload changed afterwards. Items dated
    # after this period don't affect it; earlier ones would land their
    # catch-up entry here.
    from robosystems.operations.roboledger.commands.reconciling_items import (
      find_unresolved_reconciling_items,
    )

    reconciling_rows = find_unresolved_reconciling_items(session, as_of=period_end)
    reconciling_count = len(reconciling_rows)
    reconciling_sample: list[str] = []
    if reconciling_count > 0:
      if not allow_reconciling_items:
        blockers.append(CloseableGateResult.RECONCILING_ITEMS)
      reconciling_sample = [
        str(external_id or event_id) for event_id, external_id in reconciling_rows[:5]
      ]

    # Events dated in the period that produced no ledger rows (a bank line
    # never committed, a QuickBooks bill whose auto-commit failed): once the
    # period closes, commit is fenced out of it, so they could never post. A
    # drafted entry (a manual journal entry) has rows, which close posts.
    # Obligations are counted above.
    from robosystems.models.extensions.roboledger.entry import Entry

    posting_date = func.date(func.coalesce(Event.effective_at, Event.occurred_at))
    has_rows = select(Entry.id).where(Entry.triggered_by_event_id == Event.id).exists()
    unposted_query = session.query(Event).filter(
      Event.status.in_(("captured", "classified")),
      Event.event_type != "schedule_entry_due",
      posting_date >= period_start,
      posting_date <= period_end,
      ~has_rows,
    )
    unposted_count = unposted_query.count()
    unposted_sample: list[str] = []
    if unposted_count > 0:
      if not allow_unposted_source_events:
        blockers.append(CloseableGateResult.UNPOSTED_SOURCE_EVENTS)
      unposted_sample = [
        str(event.external_id or event.id)
        for event in unposted_query.order_by(posting_date.asc()).limit(5)
      ]

    return CloseableGateResult(
      is_closeable=not blockers,
      blockers=blockers,
      pending_obligation_count=pending_count,
      pending_obligation_sample=pending_sample,
      earliest_pending_period=earliest_pending_period,
      sync_stale_days=sync_stale_days,
      stranded_obligation_count=stranded_count,
      stranded_obligation_sample=stranded_sample,
      reconciling_item_count=reconciling_count,
      reconciling_item_sample=reconciling_sample,
      unposted_source_event_count=unposted_count,
      unposted_source_event_sample=unposted_sample,
    )

  @staticmethod
  def _obligation_sample(
    session: Session, events: list[Event]
  ) -> list[PendingObligationDetail]:
    from robosystems.models.extensions.roboledger import Structure

    schedule_ids = {
      evt.metadata_.get("schedule_id")
      for evt in events
      if evt.metadata_ and evt.metadata_.get("schedule_id")
    }
    schedule_names: dict[str, str] = {}
    if schedule_ids:
      for struct in (
        session.query(Structure).filter(Structure.id.in_(schedule_ids)).all()
      ):
        schedule_names[str(struct.id)] = str(struct.name)
    sample: list[PendingObligationDetail] = []
    for evt in events:
      meta = evt.metadata_ or {}
      sid = meta.get("schedule_id")
      period_end_iso = meta.get("period_end") or ""
      evt_period = period_end_iso[:7] if period_end_iso else ""
      sample.append(
        PendingObligationDetail(
          event_id=str(evt.id),
          schedule_id=str(sid) if sid else None,
          schedule_name=schedule_names.get(sid) if sid else None,
          period=evt_period,
        )
      )
    return sample

  def gap_periods(
    self,
    calendar: FiscalCalendar,
    *,
    session: Session | None = None,
    graph_id: str | None = None,
  ) -> int:
    """Number of periods still to close up to the target."""
    seq = self.catch_up_sequence(calendar, session=session, graph_id=graph_id)
    return len(seq)

  def catch_up_sequence(
    self,
    calendar: FiscalCalendar,
    *,
    session: Session | None = None,
    graph_id: str | None = None,
  ) -> list[str]:
    """Ordered periods to close up to the current target.

    With `closed_through` unset, the sequence starts at the earliest open
    FiscalPeriod; that lookup needs `session` and `graph_id`, and without
    them the result is just ``[close_target]``.
    """
    if calendar.close_target_period is None:
      return []
    if calendar.closed_through_period is not None:
      if calendar.close_target_period <= calendar.closed_through_period:
        return []
      start = next_period(calendar.closed_through_period)
    else:
      if session is not None and graph_id is not None:
        earliest = self._earliest_open_period(session, graph_id)
        start = earliest or calendar.close_target_period
      else:
        return [calendar.close_target_period]

    periods: list[str] = []
    current = start
    while current <= calendar.close_target_period:
      periods.append(current)
      current = next_period(current)
    return periods

  def ensure_fiscal_periods(
    self,
    session: Session,
    graph_id: str,
    *,
    start_period: str,
    end_period: str,
    closed_through: str | None = None,
  ) -> int:
    """Create missing monthly FiscalPeriod rows from start to end, inclusive.

    Periods ≤ `closed_through` are created closed. Returns rows inserted.
    """
    existing_names = {
      name
      for (name,) in session.query(FiscalPeriod.name)
      .filter(FiscalPeriod.graph_id == graph_id)
      .all()
    }

    inserted = 0
    current = start_period
    now = datetime.now(UTC)
    while current <= end_period:
      if current not in existing_names:
        period_start, period_end = period_date_range(current)
        status = "closed" if (closed_through and current <= closed_through) else "open"
        period = FiscalPeriod(
          graph_id=graph_id,
          name=current,
          start_date=period_start,
          end_date=period_end,
          period_type="monthly",
          status=status,
          closed_at=now if status == "closed" else None,
          closed_by="initialize" if status == "closed" else None,
        )
        session.add(period)
        inserted += 1
      current = next_period(current)

    if inserted:
      session.flush()
    return inserted

  def record_event(
    self,
    session: Session,
    calendar: FiscalCalendar,
    *,
    event_type: str,
    period: str | None = None,
    from_value: str | None = None,
    to_value: str | None = None,
    actor_id: str | None = None,
    actor_type: str = "user",
    note: str | None = None,
    reason: str | None = None,
  ) -> FiscalCalendarEvent:
    event = FiscalCalendarEvent(
      fiscal_calendar_id=calendar.id,
      graph_id=calendar.graph_id,
      event_type=event_type,
      period=period,
      from_value=from_value,
      to_value=to_value,
      actor_id=actor_id,
      actor_type=actor_type,
      note=note,
      reason=reason,
    )
    session.add(event)
    session.flush()
    return event
