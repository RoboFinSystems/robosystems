"""Rolling close state for the fiscal calendar.

Tracks closed_through and close_target pointers and validates closeable
preconditions; close workflow orchestration lives in the close-period
operation.

Responsibilities:

- Lazily creates a `FiscalCalendar` row per graph (idempotent)
- Maintains the `closed_through_period` ← `close_target_period` pointers
- Validates every closeable precondition (sequence / period complete / sync current)
- Advances `closed_through` when a period is closed, auto-advancing
  `close_target` when reached
- Appends an audit event to `fiscal_calendar_events` on every mutation

Routers / higher-level services call this class; it takes an extensions
DB session and operates within the caller's transaction.

The sync-current gate needs information from the platform DB (connections
table), which the service does not access directly. Callers pass it in via
the `last_sync_at` parameter on `closeable_gate`. None means "no QB
connection exists," which passes the gate unconditionally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time

from sqlalchemy.orm import Session

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

# ────────────────────────────────────────────────────────────────────────────
# Types
# ────────────────────────────────────────────────────────────────────────────


@dataclass
class PendingObligationDetail:
  """One pending schedule-derived obligation that's blocking close.

  Surfaced on `CloseableGateResult` when the `pending_obligations`
  blocker is present so callers (AI close agent, UI, audit) can name
  which schedules need promotion without a sidecar query.
  """

  event_id: str
  schedule_id: str | None
  schedule_name: str | None
  period: str  # YYYY-MM


@dataclass
class CloseableGateResult:
  """Outcome of the closeable gate check for a single period.

  When `is_closeable` is False, `blockers` holds one or more well-known
  reason codes that the caller can surface as structured validation errors.

  When a specific blocker has actionable detail (count of items, sample
  names, age), the relevant `*_detail` fields below are populated. These
  default to None / 0 / [] when the corresponding blocker isn't active.
  """

  is_closeable: bool
  blockers: list[str] = field(default_factory=list)

  # Detail fields for `pending_obligations` — populated when the blocker
  # fires so an agent can name which schedules to promote rather than
  # falling back to a sidecar Cypher / SQL query.
  pending_obligation_count: int = 0
  pending_obligation_sample: list[PendingObligationDetail] = field(default_factory=list)
  earliest_pending_period: str | None = None

  # Detail field for `sync_stale` — days since the last successful sync,
  # so callers can phrase "sync is N days stale" instead of guessing.
  # None when the blocker is not active.
  sync_stale_days: int | None = None

  # Machine-readable codes
  SEQUENCE = "sequence_violation"
  PERIOD_INCOMPLETE = "period_incomplete"
  SYNC_STALE = "sync_stale"
  NO_CALENDAR = "calendar_not_initialized"
  ALREADY_CLOSED = "period_already_closed"
  PENDING_OBLIGATIONS = "pending_obligations"


# ────────────────────────────────────────────────────────────────────────────
# Validation errors
# ────────────────────────────────────────────────────────────────────────────


class FiscalCalendarError(ValueError):
  """Base class for fiscal calendar validation errors."""


class CalendarAlreadyInitializedError(FiscalCalendarError):
  """Raised when initialize() is called on a graph that already has a calendar."""


class InvalidCloseTargetError(FiscalCalendarError):
  """Raised when set_close_target() receives a value that fails validation."""


class AdvanceSequenceError(FiscalCalendarError):
  """Raised when advance_closed_through() receives a non-sequential period."""


# ────────────────────────────────────────────────────────────────────────────
# Service
# ────────────────────────────────────────────────────────────────────────────


class FiscalCalendarService:
  """Manages fiscal calendar state for a graph.

  All methods take an extensions DB session with search_path already set
  to the tenant schema. Callers (routers, other services) handle session
  lifecycle and transaction management.
  """

  # ── Read ────────────────────────────────────────────────────────────────

  def get(self, session: Session, graph_id: str) -> FiscalCalendar | None:
    """Return the fiscal calendar for a graph, or None if not initialized."""
    return (
      session.query(FiscalCalendar)
      .filter(FiscalCalendar.graph_id == graph_id)
      .one_or_none()
    )

  def require(self, session: Session, graph_id: str) -> FiscalCalendar:
    """Return the fiscal calendar for a graph, raising if not initialized."""
    calendar = self.get(session, graph_id)
    if calendar is None:
      raise FiscalCalendarError(
        f"Fiscal calendar not initialized for graph {graph_id}. "
        "Call POST /extensions/roboledger/{graph_id}/operations/initialize first."
      )
    return calendar

  # ── Create / initialize ─────────────────────────────────────────────────

  def get_or_create(
    self,
    session: Session,
    graph_id: str,
    *,
    fiscal_year_start_month: int = 1,
    created_by: str | None = None,
  ) -> FiscalCalendar:
    """Return the existing calendar for a graph, or create a fresh one.

    This is the idempotent entry point — safe to call repeatedly without
    side effects. Does NOT set `closed_through_period` or `close_target_period`
    on creation. Use `initialize` for the full setup flow.
    """
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
    session.flush()
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

    Creates the fiscal calendar, sets `closed_through_period` to the given
    value (or None for "fresh business, never closed"), and sets
    `close_target_period` to the period immediately after `closed_through`
    (or None if closed_through is None — caller will set target explicitly).

    Args:
        session: extensions DB session
        graph_id: target graph
        closed_through: YYYY-MM string, or None. Must be ≤ last completed
            calendar month if provided.
        fiscal_year_start_month: 1-12, default 1 (calendar year)
        actor_id: user / agent performing initialization
        actor_type: one of 'user', 'agent', 'system'
        note: optional free-form note for the audit event

    Raises:
        CalendarAlreadyInitializedError: if graph already has an initialized
            fiscal calendar. Use reopen flow to undo prior closes.
        InvalidCloseTargetError: if `closed_through` is malformed or in the
            future.
    """
    existing = self.get(session, graph_id)
    if existing is not None and existing.initialized_at is not None:
      raise CalendarAlreadyInitializedError(
        f"Fiscal calendar for graph {graph_id} is already initialized "
        f"(initialized_at={existing.initialized_at.isoformat()})."
      )

    # Validate closed_through if provided
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

  # ── Target management ───────────────────────────────────────────────────

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
    """Set the close target for a graph.

    Validates that:
    - `period` is a well-formed YYYY-MM string
    - `period` is ≤ last completed calendar month
    - `period` is ≥ current `closed_through_period` (moving target backward
      uses the reopen flow, not this operation)

    Emits a `target_changed` event. Safe to call with the current value
    (no-op with an audit event, useful for "touching" the target).

    Raises:
        InvalidCloseTargetError on any validation failure.
        FiscalCalendarError if the calendar has not been initialized.
    """
    try:
      parse_period(period)
    except ValueError as e:
      raise InvalidCloseTargetError(str(e)) from e

    calendar = self.require(session, graph_id)

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

  # ── Closed-through advance / retreat ───────────────────────────────────

  def _earliest_open_period(self, session: Session, graph_id: str) -> str | None:
    """Return the name (YYYY-MM) of the earliest non-closed FiscalPeriod, or None.

    Used to determine the expected first close when `closed_through` is None.
    The invariant is that the first close must be the chronologically earliest
    open period — otherwise closing (say) March first would strand Jan and Feb
    forever because `closed_through` jumps past them and the sequence gate
    would permanently reject earlier periods.

    A period is considered "open" if its status is not 'closed'. An empty
    return means no FiscalPeriod rows exist at all, in which case the caller
    should accept any well-formed period (initialize hasn't run, or the graph
    has no period rows yet).
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
    """Whether `period` is the "next sequential close" for this calendar.

    This is the public version of the comparison used by the close flow to
    route a re-close of a reopened FiscalPeriod:

    - ``True``  → the period IS the sequential next close (either because
      ``next_period(closed_through) == period`` when the pointer is set, or
      because ``period`` equals the earliest open FiscalPeriod when it is
      ``None``). The close flow should advance ``closed_through``.
    - ``False`` → anything else. For a reopened period that does not match,
      the close flow should just record a reclose event without moving the
      pointer.

    Callers should use this helper rather than reaching into
    ``_earliest_open_period`` directly.
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
    """Advance `closed_through_period` to `period`.

    Called by the period-close flow after a period has been marked closed.
    Requires strict sequence:

    - If `closed_through` is set: `period == closed_through + 1`
    - If `closed_through` is None: `period` must equal the earliest open
      FiscalPeriod for this graph. This prevents closing Mar first when
      Jan and Feb are still open — which would orphan the earlier months.

    Auto-advances `close_target_period` to `period + 1` when advancement
    reaches the current target, ensuring a healthy steady-state where the
    user always has "next month" ready and waiting.

    Emits a `period_closed` event. If auto-advance fires, also emits a
    `target_advanced_auto` event.

    Raises:
        AdvanceSequenceError if `period` is not the next sequential period.
    """
    calendar = self.require(session, graph_id)

    if calendar.closed_through_period:
      expected: str | None = next_period(calendar.closed_through_period)
    else:
      # First close — must target the earliest open FiscalPeriod to avoid
      # stranding earlier months that haven't been closed yet.
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

    # Auto-advance target when closed_through reaches it
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
    """Record the re-close of a previously reopened period.

    Unlike `advance_closed_through`, this does NOT move the
    `closed_through_period` pointer — a reopened period that is not the
    latest close never caused the pointer to retreat, so re-closing it
    shouldn't shift it either. We just bump `last_close_at` and emit a
    `period_closed` event for the audit trail.
    """
    calendar = self.require(session, graph_id)
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
    """Move `closed_through_period` backward in response to a reopen.

    Used by the reopen flow. If the reopened period is the current
    `closed_through_period`, retreats it to `reopened_period - 1`.
    Otherwise this is a no-op — you can reopen an older period without
    changing the pointer, but you lose sequential invariance.

    Does NOT modify `close_target_period` — that's a separate user decision
    after reopening.

    Emits a `period_reopened` event with the required `reason`.
    """
    if not reason:
      raise FiscalCalendarError("reopen requires a non-empty reason")

    calendar = self.require(session, graph_id)
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

  # ── Closeable gate ──────────────────────────────────────────────────────

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
  ) -> CloseableGateResult:
    """Check whether `period` can be closed right now.

    Three gates (checked in order; all blockers returned, not short-circuited):

    1. **Sequence**: `period == closed_through + 1` (or `period` is the first
       close if `closed_through` is None).
    2. **Period complete**: `period.end_date < today`. The current month cannot
       be closed until it ends.
    3. **Sync current**: if `has_sync_connection` is True, `last_sync_at` must
       be >= `period.end_date`. A missing `last_sync_at` with an active
       connection is treated as stale (never synced). When `has_sync_connection`
       is False the gate passes unconditionally.

    Callers fetch `has_sync_connection` and `last_sync_at` from the platform
    DB (connections table). The two are distinct signals — a fresh connection
    exists with `last_sync_at=None`, and that MUST block closing, whereas no
    connection at all passes.

    This method is **read-only** — it does not mutate state or emit events.
    """
    today = today or date.today()
    blockers: list[str] = []

    calendar = self.get(session, graph_id)
    if calendar is None:
      return CloseableGateResult(
        is_closeable=False,
        blockers=[CloseableGateResult.NO_CALENDAR],
      )

    # Is this a re-close of a previously reopened period? A reopened
    # FiscalPeriod has status='closing' and may live anywhere in the
    # closed range (we intentionally don't retreat closed_through for
    # non-latest reopens). Re-closes skip the sequence / already_closed
    # gates — they're filling in an existing gap, not advancing the cursor.
    fp_row = (
      session.query(FiscalPeriod)
      .filter(FiscalPeriod.graph_id == graph_id, FiscalPeriod.name == period)
      .first()
    )
    is_reclose = fp_row is not None and fp_row.status == "closing"

    # Gate 1: sequence
    #
    # When closed_through is set, the next close must be closed_through + 1.
    # When it's None (fresh business, first close), the next close must be
    # the earliest open FiscalPeriod for this graph — otherwise closing a
    # later period first would permanently strand earlier open months.
    if not is_reclose:
      if calendar.closed_through_period:
        expected: str | None = next_period(calendar.closed_through_period)
      else:
        expected = self._earliest_open_period(session, graph_id)
      if expected is not None and period != expected:
        blockers.append(CloseableGateResult.SEQUENCE)

    # Gate 1b: period already closed? Skipped for re-closes.
    if (
      not is_reclose
      and calendar.closed_through_period is not None
      and period <= calendar.closed_through_period
    ):
      blockers.append(CloseableGateResult.ALREADY_CLOSED)

    # Gate 2: period complete
    #
    # Use strict `>=` not `>`: a period cannot be closed until the day
    # AFTER its last calendar day. Example: January cannot be closed on
    # Jan 31 — you must wait until Feb 1. The reason is that intraday
    # transactions (bank feeds, QB polling) can still post on the last
    # day of the month, and closing before EOD risks missing them. This
    # is stricter than some accounting systems that allow "close on
    # last day," and it's intentional.
    period_start, period_end = period_date_range(period)
    if period_end >= today:
      blockers.append(CloseableGateResult.PERIOD_INCOMPLETE)

    # Gate 3: sync current (soft fail). Only applies when a sync connection exists.
    # "Connection exists but never synced" (last_sync_at is None) is treated as
    # stale and blocks the close — you can't commit a period that hasn't seen
    # any data from the authoritative source.
    sync_stale_days: int | None = None
    if not allow_stale_sync and has_sync_connection:
      if last_sync_at is None:
        blockers.append(CloseableGateResult.SYNC_STALE)
        # "Never synced" — no meaningful day count to report.
      else:
        last_sync_date = (
          last_sync_at.date() if hasattr(last_sync_at, "date") else last_sync_at
        )
        if last_sync_date < period_end:
          blockers.append(CloseableGateResult.SYNC_STALE)
          sync_stale_days = (period_end - last_sync_date).days

    # Gate 4: pending obligations. Schedules materialize one `pending`
    # `schedule_entry_due` event per period. The period cannot be closed
    # while any of those obligations remain pending: the corresponding
    # draft entries have either not yet been classified by the obligation
    # sensor or the operator has not yet voided them. The user-facing
    # remedy is to promote each obligation (manually via
    # `update-event-block`, or automatically via the obligation sensor)
    # or to void them.
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
      # Look up the earliest few pending obligations + their schedule names
      # so callers can name what's blocking close. Cap at 5 to keep the
      # response compact; full enumeration is available via list-event-blocks
      # filtered by event_type=schedule_entry_due&status=pending.
      from robosystems.models.extensions.roboledger import Structure

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
      schedule_ids = {
        evt.metadata_.get("schedule_id")
        for evt in pending_events
        if evt.metadata_ and evt.metadata_.get("schedule_id")
      }
      schedule_names: dict[str, str] = {}
      if schedule_ids:
        for struct in (
          session.query(Structure).filter(Structure.id.in_(schedule_ids)).all()
        ):
          schedule_names[str(struct.id)] = str(struct.name)
      for evt in pending_events:
        meta = evt.metadata_ or {}
        sid = meta.get("schedule_id")
        period_end_iso = meta.get("period_end") or ""
        # period_end is "YYYY-MM-DD" — derive "YYYY-MM" for the response.
        period = period_end_iso[:7] if period_end_iso else ""
        pending_sample.append(
          PendingObligationDetail(
            event_id=str(evt.id),
            schedule_id=str(sid) if sid else None,
            schedule_name=schedule_names.get(sid) if sid else None,
            period=period,
          )
        )
      # earliest = the period of the first pending event (already
      # ordered ASC by occurred_at).
      if pending_sample:
        earliest_pending_period = pending_sample[0].period

    return CloseableGateResult(
      is_closeable=not blockers,
      blockers=blockers,
      pending_obligation_count=pending_count,
      pending_obligation_sample=pending_sample,
      earliest_pending_period=earliest_pending_period,
      sync_stale_days=sync_stale_days,
    )

  # ── Derived state ──────────────────────────────────────────────────────

  def gap_periods(
    self,
    calendar: FiscalCalendar,
    *,
    session: Session | None = None,
    graph_id: str | None = None,
  ) -> int:
    """Return the number of periods between the first-to-close and `close_target`.

    - If target is None → 0 (nothing pending)
    - If both set → count of periods in (closed_through, target]
    - If closed_through is None AND session is provided → count from the
      earliest open FiscalPeriod up to and including target
    - If closed_through is None and no session → fallback to 1 (legacy)
    """
    seq = self.catch_up_sequence(calendar, session=session, graph_id=graph_id)
    return len(seq)

  def catch_up_sequence(
    self,
    calendar: FiscalCalendar,
    *,
    session: Session | None = None,
    graph_id: str | None = None,
  ) -> list[str]:
    """Return the ordered list of periods to close up to the current target.

    When `closed_through` is set, the sequence starts at `closed_through + 1`.
    When it is None (fresh business, first close), the sequence starts at
    the earliest open FiscalPeriod for the graph — so a user who sets a
    catch-up target of 2026-03 on a fresh tenant with data going back to
    2026-01 sees ``[2026-01, 2026-02, 2026-03]`` instead of just the target.

    The session/graph_id parameters are optional for backward compatibility;
    when omitted the fresh-business case returns just ``[close_target]``.
    """
    if calendar.close_target_period is None:
      return []
    if calendar.closed_through_period is not None:
      if calendar.close_target_period <= calendar.closed_through_period:
        return []
      start = next_period(calendar.closed_through_period)
    else:
      # Fresh business — sequence starts at the earliest open period.
      if session is not None and graph_id is not None:
        earliest = self._earliest_open_period(session, graph_id)
        start = earliest or calendar.close_target_period
      else:
        # Legacy fallback: single-element list.
        return [calendar.close_target_period]

    periods: list[str] = []
    current = start
    while current <= calendar.close_target_period:
      periods.append(current)
      current = next_period(current)
    return periods

  # ── FiscalPeriod row management ────────────────────────────────────────

  def ensure_fiscal_periods(
    self,
    session: Session,
    graph_id: str,
    *,
    start_period: str,
    end_period: str,
    closed_through: str | None = None,
  ) -> int:
    """Create FiscalPeriod rows for every month from start to end (inclusive).

    Idempotent — existing rows are left alone. Periods ≤ `closed_through`
    are created with status='closed'; others with status='open'.

    Returns the number of rows actually inserted.
    """
    # Pull existing period names so we don't re-insert
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

  # ── Event emission ─────────────────────────────────────────────────────

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
    """Append an audit event for a fiscal calendar mutation.

    Called internally by every mutating method. Exposed publicly so that
    higher-level flows (e.g., CloseAgent) can record their own meta-events
    against the same calendar.
    """
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
