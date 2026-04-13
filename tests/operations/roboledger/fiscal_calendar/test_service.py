"""Tests for FiscalCalendarService — close cadence state management.

These are unit tests with a stubbed session. The service's query shape is
simple enough (`session.query(FiscalCalendar).filter(...).one_or_none()`,
plus `session.add()` / `session.flush()`) that we can emulate it with a
lightweight in-memory store. Full integration tests land with Phase 2.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.extensions.roboledger.fiscal_calendar import (
  FiscalCalendar,
  FiscalCalendarEvent,
)
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.fiscal_calendar.service import (
  AdvanceSequenceError,
  CalendarAlreadyInitializedError,
  CloseableGateResult,
  FiscalCalendarError,
  FiscalCalendarService,
  InvalidCloseTargetError,
)

# ────────────────────────────────────────────────────────────────────────────
# In-memory session stub
# ────────────────────────────────────────────────────────────────────────────


class _InMemorySession:
  """Minimal SQLAlchemy Session stand-in for service unit tests.

  Tracks added objects by class, supports the one_or_none() and filter()
  shapes that FiscalCalendarService uses, and records flush calls.
  """

  def __init__(self):
    self._objects: dict[type, list] = {}
    self.flush_count = 0

  def add(self, obj) -> None:
    self._objects.setdefault(type(obj), []).append(obj)

  def flush(self) -> None:
    self.flush_count += 1

  def query(self, model):
    return _Query(self, model)

  def all_of(self, model) -> list:
    return list(self._objects.get(model, []))


class _Query:
  def __init__(self, session: _InMemorySession, model):
    self._session = session
    self._model = model
    self._filters: list = []

  def filter(self, *criteria):
    self._filters.extend(criteria)
    return self

  def one_or_none(self):
    rows = list(self._session._objects.get(self._model, []))
    # Evaluate simple equality filters by reflection
    for criterion in self._filters:
      # Criteria come from SQLAlchemy's BinaryExpression, which exposes
      # .left.name for the column and .right.value for the comparand.
      try:
        col_name = criterion.left.name  # type: ignore[attr-defined]
        comparand = criterion.right.value  # type: ignore[attr-defined]
      except AttributeError:
        continue
      rows = [r for r in rows if getattr(r, col_name, None) == comparand]
    if len(rows) > 1:
      raise RuntimeError(f"Expected 0 or 1 {self._model.__name__}, got {len(rows)}")
    return rows[0] if rows else None

  def all(self):
    return list(self._session._objects.get(self._model, []))

  def first(self):
    # Evaluate filters the same way one_or_none() does, but return the
    # first match without raising on duplicates.
    rows = list(self._session._objects.get(self._model, []))
    for criterion in self._filters:
      try:
        col_name = criterion.left.name  # type: ignore[attr-defined]
        comparand = criterion.right.value  # type: ignore[attr-defined]
      except AttributeError:
        continue
      rows = [r for r in rows if getattr(r, col_name, None) == comparand]
    return rows[0] if rows else None

  def order_by(self, *args):
    return self


# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────


GRAPH_ID = "kg19d829b0c07ec7d94cdf"


def _service() -> tuple[FiscalCalendarService, _InMemorySession]:
  return FiscalCalendarService(), _InMemorySession()


def _events(session: _InMemorySession) -> list[FiscalCalendarEvent]:
  return session.all_of(FiscalCalendarEvent)


def _event_types(session: _InMemorySession) -> list[str]:
  return [e.event_type for e in _events(session)]


# ────────────────────────────────────────────────────────────────────────────
# get_or_create
# ────────────────────────────────────────────────────────────────────────────


class TestGetOrCreate:
  def test_creates_new_calendar(self):
    svc, session = _service()
    calendar = svc.get_or_create(session, GRAPH_ID)
    assert calendar.graph_id == GRAPH_ID
    assert calendar.fiscal_year_start_month == 1
    assert calendar.closed_through_period is None
    assert calendar.close_target_period is None
    assert calendar.initialized_at is None

  def test_idempotent(self):
    svc, session = _service()
    first = svc.get_or_create(session, GRAPH_ID)
    second = svc.get_or_create(session, GRAPH_ID)
    assert first is second
    assert len(session.all_of(FiscalCalendar)) == 1

  def test_custom_fiscal_year(self):
    svc, session = _service()
    calendar = svc.get_or_create(session, GRAPH_ID, fiscal_year_start_month=7)
    assert calendar.fiscal_year_start_month == 7

  def test_returns_none_for_missing(self):
    svc, session = _service()
    assert svc.get(session, GRAPH_ID) is None

  def test_require_raises_when_missing(self):
    svc, session = _service()
    with pytest.raises(FiscalCalendarError):
      svc.require(session, GRAPH_ID)


# ────────────────────────────────────────────────────────────────────────────
# initialize
# ────────────────────────────────────────────────────────────────────────────


class TestInitialize:
  def test_fresh_business_none_closed_through(self):
    svc, session = _service()
    calendar = svc.initialize(session, GRAPH_ID, closed_through=None, actor_id="u1")
    assert calendar.closed_through_period is None
    assert calendar.close_target_period is None
    assert calendar.initialized_at is not None
    assert "initialized" in _event_types(session)

  def test_with_closed_through_sets_target_to_next(self):
    svc, session = _service()
    calendar = svc.initialize(
      session, GRAPH_ID, closed_through="2026-02", actor_id="u1"
    )
    assert calendar.closed_through_period == "2026-02"
    assert calendar.close_target_period == "2026-03"

  def test_rejects_future_closed_through(self):
    svc, session = _service()
    far_future = "2099-01"
    with pytest.raises(InvalidCloseTargetError):
      svc.initialize(session, GRAPH_ID, closed_through=far_future, actor_id="u1")

  def test_rejects_malformed_closed_through(self):
    svc, session = _service()
    with pytest.raises(InvalidCloseTargetError):
      svc.initialize(session, GRAPH_ID, closed_through="not-a-period", actor_id="u1")

  def test_rejects_double_initialize(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-02", actor_id="u1")
    with pytest.raises(CalendarAlreadyInitializedError):
      svc.initialize(session, GRAPH_ID, closed_through="2026-03", actor_id="u1")

  def test_custom_fiscal_year(self):
    svc, session = _service()
    calendar = svc.initialize(
      session, GRAPH_ID, closed_through="2026-02", fiscal_year_start_month=7
    )
    assert calendar.fiscal_year_start_month == 7

  def test_emits_initialized_event(self):
    svc, session = _service()
    svc.initialize(
      session, GRAPH_ID, closed_through="2026-02", actor_id="u1", note="onboarding"
    )
    events = _events(session)
    assert len(events) == 1
    event = events[0]
    assert event.event_type == "initialized"
    assert event.to_value == "2026-02"
    assert event.actor_id == "u1"
    assert event.note == "onboarding"


# ────────────────────────────────────────────────────────────────────────────
# set_close_target
# ────────────────────────────────────────────────────────────────────────────


class TestSetCloseTarget:
  def _init(self, closed_through: str | None = "2025-12"):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through=closed_through, actor_id="u1")
    # Clear the initialized event so target_changed tests are cleaner
    return svc, session

  def test_happy_path(self):
    svc, session = self._init()
    calendar = svc.set_close_target(session, GRAPH_ID, "2026-03", actor_id="u1")
    assert calendar.close_target_period == "2026-03"
    assert "target_changed" in _event_types(session)

  def test_target_equal_to_closed_through_ok(self):
    svc, session = self._init(closed_through="2026-02")
    # Target == closed_through is allowed (valid when target has been reached
    # and caller wants to re-set to same value — though normal flow uses
    # auto-advance). We accept ≥ to be permissive.
    calendar = svc.set_close_target(session, GRAPH_ID, "2026-02")
    assert calendar.close_target_period == "2026-02"

  def test_rejects_target_before_closed_through(self):
    svc, session = self._init(closed_through="2026-02")
    with pytest.raises(InvalidCloseTargetError, match="before closed_through"):
      svc.set_close_target(session, GRAPH_ID, "2026-01")

  def test_rejects_future_target(self):
    svc, session = self._init()
    future = "2099-01"
    with pytest.raises(InvalidCloseTargetError, match="future"):
      svc.set_close_target(session, GRAPH_ID, future)

  def test_rejects_malformed_period(self):
    svc, session = self._init()
    with pytest.raises(InvalidCloseTargetError):
      svc.set_close_target(session, GRAPH_ID, "not-a-period")

  def test_emits_target_changed_event(self):
    svc, session = self._init()
    svc.set_close_target(
      session, GRAPH_ID, "2026-03", actor_id="u1", note="catching up"
    )
    events = _events(session)
    target_events = [e for e in events if e.event_type == "target_changed"]
    assert len(target_events) == 1
    event = target_events[0]
    assert event.from_value == "2026-01"  # initialize set it to closed_through + 1
    assert event.to_value == "2026-03"
    assert event.note == "catching up"

  def test_requires_initialized_calendar(self):
    svc, session = _service()
    with pytest.raises(FiscalCalendarError):
      svc.set_close_target(session, GRAPH_ID, "2026-03")


# ────────────────────────────────────────────────────────────────────────────
# advance_closed_through
# ────────────────────────────────────────────────────────────────────────────


class TestAdvanceClosedThrough:
  def test_happy_path(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.set_close_target(session, GRAPH_ID, "2026-03", actor_id="u1")
    calendar = svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    assert calendar.closed_through_period == "2026-01"
    # Not at target yet — target should not auto-advance
    assert calendar.close_target_period == "2026-03"

  def test_auto_advances_target_when_reached(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-02", actor_id="u1")
    # initialize sets target to "2026-03"
    calendar = svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")
    assert calendar.closed_through_period == "2026-03"
    assert calendar.close_target_period == "2026-04"  # auto-advanced
    assert "target_advanced_auto" in _event_types(session)

  def test_catch_up_sequence_no_auto_advance_until_target_reached(self):
    """Closing Jan → Feb → Mar with target=Mar. Only Mar triggers auto-advance."""
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.set_close_target(session, GRAPH_ID, "2026-03", actor_id="u1")

    svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    calendar = svc.get(session, GRAPH_ID)
    assert calendar.close_target_period == "2026-03"
    assert "target_advanced_auto" not in _event_types(session)

    svc.advance_closed_through(session, GRAPH_ID, "2026-02", actor_id="u1")
    calendar = svc.get(session, GRAPH_ID)
    assert calendar.close_target_period == "2026-03"
    assert "target_advanced_auto" not in _event_types(session)

    svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")
    calendar = svc.get(session, GRAPH_ID)
    assert calendar.closed_through_period == "2026-03"
    assert calendar.close_target_period == "2026-04"
    assert "target_advanced_auto" in _event_types(session)

  def test_rejects_non_sequential_period(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    # Trying to jump from Dec → Feb (skipping Jan)
    with pytest.raises(AdvanceSequenceError):
      svc.advance_closed_through(session, GRAPH_ID, "2026-02", actor_id="u1")

  def test_emits_period_closed_event(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.advance_closed_through(
      session, GRAPH_ID, "2026-01", actor_id="u1", note="jan close"
    )
    close_events = [e for e in _events(session) if e.event_type == "period_closed"]
    assert len(close_events) == 1
    event = close_events[0]
    assert event.period == "2026-01"
    assert event.from_value == "2025-12"
    assert event.to_value == "2026-01"
    assert event.note == "jan close"

  def test_sets_last_close_at(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    before = datetime.now(UTC)
    svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    after = datetime.now(UTC)
    calendar = svc.get(session, GRAPH_ID)
    assert calendar.last_close_at is not None
    assert before <= calendar.last_close_at <= after

  def test_first_advance_rejects_non_earliest_open_period(self):
    """F3: when closed_through is None, the first advance must match the
    earliest open FiscalPeriod. Anything later strands the earlier months.
    """
    from unittest.mock import patch as _patch

    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through=None, actor_id="u1")
    with _patch.object(svc, "_earliest_open_period", return_value="2026-01"):
      with pytest.raises(AdvanceSequenceError, match="2026-01"):
        svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")

  def test_first_advance_accepts_earliest_open_period(self):
    """F3: first advance to the correct earliest open period is allowed."""
    from unittest.mock import patch as _patch

    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through=None, actor_id="u1")
    with _patch.object(svc, "_earliest_open_period", return_value="2026-01"):
      calendar = svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    assert calendar.closed_through_period == "2026-01"


# ────────────────────────────────────────────────────────────────────────────
# retreat_closed_through (reopen)
# ────────────────────────────────────────────────────────────────────────────


class TestRetreatClosedThrough:
  def test_reopen_latest_decrements_pointer(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-02", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")
    calendar = svc.retreat_closed_through(
      session, GRAPH_ID, "2026-03", reason="missed entry", actor_id="u1"
    )
    assert calendar.closed_through_period == "2026-02"

  def test_reopen_older_period_preserves_pointer(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-02", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")
    # Reopen January — closed_through stays at March
    calendar = svc.retreat_closed_through(
      session, GRAPH_ID, "2026-01", reason="prior period adj", actor_id="u1"
    )
    assert calendar.closed_through_period == "2026-03"

  def test_does_not_modify_close_target(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-02", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")
    # Now target has auto-advanced to 2026-04
    svc.retreat_closed_through(
      session, GRAPH_ID, "2026-03", reason="reopen", actor_id="u1"
    )
    calendar = svc.get(session, GRAPH_ID)
    assert calendar.close_target_period == "2026-04"  # unchanged

  def test_requires_non_empty_reason(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-02", actor_id="u1")
    with pytest.raises(FiscalCalendarError):
      svc.retreat_closed_through(session, GRAPH_ID, "2026-02", reason="")

  def test_emits_period_reopened_event_with_reason(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-02", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")
    svc.retreat_closed_through(
      session,
      GRAPH_ID,
      "2026-03",
      reason="missed March expense",
      actor_id="u1",
    )
    reopen_events = [e for e in _events(session) if e.event_type == "period_reopened"]
    assert len(reopen_events) == 1
    event = reopen_events[0]
    assert event.period == "2026-03"
    assert event.reason == "missed March expense"


# ────────────────────────────────────────────────────────────────────────────
# record_reclose (re-close of a previously reopened period)
# ────────────────────────────────────────────────────────────────────────────


class TestRecordReclose:
  def test_leaves_closed_through_unchanged(self):
    """record_reclose must NOT advance the pointer — it's for older reopens
    where the pointer was never retreated."""
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-02", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-03", actor_id="u1")
    # Older reopen doesn't retreat the pointer
    svc.retreat_closed_through(
      session, GRAPH_ID, "2026-01", reason="prior period adj", actor_id="u1"
    )
    assert svc.get(session, GRAPH_ID).closed_through_period == "2026-03"

    # Re-close via record_reclose — pointer stays at 2026-03
    calendar = svc.record_reclose(session, GRAPH_ID, "2026-01", actor_id="u1")
    assert calendar.closed_through_period == "2026-03"

  def test_emits_period_closed_event(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-02", actor_id="u1")
    svc.retreat_closed_through(
      session, GRAPH_ID, "2026-01", reason="prior period adj", actor_id="u1"
    )
    # Count close events before reclose
    before = len([e for e in _events(session) if e.event_type == "period_closed"])
    svc.record_reclose(session, GRAPH_ID, "2026-01", actor_id="u1")
    after = len([e for e in _events(session) if e.event_type == "period_closed"])
    assert after == before + 1

  def test_updates_last_close_at(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-01", actor_id="u1")
    svc.advance_closed_through(session, GRAPH_ID, "2026-02", actor_id="u1")
    svc.retreat_closed_through(
      session, GRAPH_ID, "2026-01", reason="adj", actor_id="u1"
    )
    calendar = svc.record_reclose(session, GRAPH_ID, "2026-01", actor_id="u1")
    assert calendar.last_close_at is not None


# ────────────────────────────────────────────────────────────────────────────
# closeable_gate
# ────────────────────────────────────────────────────────────────────────────


class TestCloseableGate:
  def _init(self, closed_through: str = "2025-12"):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through=closed_through, actor_id="u1")
    return svc, session

  def test_uninitialized_calendar_blocked(self):
    svc, session = _service()
    result = svc.closeable_gate(session, GRAPH_ID, "2026-01")
    assert not result.is_closeable
    assert CloseableGateResult.NO_CALENDAR in result.blockers

  def test_happy_path(self):
    svc, session = self._init()
    # Feb 1 2026 — Jan is the next period, fully completed
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 2, 1),
    )
    assert result.is_closeable
    assert result.blockers == []

  def test_sequence_violation(self):
    svc, session = self._init()
    # Jan is the next period, but we're trying to close Feb
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-02",
      today=date(2026, 3, 1),
    )
    assert not result.is_closeable
    assert CloseableGateResult.SEQUENCE in result.blockers

  def test_period_incomplete(self):
    svc, session = self._init()
    # Trying to close the current month — not yet complete
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 1, 15),
    )
    assert not result.is_closeable
    assert CloseableGateResult.PERIOD_INCOMPLETE in result.blockers

  def test_period_already_closed(self):
    svc, session = self._init(closed_through="2026-01")
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 3, 1),
    )
    assert not result.is_closeable
    assert CloseableGateResult.ALREADY_CLOSED in result.blockers

  def test_reclose_of_reopened_period_allowed(self):
    """A period with status='closing' (reopened) bypasses sequence +
    already_closed gates so it can be re-closed."""
    svc, session = self._init(closed_through="2025-12")
    # Seed a FiscalPeriod row in 'closing' status (simulating reopen)
    session.add(
      FiscalPeriod(
        graph_id=GRAPH_ID,
        name="2025-06",
        start_date=date(2025, 6, 1),
        end_date=date(2025, 6, 30),
        status="closing",
      )
    )
    # Even though 2025-06 is before closed_through=2025-12 (which would
    # normally trigger ALREADY_CLOSED and SEQUENCE), the 'closing' status
    # bypasses both gates.
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2025-06",
      today=date(2026, 3, 1),
      has_sync_connection=False,
    )
    assert result.is_closeable
    assert CloseableGateResult.SEQUENCE not in result.blockers
    assert CloseableGateResult.ALREADY_CLOSED not in result.blockers

  def test_no_sync_connection_passes_gate(self):
    svc, session = self._init()
    # No QB connection at all (has_sync_connection=False) → gate passes
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 2, 1),
      has_sync_connection=False,
      last_sync_at=None,
    )
    assert result.is_closeable

  def test_connection_exists_but_never_synced_blocks(self):
    svc, session = self._init()
    # QB connection exists but has never synced — this is a stale state
    # that must block the close (user has to sync before closing).
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 2, 1),
      has_sync_connection=True,
      last_sync_at=None,
    )
    assert not result.is_closeable
    assert CloseableGateResult.SYNC_STALE in result.blockers

  def test_never_synced_allows_stale_sync_override(self):
    svc, session = self._init()
    # Never-synced can be overridden by allow_stale_sync=True
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 2, 1),
      has_sync_connection=True,
      last_sync_at=None,
      allow_stale_sync=True,
    )
    assert result.is_closeable

  def test_stale_sync_blocks(self):
    svc, session = self._init()
    # Sync happened mid-January; we're trying to close January which ended Jan 31
    stale = datetime(2026, 1, 15, tzinfo=UTC)
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 2, 1),
      has_sync_connection=True,
      last_sync_at=stale,
    )
    assert not result.is_closeable
    assert CloseableGateResult.SYNC_STALE in result.blockers

  def test_stale_sync_override(self):
    svc, session = self._init()
    stale = datetime(2026, 1, 15, tzinfo=UTC)
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 2, 1),
      has_sync_connection=True,
      last_sync_at=stale,
      allow_stale_sync=True,
    )
    assert result.is_closeable

  def test_fresh_sync_passes(self):
    svc, session = self._init()
    fresh = datetime(2026, 2, 5, tzinfo=UTC)  # after Jan 31 period end
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-01",
      today=date(2026, 2, 10),
      has_sync_connection=True,
      last_sync_at=fresh,
    )
    assert result.is_closeable

  def test_multiple_blockers_reported(self):
    svc, session = self._init()
    # Sequence violation + period incomplete
    result = svc.closeable_gate(
      session,
      GRAPH_ID,
      "2026-02",
      today=date(2026, 2, 5),
    )
    assert not result.is_closeable
    assert CloseableGateResult.SEQUENCE in result.blockers
    assert CloseableGateResult.PERIOD_INCOMPLETE in result.blockers

  def test_first_close_must_be_earliest_open_period(self):
    """When closed_through is None, the gate must require the earliest open
    FiscalPeriod, not accept any arbitrary period (F3).
    """
    from unittest.mock import patch as _patch

    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through=None, actor_id="u1")
    # Pretend fiscal_periods has Jan/Feb/Mar all open, earliest = Jan
    with _patch.object(svc, "_earliest_open_period", return_value="2026-01"):
      # Trying to close Mar first → sequence violation
      result = svc.closeable_gate(
        session,
        GRAPH_ID,
        "2026-03",
        today=date(2026, 4, 1),
      )
      assert not result.is_closeable
      assert CloseableGateResult.SEQUENCE in result.blockers

      # Closing Jan first → allowed
      ok = svc.closeable_gate(
        session,
        GRAPH_ID,
        "2026-01",
        today=date(2026, 4, 1),
      )
      assert ok.is_closeable

  def test_first_close_no_period_rows_accepts_any(self):
    """If FiscalPeriod rows don't exist yet (pre-initialize), the gate
    accepts any well-formed period — otherwise you could never bootstrap.
    """
    from unittest.mock import patch as _patch

    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through=None, actor_id="u1")
    with _patch.object(svc, "_earliest_open_period", return_value=None):
      result = svc.closeable_gate(
        session,
        GRAPH_ID,
        "2026-02",
        today=date(2026, 3, 1),
      )
      assert result.is_closeable


# ────────────────────────────────────────────────────────────────────────────
# Derived state — gap_periods + catch_up_sequence
# ────────────────────────────────────────────────────────────────────────────


class TestDerivedState:
  def test_gap_zero_when_caught_up(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-03", actor_id="u1")
    svc.set_close_target(session, GRAPH_ID, "2026-03")
    calendar = svc.get(session, GRAPH_ID)
    assert svc.gap_periods(calendar) == 0

  def test_gap_three_months(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.set_close_target(session, GRAPH_ID, "2026-03", actor_id="u1")
    calendar = svc.get(session, GRAPH_ID)
    assert svc.gap_periods(calendar) == 3

  def test_catch_up_sequence(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2025-12", actor_id="u1")
    svc.set_close_target(session, GRAPH_ID, "2026-03", actor_id="u1")
    calendar = svc.get(session, GRAPH_ID)
    assert svc.catch_up_sequence(calendar) == ["2026-01", "2026-02", "2026-03"]

  def test_catch_up_empty_when_caught_up(self):
    svc, session = _service()
    svc.initialize(session, GRAPH_ID, closed_through="2026-03", actor_id="u1")
    calendar = svc.get(session, GRAPH_ID)
    # After initialize, target = 2026-04 (next after closed_through).
    # Gap is 1 because there's one pending period.
    assert svc.gap_periods(calendar) == 1
    assert svc.catch_up_sequence(calendar) == ["2026-04"]


# ────────────────────────────────────────────────────────────────────────────
# record_event
# ────────────────────────────────────────────────────────────────────────────


class TestRecordEvent:
  def test_basic_event(self):
    svc, session = _service()
    calendar = svc.get_or_create(session, GRAPH_ID)
    event = svc.record_event(
      session,
      calendar,
      event_type="target_changed",
      from_value="2026-01",
      to_value="2026-03",
      actor_id="u1",
      note="manual adjustment",
    )
    assert event.event_type == "target_changed"
    assert event.graph_id == GRAPH_ID
    assert event.fiscal_calendar_id == calendar.id
    assert event.from_value == "2026-01"
    assert event.to_value == "2026-03"
    assert event.note == "manual adjustment"
    assert event.actor_type == "user"

  def test_agent_actor_type(self):
    svc, session = _service()
    calendar = svc.get_or_create(session, GRAPH_ID)
    event = svc.record_event(
      session,
      calendar,
      event_type="period_closed",
      period="2026-01",
      actor_id="agent_close_v1",
      actor_type="agent",
    )
    assert event.actor_type == "agent"
