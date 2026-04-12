"""Unit tests for PeriodCloseService.

These tests exercise the close flow logic end-to-end using mocked session
interactions. They cover:

- Happy path (normal advance close)
- Gate failures translated to CloseGateFailed
- Unbalanced ledger detected pre-flight (no state mutation)
- Period not found
- Re-close routing: latest reopen → advance; older reopen → record_reclose
- allow_stale_sync audit note annotation
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from robosystems.operations.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarService,
  PeriodCloseService,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.fiscal_calendar.service import CloseableGateResult

GRAPH_ID = "kg01234567890abcdef"


def _fp(status: str, name: str = "2026-01"):
  fp = MagicMock()
  fp.name = name
  fp.status = status
  fp.closed_at = None
  fp.closed_by = None
  return fp


def _calendar(
  closed_through: str | None = "2025-12", close_target: str | None = "2026-03"
):
  cal = MagicMock()
  cal.closed_through_period = closed_through
  cal.close_target_period = close_target
  cal.last_close_at = None
  return cal


def _mock_fcs(
  *,
  gate_result: CloseableGateResult,
  cal_before=None,
  advance_return=None,
  reclose_return=None,
  is_latest: bool = True,
):
  fcs = MagicMock(spec=FiscalCalendarService)
  fcs.closeable_gate.return_value = gate_result
  fcs.get.return_value = cal_before or _calendar()
  fcs.advance_closed_through.return_value = advance_return or _calendar(
    closed_through="2026-01", close_target="2026-03"
  )
  fcs.record_reclose.return_value = reclose_return or _calendar(
    closed_through="2025-12", close_target="2026-03"
  )
  fcs.is_latest_sequential_close.return_value = is_latest
  return fcs


def _mock_session_with_fp(fp, debit: int = 0, credit: int = 0, updated: int = 0):
  """Build a mocked session where:

  - Entry bulk-update returns `updated`
  - FiscalPeriod one_or_none() returns `fp`
  - BS equation SUM query returns (debit, credit)
  """
  session = MagicMock()

  # Entry.update() path
  entry_query = MagicMock()
  entry_query.filter.return_value.update.return_value = updated

  # FiscalPeriod.first() path
  fp_query = MagicMock()
  fp_query.filter.return_value.one_or_none.return_value = fp

  # Route session.query to the right mock based on model. Our service calls:
  # session.query(Entry).filter(...).update(...)
  # session.query(FiscalPeriod).filter(...).one_or_none()
  def _query_dispatch(model):
    name = model.__name__ if hasattr(model, "__name__") else str(model)
    if name == "FiscalPeriod":
      return fp_query
    return entry_query

  session.query.side_effect = _query_dispatch

  # BS equation SQL execute
  bs_row = MagicMock()
  bs_row.total_debit = debit
  bs_row.total_credit = credit
  exec_mock = MagicMock()
  exec_mock.fetchone.return_value = bs_row
  session.execute.return_value = exec_mock

  session.flush = MagicMock()
  return session


# ────────────────────────────────────────────────────────────────────────────
# Happy path
# ────────────────────────────────────────────────────────────────────────────


class TestCloseHappyPath:
  def test_normal_close_calls_advance(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(
      _fp(status="open"), debit=5000, credit=5000, updated=3
    )

    result = svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert result.entries_posted == 3
    assert result.was_reclose is False
    fcs.advance_closed_through.assert_called_once()
    fcs.record_reclose.assert_not_called()

  def test_close_sets_period_status_and_actor(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs)
    fp = _fp(status="open")
    session = _mock_session_with_fp(fp)

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_test",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert fp.status == "closed"
    assert fp.closed_by == "usr_test"
    assert isinstance(fp.closed_at, datetime)

  def test_target_auto_advanced_flag(self):
    """When advance_closed_through returns a calendar with a different
    close_target than before, the result reports target_auto_advanced=True."""
    cal_before = _calendar(closed_through="2025-12", close_target="2026-01")
    cal_after = _calendar(closed_through="2026-01", close_target="2026-02")
    fcs = _mock_fcs(
      gate_result=CloseableGateResult(is_closeable=True),
      cal_before=cal_before,
      advance_return=cal_after,
    )
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(_fp(status="open"))

    result = svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )
    assert result.target_auto_advanced is True


# ────────────────────────────────────────────────────────────────────────────
# Gate failures
# ────────────────────────────────────────────────────────────────────────────


class TestCloseGateFailure:
  def test_raises_close_gate_failed_with_blockers(self):
    fcs = _mock_fcs(
      gate_result=CloseableGateResult(
        is_closeable=False,
        blockers=[CloseableGateResult.SEQUENCE],
      )
    )
    svc = PeriodCloseService(fcs)
    session = MagicMock()

    with pytest.raises(CloseGateFailed) as exc_info:
      svc.close(
        session,
        GRAPH_ID,
        "2026-03",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )
    assert CloseableGateResult.SEQUENCE in exc_info.value.blockers
    assert exc_info.value.no_calendar is False

  def test_no_calendar_blocker_flagged(self):
    fcs = _mock_fcs(
      gate_result=CloseableGateResult(
        is_closeable=False,
        blockers=[CloseableGateResult.NO_CALENDAR],
      )
    )
    svc = PeriodCloseService(fcs)

    with pytest.raises(CloseGateFailed) as exc_info:
      svc.close(
        MagicMock(),
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )
    assert exc_info.value.no_calendar is True


# ────────────────────────────────────────────────────────────────────────────
# Unbalanced ledger — preflight check
# ────────────────────────────────────────────────────────────────────────────


class TestUnbalancedLedger:
  def test_preflight_raises_before_mutation(self):
    """The BS check runs BEFORE any state mutation, so raising it leaves
    the session unchanged (no rollback needed)."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(_fp(status="open"), debit=1000, credit=900)

    with pytest.raises(UnbalancedLedgerError) as exc_info:
      svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )

    assert exc_info.value.total_debit == 1000
    assert exc_info.value.total_credit == 900
    # Calendar advance was NOT called — we raised before mutation
    fcs.advance_closed_through.assert_not_called()
    fcs.record_reclose.assert_not_called()


# ────────────────────────────────────────────────────────────────────────────
# Period not found
# ────────────────────────────────────────────────────────────────────────────


class TestPeriodNotFound:
  def test_raises_when_fiscal_period_missing(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(None)

    with pytest.raises(PeriodNotFoundError):
      svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )


# ────────────────────────────────────────────────────────────────────────────
# Re-close routing
# ────────────────────────────────────────────────────────────────────────────


class TestRecloseRouting:
  def test_latest_reopen_routes_to_advance(self):
    """Reopened FiscalPeriod + is_latest_sequential_close=True → advance."""
    fcs = _mock_fcs(
      gate_result=CloseableGateResult(is_closeable=True),
      is_latest=True,
    )
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(_fp(status="closing"))

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    fcs.advance_closed_through.assert_called_once()
    fcs.record_reclose.assert_not_called()

  def test_older_reopen_routes_to_record_reclose(self):
    """Reopened FiscalPeriod + is_latest_sequential_close=False → record_reclose."""
    fcs = _mock_fcs(
      gate_result=CloseableGateResult(is_closeable=True),
      is_latest=False,
    )
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(_fp(status="closing", name="2025-06"))

    result = svc.close(
      session,
      GRAPH_ID,
      "2025-06",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    fcs.record_reclose.assert_called_once()
    fcs.advance_closed_through.assert_not_called()
    assert result.was_reclose is True


# ────────────────────────────────────────────────────────────────────────────
# allow_stale_sync audit note
# ────────────────────────────────────────────────────────────────────────────


class TestStaleSyncAudit:
  """When a user overrides the sync gate, the audit note records it."""

  def test_override_annotates_note(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(_fp(status="open"))

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=True,
      last_sync_at=None,
      allow_stale_sync=True,
      note="month-end close",
    )

    call = fcs.advance_closed_through.call_args
    note = call.kwargs["note"]
    assert "sync gate overridden" in note
    assert "month-end close" in note

  def test_override_without_sync_connection_is_not_annotated(self):
    """No sync connection → allow_stale_sync is a no-op, no annotation."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(_fp(status="open"))

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
      allow_stale_sync=True,
      note="plain note",
    )

    call = fcs.advance_closed_through.call_args
    assert call.kwargs["note"] == "plain note"

  def test_no_override_passes_note_unchanged(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs)
    session = _mock_session_with_fp(_fp(status="open"))

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=True,
      last_sync_at=datetime(2026, 2, 1, tzinfo=UTC),
      allow_stale_sync=False,
      note="month-end close",
    )

    call = fcs.advance_closed_through.call_args
    assert call.kwargs["note"] == "month-end close"
