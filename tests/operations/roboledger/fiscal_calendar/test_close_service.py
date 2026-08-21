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
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarService,
  PeriodAlreadyClosedError,
  PeriodCloseService,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampError,
  StatementStampResult,
)

GRAPH_ID = "kg01234567890abcdef"


def _noop_stamper(session, **kwargs):
  """Stub statement stamper for tests exercising the close flow itself.

  The real stamper resolves mapping/style/taxonomy and runs the pivot —
  against these MagicMock sessions it would consume the ordered
  ``session.execute.side_effect`` lists and 'find' a mapping in mock
  truthiness. Injection keeps every pre-existing assertion exact.
  """
  return StatementStampResult(stamped=False, note="no_coa_mapping")


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

  # FiscalPeriod path: filter → populate_existing → with_for_update → one_or_none
  fp_query = MagicMock()
  locked = fp_query.filter.return_value
  locked.populate_existing.return_value = locked
  locked.with_for_update.return_value = locked
  locked.one_or_none.return_value = fp

  # Route session.query to the right mock based on model. Our service calls:
  # session.query(Entry).filter(...).update(...)
  # session.query(FiscalPeriod).filter(...).one_or_none()
  # session.query(Event.id).filter(...) — retracted-event exclusion
  def _query_dispatch(model):
    name = getattr(model, "__name__", "")
    if name == "FiscalPeriod":
      return fp_query
    parent = getattr(model, "class_", None)
    if name == "Event" or getattr(parent, "__name__", "") == "Event":
      retracted = MagicMock()
      retracted.filter.return_value = []
      return retracted
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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

  def test_entries_posted_counts_both_post_paths(self):
    """The QB pre-publish step promotes each published draft before the
     bulk transition runs, so the receipt must sum both paths — counting
     only the bulk pass reported entries_posted=0 while posting everything
    ."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
    # All 36 drafts were published (and promoted) by the pre-publish
    # step; the bulk transition finds none left.
    session = _mock_session_with_fp(
      _fp(status="open"), debit=5000, credit=5000, updated=0
    )

    with patch.object(svc, "_publish_drafts_to_qb", return_value=36):
      result = svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )

    assert result.entries_posted == 36
    assert result.entries_published_to_qb == 36
    assert result.entries_posted_locally == 0

  def test_close_sets_period_status_and_actor(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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

  def test_calendar_advance_runs_before_fiscal_period_flip(self):
    """Never-closed regression: with `closed_through IS NULL`, the
    advance's sequence check resolves the expected close from the
    earliest NON-closed FiscalPeriod. The period being closed must
    therefore still be un-flipped when advance_closed_through runs —
    flipping first made every first close on a fresh calendar reject
    with 'Next period must be <the following month>'."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    fp = _fp(status="open")
    observed = {}

    def _advance(session, graph_id, period, **kwargs):
      observed["fp_status_at_advance"] = fp.status
      return _calendar(closed_through="2026-01", close_target="2026-03")

    fcs.advance_closed_through.side_effect = _advance
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
    session = _mock_session_with_fp(fp)

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert observed["fp_status_at_advance"] == "open"
    assert fp.status == "closed"

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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)

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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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


class TestAlreadyClosedRevalidation:
  def test_closed_status_after_publish_is_rejected(self):
    """Post-publish row lock + revalidation: a loser must not stamp again."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
    session = _mock_session_with_fp(_fp(status="closed"))

    with pytest.raises(PeriodAlreadyClosedError):
      svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )

    fcs.advance_closed_through.assert_not_called()
    fcs.record_reclose.assert_not_called()


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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
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


# ────────────────────────────────────────────────────────────────────────────
# Auto-run rules on close
# ────────────────────────────────────────────────────────────────────────────


class TestCloseAutoRunsRules:
  """Verify the close service evaluates rules for schedules with facts in
  the closing period (rule-engine auto-run)."""

  def _session_with_schedules_and_rule_results(
    self, fp, schedule_ids: list[str], rule_results_per_struct: dict
  ):
    """Build a session whose schedule query returns the given structure
    ids and whose rule eval returns the configured results per id."""
    from unittest.mock import patch

    session = _mock_session_with_fp(fp)

    # Override session.execute to return the BS row first, then the
    # schedule id list for the rule-engine auto-run query.
    bs_row = MagicMock()
    bs_row.total_debit = 0
    bs_row.total_credit = 0
    bs_exec = MagicMock()
    bs_exec.fetchone.return_value = bs_row
    schedules_exec = MagicMock()
    schedules_exec.scalars.return_value.all.return_value = schedule_ids
    # BS preflight, then SET LOCAL lock_timeout for the post-publish
    # FiscalPeriod row lock, then the schedule-id query.
    session.execute.side_effect = [bs_exec, MagicMock(), schedules_exec]

    eval_patcher = patch(
      "robosystems.operations.information_block.rules.engine."
      "evaluate_rules_for_structure",
      side_effect=lambda s, sid, **kw: rule_results_per_struct.get(sid, []),
    )
    return session, eval_patcher

  def test_rule_summary_aggregates_across_schedules(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)

    _r = lambda status: MagicMock(status=status)  # noqa: E731
    session, eval_patcher = self._session_with_schedules_and_rule_results(
      _fp(status="open"),
      schedule_ids=["struct_a", "struct_b"],
      rule_results_per_struct={
        "struct_a": [_r("pass"), _r("pass"), _r("fail")],
        "struct_b": [_r("pass"), _r("error")],
      },
    )

    with eval_patcher as mock_eval:
      result = svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )

    assert result.rule_summary == {"pass": 3, "fail": 1, "error": 1, "skipped": 0}
    assert result.evaluated_structure_ids == ("struct_a", "struct_b")
    assert mock_eval.call_count == 2

  def test_no_schedules_returns_none_summary(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)

    session, eval_patcher = self._session_with_schedules_and_rule_results(
      _fp(status="open"), schedule_ids=[], rule_results_per_struct={}
    )

    with eval_patcher as mock_eval:
      result = svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )

    assert result.rule_summary is None
    assert result.evaluated_structure_ids == ()
    mock_eval.assert_not_called()

  def test_rule_eval_failure_isolated_from_close_success(self):
    """A bare exception out of the rule engine is logged and skipped — the
    close still succeeds with whatever results were tallied beforehand."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)

    _r = lambda status: MagicMock(status=status)  # noqa: E731
    session = _mock_session_with_fp(_fp(status="open"))
    bs_row = MagicMock()
    bs_row.total_debit = 0
    bs_row.total_credit = 0
    bs_exec = MagicMock()
    bs_exec.fetchone.return_value = bs_row
    schedules_exec = MagicMock()
    schedules_exec.scalars.return_value.all.return_value = ["struct_ok", "struct_bad"]
    # BS preflight, then SET LOCAL lock_timeout for the post-publish
    # FiscalPeriod row lock, then the schedule-id query.
    session.execute.side_effect = [bs_exec, MagicMock(), schedules_exec]

    from unittest.mock import patch

    def _eval(session, sid, **kw):
      if sid == "struct_bad":
        raise RuntimeError("schema mismatch")
      return [_r("pass")]

    with patch(
      "robosystems.operations.information_block.rules.engine."
      "evaluate_rules_for_structure",
      side_effect=_eval,
    ):
      result = svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )

    # close succeeded — only the good schedule's results land in the summary
    assert result.rule_summary == {"pass": 1, "fail": 0, "error": 0, "skipped": 0}
    assert result.evaluated_structure_ids == ("struct_ok", "struct_bad")


class TestCloseStampsStatementSets:
  """Step 5b: the close stamps the period's canonical statement sets.

  Contract: stamp outcome rides `PeriodCloseResult`; a soft-skip never
  blocks the close; `StatementStampError` propagates (the command layer
  never commits, so the whole close rolls back); stamping runs after the
  draft→posted transition (the pivot reads posted entries only) and
  before the schedule-rule pass; reclose stamps too (replace semantics
  live inside the stamper).
  """

  def test_stamp_result_rides_close_result(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    sets = {"struct_bs": "fs_bs", "struct_is": "fs_is"}
    summary = {"pass": 4, "fail": 1, "error": 0, "skipped": 0}

    def _stamper(session, **kwargs):
      return StatementStampResult(stamped=True, fact_set_ids=sets, rule_summary=summary)

    svc = PeriodCloseService(fcs, statement_stamper=_stamper)
    session = _mock_session_with_fp(_fp(status="open"))

    result = svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert result.statements_stamped is True
    assert result.statement_stamp_note is None
    assert result.stamped_statement_sets == sets
    assert result.statement_rule_summary == summary

  def test_soft_skip_keeps_close_succeeding(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    svc = PeriodCloseService(fcs, statement_stamper=_noop_stamper)
    fp = _fp(status="open")
    session = _mock_session_with_fp(fp)

    result = svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert fp.status == "closed"
    assert result.statements_stamped is False
    assert result.statement_stamp_note == "no_coa_mapping"
    assert result.stamped_statement_sets == {}

  def test_stamp_error_propagates(self):
    """Reporting is configured but the pivot failed — the exception must
    reach the command layer so nothing commits (full close rollback)."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))

    def _stamper(session, **kwargs):
      raise StatementStampError("pivot exploded")

    svc = PeriodCloseService(fcs, statement_stamper=_stamper)
    session = _mock_session_with_fp(_fp(status="open"))

    with pytest.raises(StatementStampError):
      svc.close(
        session,
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        has_sync_connection=False,
        last_sync_at=None,
      )

  def test_stamp_runs_after_post_and_before_schedule_rules(self):
    """At stamp time the FiscalPeriod is already closed (drafts posted)
    and only the BS-preflight execute plus the post-publish lock_timeout
    SET have run — the schedule-rule query hasn't happened yet."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    fp = _fp(status="open")
    observed = {}

    def _stamper(session, **kwargs):
      observed["fp_status"] = fp.status
      observed["execute_calls"] = session.execute.call_count
      return StatementStampResult(stamped=True)

    svc = PeriodCloseService(fcs, statement_stamper=_stamper)
    session = _mock_session_with_fp(fp)

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert observed["fp_status"] == "closed"
    assert observed["execute_calls"] == 2

  def test_stamper_receives_period_window_and_actor(self):
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True))
    captured = {}

    def _stamper(session, **kwargs):
      captured.update(kwargs)
      return StatementStampResult(stamped=True)

    svc = PeriodCloseService(fcs, statement_stamper=_stamper)
    session = _mock_session_with_fp(_fp(status="open"))

    svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_close",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert captured["graph_id"] == GRAPH_ID
    assert captured["actor_id"] == "usr_close"
    assert captured["period_start"] == datetime(2026, 1, 1).date()
    assert captured["period_end"] == datetime(2026, 1, 31).date()

  def test_reclose_also_stamps(self):
    """A reclose (status='closing') replaces the month's canonical sets —
    the stamper must run on that path too."""
    fcs = _mock_fcs(gate_result=CloseableGateResult(is_closeable=True), is_latest=True)
    calls = []

    def _stamper(session, **kwargs):
      calls.append(kwargs["period_end"])
      return StatementStampResult(stamped=True)

    svc = PeriodCloseService(fcs, statement_stamper=_stamper)
    session = _mock_session_with_fp(_fp(status="closing"))

    result = svc.close(
      session,
      GRAPH_ID,
      "2026-01",
      actor_id="usr_1",
      has_sync_connection=False,
      last_sync_at=None,
    )

    assert result.was_reclose is True
    assert len(calls) == 1


class TestClosePrePublishWriteback:
  """`_publish_drafts_to_qb` runs between BS pre-flight and draft→posted.
  QB rejections raise `WritebackFailed` and roll back the entire close."""

  def test_no_qb_authoritative_connection_skips_pre_publish(self):
    """Graph has no QB connection in qb_authoritative mode → no work
    done, close proceeds as today (existing behavior preserved)."""
    from unittest.mock import patch

    from robosystems.operations.roboledger.fiscal_calendar.close_service import (
      PeriodCloseService,
    )

    svc = PeriodCloseService()
    session = MagicMock()
    mock_platform_session = MagicMock()
    mock_platform_session.__enter__ = MagicMock(return_value=mock_platform_session)
    mock_platform_session.__exit__ = MagicMock(return_value=False)
    # No qb_authoritative connection.
    mock_platform_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    with patch(
      "robosystems.database.SessionFactory", return_value=mock_platform_session
    ):
      # Should return without touching the extensions session.
      svc._publish_drafts_to_qb(
        session,
        graph_id=GRAPH_ID,
        period_start=datetime(2026, 5, 1).date(),
        period_end=datetime(2026, 5, 31).date(),
        actor_id="usr_1",
      )

    # No drafts queried — we returned at the no-connection branch.
    session.query.assert_not_called()

  def test_qb_rejection_raises_writeback_failed(self):
    """A single QB rejection in the batch collects into WritebackFailed
    and rolls back the close (rest of close.flow not executed)."""
    from unittest.mock import patch

    from robosystems.models.api.event_block import ExecuteEventBlockResponse
    from robosystems.operations.roboledger.fiscal_calendar.close_service import (
      PeriodCloseService,
      WritebackFailed,
    )

    svc = PeriodCloseService()
    session = MagicMock()

    # Two draft entries triggered by manual events.
    entry1 = MagicMock(id="ent_1", memo="Test 1", posting_date="2026-05-10")
    event1 = MagicMock(id="evt_1", source="manual")
    entry2 = MagicMock(id="ent_2", memo="Test 2", posting_date="2026-05-15")
    event2 = MagicMock(id="evt_2", source="schedule")

    session.query.return_value.join.return_value.filter.return_value.all.return_value = [
      (entry1, event1),
      (entry2, event2),
    ]

    # Mock the platform-DB connection lookup.
    mock_conn = MagicMock(id="conn_qb_1")
    mock_platform_session = MagicMock()
    mock_platform_session.__enter__ = MagicMock(return_value=mock_platform_session)
    mock_platform_session.__exit__ = MagicMock(return_value=False)
    mock_platform_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = mock_conn

    def fake_execute(_sess, body, created_by, **_kwargs):
      # First event accepts, second rejects.
      if body.event_id == "evt_1":
        return ExecuteEventBlockResponse(
          event_id="evt_1",
          status="fulfilled",
          qb_external_id="QB_TXN_AA",
          qb_error=None,
        )
      return ExecuteEventBlockResponse(
        event_id="evt_2",
        status="pending",
        qb_external_id=None,
        qb_error={"code": "validation", "message": "Closed in QB"},
      )

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_platform_session),
      patch(
        "robosystems.operations.event_block.commands.execute_event_block",
        side_effect=fake_execute,
      ),
    ):
      with pytest.raises(WritebackFailed) as exc_info:
        svc._publish_drafts_to_qb(
          session,
          graph_id=GRAPH_ID,
          period_start=datetime(2026, 5, 1).date(),
          period_end=datetime(2026, 5, 31).date(),
          actor_id="usr_1",
        )

    # The failure carries both offenders' detail (only evt_2 failed,
    # but the structure shows the failure trail).
    assert len(exc_info.value.failed_events) == 1
    assert exc_info.value.failed_events[0]["event_id"] == "evt_2"
    assert exc_info.value.failed_events[0]["qb_error"]["code"] == "validation"
