"""Unit tests for the fiscal-calendar command wrappers.

Focus: the close-time statement-stamping surface — `close_period` maps
the stamp outcome onto `ClosePeriodResponse`, `reopen_period` retracts
the reopened month's canonical statement sets (returning
`ReopenPeriodResult` with the count), and `backfill_plan_history`
compiles closed history through chunked reopen → reclose cycles.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.fiscal_calendar import (
  BackfillPlanHistoryRequest,
  FiscalCalendarResponse,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  BackfillPreconditionError,
  PeriodNotClosedError,
  PeriodNotFoundInLedgerError,
  ReopenPeriodResult,
  backfill_plan_history,
  close_period,
  reopen_period,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  PeriodCloseResult,
  UnbalancedLedgerError,
)

GRAPH_ID = "kg01234567890abcdef"

_MOD = "robosystems.operations.roboledger.commands.fiscal_calendar"


def _fc_response() -> FiscalCalendarResponse:
  return FiscalCalendarResponse(graph_id=GRAPH_ID, fiscal_year_start_month=1)


def _close_result(**overrides) -> PeriodCloseResult:
  defaults = {
    "period": "2026-01",
    "entries_posted": 3,
    "target_auto_advanced": False,
    "calendar": MagicMock(),
    "was_reclose": False,
    "rule_summary": None,
    "evaluated_structure_ids": (),
    "statements_stamped": True,
    "statement_stamp_note": None,
    "stamped_statement_sets": {"struct_bs": "fs_bs"},
    "statement_rule_summary": {"pass": 3, "fail": 0, "error": 0, "skipped": 0},
  }
  defaults.update(overrides)
  return PeriodCloseResult(**defaults)


class TestClosePeriodResponseMapping:
  def _run(self, result: PeriodCloseResult):
    close_service = MagicMock()
    close_service.close.return_value = result
    with (
      patch(f"{_MOD}.qb_sync_state", return_value=(False, None)),
      patch(f"{_MOD}.build_fiscal_calendar_response", return_value=_fc_response()),
    ):
      return close_period(
        MagicMock(),
        MagicMock(),
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        allow_stale_sync=False,
        note=None,
        service=MagicMock(),
        close_service=close_service,
      )

  def test_stamp_fields_ride_the_response(self):
    resp = self._run(_close_result())
    assert resp.statements_stamped is True
    assert resp.statement_stamp_note is None
    assert resp.stamped_statement_sets == {"struct_bs": "fs_bs"}
    assert resp.statement_rule_summary == {
      "pass": 3,
      "fail": 0,
      "error": 0,
      "skipped": 0,
    }

  def test_soft_skip_maps_note(self):
    resp = self._run(
      _close_result(
        statements_stamped=False,
        statement_stamp_note="no_coa_mapping",
        stamped_statement_sets={},
        statement_rule_summary=None,
      )
    )
    assert resp.statements_stamped is False
    assert resp.statement_stamp_note == "no_coa_mapping"
    assert resp.stamped_statement_sets == {}
    assert resp.statement_rule_summary is None


class TestReopenRetractsCanonicalSets:
  def _run(self, fp_status="closed", retracted=("fs_a", "fs_b")):
    session = MagicMock()
    fp = MagicMock()
    fp.status = fp_status
    session.query.return_value.filter.return_value.one_or_none.return_value = fp

    retract = MagicMock(return_value=list(retracted))
    with (
      patch(f"{_MOD}.qb_sync_state", return_value=(False, None)),
      patch(f"{_MOD}.build_fiscal_calendar_response", return_value=_fc_response()),
      patch(
        "robosystems.operations.roboledger.commands.schedules."
        "reinstate_reopened_schedule_scopes"
      ),
      patch(
        "robosystems.operations.roboledger.reports.statement_sets."
        "retract_canonical_statement_sets",
        retract,
      ),
    ):
      result = reopen_period(
        session,
        MagicMock(),
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        reason="missed accrual",
        note=None,
        service=MagicMock(),
      )
    return result, retract, fp

  def test_returns_result_with_retraction_count(self):
    result, retract, fp = self._run()
    assert isinstance(result, ReopenPeriodResult)
    assert result.statement_sets_retracted == 2
    assert isinstance(result.fiscal_calendar, FiscalCalendarResponse)
    assert fp.status == "closing"

  def test_retraction_keyed_by_month_window(self):
    _, retract, _ = self._run()
    kwargs = retract.call_args.kwargs
    assert kwargs["period_start"] == date(2026, 1, 1)
    assert kwargs["period_end"] == date(2026, 1, 31)

  def test_month_without_canonical_sets_counts_zero(self):
    result, _, _ = self._run(retracted=())
    assert result.statement_sets_retracted == 0

  def test_period_not_found_raises(self):
    session = MagicMock()
    session.query.return_value.filter.return_value.one_or_none.return_value = None
    with pytest.raises(PeriodNotFoundInLedgerError):
      reopen_period(
        session,
        MagicMock(),
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        reason="r",
        note=None,
        service=MagicMock(),
      )

  def test_period_not_closed_raises(self):
    session = MagicMock()
    fp = MagicMock()
    fp.status = "open"
    session.query.return_value.filter.return_value.one_or_none.return_value = fp
    with pytest.raises(PeriodNotClosedError):
      reopen_period(
        session,
        MagicMock(),
        GRAPH_ID,
        "2026-01",
        actor_id="usr_1",
        reason="r",
        note=None,
        service=MagicMock(),
      )


class TestBackfillPlanHistory:
  """Chunked reopen → reclose compilation of closed monthly history.

  The calendar boundary is 2026-02 and ledger data starts 2025-11-03,
  so the full candidate range is 2025-11 .. 2026-02 (4 months).
  """

  CLOSED_THROUGH = "2026-02"
  EARLIEST = date(2025, 11, 3)

  def _service(self, closed_through=CLOSED_THROUGH, rows_created=0):
    service = MagicMock()
    calendar = MagicMock()
    calendar.closed_through_period = closed_through
    service.require.return_value = calendar
    service.ensure_fiscal_periods.return_value = rows_created
    return service

  def _session(self, earliest=EARLIEST, draft_counts=(), fp_statuses=()):
    """Session mock that routes by query target.

    ``draft_counts`` / ``fp_statuses`` are consumed one per attempted
    month, in order.
    """
    session = MagicMock()
    draft_iter = iter(draft_counts)
    fp_iter = iter(fp_statuses)

    def _query(arg):
      q = MagicMock()
      if arg is Entry:
        q.filter.return_value.count.side_effect = lambda: next(draft_iter)
      elif arg is FiscalPeriod:
        fp = MagicMock()
        fp.status = next(fp_iter)
        q.filter.return_value.one.return_value = fp
      else:  # the min(posting_date) expression
        q.scalar.return_value = earliest
      return q

    session.query.side_effect = _query
    return session

  def _run(
    self,
    session,
    service,
    body=None,
    stamped_windows=frozenset(),
    close_side_effect=None,
  ):
    """Run the backfill with the reopen/close commands patched out.

    ``stamped_windows`` holds YYYY-MM months that already have canonical
    sets (the idempotency skip).
    """
    body = body or BackfillPlanHistoryRequest()

    def _has_sets(_session, *, period_start, period_end):
      return period_start.strftime("%Y-%m") in stamped_windows

    close_result = MagicMock()
    close_result.statements_stamped = True
    close_result.statement_stamp_note = None
    close_result.statement_rule_summary = {
      "pass": 3,
      "fail": 0,
      "error": 0,
      "skipped": 0,
    }
    close_mock = MagicMock(return_value=close_result, side_effect=close_side_effect)
    reopen_mock = MagicMock()
    with (
      patch(f"{_MOD}.reopen_period", reopen_mock),
      patch(f"{_MOD}.close_period", close_mock),
      patch(f"{_MOD}.qb_sync_state", return_value=(False, None)),
      patch(f"{_MOD}.build_fiscal_calendar_response", return_value=_fc_response()),
      patch(
        "robosystems.operations.roboledger.reports.statement_sets."
        "has_canonical_statement_sets",
        side_effect=_has_sets,
      ),
    ):
      response = backfill_plan_history(
        session,
        MagicMock(),
        GRAPH_ID,
        body,
        actor_id="usr_1",
        service=service,
        close_service=MagicMock(),
      )
    return response, reopen_mock, close_mock

  def test_nothing_closed_raises(self):
    with pytest.raises(BackfillPreconditionError) as exc_info:
      self._run(self._session(), self._service(closed_through=None))
    assert exc_info.value.code == "nothing_closed"

  def test_no_ledger_data_raises(self):
    with pytest.raises(BackfillPreconditionError) as exc_info:
      self._run(self._session(earliest=None), self._service())
    assert exc_info.value.code == "no_ledger_data"

  def test_start_after_boundary_raises(self):
    with pytest.raises(BackfillPreconditionError) as exc_info:
      self._run(
        self._session(),
        self._service(),
        body=BackfillPlanHistoryRequest(start_period="2026-03"),
      )
    assert exc_info.value.code == "start_after_boundary"

  def test_clamps_start_to_earliest_data(self):
    service = self._service(rows_created=4)
    session = self._session(draft_counts=[0] * 4, fp_statuses=["closed"] * 4)
    response, _, _ = self._run(
      session,
      service,
      body=BackfillPlanHistoryRequest(start_period="2019-01"),
    )
    assert response.earliest_available_period == "2025-11"
    assert response.effective_start_period == "2025-11"
    assert response.period_rows_created == 4
    service.ensure_fiscal_periods.assert_called_once_with(
      session,
      GRAPH_ID,
      start_period="2025-11",
      end_period=self.CLOSED_THROUGH,
      closed_through=self.CLOSED_THROUGH,
    )

  def test_already_stamped_months_untouched(self):
    session = self._session(draft_counts=[0, 0], fp_statuses=["closed", "closed"])
    response, reopen_mock, close_mock = self._run(
      session,
      self._service(),
      stamped_windows={"2025-11", "2025-12"},
    )
    attempted = [outcome.period for outcome in response.processed]
    assert attempted == ["2026-01", "2026-02"]
    assert all(o.status == "stamped" for o in response.processed)
    assert response.remaining_periods == []
    assert reopen_mock.call_count == 2
    assert close_mock.call_count == 2

  def test_restamp_re_derives_already_stamped_months(self):
    """restamp=true widens the walk to every month in range — the
    healing pass after an engine improvement changes what a stamp
    produces (e.g. the two-period CF derivation)."""
    session = self._session(draft_counts=[0] * 4, fp_statuses=["closed"] * 4)
    response, reopen_mock, close_mock = self._run(
      session,
      self._service(),
      body=BackfillPlanHistoryRequest(restamp=True),
      stamped_windows={"2025-11", "2025-12"},
    )
    attempted = [outcome.period for outcome in response.processed]
    assert attempted == ["2025-11", "2025-12", "2026-01", "2026-02"]
    assert all(o.status == "stamped" for o in response.processed)
    assert reopen_mock.call_count == 4
    assert close_mock.call_count == 4

  def test_chunking_respects_max_periods(self):
    session = self._session(draft_counts=[0, 0], fp_statuses=["closed", "closed"])
    response, _, close_mock = self._run(
      session,
      self._service(),
      body=BackfillPlanHistoryRequest(max_periods=2),
    )
    attempted = [outcome.period for outcome in response.processed]
    assert attempted == ["2025-11", "2025-12"]
    assert response.remaining_periods == ["2026-01", "2026-02"]
    assert close_mock.call_count == 2

  def test_draft_months_skipped_never_posted(self):
    session = self._session(
      draft_counts=[2, 0, 0, 0],
      fp_statuses=["closed", "closed", "closed"],
    )
    response, _, close_mock = self._run(session, self._service())
    assert response.processed[0].period == "2025-11"
    assert response.processed[0].status == "skipped_drafts"
    assert [o.period for o in response.processed if o.status == "stamped"] == [
      "2025-12",
      "2026-01",
      "2026-02",
    ]
    assert close_mock.call_count == 3
    assert response.remaining_periods == []

  def test_failure_halts_and_reports_remaining(self):
    session = self._session(draft_counts=[0], fp_statuses=["closed"])
    response, _, _ = self._run(
      session,
      self._service(),
      close_side_effect=UnbalancedLedgerError(100, 90),
    )
    assert len(response.processed) == 1
    assert response.processed[0].status == "failed"
    assert "debits=100" in (response.processed[0].detail or "")
    assert response.remaining_periods == ["2025-12", "2026-01", "2026-02"]
    session.rollback.assert_called_once()

  def test_resume_skips_reopen_for_closing_period(self):
    session = self._session(
      draft_counts=[0],
      fp_statuses=["closing"],
    )
    response, reopen_mock, close_mock = self._run(
      session,
      self._service(),
      body=BackfillPlanHistoryRequest(max_periods=1),
    )
    assert response.processed[0].status == "stamped"
    reopen_mock.assert_not_called()
    close_mock.assert_called_once()

  def test_stamp_outcome_fields_ride_through(self):
    session = self._session(draft_counts=[0], fp_statuses=["closed"])
    response, _, _ = self._run(
      session,
      self._service(),
      body=BackfillPlanHistoryRequest(max_periods=1),
    )
    outcome = response.processed[0]
    assert outcome.statements_stamped is True
    assert outcome.statement_rule_summary == {
      "pass": 3,
      "fail": 0,
      "error": 0,
      "skipped": 0,
    }
