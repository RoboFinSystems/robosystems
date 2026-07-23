"""Unit tests for the fiscal-calendar command wrappers.

Focus: the close-time statement-stamping surface — `close_period` maps
the stamp outcome onto `ClosePeriodResponse`, and `reopen_period`
retracts the reopened month's canonical statement sets (returning
`ReopenPeriodResult` with the count).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.fiscal_calendar import FiscalCalendarResponse
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  PeriodNotClosedError,
  PeriodNotFoundInLedgerError,
  ReopenPeriodResult,
  close_period,
  reopen_period,
)
from robosystems.operations.roboledger.fiscal_calendar import PeriodCloseResult

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
