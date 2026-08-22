"""How a close describes itself, refused or not.

These assertions used to live against the MCP tool, which shaped its own
outcomes inline. They moved when the close gained a second caller: the tool
now returns whatever the worker task produced, so the shaping has one home
and both paths describe the same close identically.

What matters in each case is that the payload carries the part an operator
can act on — the blockers, the entries that failed to publish, the amounts
that did not balance — rather than a message they can only read.
"""

from __future__ import annotations

import pytest

from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodResponse,
  FiscalCalendarResponse,
)
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  FiscalCalendarError,
  PeriodAlreadyClosedError,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.roboledger.fiscal_calendar.close_outcomes import (
  CLOSE_DOMAIN_ERRORS,
  close_error_payload,
  close_success_payload,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  WritebackFailed,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
  PendingObligationDetail,
)
from robosystems.operations.roboledger.reports.statement_sets import StatementStampError

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"


def _response(**overrides) -> ClosePeriodResponse:
  calendar = FiscalCalendarResponse(
    graph_id=GRAPH_ID,
    fiscal_year_start_month=1,
    closed_through="2026-07",
    close_target="2026-08",
    gap_periods=1,
    catch_up_sequence=["2026-08"],
    closeable_now=True,
    blockers=[],
    last_close_at=None,
    initialized_at=None,
    last_sync_at=None,
    periods=[],
  )
  defaults: dict = {
    "fiscal_calendar": calendar,
    "period": "2026-07",
    "entries_posted": 34,
    "target_auto_advanced": True,
  }
  defaults.update(overrides)
  return ClosePeriodResponse(**defaults)


class TestSuccessPayload:
  def test_it_carries_the_numbers_an_operator_reports_back(self):
    payload = close_success_payload(
      _response(entries_published_to_qb=31, entries_posted_locally=3),
      has_sync_connection=True,
    )

    assert payload["period"] == "2026-07"
    assert payload["entries_posted"] == 34
    assert payload["entries_published_to_qb"] == 31
    assert payload["entries_posted_locally"] == 3
    assert payload["target_auto_advanced"] is True

  def test_the_calendar_says_whether_a_sync_gate_applies_next_time(self):
    """Not on the Pydantic response, so it has to be added here or the
    agent cannot reason about the next period's sync blocker."""
    payload = close_success_payload(_response(), has_sync_connection=True)
    assert payload["fiscal_calendar"]["has_sync_connection"] is True

    payload = close_success_payload(_response(), has_sync_connection=False)
    assert payload["fiscal_calendar"]["has_sync_connection"] is False

  def test_statement_stamping_is_visible(self):
    """Without these keys an agent never learns the close persisted the
    month's statements, which is most of what the close is for."""
    payload = close_success_payload(
      _response(
        statements_stamped=True,
        stamped_statement_sets={"struct_bs": "fs_1"},
        statement_rule_summary={"pass": 20, "fail": 0},
      ),
      has_sync_connection=False,
    )

    assert payload["statements_stamped"] is True
    assert payload["stamped_statement_sets"] == {"struct_bs": "fs_1"}
    assert payload["statement_rule_summary"] == {"pass": 20, "fail": 0}

  def test_rule_outcomes_survive_being_empty(self):
    payload = close_success_payload(
      _response(rule_summary=None, evaluated_structure_ids=[]),
      has_sync_connection=False,
    )
    assert payload["rule_summary"] is None
    assert payload["evaluated_structure_ids"] == []


class TestRefusals:
  def test_a_sequence_violation_names_the_blocker(self):
    payload = close_error_payload(
      CloseGateFailed(
        CloseableGateResult(is_closeable=False, blockers=[CloseableGateResult.SEQUENCE])
      ),
      period="2026-03",
    )
    assert payload is not None
    assert payload["error"] == "not_closeable"
    assert CloseableGateResult.SEQUENCE in payload["blockers"]

  def test_an_uninitialized_calendar_is_its_own_answer(self):
    payload = close_error_payload(
      CloseGateFailed(
        CloseableGateResult(
          is_closeable=False, blockers=[CloseableGateResult.NO_CALENDAR]
        )
      ),
      period="2026-03",
    )
    assert payload is not None
    assert payload["error"] == "calendar_not_initialized"

  def test_pending_obligations_name_the_schedules_to_go_and_promote(self):
    payload = close_error_payload(
      CloseGateFailed(
        CloseableGateResult(
          is_closeable=False,
          blockers=[CloseableGateResult.PENDING_OBLIGATIONS],
          pending_obligation_count=3,
          pending_obligation_sample=[
            PendingObligationDetail(
              event_id="evt_1",
              schedule_id="struct_dep",
              schedule_name="Computer Depreciation",
              period="2026-04",
            ),
          ],
          earliest_pending_period="2026-04",
        )
      ),
      period="2026-04",
    )

    assert payload is not None
    assert payload["pending_obligation_count"] == 3
    assert payload["pending_obligation_sample"][0]["schedule_name"] == (
      "Computer Depreciation"
    )
    assert payload["earliest_pending_period"] == "2026-04"
    # Detail rides only for the blockers that fired.
    assert "sync_stale_days" not in payload
    assert "reconciling_item_count" not in payload

  def test_reconciling_items_name_the_transactions_holding_the_close(self):
    payload = close_error_payload(
      CloseGateFailed(
        CloseableGateResult(
          is_closeable=False,
          blockers=[CloseableGateResult.RECONCILING_ITEMS],
          reconciling_item_count=11,
          reconciling_item_sample=["Expense_1721", "Expense_1727"],
        )
      ),
      period="2026-08",
    )

    assert payload is not None
    assert payload["reconciling_item_count"] == 11
    assert payload["reconciling_item_sample"] == ["Expense_1721", "Expense_1727"]

  def test_an_unbalanced_ledger_reports_both_sides(self):
    payload = close_error_payload(
      UnbalancedLedgerError(total_debit=1000, total_credit=900), period="2026-03"
    )
    assert payload is not None
    assert payload["error"] == "unbalanced"
    assert "1000" in payload["message"]
    assert "900" in payload["message"]

  def test_a_failed_writeback_names_the_entries_that_did_not_publish(self):
    """The close commits its publish markers before raising, so the entries
    that did reach QuickBooks are not re-sent on retry — the operator needs
    to know which ones to fix, not to start over."""
    payload = close_error_payload(
      WritebackFailed(
        [{"event_id": "evt_1", "memo": "AWS", "qb_error": {"code": "6000"}}]
      ),
      period="2026-07",
    )

    assert payload is not None
    assert payload["error"] == "write_back_failed"
    assert payload["failed_events"][0]["event_id"] == "evt_1"

  def test_a_stamp_failure_says_the_close_rolled_back(self):
    payload = close_error_payload(
      StatementStampError("pivot failed for 2026-03"), period="2026-03"
    )
    assert payload is not None
    assert payload["error"] == "statement_stamp_failed"
    assert "rolled back" in payload["message"]
    assert "pivot failed for 2026-03" in payload["message"]

  @pytest.mark.parametrize(
    ("error", "expected"),
    [
      (PeriodNotFoundError("2026-03"), "period_not_found"),
      (PeriodAlreadyClosedError("2026-03"), "already_closed"),
      (RowLockedError("Period 2026-03 is being closed"), "row_locked"),
      (FiscalCalendarError("bad target"), "calendar_error"),
    ],
  )
  def test_the_remaining_domain_errors_keep_their_codes(self, error, expected):
    payload = close_error_payload(error, period="2026-03")
    assert payload is not None
    assert payload["error"] == expected


class TestFaultsAreNotOutcomes:
  def test_an_unrecognized_exception_is_declined(self):
    """None means "this is a fault" — the caller lets it propagate so the
    operation is marked failed rather than described as a refusal."""
    assert close_error_payload(RuntimeError("boom"), period="2026-03") is None

  def test_every_declared_domain_error_has_a_payload(self):
    """`CLOSE_DOMAIN_ERRORS` is what the worker catches. A type listed there
    with no shaping would be swallowed and returned as an empty result."""
    samples: dict[type, Exception] = {
      CloseGateFailed: CloseGateFailed(
        CloseableGateResult(is_closeable=False, blockers=["x"])
      ),
      PeriodNotFoundError: PeriodNotFoundError("2026-03"),
      PeriodAlreadyClosedError: PeriodAlreadyClosedError("2026-03"),
      RowLockedError: RowLockedError("busy"),
      UnbalancedLedgerError: UnbalancedLedgerError(1, 2),
      WritebackFailed: WritebackFailed([]),
      StatementStampError: StatementStampError("x"),
      FiscalCalendarError: FiscalCalendarError("x"),
    }
    assert set(samples) == set(CLOSE_DOMAIN_ERRORS)
    for error_type, sample in samples.items():
      assert close_error_payload(sample, period="2026-03") is not None, error_type
