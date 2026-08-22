"""The close, run on the worker.

The distinction under test is what counts as a *failure*. The consumer
reduces any raised exception to a plain string, so a gate rejection that
propagated would reach the operator as text with its blockers stripped —
the one part they can act on. Every domain outcome must therefore come back
as a result, and only genuine faults may raise.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  PeriodAlreadyClosedError,
  UnbalancedLedgerError,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  WritebackFailed,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)
from robosystems.operations.roboledger.tasks.period_close import PeriodCloseTask

GRAPH_ID = "kg01234567890abcdef"
MODULE = "robosystems.operations.roboledger.tasks.period_close"


def _task(**params) -> PeriodCloseTask:
  manager = MagicMock()
  manager.emit_progress = AsyncMock()
  manager.event_storage = MagicMock()
  manager.event_storage.store_event = AsyncMock()
  return PeriodCloseTask(
    task_id="op_close_1",
    graph_id=GRAPH_ID,
    user_id="user_1",
    params={"period": "2026-07", **params},
    manager=manager,
  )


def _close_response() -> SimpleNamespace:
  """The shape `close_period` returns on success."""
  return SimpleNamespace(
    period="2026-07",
    entries_posted=34,
    entries_published_to_qb=31,
    entries_posted_locally=3,
    target_auto_advanced=True,
    fiscal_calendar=SimpleNamespace(
      model_dump=lambda mode="json": {"closed_through": "2026-07"}
    ),
    rule_summary={"pass": 7, "fail": 0},
    evaluated_structure_ids=["struct_1"],
    statements_stamped=True,
    statement_stamp_note=None,
    stamped_statement_sets={"struct_bs": "fs_1"},
    statement_rule_summary={"pass": 20, "fail": 0},
  )


class _Ctx:
  """Context manager yielding a session stand-in."""

  def __init__(self, value):
    self.value = value

  def __enter__(self):
    return self.value

  def __exit__(self, *exc):
    return False


def _patched(close_result=None, close_error=None, session=None, close_spy=None):
  """Patch the task's collaborators where it imports them from.

  The task imports lazily inside `_run_close`, so each patch targets the
  defining module rather than the task's namespace.
  """
  session = session or MagicMock()

  def _close(*args, **kwargs):
    if close_spy is not None:
      close_spy(*args, **kwargs)
    if close_error is not None:
      raise close_error
    return close_result

  return (
    patch(
      "robosystems.operations.roboledger.commands.fiscal_calendar.close_period",
      side_effect=_close,
    ),
    patch(
      "robosystems.db.extensions.extensions_session",
      return_value=_Ctx(session),
    ),
    patch(
      "robosystems.db.platform.platform_session",
      return_value=_Ctx(MagicMock()),
    ),
    patch(
      "robosystems.operations.roboledger.reads.fiscal_calendar.qb_sync_state",
      return_value=(True, None),
    ),
  )


def _run(task, **kwargs):
  patches = _patched(**kwargs)
  for patcher in patches:
    patcher.start()
  try:
    return asyncio.run(task.execute())
  finally:
    for patcher in patches:
      patcher.stop()


def _gate(*blockers, **detail) -> CloseGateFailed:
  gate = CloseableGateResult(is_closeable=False, blockers=list(blockers), **detail)
  return CloseGateFailed(gate)


class TestSuccess:
  def test_the_receipt_carries_the_close_and_names_the_operation(self):
    result = _run(_task(), close_result=_close_response())

    assert result["outcome"] == "closed"
    assert result["operation_id"] == "op_close_1"
    assert result["period"] == "2026-07"
    assert result["entries_posted"] == 34
    assert result["entries_published_to_qb"] == 31
    assert result["statements_stamped"] is True
    assert result["fiscal_calendar"]["has_sync_connection"] is True

  def test_the_operation_is_moved_off_pending_before_the_work_starts(self):
    """Nothing else does it, and the tool's handback depends on it: 'queued'
    and 'running' are different things to tell an operator."""
    task = _task()
    _run(task, close_result=_close_response())

    task.manager.event_storage.store_event.assert_awaited()
    args = task.manager.event_storage.store_event.await_args
    assert args.args[0] == "op_close_1"
    assert args.args[1].value == "operation_started"
    assert args.args[2]["period"] == "2026-07"

  def test_the_overrides_and_actor_reach_the_close(self):
    """The bypass flags an operator agreed to must survive the hop to the
    worker — silently dropping one turns an approved close into a refusal."""
    spy = MagicMock()
    task = _task(
      actor_id="user_9",
      actor_type="agent",
      allow_stale_sync=True,
      allow_stranded_obligations=True,
      allow_reconciling_items=True,
      note="August close",
    )

    _run(task, close_result=_close_response(), close_spy=spy)

    kwargs = spy.call_args.kwargs
    assert kwargs["actor_id"] == "user_9"
    assert kwargs["actor_type"] == "agent"
    assert kwargs["allow_stale_sync"] is True
    assert kwargs["allow_stranded_obligations"] is True
    assert kwargs["allow_reconciling_items"] is True
    assert kwargs["note"] == "August close"
    assert spy.call_args.args[3] == "2026-07"


class TestRefusals:
  """A refused close is a result. None of these may raise."""

  @pytest.mark.parametrize(
    ("error", "expected"),
    [
      (_gate("pending_obligations", pending_obligation_count=3), "not_closeable"),
      (PeriodAlreadyClosedError("2026-07"), "already_closed"),
      (RowLockedError("busy"), "row_locked"),
      (UnbalancedLedgerError(100, 90), "unbalanced"),
      (
        WritebackFailed([{"event_id": "evt_1", "qb_error": {"code": "x"}}]),
        "write_back_failed",
      ),
    ],
  )
  def test_domain_outcomes_come_back_as_results(self, error, expected):
    result = _run(_task(), close_error=error)

    assert result["outcome"] == "rejected"
    assert result["error"] == expected
    assert result["operation_id"] == "op_close_1"

  def test_a_gate_rejection_keeps_the_blockers_the_operator_needs(self):
    result = _run(
      _task(),
      close_error=_gate(
        "reconciling_items",
        reconciling_item_count=11,
        reconciling_item_sample=["Expense_1721"],
      ),
    )

    assert result["blockers"] == ["reconciling_items"]
    assert result["reconciling_item_count"] == 11
    assert result["reconciling_item_sample"] == ["Expense_1721"]

  def test_write_back_failure_names_the_entries_that_did_not_publish(self):
    result = _run(
      _task(),
      close_error=WritebackFailed(
        [{"event_id": "evt_1", "memo": "AWS", "qb_error": {"code": "6000"}}]
      ),
    )

    assert result["failed_events"][0]["event_id"] == "evt_1"

  def test_an_already_closed_period_hands_back_the_receipt_it_wrote(self):
    """The stalled-task reaper measures age from enqueue, so it can re-queue
    a close that already landed. Reporting only 'already closed' would
    describe a success as a refusal."""
    session = MagicMock()
    session.query.return_value.filter.return_value.scalar.return_value = {
      "version": 1,
      "entries_posted": 34,
    }

    result = _run(
      _task(),
      close_error=PeriodAlreadyClosedError("2026-07"),
      session=session,
    )

    assert result["error"] == "already_closed"
    assert result["close_receipt"]["entries_posted"] == 34

  def test_an_unfinished_close_reports_no_receipt_rather_than_inventing_one(self):
    session = MagicMock()
    session.query.return_value.filter.return_value.scalar.return_value = None

    result = _run(
      _task(),
      close_error=PeriodAlreadyClosedError("2026-07"),
      session=session,
    )

    assert "close_receipt" not in result


class TestFaults:
  def test_a_database_fault_still_raises(self):
    """Only outcomes are results. A fault must reach the consumer so the
    operation is marked failed rather than reported as a refusal."""
    fault = OperationalError("SELECT 1", {}, Exception("connection reset"))

    with pytest.raises(OperationalError):
      _run(_task(), close_error=fault)
