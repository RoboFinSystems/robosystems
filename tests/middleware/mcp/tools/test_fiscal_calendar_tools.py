"""Unit tests for the fiscal calendar MCP tools.

Focus is on ClosePeriodTool, which no longer runs the close itself: it
dispatches to the worker and waits inside the tool budget. So what is tested
here is the dispatch and the handback — the arguments that reach the task,
the receipt coming back untouched, and what the operator is told when the
close outlives the call.

The outcome *shapes* moved with the shaping, to
`tests/operations/roboledger/fiscal_calendar/test_close_outcomes.py`, and the
task's own behaviour lives in `tests/operations/roboledger/tasks/`. Keeping
them apart is what stops this file re-asserting a payload it no longer
builds.
"""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools._gate import MCPExtensionGateError
from robosystems.middleware.mcp.tools.fiscal_calendar_tools import (
  BackfillPlanHistoryTool,
  ClosePeriodTool,
  ReopenPeriodTool,
)
from robosystems.middleware.sse.event_storage import OperationStatus
from robosystems.models.api.extensions.fiscal_calendar import (
  BackfillPeriodOutcome,
  BackfillPlanHistoryResponse,
  ClosePeriodResponse,
  FiscalCalendarResponse,
)
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  BackfillPreconditionError,
  ReopenPeriodResult,
)

MODULE = "robosystems.middleware.mcp.tools.fiscal_calendar_tools"
GRAPH_ID = "kg01234567890abcdef"


def _client(*, user_id: str | None = None):
  client = MagicMock()
  client.graph_id = GRAPH_ID
  if user_id is not None:
    client.user_id = user_id
  else:
    # Explicitly no user_id attribute to exercise the fallback
    del client.user_id
  return client


@contextmanager
def _patch_sessions():
  """Patch both extensions_session and _platform_session as no-op CMs.

  Also stubs `require_graph_extension_mcp` so the tests don't need a real
  platform-DB graph row. Each test class that wants to exercise the gate
  path patches it directly.
  """
  ext_session = MagicMock()
  ext_cm = MagicMock()
  ext_cm.__enter__ = MagicMock(return_value=ext_session)
  ext_cm.__exit__ = MagicMock(return_value=False)

  plat_session = MagicMock()
  plat_cm = MagicMock()
  plat_cm.__enter__ = MagicMock(return_value=plat_session)
  plat_cm.__exit__ = MagicMock(return_value=False)

  with (
    patch(f"{MODULE}.extensions_session", return_value=ext_cm),
    patch(f"{MODULE}._platform_session", return_value=plat_cm),
    patch(f"{MODULE}.qb_sync_state", return_value=(False, None)),
    patch(f"{MODULE}.require_graph_extension_mcp", return_value=MagicMock()),
  ):
    yield


def _fc_response(**overrides) -> FiscalCalendarResponse:
  defaults: dict = {
    "graph_id": GRAPH_ID,
    "fiscal_year_start_month": 1,
    "closed_through": "2026-01",
    "close_target": "2026-03",
    "gap_periods": 2,
    "catch_up_sequence": ["2026-02", "2026-03"],
    "closeable_now": True,
    "blockers": [],
    "last_close_at": None,
    "initialized_at": None,
    "last_sync_at": None,
    "periods": [],
  }
  defaults.update(overrides)
  return FiscalCalendarResponse(**defaults)


def _close_response(**overrides) -> ClosePeriodResponse:
  defaults: dict = {
    "fiscal_calendar": _fc_response(),
    "period": "2026-01",
    "entries_posted": 2,
    "target_auto_advanced": False,
  }
  defaults.update(overrides)
  return ClosePeriodResponse(**defaults)


# ────────────────────────────────────────────────────────────────────────────
# close-period — dispatch and poll
# ────────────────────────────────────────────────────────────────────────────


class _Storage:
  """Event storage returning a scripted sequence of operation states.

  The last state repeats: an operation that is still running stays running
  however many times it is polled, and a script that ran dry would otherwise
  read as "no such operation".
  """

  def __init__(self, *states):
    self._states = list(states)

  async def get_operation_metadata(self, operation_id):
    if not self._states:
      return None
    state = self._states.pop(0) if len(self._states) > 1 else self._states[0]
    if state is None:
      return None
    status, payload = state
    return SimpleNamespace(
      operation_id=operation_id,
      status=status,
      result_data=payload if status is OperationStatus.COMPLETED else None,
      error_message=payload if status is OperationStatus.FAILED else None,
    )


@contextmanager
def _patch_dispatch(storage, *, deduplicated=False, gate=None):
  """Patch the tool's dispatch path: gate check, enqueue, and polling."""
  enqueue = AsyncMock(
    return_value={"operation_id": "op_close_1", "deduplicated": deduplicated}
  )
  with (
    patch.object(ClosePeriodTool, "_gate_check", return_value=gate),
    patch("robosystems.worker.client.enqueue_task", enqueue),
    patch(
      "robosystems.middleware.sse.event_storage.get_event_storage",
      return_value=storage,
    ),
  ):
    yield enqueue


def _receipt(**overrides) -> dict:
  payload = {
    "outcome": "closed",
    "operation_id": "op_close_1",
    "period": "2026-01",
    "entries_posted": 5,
    "entries_published_to_qb": 4,
    "entries_posted_locally": 1,
    "target_auto_advanced": False,
    "fiscal_calendar": {"closed_through": "2026-01", "has_sync_connection": True},
    "rule_summary": None,
    "evaluated_structure_ids": [],
    "statements_stamped": True,
    "statement_stamp_note": None,
    "stamped_statement_sets": {},
    "statement_rule_summary": None,
  }
  payload.update(overrides)
  return payload


class TestClosePeriodToolDispatch:
  @pytest.mark.asyncio
  async def test_a_close_that_lands_in_budget_returns_its_receipt(self):
    """The tool hands back what the task produced, untouched — the receipt
    must not depend on how quickly the worker answered."""
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    storage = _Storage((OperationStatus.COMPLETED, _receipt()))

    with _patch_dispatch(storage) as enqueue:
      result = await tool.execute({"period": "2026-01"})

    assert result["period"] == "2026-01"
    assert result["entries_posted"] == 5
    assert result["statements_stamped"] is True
    assert result["fiscal_calendar"]["has_sync_connection"] is True
    assert enqueue.await_count == 1

  @pytest.mark.asyncio
  async def test_the_close_is_dispatched_with_the_arguments_it_was_given(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    storage = _Storage((OperationStatus.COMPLETED, _receipt()))

    with _patch_dispatch(storage) as enqueue:
      await tool.execute(
        {
          "period": "2026-01",
          "allow_stale_sync": True,
          "allow_reconciling_items": True,
          "note": "verified",
        }
      )

    kwargs = enqueue.await_args.kwargs
    assert kwargs["task_type"] == "period_close"
    assert kwargs["graph_id"] == GRAPH_ID
    # A real user id: the operation is readable only by the user it was
    # created for, so a sentinel would enqueue work nobody could look up.
    assert kwargs["user_id"] == "usr_abc"
    params = kwargs["params"]
    assert params["period"] == "2026-01"
    assert params["actor_type"] == "agent"
    assert params["actor_id"] == "usr_abc"
    assert params["allow_stale_sync"] is True
    assert params["allow_reconciling_items"] is True
    assert params["allow_stranded_obligations"] is False
    assert params["note"] == "verified"

  @pytest.mark.asyncio
  async def test_a_refused_close_reaches_the_caller_as_the_task_shaped_it(self):
    """A gate rejection is a completed operation carrying a refusal, not a
    failed one — the blockers have to survive the trip."""
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    refusal = {
      "outcome": "rejected",
      "error": "not_closeable",
      "blockers": ["reconciling_items"],
      "reconciling_item_count": 11,
    }
    storage = _Storage((OperationStatus.COMPLETED, refusal))

    with _patch_dispatch(storage):
      result = await tool.execute({"period": "2026-01"})

    assert result["error"] == "not_closeable"
    assert result["reconciling_item_count"] == 11

  @pytest.mark.asyncio
  async def test_without_a_user_nothing_is_dispatched(self):
    tool = ClosePeriodTool(_client(user_id=None))
    storage = _Storage()

    with _patch_dispatch(storage) as enqueue:
      result = await tool.execute({"period": "2026-01"})

    assert result["error"] == "authentication_required"
    assert enqueue.await_count == 0

  @pytest.mark.asyncio
  async def test_an_uncloseable_period_is_answered_without_a_worker(self):
    """The task re-runs the gate authoritatively; this only spares the
    operator a dispatch and a poll to learn something already knowable."""
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    storage = _Storage()
    gate_answer = {"error": "not_closeable", "blockers": ["sequence_violation"]}

    with _patch_dispatch(storage, gate=gate_answer) as enqueue:
      result = await tool.execute({"period": "2026-05"})

    assert result == gate_answer
    assert enqueue.await_count == 0


class TestClosePeriodToolHandback:
  """What the operator is told when the close outlives the call."""

  @pytest.mark.asyncio
  async def test_a_running_close_is_handed_back_with_where_to_look(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    tool.POLL_INTERVAL_S = 0
    tool.POLL_BUDGET_S = 0.01
    storage = _Storage((OperationStatus.RUNNING, None))

    with _patch_dispatch(storage):
      result = await tool.execute({"period": "2026-01"})

    assert result["status"] == "in_progress"
    assert result["operation_id"] == "op_close_1"
    assert result["worker_started"] is True
    # The instruction that matters: the close is atomic and already running.
    assert "NEVER call close-period again" in result["message"]
    assert "get-period-close-status" in result["message"]

  @pytest.mark.asyncio
  async def test_a_queued_close_says_so_rather_than_claiming_to_be_running(self):
    """'Queued behind a busy worker' and 'running now' need different
    words — the operator's next question is whether anything is happening."""
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    tool.POLL_INTERVAL_S = 0
    tool.POLL_BUDGET_S = 0.01
    storage = _Storage((OperationStatus.PENDING, None))

    with _patch_dispatch(storage):
      result = await tool.execute({"period": "2026-01"})

    assert result["status"] == "in_progress"
    assert result["worker_started"] is False
    assert "has not started yet" in result["message"]
    assert "do not re-dispatch" in result["message"]

  @pytest.mark.asyncio
  async def test_a_deduplicated_dispatch_is_disclosed(self):
    """Two identical calls inside the dedup window share one operation;
    saying so stops the agent inferring a second close is running."""
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    tool.POLL_INTERVAL_S = 0
    tool.POLL_BUDGET_S = 0.01
    storage = _Storage((OperationStatus.RUNNING, None))

    with _patch_dispatch(storage, deduplicated=True):
      result = await tool.execute({"period": "2026-01"})

    assert result["deduplicated"] is True

  @pytest.mark.asyncio
  async def test_a_failed_operation_warns_the_close_may_still_have_landed(self):
    """The worker's timeout cancels the coroutine, not the thread doing the
    QuickBooks publish — so a FAILED close can still commit afterwards."""
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    storage = _Storage((OperationStatus.FAILED, "Task timed out after 600s"))

    with _patch_dispatch(storage):
      result = await tool.execute({"period": "2026-01"})

    assert result["error"] == "close_failed"
    assert "may still have landed" in result["message"]
    assert "get-period-close-status" in result["message"]

  @pytest.mark.asyncio
  async def test_a_cancelled_operation_says_so(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    storage = _Storage((OperationStatus.CANCELLED, None))

    with _patch_dispatch(storage):
      result = await tool.execute({"period": "2026-01"})

    assert result["error"] == "cancelled"


class TestReopenPeriodToolResult:
  @pytest.mark.asyncio
  async def test_retraction_count_surfaces(self):
    tool = ReopenPeriodTool(_client(user_id="usr_abc"))
    result_obj = ReopenPeriodResult(
      fiscal_calendar=_fc_response(), statement_sets_retracted=3
    )
    with (
      _patch_sessions(),
      patch(f"{MODULE}.ops_reopen_period", return_value=result_obj),
    ):
      result = await tool.execute({"period": "2026-01", "reason": "missed accrual"})

    assert result["period"] == "2026-01"
    assert result["statement_sets_retracted"] == 3
    assert result["fiscal_calendar"]["graph_id"] == GRAPH_ID


# ────────────────────────────────────────────────────────────────────────────
# Extension gate — ClosePeriod / ReopenPeriod both reject pre-DB
# ────────────────────────────────────────────────────────────────────────────


class TestFiscalCalendarGate:
  """Both write tools must surface the MCP extension-gate error code
  before ever opening a session or calling the ops layer."""

  @pytest.mark.asyncio
  async def test_close_rejects_on_repo_graph(self):
    """The refusal has to come before the dispatch, not after — otherwise a
    forbidden close still occupies a worker slot to be told no."""
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    enqueue = AsyncMock()
    with (
      patch(
        f"{MODULE}.require_graph_extension_mcp",
        side_effect=MCPExtensionGateError(
          "repository_write_forbidden",
          "roboledger commands are not available on repository graphs",
        ),
      ),
      patch(f"{MODULE}.extensions_session"),
      patch(f"{MODULE}._platform_session"),
      patch("robosystems.worker.client.enqueue_task", enqueue),
    ):
      result = await tool.execute({"period": "2026-03"})

    assert result["error"] == "repository_write_forbidden"
    enqueue.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_reopen_rejects_on_unprovisioned_graph(self):
    tool = ReopenPeriodTool(_client(user_id="usr_abc"))
    with (
      patch(
        f"{MODULE}.require_graph_extension_mcp",
        side_effect=MCPExtensionGateError(
          "extension_not_provisioned",
          "roboledger is not provisioned for this graph",
        ),
      ),
      patch(f"{MODULE}.ops_reopen_period") as ops,
    ):
      result = await tool.execute({"period": "2026-03", "reason": "fix"})

    assert result["error"] == "extension_not_provisioned"
    ops.assert_not_called()


# ────────────────────────────────────────────────────────────────────────────
# BackfillPlanHistoryTool
# ────────────────────────────────────────────────────────────────────────────


def _backfill_response(**overrides) -> BackfillPlanHistoryResponse:
  defaults: dict = {
    "fiscal_calendar": _fc_response(),
    "earliest_available_period": "2024-07",
    "effective_start_period": "2024-07",
    "closed_through": "2026-05",
    "period_rows_created": 0,
    "processed": [
      BackfillPeriodOutcome(
        period="2024-07",
        status="stamped",
        statements_stamped=True,
        statement_rule_summary={"pass": 3, "fail": 0, "error": 0, "skipped": 0},
      )
    ],
    "remaining_periods": ["2024-08", "2024-09"],
  }
  defaults.update(overrides)
  return BackfillPlanHistoryResponse(**defaults)


class TestBackfillPlanHistoryTool:
  @pytest.mark.asyncio
  async def test_delegates_and_reshapes(self):
    tool = BackfillPlanHistoryTool(_client(user_id="usr_abc"))
    with (
      _patch_sessions(),
      patch(
        f"{MODULE}.ops_backfill_plan_history", return_value=_backfill_response()
      ) as ops,
    ):
      result = await tool.execute({"start_period": "2024-07", "max_periods": 1})

    ops.assert_called_once()
    body = ops.call_args.args[3]
    assert body.start_period == "2024-07"
    assert body.max_periods == 1
    assert ops.call_args.kwargs["actor_type"] == "agent"
    assert result["effective_start_period"] == "2024-07"
    assert result["remaining_periods"] == ["2024-08", "2024-09"]
    assert result["processed"][0]["period"] == "2024-07"
    assert result["processed"][0]["status"] == "stamped"
    assert result["fiscal_calendar"]["graph_id"] == GRAPH_ID

  @pytest.mark.asyncio
  async def test_defaults_apply_when_arguments_empty(self):
    tool = BackfillPlanHistoryTool(_client(user_id="usr_abc"))
    with (
      _patch_sessions(),
      patch(
        f"{MODULE}.ops_backfill_plan_history", return_value=_backfill_response()
      ) as ops,
    ):
      await tool.execute({})

    body = ops.call_args.args[3]
    assert body.start_period is None
    assert body.max_periods == 12
    assert body.allow_stale_sync is False

  @pytest.mark.asyncio
  async def test_precondition_error_maps_to_code(self):
    tool = BackfillPlanHistoryTool(_client(user_id="usr_abc"))
    with (
      _patch_sessions(),
      patch(
        f"{MODULE}.ops_backfill_plan_history",
        side_effect=BackfillPreconditionError(
          "nothing_closed", "Nothing is closed yet."
        ),
      ),
    ):
      result = await tool.execute({})

    assert result["error"] == "nothing_closed"
    assert "closed" in result["message"]

  @pytest.mark.asyncio
  async def test_rejects_on_repo_graph_before_ops(self):
    tool = BackfillPlanHistoryTool(_client(user_id="usr_abc"))
    with (
      patch(
        f"{MODULE}.require_graph_extension_mcp",
        side_effect=MCPExtensionGateError(
          "repository_write_forbidden",
          "roboledger commands are not available on repository graphs",
        ),
      ),
      patch(f"{MODULE}.ops_backfill_plan_history") as ops,
    ):
      result = await tool.execute({})

    assert result["error"] == "repository_write_forbidden"
    ops.assert_not_called()
