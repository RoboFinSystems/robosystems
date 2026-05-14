"""Unit tests for the fiscal calendar MCP tools.

Focus is on ClosePeriodTool — it's the thinnest wrapper over
PeriodCloseService but the one most likely to drift from the REST path.
We verify:

- Happy path delegates to ops `close_period` with the expected arguments
- All domain errors map to the right MCP error shape
- actor_id falls back to "mcp:{graph_id}" when the client has no user identity

Mocks live at the **operations layer boundary** (`ops_close_period`) — the
MCP tool is now a thin shim that translates exceptions and re-shapes the
response. Tests against mocked SQLAlchemy internals would only re-test what
`tests/operations/roboledger/fiscal_calendar/` already covers.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools._gate import MCPExtensionGateError
from robosystems.middleware.mcp.tools.fiscal_calendar_tools import (
  ClosePeriodTool,
  ReopenPeriodTool,
)
from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodResponse,
  FiscalCalendarResponse,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
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
# Happy path
# ────────────────────────────────────────────────────────────────────────────


class TestClosePeriodToolHappyPath:
  @pytest.mark.asyncio
  async def test_delegates_to_ops_layer(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    response = _close_response(entries_posted=5)

    with (
      _patch_sessions(),
      patch(f"{MODULE}.ops_close_period", return_value=response) as ops,
    ):
      result = await tool.execute({"period": "2026-01"})

    assert result["period"] == "2026-01"
    assert result["entries_posted"] == 5
    assert result["target_auto_advanced"] is False
    assert "fiscal_calendar" in result
    assert "has_sync_connection" in result["fiscal_calendar"]
    assert ops.call_count == 1
    call = ops.call_args
    assert call.kwargs["actor_id"] == "usr_abc"
    assert call.kwargs["actor_type"] == "agent"
    assert call.kwargs["allow_stale_sync"] is False

  @pytest.mark.asyncio
  async def test_actor_id_falls_back_to_graph_scoped_sentinel(self):
    """When the MCP client has no user_id, fall back to mcp:{graph_id}
    so audit logs stay traceable per tenant (matches ReopenPeriodTool)."""
    tool = ClosePeriodTool(_client(user_id=None))

    with (
      _patch_sessions(),
      patch(f"{MODULE}.ops_close_period", return_value=_close_response()) as ops,
    ):
      await tool.execute({"period": "2026-01"})

    call = ops.call_args
    assert call.kwargs["actor_id"] == f"mcp:{GRAPH_ID}"

  @pytest.mark.asyncio
  async def test_allow_stale_sync_flag_propagates(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))

    with (
      _patch_sessions(),
      patch(f"{MODULE}.ops_close_period", return_value=_close_response()) as ops,
    ):
      await tool.execute({"period": "2026-01", "allow_stale_sync": True})

    call = ops.call_args
    assert call.kwargs["allow_stale_sync"] is True


# ────────────────────────────────────────────────────────────────────────────
# Error translation
# ────────────────────────────────────────────────────────────────────────────


class TestClosePeriodToolErrors:
  async def _run_with_side_effect(self, side_effect):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    with (
      _patch_sessions(),
      patch(f"{MODULE}.ops_close_period", side_effect=side_effect),
    ):
      return await tool.execute({"period": "2026-03"})

  @pytest.mark.asyncio
  async def test_gate_failed_returns_not_closeable(self):
    result = await self._run_with_side_effect(
      CloseGateFailed(
        CloseableGateResult(is_closeable=False, blockers=[CloseableGateResult.SEQUENCE])
      )
    )
    assert result["error"] == "not_closeable"
    assert CloseableGateResult.SEQUENCE in result["blockers"]

  @pytest.mark.asyncio
  async def test_no_calendar_returns_calendar_not_initialized(self):
    result = await self._run_with_side_effect(
      CloseGateFailed(
        CloseableGateResult(
          is_closeable=False, blockers=[CloseableGateResult.NO_CALENDAR]
        )
      )
    )
    assert result["error"] == "calendar_not_initialized"

  @pytest.mark.asyncio
  async def test_pending_obligations_enriches_detail(self):
    """§3.12 follow-up: pending_obligations blocker surfaces count + sample
    + earliest_pending_period on the close-period response."""
    from robosystems.operations.roboledger.fiscal_calendar.service import (
      PendingObligationDetail,
    )

    result = await self._run_with_side_effect(
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
      )
    )
    assert result["error"] == "not_closeable"
    assert result["pending_obligation_count"] == 3
    assert result["pending_obligation_sample"][0]["schedule_name"] == (
      "Computer Depreciation"
    )
    assert result["earliest_pending_period"] == "2026-04"
    assert "sync_stale_days" not in result  # only populated when sync blocker active

  @pytest.mark.asyncio
  async def test_period_not_found_error(self):
    result = await self._run_with_side_effect(PeriodNotFoundError("2026-03"))
    assert result["error"] == "period_not_found"

  @pytest.mark.asyncio
  async def test_unbalanced_ledger_error(self):
    result = await self._run_with_side_effect(
      UnbalancedLedgerError(total_debit=1000, total_credit=900)
    )
    assert result["error"] == "unbalanced"
    assert "1000" in result["message"]
    assert "900" in result["message"]


# ────────────────────────────────────────────────────────────────────────────
# Re-close routing (end-to-end through the ops stub)
# ────────────────────────────────────────────────────────────────────────────


class TestClosePeriodToolRecloseRouting:
  """The tool doesn't surface `was_reclose` directly, but a successful
  re-close call must round-trip through the wrapper unchanged."""

  @pytest.mark.asyncio
  async def test_reclose_round_trips(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    with (
      _patch_sessions(),
      patch(
        f"{MODULE}.ops_close_period", return_value=_close_response(entries_posted=0)
      ) as ops,
    ):
      result = await tool.execute({"period": "2026-01"})

    ops.assert_called_once()
    assert "period" in result
    assert result["entries_posted"] == 0


# ────────────────────────────────────────────────────────────────────────────
# Extension gate — ClosePeriod / ReopenPeriod both reject pre-DB
# ────────────────────────────────────────────────────────────────────────────


class TestFiscalCalendarGate:
  """Both write tools must surface the MCP extension-gate error code
  before ever opening a session or calling the ops layer."""

  @pytest.mark.asyncio
  async def test_close_rejects_on_repo_graph(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    with (
      patch(
        f"{MODULE}.require_graph_extension_mcp",
        side_effect=MCPExtensionGateError(
          "repository_write_forbidden",
          "roboledger commands are not available on repository graphs",
        ),
      ),
      patch(f"{MODULE}.ops_close_period") as ops,
    ):
      result = await tool.execute({"period": "2026-03"})

    assert result["error"] == "repository_write_forbidden"
    ops.assert_not_called()

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
