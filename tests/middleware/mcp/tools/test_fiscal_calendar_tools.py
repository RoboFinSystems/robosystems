"""Unit tests for the fiscal calendar MCP tools.

Focus is on ClosePeriodTool — it's the thinnest wrapper over
PeriodCloseService but the one most likely to drift from the REST path.
We verify:

- Happy path delegates to the service with the expected arguments
- All domain errors map to the right MCP error shape
- actor_id falls back to "mcp_agent" when the client has no user identity
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.fiscal_calendar_tools import ClosePeriodTool
from robosystems.operations.fiscal_calendar import (
  CloseGateFailed,
  PeriodCloseResult,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.fiscal_calendar.service import CloseableGateResult

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


def _mock_session():
  session = MagicMock()
  session.__enter__ = MagicMock(return_value=session)
  session.__exit__ = MagicMock(return_value=False)
  return session


def _close_result(**overrides):
  defaults = {
    "period": "2026-01",
    "entries_posted": 2,
    "target_auto_advanced": False,
    "calendar": MagicMock(),
    "was_reclose": False,
  }
  defaults.update(overrides)
  return PeriodCloseResult(**defaults)


# ────────────────────────────────────────────────────────────────────────────
# Happy path
# ────────────────────────────────────────────────────────────────────────────


class TestClosePeriodToolHappyPath:
  @pytest.mark.asyncio
  async def test_delegates_to_service(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    svc_instance = MagicMock()
    svc_instance.close.return_value = _close_result(entries_posted=5)

    with (
      patch(
        "robosystems.db.extensions.extensions_session",
        return_value=_mock_session(),
      ),
      patch(
        f"{MODULE}._get_qb_sync_state",
        return_value=(False, None),
      ),
      patch(
        "robosystems.operations.fiscal_calendar.PeriodCloseService",
        return_value=svc_instance,
      ),
      patch(f"{MODULE}._build_response_payload", return_value={"graph_id": GRAPH_ID}),
    ):
      result = await tool.execute({"period": "2026-01"})

    assert result["period"] == "2026-01"
    assert result["entries_posted"] == 5
    assert result["target_auto_advanced"] is False
    svc_instance.close.assert_called_once()
    call = svc_instance.close.call_args
    assert call.kwargs["actor_id"] == "usr_abc"
    assert call.kwargs["actor_type"] == "agent"

  @pytest.mark.asyncio
  async def test_actor_id_falls_back_to_graph_scoped_sentinel(self):
    """When the MCP client has no user_id, fall back to mcp:{graph_id}
    so audit logs stay traceable per tenant (matches ReopenPeriodTool)."""
    tool = ClosePeriodTool(_client(user_id=None))
    svc_instance = MagicMock()
    svc_instance.close.return_value = _close_result()

    with (
      patch(
        "robosystems.db.extensions.extensions_session",
        return_value=_mock_session(),
      ),
      patch(f"{MODULE}._get_qb_sync_state", return_value=(False, None)),
      patch(
        "robosystems.operations.fiscal_calendar.PeriodCloseService",
        return_value=svc_instance,
      ),
      patch(f"{MODULE}._build_response_payload", return_value={}),
    ):
      await tool.execute({"period": "2026-01"})

    call = svc_instance.close.call_args
    assert call.kwargs["actor_id"] == f"mcp:{GRAPH_ID}"

  @pytest.mark.asyncio
  async def test_allow_stale_sync_flag_propagates(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    svc_instance = MagicMock()
    svc_instance.close.return_value = _close_result()

    with (
      patch(
        "robosystems.db.extensions.extensions_session",
        return_value=_mock_session(),
      ),
      patch(f"{MODULE}._get_qb_sync_state", return_value=(True, None)),
      patch(
        "robosystems.operations.fiscal_calendar.PeriodCloseService",
        return_value=svc_instance,
      ),
      patch(f"{MODULE}._build_response_payload", return_value={}),
    ):
      await tool.execute({"period": "2026-01", "allow_stale_sync": True})

    call = svc_instance.close.call_args
    assert call.kwargs["allow_stale_sync"] is True


# ────────────────────────────────────────────────────────────────────────────
# Error translation
# ────────────────────────────────────────────────────────────────────────────


class TestClosePeriodToolErrors:
  async def _run_with_side_effect(self, side_effect):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    svc_instance = MagicMock()
    svc_instance.close.side_effect = side_effect

    with (
      patch(
        "robosystems.db.extensions.extensions_session",
        return_value=_mock_session(),
      ),
      patch(f"{MODULE}._get_qb_sync_state", return_value=(False, None)),
      patch(
        "robosystems.operations.fiscal_calendar.PeriodCloseService",
        return_value=svc_instance,
      ),
    ):
      return await tool.execute({"period": "2026-03"})

  @pytest.mark.asyncio
  async def test_gate_failed_returns_not_closeable(self):
    result = await self._run_with_side_effect(
      CloseGateFailed(blockers=[CloseableGateResult.SEQUENCE])
    )
    assert result["error"] == "not_closeable"
    assert CloseableGateResult.SEQUENCE in result["blockers"]

  @pytest.mark.asyncio
  async def test_no_calendar_returns_calendar_not_initialized(self):
    result = await self._run_with_side_effect(
      CloseGateFailed(blockers=[CloseableGateResult.NO_CALENDAR])
    )
    assert result["error"] == "calendar_not_initialized"

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
# Re-close routing (end-to-end through the service stub)
# ────────────────────────────────────────────────────────────────────────────


class TestClosePeriodToolRecloseRouting:
  """These parallel the service-level reclose tests so the MCP path is
  explicitly covered even though the logic lives in the service."""

  @pytest.mark.asyncio
  async def test_reclose_reported_in_result(self):
    tool = ClosePeriodTool(_client(user_id="usr_abc"))
    svc_instance = MagicMock()
    svc_instance.close.return_value = _close_result(was_reclose=True)

    with (
      patch(
        "robosystems.db.extensions.extensions_session",
        return_value=_mock_session(),
      ),
      patch(f"{MODULE}._get_qb_sync_state", return_value=(False, None)),
      patch(
        "robosystems.operations.fiscal_calendar.PeriodCloseService",
        return_value=svc_instance,
      ),
      patch(f"{MODULE}._build_response_payload", return_value={}),
    ):
      result = await tool.execute({"period": "2026-01"})

    # The MCP response shape doesn't surface `was_reclose` directly but the
    # underlying service call must succeed and return a result object.
    svc_instance.close.assert_called_once()
    assert "period" in result
