"""Unit tests for the fiscal calendar router endpoints.

These test the router handlers directly with mocked dependencies, matching
the pattern used by `test_schedules.py`. Full-stack integration is covered
by running `just demo-close` against the live stack.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.extensions.fiscal_calendar import (
  InitializeLedgerRequest,
  SetCloseTargetRequest,
)
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CalendarAlreadyInitializedError,
  CloseableGateResult,
  FiscalCalendarError,
  InvalidCloseTargetError,
)
from robosystems.routers.ledger.fiscal_calendar import (
  get_fiscal_calendar,
  initialize_ledger,
  set_close_target,
)

MODULE = "robosystems.routers.ledger.fiscal_calendar"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _mock_session():
  """Mock extensions_session context manager."""
  session = MagicMock()
  session.__enter__ = MagicMock(return_value=session)
  session.__exit__ = MagicMock(return_value=False)
  session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
  return session


def _mock_calendar(
  *,
  closed_through: str | None = "2025-12",
  close_target: str | None = "2026-03",
  fiscal_year_start_month: int = 1,
):
  calendar = FiscalCalendar(
    graph_id=GRAPH_ID,
    fiscal_year_start_month=fiscal_year_start_month,
    closed_through_period=closed_through,
    close_target_period=close_target,
    initialized_at=datetime(2026, 4, 12, tzinfo=UTC),
    last_close_at=None,
  )
  return calendar


# ────────────────────────────────────────────────────────────────────────────
# initialize_ledger
# ────────────────────────────────────────────────────────────────────────────


class TestInitializeLedger:
  @pytest.mark.asyncio
  async def test_happy_path(self):
    calendar = _mock_calendar(closed_through="2026-02", close_target="2026-03")
    mock_svc = MagicMock()
    mock_svc.initialize.return_value = calendar
    mock_svc.ensure_fiscal_periods.return_value = 24
    mock_svc.catch_up_sequence.return_value = ["2026-03"]
    mock_svc.gap_periods.return_value = 1
    mock_svc.closeable_gate.return_value = CloseableGateResult(
      is_closeable=True, blockers=[]
    )

    session = _mock_session()
    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    body = InitializeLedgerRequest(closed_through="2026-02")

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await initialize_ledger(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert result.periods_created == 24
    assert result.warnings == []
    assert result.fiscal_calendar.closed_through == "2026-02"
    assert result.fiscal_calendar.close_target == "2026-03"
    assert result.fiscal_calendar.closeable_now is True
    mock_svc.initialize.assert_called_once()
    mock_svc.ensure_fiscal_periods.assert_called_once()

  @pytest.mark.asyncio
  async def test_auto_seed_schedules_returns_warning(self):
    calendar = _mock_calendar(closed_through="2026-02")
    mock_svc = MagicMock()
    mock_svc.initialize.return_value = calendar
    mock_svc.ensure_fiscal_periods.return_value = 0
    mock_svc.catch_up_sequence.return_value = []
    mock_svc.gap_periods.return_value = 0
    mock_svc.closeable_gate.return_value = CloseableGateResult(is_closeable=True)

    session = _mock_session()
    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    body = InitializeLedgerRequest(closed_through="2026-02", auto_seed_schedules=True)

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await initialize_ledger(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert len(result.warnings) == 1
    assert "auto_seed_schedules" in result.warnings[0]

  @pytest.mark.asyncio
  async def test_conflict_when_already_initialized(self):
    mock_svc = MagicMock()
    mock_svc.initialize.side_effect = CalendarAlreadyInitializedError("already done")

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await initialize_ledger(
        graph_id=GRAPH_ID,
        body=InitializeLedgerRequest(closed_through="2026-02"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 409

  @pytest.mark.asyncio
  async def test_422_on_invalid_closed_through(self):
    mock_svc = MagicMock()
    mock_svc.initialize.side_effect = InvalidCloseTargetError("in the future")

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await initialize_ledger(
        graph_id=GRAPH_ID,
        body=InitializeLedgerRequest(closed_through="2099-01"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 422

  @pytest.mark.asyncio
  async def test_fresh_business_none_closed_through(self):
    calendar = _mock_calendar(closed_through=None, close_target=None)
    mock_svc = MagicMock()
    mock_svc.initialize.return_value = calendar
    mock_svc.ensure_fiscal_periods.return_value = 24
    mock_svc.catch_up_sequence.return_value = []
    mock_svc.gap_periods.return_value = 0
    mock_svc.closeable_gate.return_value = CloseableGateResult(is_closeable=True)

    session = _mock_session()
    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await initialize_ledger(
        graph_id=GRAPH_ID,
        body=InitializeLedgerRequest(closed_through=None),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert result.fiscal_calendar.closed_through is None
    assert result.fiscal_calendar.close_target is None
    assert result.fiscal_calendar.gap_periods == 0


# ────────────────────────────────────────────────────────────────────────────
# get_fiscal_calendar
# ────────────────────────────────────────────────────────────────────────────


class TestGetFiscalCalendar:
  @pytest.mark.asyncio
  async def test_happy_path(self):
    calendar = _mock_calendar(closed_through="2025-12", close_target="2026-03")
    mock_svc = MagicMock()
    mock_svc.get.return_value = calendar
    mock_svc.catch_up_sequence.return_value = ["2026-01", "2026-02", "2026-03"]
    mock_svc.gap_periods.return_value = 3
    mock_svc.closeable_gate.return_value = CloseableGateResult(
      is_closeable=True, blockers=[]
    )

    session = _mock_session()
    platform_db = MagicMock()
    conn = MagicMock()
    conn.last_sync = datetime(2026, 3, 5, tzinfo=UTC)
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = conn

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await get_fiscal_calendar(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert result.closed_through == "2025-12"
    assert result.close_target == "2026-03"
    assert result.gap_periods == 3
    assert result.catch_up_sequence == ["2026-01", "2026-02", "2026-03"]
    assert result.last_sync_at == datetime(2026, 3, 5, tzinfo=UTC)

  @pytest.mark.asyncio
  async def test_404_when_not_initialized(self):
    mock_svc = MagicMock()
    mock_svc.get.return_value = None

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await get_fiscal_calendar(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_surfaces_blockers_when_not_closeable(self):
    calendar = _mock_calendar(closed_through="2025-12", close_target="2026-03")
    mock_svc = MagicMock()
    mock_svc.get.return_value = calendar
    mock_svc.catch_up_sequence.return_value = ["2026-01"]
    mock_svc.gap_periods.return_value = 3
    mock_svc.closeable_gate.return_value = CloseableGateResult(
      is_closeable=False,
      blockers=[CloseableGateResult.SYNC_STALE],
    )

    session = _mock_session()
    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await get_fiscal_calendar(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert result.closeable_now is False
    assert CloseableGateResult.SYNC_STALE in result.blockers


# ────────────────────────────────────────────────────────────────────────────
# set_close_target
# ────────────────────────────────────────────────────────────────────────────


class TestSetCloseTarget:
  @pytest.mark.asyncio
  async def test_happy_path(self):
    calendar = _mock_calendar(closed_through="2025-12", close_target="2026-03")
    mock_svc = MagicMock()
    mock_svc.set_close_target.return_value = calendar
    mock_svc.catch_up_sequence.return_value = ["2026-01", "2026-02", "2026-03"]
    mock_svc.gap_periods.return_value = 3
    mock_svc.closeable_gate.return_value = CloseableGateResult(is_closeable=True)

    session = _mock_session()
    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
    ):
      result = await set_close_target(
        graph_id=GRAPH_ID,
        body=SetCloseTargetRequest(period="2026-03", note="catching up"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert result.close_target == "2026-03"
    mock_svc.set_close_target.assert_called_once()

  @pytest.mark.asyncio
  async def test_422_on_invalid_target(self):
    mock_svc = MagicMock()
    mock_svc.set_close_target.side_effect = InvalidCloseTargetError(
      "target is before closed_through"
    )

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await set_close_target(
        graph_id=GRAPH_ID,
        body=SetCloseTargetRequest(period="2024-01"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 422

  @pytest.mark.asyncio
  async def test_404_when_calendar_missing(self):
    mock_svc = MagicMock()
    mock_svc.set_close_target.side_effect = FiscalCalendarError("not initialized")

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await set_close_target(
        graph_id=GRAPH_ID,
        body=SetCloseTargetRequest(period="2026-03"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_rejects_malformed_period_via_pydantic(self):
    """Pydantic pattern validation should reject malformed period strings."""
    with pytest.raises(ValueError):
      SetCloseTargetRequest(period="not-a-period")
