"""Unit tests for the ledger period close / reopen router endpoints."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.extensions.fiscal_calendar import (
  ClosePeriodRequest,
  FiscalCalendarResponse,
  ReopenPeriodRequest,
)
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.operations.roboledger.fiscal_calendar import (
  CloseGateFailed,
  PeriodCloseResult,
  PeriodNotFoundError,
  UnbalancedLedgerError,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)
from robosystems.routers.ledger.periods import (
  close_period,
  list_period_drafts,
  reopen_period,
)

MODULE = "robosystems.routers.ledger.periods"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _mock_session():
  session = MagicMock()
  session.__enter__ = MagicMock(return_value=session)
  session.__exit__ = MagicMock(return_value=False)
  return session


def _mock_calendar(
  *, closed_through: str | None = "2025-12", close_target: str | None = "2026-03"
):
  return FiscalCalendar(
    graph_id=GRAPH_ID,
    fiscal_year_start_month=1,
    closed_through_period=closed_through,
    close_target_period=close_target,
    initialized_at=datetime(2026, 4, 12, tzinfo=UTC),
    last_close_at=None,
  )


def _mock_period(name: str = "2026-01", status: str = "open"):
  fp = MagicMock()
  fp.name = name
  fp.start_date = date(2026, 1, 1)
  fp.end_date = date(2026, 1, 31)
  fp.status = status
  fp.closed_at = None
  fp.closed_by = None
  return fp


def _wire_period_query(session, fp):
  """Make `session.query(FiscalPeriod).filter(...).one_or_none()` return fp."""
  query = MagicMock()
  query.filter.return_value.one_or_none.return_value = fp
  query.filter.return_value.order_by.return_value.all.return_value = []
  # Default query for entries update — returns 0 updated rows
  query.filter.return_value.update.return_value = 0
  session.query.return_value = query
  return session


def _wire_bs_equation(session, total_debit: int = 0, total_credit: int = 0):
  """Stub the BS equation SQL execute() result."""
  exec_mock = MagicMock()
  row = MagicMock()
  row.total_debit = total_debit
  row.total_credit = total_credit
  exec_mock.fetchone.return_value = row
  session.execute.return_value = exec_mock


def _fake_response(close_target: str | None = "2026-03") -> FiscalCalendarResponse:
  """Real FiscalCalendarResponse for Pydantic validation to succeed."""
  return FiscalCalendarResponse(
    graph_id=GRAPH_ID,
    fiscal_year_start_month=1,
    closed_through="2026-01",
    close_target=close_target,
    gap_periods=0,
    catch_up_sequence=[],
    closeable_now=False,
    blockers=[],
  )


# ────────────────────────────────────────────────────────────────────────────
# close_period
# ────────────────────────────────────────────────────────────────────────────


class TestClosePeriod:
  """Router tests — verify the endpoint translates `PeriodCloseService`
  outcomes into HTTP responses. Close-flow mechanics (gate, BS check,
  reclose routing) are tested directly against the service in
  `tests/operations/fiscal_calendar/test_close_service.py`.
  """

  def _result(self, *, entries_posted: int = 0, target_auto_advanced: bool = False):
    return PeriodCloseResult(
      period="2026-01",
      entries_posted=entries_posted,
      target_auto_advanced=target_auto_advanced,
      calendar=_mock_calendar(closed_through="2026-01", close_target="2026-03"),
      was_reclose=False,
    )

  @pytest.mark.asyncio
  async def test_happy_path_calls_service(self):
    mock_close_svc = MagicMock()
    mock_close_svc.close.return_value = self._result(entries_posted=3)

    session = _mock_session()
    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._close_svc", mock_close_svc),
      patch(f"{MODULE}._build_response") as mock_build,
    ):
      mock_build.return_value = _fake_response(close_target="2026-03")
      result = await close_period(
        graph_id=GRAPH_ID,
        period="2026-01",
        body=ClosePeriodRequest(),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert result.period == "2026-01"
    assert result.entries_posted == 3
    mock_close_svc.close.assert_called_once()
    # Verify the router passed the authenticated user ID as actor
    call = mock_close_svc.close.call_args
    assert call.kwargs["actor_id"] == "usr_test123"
    assert call.kwargs["actor_type"] == "user"

  @pytest.mark.asyncio
  async def test_allow_stale_sync_propagates_to_service(self):
    mock_close_svc = MagicMock()
    mock_close_svc.close.return_value = self._result()

    session = _mock_session()
    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._close_svc", mock_close_svc),
      patch(f"{MODULE}._build_response") as mock_build,
    ):
      mock_build.return_value = _fake_response(close_target="2026-03")
      await close_period(
        graph_id=GRAPH_ID,
        period="2026-01",
        body=ClosePeriodRequest(allow_stale_sync=True),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    call = mock_close_svc.close.call_args
    assert call.kwargs["allow_stale_sync"] is True

  @pytest.mark.asyncio
  async def test_gate_failure_translates_to_422(self):
    mock_close_svc = MagicMock()
    mock_close_svc.close.side_effect = CloseGateFailed(
      blockers=[CloseableGateResult.SEQUENCE]
    )

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._close_svc", mock_close_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await close_period(
        graph_id=GRAPH_ID,
        period="2026-03",
        body=ClosePeriodRequest(),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 422
    assert CloseableGateResult.SEQUENCE in exc_info.value.detail["blockers"]

  @pytest.mark.asyncio
  async def test_no_calendar_gate_translates_to_404(self):
    mock_close_svc = MagicMock()
    mock_close_svc.close.side_effect = CloseGateFailed(
      blockers=[CloseableGateResult.NO_CALENDAR]
    )

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._close_svc", mock_close_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await close_period(
        graph_id=GRAPH_ID,
        period="2026-01",
        body=ClosePeriodRequest(),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_unbalanced_ledger_translates_to_422(self):
    mock_close_svc = MagicMock()
    mock_close_svc.close.side_effect = UnbalancedLedgerError(
      total_debit=1000, total_credit=900
    )

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._close_svc", mock_close_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await close_period(
        graph_id=GRAPH_ID,
        period="2026-01",
        body=ClosePeriodRequest(),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 422
    assert "debits=1000" in exc_info.value.detail
    assert "credits=900" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_period_not_found_translates_to_404(self):
    mock_close_svc = MagicMock()
    mock_close_svc.close.side_effect = PeriodNotFoundError("2026-01")

    session = _mock_session()
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._close_svc", mock_close_svc),
      pytest.raises(HTTPException) as exc_info,
    ):
      await close_period(
        graph_id=GRAPH_ID,
        period="2026-01",
        body=ClosePeriodRequest(),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 404


# ────────────────────────────────────────────────────────────────────────────
# reopen_period
# ────────────────────────────────────────────────────────────────────────────


class TestReopenPeriod:
  @pytest.mark.asyncio
  async def test_happy_path(self):
    calendar = _mock_calendar(closed_through="2026-02", close_target="2026-04")
    mock_svc = MagicMock()
    mock_svc.retreat_closed_through.return_value = calendar
    mock_svc.catch_up_sequence.return_value = []
    mock_svc.gap_periods.return_value = 0
    mock_svc.closeable_gate.return_value = CloseableGateResult(is_closeable=False)

    session = _mock_session()
    fp = _mock_period(name="2026-03", status="closed")
    _wire_period_query(session, fp)

    platform_db = MagicMock()
    platform_db.query.return_value.filter.return_value.one_or_none.return_value = None

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      patch(f"{MODULE}._svc", mock_svc),
      patch(f"{MODULE}._build_response") as mock_build,
    ):
      mock_build.return_value = _fake_response()
      await reopen_period(
        graph_id=GRAPH_ID,
        period="2026-03",
        body=ReopenPeriodRequest(reason="missed March expense"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )

    assert fp.status == "closing"
    assert fp.closed_at is None
    assert fp.closed_by is None
    mock_svc.retreat_closed_through.assert_called_once()
    # Verify reason propagated
    call = mock_svc.retreat_closed_through.call_args
    assert call.kwargs["reason"] == "missed March expense"

  @pytest.mark.asyncio
  async def test_404_when_period_missing(self):
    session = _mock_session()
    _wire_period_query(session, None)  # No such period
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      pytest.raises(HTTPException) as exc_info,
    ):
      await reopen_period(
        graph_id=GRAPH_ID,
        period="2026-03",
        body=ReopenPeriodRequest(reason="test"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_422_when_period_not_closed(self):
    session = _mock_session()
    fp = _mock_period(name="2026-03", status="open")
    _wire_period_query(session, fp)
    platform_db = MagicMock()

    with (
      patch(f"{MODULE}.extensions_session", return_value=session),
      pytest.raises(HTTPException) as exc_info,
    ):
      await reopen_period(
        graph_id=GRAPH_ID,
        period="2026-03",
        body=ReopenPeriodRequest(reason="test"),
        current_user=_make_user(),
        _rate_limit=None,
        platform_db=platform_db,
      )
    assert exc_info.value.status_code == 422

  @pytest.mark.asyncio
  async def test_pydantic_rejects_empty_reason(self):
    with pytest.raises(ValueError):
      ReopenPeriodRequest(reason="")


# ────────────────────────────────────────────────────────────────────────────
# list_period_drafts (review endpoint)
# ────────────────────────────────────────────────────────────────────────────


def _mock_draft_rows(balanced: bool = True):
  """Return rows emulating the SQL join output for one or two draft entries."""
  row1_dr = MagicMock(
    entry_id="je_01",
    posting_date=date(2026, 3, 31),
    entry_type="closing",
    memo="Monthly amortization - Computer Equipment Depreciation",
    provenance="ai_generated",
    source_structure_id="struct_comp",
    source_structure_name="Computer Equipment Depreciation",
    line_item_id="li_01",
    element_id="elem_depr",
    element_code="7000",
    element_name="Depreciation Expense",
    debit_amount=13333,
    credit_amount=0,
    line_description="Monthly depreciation",
  )
  row1_cr = MagicMock(
    entry_id="je_01",
    posting_date=date(2026, 3, 31),
    entry_type="closing",
    memo="Monthly amortization - Computer Equipment Depreciation",
    provenance="ai_generated",
    source_structure_id="struct_comp",
    source_structure_name="Computer Equipment Depreciation",
    line_item_id="li_02",
    element_id="elem_accum",
    element_code="1350",
    element_name="Accumulated Depreciation",
    debit_amount=0,
    credit_amount=13333 if balanced else 13000,
    line_description="Monthly depreciation",
  )
  return [row1_dr, row1_cr]


class TestListPeriodDrafts:
  @pytest.mark.asyncio
  async def test_happy_path_single_balanced_draft(self):
    session = _mock_session()
    exec_mock = MagicMock()
    exec_mock.fetchall.return_value = _mock_draft_rows(balanced=True)
    session.execute.return_value = exec_mock

    with patch(f"{MODULE}.extensions_session", return_value=session):
      result = await list_period_drafts(
        graph_id=GRAPH_ID,
        period="2026-03",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.period == "2026-03"
    assert result.draft_count == 1
    assert result.all_balanced is True
    assert result.total_debit == 13333
    assert result.total_credit == 13333
    assert len(result.drafts) == 1
    draft = result.drafts[0]
    assert draft.entry_id == "je_01"
    assert draft.source_structure_name == "Computer Equipment Depreciation"
    assert draft.balanced is True
    assert len(draft.line_items) == 2
    assert draft.line_items[0].element_code == "7000"
    assert draft.line_items[0].debit_amount == 13333
    assert draft.line_items[1].credit_amount == 13333

  @pytest.mark.asyncio
  async def test_unbalanced_draft_flagged(self):
    session = _mock_session()
    exec_mock = MagicMock()
    exec_mock.fetchall.return_value = _mock_draft_rows(balanced=False)
    session.execute.return_value = exec_mock

    with patch(f"{MODULE}.extensions_session", return_value=session):
      result = await list_period_drafts(
        graph_id=GRAPH_ID,
        period="2026-03",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.draft_count == 1
    assert result.all_balanced is False
    assert result.drafts[0].balanced is False
    assert result.drafts[0].total_debit == 13333
    assert result.drafts[0].total_credit == 13000

  @pytest.mark.asyncio
  async def test_empty_period_returns_zero_count(self):
    session = _mock_session()
    exec_mock = MagicMock()
    exec_mock.fetchall.return_value = []
    session.execute.return_value = exec_mock

    with patch(f"{MODULE}.extensions_session", return_value=session):
      result = await list_period_drafts(
        graph_id=GRAPH_ID,
        period="2026-04",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.draft_count == 0
    assert result.total_debit == 0
    assert result.total_credit == 0
    assert result.all_balanced is True  # vacuously true
    assert result.drafts == []
