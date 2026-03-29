"""Unit tests for ledger report endpoints."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
)
from robosystems.routers.ledger.reports import (
  create_report,
  delete_report,
  get_report,
  list_reports,
)

MODULE = "robosystems.routers.ledger.reports"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_report_def(**overrides):
  """Create a mock ReportDefinition."""
  rd = MagicMock()
  rd.id = overrides.get("id", "rpt_01ABC")
  rd.name = overrides.get("name", "Q1 Income Statement")
  rd.report_type = overrides.get("report_type", "income_statement")
  rd.generation_status = overrides.get("generation_status", "published")
  rd.period_type = overrides.get("period_type", "quarterly")
  rd.comparative = overrides.get("comparative", True)
  rd.mapping_id = overrides.get("mapping_id", "struct_income_statement")
  rd.period_start = overrides.get("period_start", date(2026, 1, 1))
  rd.period_end = overrides.get("period_end", date(2026, 3, 31))
  rd.ai_generated = overrides.get("ai_generated", False)
  rd.ai_intent = None
  rd.ai_workspace_id = None
  rd.ai_confidence = None
  rd.created_at = overrides.get("created_at", datetime(2026, 3, 29, tzinfo=UTC))
  rd.last_generated = overrides.get("last_generated", datetime(2026, 3, 29, tzinfo=UTC))
  rd.created_by = "usr_test123"
  return rd


def _make_fact_grid():
  """Create a mock FactGrid."""
  from robosystems.operations.reports.fact_grid import FactGrid, FactRow
  from robosystems.operations.reports.guard_rails import ValidationResult

  grid = FactGrid(
    report_type="income_statement",
    structure_id="struct_income_statement",
    structure_name="US GAAP Income Statement",
    period_start=date(2026, 1, 1),
    period_end=date(2026, 3, 31),
    comparative_period_start=date(2025, 10, 3),
    comparative_period_end=date(2025, 12, 31),
    rows=[
      FactRow(
        element_id="elem_gaap_revenues",
        element_qname="us-gaap:Revenues",
        element_name="Revenue",
        classification="revenue",
        balance_type="credit",
        current_value=500000.0,
        prior_value=400000.0,
        is_subtotal=False,
        depth=1,
      ),
    ],
    unmapped_count=0,
  )
  grid.validation = ValidationResult(
    passed=True, checks=["totals_foot"], failures=[], warnings=[]
  )
  return grid


def _mock_session_context(mock_session):
  """Create a patched extensions_session context manager."""
  ctx = MagicMock()
  ctx.__enter__ = MagicMock(return_value=mock_session)
  ctx.__exit__ = MagicMock(return_value=False)
  return ctx


class TestCreateReport:
  @pytest.mark.asyncio
  async def test_creates_report_successfully(self):
    mock_session = MagicMock()
    mock_session.flush = MagicMock()
    mock_session.commit = MagicMock()

    grid = _make_fact_grid()

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext,
      patch(f"{MODULE}.build_fact_grid", return_value=grid),
      patch(f"{MODULE}.validate_report", return_value=grid.validation),
    ):
      mock_ext.return_value = _mock_session_context(mock_session)

      body = CreateReportRequest(
        name="Q1 Income Statement",
        report_type="income_statement",
        mapping_id="struct_income_statement",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
      )

      result = await create_report(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.report_type == "income_statement"
    assert len(result.rows) == 1
    assert result.rows[0].current_value == 500000.0
    assert result.validation.passed is True
    mock_session.add.assert_called_once()
    mock_session.commit.assert_called_once()

  @pytest.mark.asyncio
  async def test_rejects_invalid_report_type(self):
    body = CreateReportRequest(
      name="Bad Report",
      report_type="invalid_type",
      mapping_id="struct_1",
      period_start=date(2026, 1, 1),
      period_end=date(2026, 3, 31),
    )

    with pytest.raises(HTTPException) as exc_info:
      await create_report(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 422
    assert "Invalid report_type" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_rejects_end_before_start(self):
    body = CreateReportRequest(
      name="Bad Dates",
      report_type="income_statement",
      mapping_id="struct_1",
      period_start=date(2026, 6, 30),
      period_end=date(2026, 1, 1),
    )

    with pytest.raises(HTTPException) as exc_info:
      await create_report(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 422


class TestListReports:
  @pytest.mark.asyncio
  async def test_lists_reports(self):
    mock_session = MagicMock()
    rd1 = _make_report_def(id="rpt_1", name="Report 1")
    rd2 = _make_report_def(id="rpt_2", name="Report 2")

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [rd1, rd2]
    mock_session.execute.return_value = mock_result

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      result = await list_reports(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.reports) == 2
    assert result.reports[0].id == "rpt_1"
    assert result.reports[1].id == "rpt_2"

  @pytest.mark.asyncio
  async def test_empty_list(self):
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      result = await list_reports(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.reports) == 0


class TestGetReport:
  @pytest.mark.asyncio
  async def test_gets_report_with_data(self):
    mock_session = MagicMock()
    rd = _make_report_def()
    mock_session.get.return_value = rd

    grid = _make_fact_grid()

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext,
      patch(f"{MODULE}.build_fact_grid", return_value=grid),
      patch(f"{MODULE}.validate_report", return_value=grid.validation),
    ):
      mock_ext.return_value = _mock_session_context(mock_session)

      result = await get_report(
        graph_id=GRAPH_ID,
        report_id="rpt_01ABC",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.id == "rpt_01ABC"
    assert len(result.rows) == 1
    assert result.structure_name == "US GAAP Income Statement"

  @pytest.mark.asyncio
  async def test_report_not_found(self):
    mock_session = MagicMock()
    mock_session.get.return_value = None

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await get_report(
          graph_id=GRAPH_ID,
          report_id="rpt_nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404


class TestDeleteReport:
  @pytest.mark.asyncio
  async def test_deletes_report(self):
    mock_session = MagicMock()
    rd = _make_report_def()
    mock_session.get.return_value = rd

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      await delete_report(
        graph_id=GRAPH_ID,
        report_id="rpt_01ABC",
        current_user=_make_user(),
        _rate_limit=None,
      )

    mock_session.delete.assert_called_once_with(rd)
    mock_session.commit.assert_called_once()

  @pytest.mark.asyncio
  async def test_delete_not_found(self):
    mock_session = MagicMock()
    mock_session.get.return_value = None

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await delete_report(
          graph_id=GRAPH_ID,
          report_id="rpt_nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404
