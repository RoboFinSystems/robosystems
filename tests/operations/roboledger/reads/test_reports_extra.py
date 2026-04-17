"""Tests for the new ops helpers added by the financial-statement split.

Covers:
- ``resolve_reporting_window`` — fuzzy-input → (start, end) date pair
- ``build_current_and_prior_periods`` — comparative period pair
- ``get_live_financial_statement`` — OLTP grid → Pydantic response
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.reads.reports import (
  CoaMappingNotFoundError,
  build_current_and_prior_periods,
  get_live_financial_statement,
  resolve_reporting_window,
)


class TestResolveReportingWindow:
  @pytest.mark.unit
  def test_explicit_dates_win(self):
    session = MagicMock()
    start, end = resolve_reporting_window(
      session,
      period_start=date(2025, 3, 1),
      period_end=date(2025, 3, 31),
      period_type="annual",
      fiscal_year=2024,
    )
    assert start == date(2025, 3, 1)
    assert end == date(2025, 3, 31)
    # Never hit the session when dates are explicit.
    assert not session.method_calls

  @pytest.mark.unit
  def test_annual_default_fiscal_year_start_january(self):
    session = MagicMock()
    with patch(
      "robosystems.operations.roboledger.reads.reports.get_fiscal_year_start_month",
      return_value=1,
    ):
      start, end = resolve_reporting_window(
        session,
        period_start=None,
        period_end=None,
        period_type="annual",
        fiscal_year=2025,
      )
    assert start == date(2025, 1, 1)
    assert end == date(2025, 12, 31)

  @pytest.mark.unit
  def test_annual_fiscal_year_start_july(self):
    """Graph with July fiscal-year start -> 2025 FY = Jul 2025 - Jun 2026."""
    session = MagicMock()
    with patch(
      "robosystems.operations.roboledger.reads.reports.get_fiscal_year_start_month",
      return_value=7,
    ):
      start, end = resolve_reporting_window(
        session,
        period_start=None,
        period_end=None,
        period_type="annual",
        fiscal_year=2025,
      )
    assert start == date(2025, 7, 1)
    assert end == date(2026, 6, 30)

  @pytest.mark.unit
  def test_quarterly_returns_current_quarter(self):
    session = MagicMock()
    with patch("robosystems.operations.roboledger.reads.reports.date") as mock_date:
      # Force "today" to Feb 15, 2026 -> Q1 = Jan-Mar.
      mock_date.today.return_value = date(2026, 2, 15)
      mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
      start, end = resolve_reporting_window(
        session,
        period_start=None,
        period_end=None,
        period_type="quarterly",
        fiscal_year=None,
      )
    assert start == date(2026, 1, 1)
    assert end == date(2026, 3, 31)

  @pytest.mark.unit
  def test_instant_defaults_to_current_month(self):
    session = MagicMock()
    with patch("robosystems.operations.roboledger.reads.reports.date") as mock_date:
      mock_date.today.return_value = date(2026, 4, 15)
      mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
      start, end = resolve_reporting_window(
        session,
        period_start=None,
        period_end=None,
        period_type=None,
        fiscal_year=None,
      )
    assert start == date(2026, 4, 1)
    assert end == date(2026, 4, 30)


class TestBuildCurrentAndPriorPeriods:
  @pytest.mark.unit
  def test_month_pair(self):
    periods = build_current_and_prior_periods(date(2026, 4, 1), date(2026, 4, 30))
    assert len(periods) == 2
    assert periods[0].label == "Current"
    assert periods[0].start == date(2026, 4, 1)
    assert periods[0].end == date(2026, 4, 30)
    assert periods[1].label == "Prior"
    assert periods[1].end == date(2026, 3, 31)
    assert periods[1].start == date(2026, 3, 2)  # 30-day duration match

  @pytest.mark.unit
  def test_quarter_pair(self):
    periods = build_current_and_prior_periods(date(2026, 1, 1), date(2026, 3, 31))
    assert periods[0].start == date(2026, 1, 1)
    assert periods[0].end == date(2026, 3, 31)
    assert periods[1].end == date(2025, 12, 31)
    # 90-day duration: prior_start = 2025-10-03
    assert periods[1].start == date(2025, 10, 3)


class TestGetLiveFinancialStatement:
  @pytest.mark.unit
  def test_happy_path_filters_subtotals_and_zeros(self):
    session = MagicMock()

    def _row(name, values, is_subtotal=False):
      r = MagicMock()
      r.element_qname = f"us-gaap:{name}"
      r.element_name = name
      r.classification = "monetary"
      r.values = values
      r.depth = 0
      r.is_subtotal = is_subtotal
      return r

    mock_grid = MagicMock()
    mock_grid.rows = [
      _row("Assets", [100.0, 90.0]),
      _row("TotalAssets", [100.0, 90.0], is_subtotal=True),  # filtered
      _row("Cash", [0.0, 0.0]),  # filtered (all zero)
      _row("Revenue", [50.0, None]),
    ]

    with patch(
      "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
      return_value=(mock_grid, 3),
    ):
      resp = get_live_financial_statement(
        session,
        graph_id="kg_test",
        statement_type="income_statement",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 4, 30),
        limit=50,
      )

    assert resp.graph_id == "kg_test"
    assert resp.statement_type == "income_statement"
    assert resp.unmapped_count == 3
    assert resp.fact_count == 2
    assert resp.truncated is False
    names = [f.name for f in resp.facts]
    assert names == ["Assets", "Revenue"]

  @pytest.mark.unit
  def test_limit_truncates(self):
    session = MagicMock()
    rows = []
    for i in range(10):
      r = MagicMock()
      r.element_qname = f"us-gaap:E{i}"
      r.element_name = f"E{i}"
      r.classification = "monetary"
      r.values = [float(i + 1)]
      r.depth = 0
      r.is_subtotal = False
      rows.append(r)
    mock_grid = MagicMock()
    mock_grid.rows = rows

    with patch(
      "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
      return_value=(mock_grid, 0),
    ):
      resp = get_live_financial_statement(
        session,
        graph_id="kg_test",
        statement_type="balance_sheet",
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 31),
        limit=3,
      )

    assert resp.fact_count == 3
    assert resp.truncated is True

  @pytest.mark.unit
  def test_coa_mapping_missing_propagates(self):
    session = MagicMock()
    with patch(
      "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
      side_effect=CoaMappingNotFoundError("missing mapping"),
    ):
      with pytest.raises(CoaMappingNotFoundError):
        get_live_financial_statement(
          session,
          graph_id="kg_test",
          statement_type="income_statement",
          period_start=date(2026, 4, 1),
          period_end=date(2026, 4, 30),
        )
