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
  VALID_BLOCK_TYPES,
  CoaMappingNotFoundError,
  build_current_and_prior_periods,
  get_live_financial_statement,
  get_statement,
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
      mock_date.side_effect = date
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
      mock_date.side_effect = date
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
  def test_happy_path_filters_abstracts_and_zeros_and_keeps_subtotals(self):
    session = MagicMock()

    def _row(name, values, is_subtotal=False, is_abstract=False):
      r = MagicMock()
      r.element_qname = f"us-gaap:{name}"
      r.element_name = name
      r.classification = "monetary"
      r.values = values
      r.depth = 0
      r.is_subtotal = is_subtotal
      r.is_abstract = is_abstract
      return r

    mock_grid = MagicMock()
    mock_grid.rows = [
      _row("Assets", [100.0, 90.0]),
      # Subtotal with value: KEPT — FAC anchors / calc targets like
      # GrossProfit are subtotals the reader most wants to see.
      _row("TotalAssets", [100.0, 90.0], is_subtotal=True),
      # Abstract container: dropped — XBRL `*Abstract`/`*Table`/
      # `*RollUp` rows are presentation scaffolding, never reportable.
      _row("AssetsAbstract", [100.0, 90.0], is_abstract=True),
      # All-zero: dropped.
      _row("Cash", [0.0, 0.0]),
      _row("Revenue", [50.0, None]),
    ]

    with (
      patch(
        "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
        return_value=(mock_grid, 3),
      ),
      patch(
        "robosystems.operations.roboledger.reports.network_picker.load_primary_reporting_style",
        return_value="025f5d48-12ce-5d65-b9eb-4f137a10ef06",
      ),
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
    assert resp.fact_count == 3
    assert resp.truncated is False
    names = [f.name for f in resp.facts]
    assert names == ["Assets", "TotalAssets", "Revenue"]

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
      r.is_abstract = False
      rows.append(r)
    mock_grid = MagicMock()
    mock_grid.rows = rows

    with (
      patch(
        "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
        return_value=(mock_grid, 0),
      ),
      patch(
        "robosystems.operations.roboledger.reports.network_picker.load_primary_reporting_style",
        return_value="025f5d48-12ce-5d65-b9eb-4f137a10ef06",
      ),
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
    with (
      patch(
        "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
        side_effect=CoaMappingNotFoundError("missing mapping"),
      ),
      patch(
        "robosystems.operations.roboledger.reports.network_picker.load_primary_reporting_style",
        return_value="025f5d48-12ce-5d65-b9eb-4f137a10ef06",
      ),
    ):
      with pytest.raises(CoaMappingNotFoundError):
        get_live_financial_statement(
          session,
          graph_id="kg_test",
          statement_type="income_statement",
          period_start=date(2026, 4, 1),
          period_end=date(2026, 4, 30),
        )


class TestGetStatementBlockTypeFilter:
  """Regression pin: ``get_statement`` filters facts by the owning
  structure's ``block_type``.

  Without this filter, elements persisted into multiple FactSets
  (``rs-gaap:NetIncomeLoss`` lives in IS + CF + SE per the
  ``_persist_report_facts`` design) all flow into
  ``_facts_to_balance_dict``, which sums them - inflating the rendered
  value 2x / 3x.

  Surfaced by the Seattle Method demo when ``create-report`` rendered
  the IS with NetIncomeLoss = $6,150 ($2,050 x 3) instead of $2,050.
  """

  @pytest.mark.unit
  def test_sql_filters_by_block_type(self):
    """The SQL query passed to ``session.execute`` must bind a
    ``block_type`` param and filter on ``s.block_type``. We assert on
    the bind params directly — they're the contract that prevents
    cross-FactSet duplicate-summing."""
    session = MagicMock()
    # Report row exists so the function progresses to the SQL fact load.
    report = MagicMock()
    report.id = "rpt_01"
    report.period_start = date(2024, 1, 1)
    report.period_end = date(2024, 3, 31)
    report.comparative = False
    report.periods = None
    session.get.return_value = report
    # SQL returns no rows → renderer short-circuits before touching
    # the structure picker; we don't care about the render output here,
    # only that the query was bound with block_type.
    session.execute.return_value = iter([])

    resp = get_statement(
      session,
      graph_id="kg_test",
      report_id="rpt_01",
      block_type="income_statement",
      reporting_style_id="style_01",
    )

    assert resp is not None
    # Inspect the SQL call: bind params must include block_type.
    call = session.execute.call_args
    sql_text = str(call.args[0])
    params = call.args[1]
    assert "s.block_type = :block_type" in sql_text, (
      "SQL must filter by block_type to avoid cross-FactSet summing"
    )
    assert params["block_type"] == "income_statement"
    assert params["report_id"] == "rpt_01"


class TestGetStatementValidBlockTypes:
  """Regression pin: every statement type accepted by
  ``live-financial-statement`` must also be accepted by ``get_statement``.

  Pre-fix, ``cash_flow_statement`` was missing from ``VALID_BLOCK_TYPES``
  so ``get_statement(blockType: 'cash_flow_statement')`` raised
  ``ValueError`` that the GraphQL resolver translated into a misleading
  ``LEDGER_NOT_INITIALIZED`` error.
  """

  @pytest.mark.unit
  def test_cash_flow_statement_is_valid(self):
    """The canonical four-statement set must all be accepted —
    cash_flow_statement included."""
    assert "cash_flow_statement" in VALID_BLOCK_TYPES
    assert "balance_sheet" in VALID_BLOCK_TYPES
    assert "income_statement" in VALID_BLOCK_TYPES
    assert "equity_statement" in VALID_BLOCK_TYPES

  @pytest.mark.unit
  def test_cash_flow_statement_passes_validation(self):
    """Concrete: a CF call doesn't raise ``ValueError``. Without the
    fix, this raised ``Invalid block_type 'cash_flow_statement'``."""
    session = MagicMock()
    session.get.return_value = None  # Report missing → returns None gracefully

    # Must not raise. If VALID_BLOCK_TYPES regresses to omit CF, this
    # line throws before reaching session.get.
    result = get_statement(
      session,
      graph_id="kg_test",
      report_id="rpt_missing",
      block_type="cash_flow_statement",
    )
    assert result is None  # Missing Report returns None, but no exception

  @pytest.mark.unit
  def test_invalid_block_type_still_rejected(self):
    """Tighten: unknown block types still get rejected. Loosening the
    validator past the canonical set would let bad inputs through."""
    session = MagicMock()
    with pytest.raises(ValueError, match="Invalid block_type"):
      get_statement(
        session,
        graph_id="kg_test",
        report_id="rpt_01",
        block_type="not_a_real_block_type",
      )


class TestGetStatementNumericFilter:
  def test_fact_query_filters_to_numeric(self) -> None:
    """``get_statement`` renders the numeric grid for a report's block —
    once FactSets can carry Nonnumeric (text-block) facts, the fact query
    must exclude them at the SQL level."""
    import inspect

    src = inspect.getsource(get_statement)
    assert "rf.fact_type = 'Numeric'" in src
