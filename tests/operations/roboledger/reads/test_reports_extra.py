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
  rendered_period_indexes,
  resolve_reporting_window,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  FactRow,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec as FactPeriodSpec,
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
  """Whole-month ranges compare against whole calendar months.

  Both assertions here previously pinned day-count arithmetic — April's
  prior was 2026-03-02 ("30-day duration match") and Q1's was 2025-10-03
  ("90-day duration"). Those are the defect written down: neither window
  matches a stored monthly FactSet, so the comparative column queried a
  range nothing was stamped into.
  """

  @pytest.mark.unit
  def test_month_pair(self):
    periods = build_current_and_prior_periods(date(2026, 4, 1), date(2026, 4, 30))
    assert len(periods) == 2
    assert periods[0].label == "Current"
    assert periods[0].start == date(2026, 4, 1)
    assert periods[0].end == date(2026, 4, 30)
    assert periods[1].label == "Prior"
    assert periods[1].start == date(2026, 3, 1)
    assert periods[1].end == date(2026, 3, 31)

  @pytest.mark.unit
  def test_quarter_pair(self):
    periods = build_current_and_prior_periods(date(2026, 1, 1), date(2026, 3, 31))
    assert periods[0].start == date(2026, 1, 1)
    assert periods[0].end == date(2026, 3, 31)
    assert periods[1].start == date(2025, 10, 1)
    assert periods[1].end == date(2025, 12, 31)


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


class TestRenderedPeriodIndexes:
  """The earliest period of a cash flow pivot is the indirect-method delta
  basis, not a statement. ``_derive_cash_flow_facts`` and
  ``_reconcile_operating_to_cash`` populate every period but the first, so
  rendered it foots and is wrong — on a live tenant the comparative month
  showed 2.19x the balance-sheet cash movement. The close-time stamp already
  keeps only the close month (``statement_sets``); the read paths apply the
  same rule here."""

  @pytest.mark.unit
  def test_cash_flow_drops_the_earliest_period(self):
    periods = build_current_and_prior_periods(date(2026, 7, 1), date(2026, 7, 31))
    assert rendered_period_indexes("cash_flow_statement", periods) == [0]

  @pytest.mark.unit
  def test_drops_by_date_not_position(self):
    """Multi-period reports store periods in authored order; the basis is
    whichever ends first, wherever it sits."""
    periods = [
      FactPeriodSpec(start=date(2026, 5, 1), end=date(2026, 5, 31), label="May"),
      FactPeriodSpec(start=date(2026, 7, 1), end=date(2026, 7, 31), label="Jul"),
      FactPeriodSpec(start=date(2026, 6, 1), end=date(2026, 6, 30), label="Jun"),
    ]
    assert rendered_period_indexes("cash_flow_statement", periods) == [1, 2]

  @pytest.mark.unit
  @pytest.mark.parametrize(
    "statement_type", ["income_statement", "balance_sheet", "equity_statement"]
  )
  def test_other_statements_render_every_period(self, statement_type):
    periods = build_current_and_prior_periods(date(2026, 7, 1), date(2026, 7, 31))
    assert rendered_period_indexes(statement_type, periods) == [0, 1]

  @pytest.mark.unit
  def test_single_period_cash_flow_is_left_alone(self):
    periods = [
      FactPeriodSpec(start=date(2026, 7, 1), end=date(2026, 7, 31), label="Jul")
    ]
    assert rendered_period_indexes("cash_flow_statement", periods) == [0]


class TestLiveCashFlowRendersCurrentOnly:
  @pytest.mark.unit
  def test_prior_column_is_pivoted_but_not_rendered(self):
    session = MagicMock()

    def _row(name, values):
      r = MagicMock()
      r.element_qname = f"rs-gaap:{name}"
      r.element_name = name
      r.classification = "monetary"
      r.values = values
      r.depth = 0
      r.is_subtotal = False
      r.is_abstract = False
      return r

    mock_grid = MagicMock()
    mock_grid.rows = [
      _row("NetIncomeLoss", [1_000.0, 900.0]),
      # Derived only for the current column — the basis period has none.
      _row("IncreaseDecreaseInAccountsReceivable", [-250.0, None]),
      # Non-zero only in the basis column: nothing to render.
      _row("PaymentsToAcquirePropertyPlantAndEquipment", [0.0, -400.0]),
    ]

    with (
      patch(
        "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
        return_value=(mock_grid, 0),
      ) as generate,
      patch(
        "robosystems.operations.roboledger.reports.network_picker.load_primary_reporting_style",
        return_value="025f5d48-12ce-5d65-b9eb-4f137a10ef06",
      ),
    ):
      resp = get_live_financial_statement(
        session,
        graph_id="kg_test",
        statement_type="cash_flow_statement",
        period_start=date(2026, 7, 1),
        period_end=date(2026, 7, 31),
      )

    # Both periods reach the generator — the prior month is the delta basis.
    pivoted = generate.call_args.kwargs["periods"]
    assert [p.label for p in pivoted] == ["Current", "Prior"]
    # Only the current period is rendered, and every row is one column wide.
    assert [p.label for p in resp.periods] == ["Current"]
    assert resp.periods[0].end == date(2026, 7, 31)
    assert [f.name for f in resp.facts] == [
      "NetIncomeLoss",
      "IncreaseDecreaseInAccountsReceivable",
    ]
    assert [f.values for f in resp.facts] == [[1_000.0], [-250.0]]

  @pytest.mark.unit
  def test_income_statement_still_renders_both(self):
    session = MagicMock()
    r = MagicMock()
    r.element_qname = "rs-gaap:Revenues"
    r.element_name = "Revenues"
    r.classification = "monetary"
    r.values = [500.0, 450.0]
    r.depth = 0
    r.is_subtotal = False
    r.is_abstract = False
    mock_grid = MagicMock()
    mock_grid.rows = [r]

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
        statement_type="income_statement",
        period_start=date(2026, 7, 1),
        period_end=date(2026, 7, 31),
      )

    assert [p.label for p in resp.periods] == ["Current", "Prior"]
    assert resp.facts[0].values == [500.0, 450.0]


class TestSavedCashFlowRendersCurrentOnly:
  """``get_statement`` on a comparative report: the persisted facts for the
  prior period are the delta basis, so a cash flow renders one column."""

  def _report(self, comparative: bool):
    report = MagicMock()
    report.id = "rpt_01"
    report.period_start = date(2026, 7, 1)
    report.period_end = date(2026, 7, 31)
    report.comparative = comparative
    report.periods = None
    return report

  @pytest.mark.unit
  def test_comparative_cash_flow_renders_the_current_period(self):
    session = MagicMock()
    session.get.return_value = self._report(comparative=True)
    session.execute.return_value = iter([])

    resp = get_statement(
      session,
      graph_id="kg_test",
      report_id="rpt_01",
      block_type="cash_flow_statement",
      reporting_style_id="style_01",
    )

    assert resp is not None
    assert [p.label for p in resp.periods] == ["Current"]

  @pytest.mark.unit
  def test_comparative_balance_sheet_renders_both(self):
    session = MagicMock()
    session.get.return_value = self._report(comparative=True)
    session.execute.return_value = iter([])

    resp = get_statement(
      session,
      graph_id="kg_test",
      report_id="rpt_01",
      block_type="balance_sheet",
      reporting_style_id="style_01",
    )

    assert resp is not None
    assert [p.label for p in resp.periods] == ["Current", "Prior"]


def _fact_row(
  name: str,
  values: list[float | None],
  *,
  classification: str = "revenue",
  balance_type: str | None = None,
  is_subtotal: bool = False,
  depth: int = 0,
  qname: str | None = None,
) -> FactRow:
  return FactRow(
    element_id=f"e_{name}",
    element_qname=qname or f"rs-gaap:{name}",
    element_name=name,
    classification=classification,
    balance_type=balance_type
    or ("credit" if classification in ("revenue", "liability", "equity") else "debit"),
    values=values,
    is_subtotal=is_subtotal,
    depth=depth,
  )


def _live(session, statement_type, grid):
  with (
    patch(
      "robosystems.operations.roboledger.reads.reports.generate_adhoc_private_statement",
      return_value=(grid, 0),
    ),
    patch(
      "robosystems.operations.roboledger.reports.network_picker.load_primary_reporting_style",
      return_value="025f5d48-12ce-5d65-b9eb-4f137a10ef06",
    ),
  ):
    return get_live_financial_statement(
      session,
      graph_id="kg_test",
      statement_type=statement_type,
      period_start=date(2026, 7, 1),
      period_end=date(2026, 7, 31),
    )


class TestLiveStatementValidation:
  """The live statement — the surface the app renders and the MCP tool
  returns — carries the guard-rail outcome for exactly its rendered columns."""

  @pytest.mark.unit
  def test_validation_runs_on_both_rendered_columns(self):
    grid = MagicMock()
    grid.rows = [
      _fact_row("Assets", [100.0, 100.0], classification="asset", is_subtotal=True),
      _fact_row(
        "Liabilities", [40.0, 40.0], classification="liability", is_subtotal=True
      ),
      # Balances in Current, not in Prior.
      _fact_row("Equity", [60.0, 55.0], classification="equity", is_subtotal=True),
    ]
    resp = _live(MagicMock(), "balance_sheet", grid)

    assert resp.validation is not None
    assert resp.validation.status == "failed"
    assert resp.validation.passed is False
    assert "accounting_equation" in resp.validation.checks
    assert len(resp.validation.failures) == 1
    assert resp.validation.failures[0].startswith(
      "[Prior] Balance sheet does not balance"
    )

  @pytest.mark.unit
  def test_clean_statement_passes(self):
    grid = MagicMock()
    grid.rows = [
      _fact_row("Assets", [100.0, 90.0], classification="asset", is_subtotal=True),
      _fact_row(
        "Liabilities", [40.0, 30.0], classification="liability", is_subtotal=True
      ),
      _fact_row("Equity", [60.0, 60.0], classification="equity", is_subtotal=True),
    ]
    resp = _live(MagicMock(), "balance_sheet", grid)
    assert resp.validation is not None
    assert (resp.validation.status, resp.validation.passed) == ("passed", True)
    assert resp.validation.failures == []

  @pytest.mark.unit
  def test_cash_flow_validates_only_the_rendered_column(self):
    """The prior period rides the pivot as the delta basis and is not a
    statement; its plug must not be reported against the rendered column."""
    op = "rs-gaap:NetCashProvidedByUsedInOperatingActivities"
    plug = "rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"
    grid = MagicMock()
    grid.rows = [
      _fact_row("Operating", [1000.0, 100.0], qname=op),
      # Quiet in Current (10%), loud in the basis column (500%).
      _fact_row("OtherOperatingCapital", [100.0, 500.0], qname=plug),
    ]
    resp = _live(MagicMock(), "cash_flow_statement", grid)

    assert [p.label for p in resp.periods] == ["Current"]
    assert resp.validation is not None
    assert resp.validation.status == "passed"
    assert "operating_plug" in resp.validation.checks
    assert not any("Other operating capital" in w for w in resp.validation.warnings)

  @pytest.mark.unit
  def test_cash_flow_plug_in_the_rendered_column_warns(self):
    op = "rs-gaap:NetCashProvidedByUsedInOperatingActivities"
    plug = "rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"
    grid = MagicMock()
    grid.rows = [
      _fact_row("Operating", [-1391.39, 0.0], qname=op),
      _fact_row("OtherOperatingCapital", [4153.73, 0.0], qname=plug),
    ]
    resp = _live(MagicMock(), "cash_flow_statement", grid)

    assert resp.validation is not None
    warnings = [w for w in resp.validation.warnings if "Other operating capital" in w]
    assert len(warnings) == 1
    assert "4153.73" in warnings[0] and "-1391.39" in warnings[0]
