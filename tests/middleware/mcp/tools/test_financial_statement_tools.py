"""Tests for the two financial-statement MCP tools.

Replaces the old `test_financial_statement.py` which covered the
pre-split `GetFinancialStatementTool`. Coverage here:
- tool definitions expose expected names and inputs
- LiveFinancialStatementTool delegates to ops-layer helpers
- FinancialStatementAnalysisTool dispatches shared-repo vs tenant correctly
- error paths: missing inputs, invalid types, CoaMappingNotFoundError
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.financial_statement_tools import (
  PERIODS_DEFAULT_DURATION,
  PERIODS_DEFAULT_INSTANT,
  PERIODS_MAX,
  QUERY_ROW_CEILING,
  FinancialStatementAnalysisTool,
  LiveFinancialStatementTool,
  cap_periods,
  compact_fact,
  default_period_type,
)
from robosystems.operations.roboledger.reads.reports import CoaMappingNotFoundError

MODULE = "robosystems.middleware.mcp.tools.financial_statement_tools"


def _make_client(graph_id: str = "kg_test"):
  client = MagicMock()
  client.graph_id = graph_id
  return client


# ──────────────────────────────────────────────────────────────────────────
# LiveFinancialStatementTool
# ──────────────────────────────────────────────────────────────────────────


class TestLiveFinancialStatementToolDefinition:
  @pytest.mark.unit
  def test_name_and_required_inputs(self):
    tool = LiveFinancialStatementTool(_make_client())
    d = tool.get_tool_definition()
    assert d["name"] == "live-financial-statement"
    assert "statement_type" in d["inputSchema"]["required"]
    enum = d["inputSchema"]["properties"]["statement_type"]["enum"]
    assert "income_statement" in enum
    assert "balance_sheet" in enum
    assert "cash_flow_statement" in enum
    # Served on REST for the app's tab, not offered to an operator until it
    # articulates — today it is equity balances, not a rollforward.
    assert "equity_statement" not in enum

  @pytest.mark.unit
  def test_limit_defaults_to_the_whole_statement(self):
    tool = LiveFinancialStatementTool(_make_client())
    d = tool.get_tool_definition()
    assert d["inputSchema"]["properties"]["limit"]["default"] == 1000


@pytest.mark.asyncio
class TestLiveFinancialStatementToolExecute:
  @pytest.mark.unit
  async def test_happy_path(self):
    tool = LiveFinancialStatementTool(_make_client("kg_123"))
    from robosystems.models.api.extensions.reports import (
      LiveFinancialStatementResponse,
    )

    mock_response = LiveFinancialStatementResponse(
      graph_id="kg_123",
      statement_type="income_statement",
      periods=[],
      facts=[],
      fact_count=0,
      unmapped_count=0,
    )

    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__exit__.return_value = False

    with (
      patch(f"{MODULE}.extensions_session", return_value=cm),
      patch(
        f"{MODULE}.resolve_reporting_window",
        return_value=(date(2026, 4, 1), date(2026, 4, 30)),
      ),
      patch(f"{MODULE}.get_live_financial_statement", return_value=mock_response),
    ):
      result = await tool.execute({"statement_type": "income_statement"})

    assert result["statement_type"] == "income_statement"
    assert result["fact_count"] == 0
    assert "tip" in result  # empty results trigger the tip

  @pytest.mark.unit
  async def test_validation_rides_the_payload(self):
    """The guard-rail outcome reaches the model verbatim — it is the only
    signal that a returned number does not foot."""
    tool = LiveFinancialStatementTool(_make_client("kg_123"))
    from robosystems.models.api.extensions.reports import (
      LiveFinancialStatementResponse,
      LiveStatementFactRow,
      PeriodSpec,
      ValidationCheckResponse,
    )

    mock_response = LiveFinancialStatementResponse(
      graph_id="kg_123",
      statement_type="balance_sheet",
      periods=[
        PeriodSpec(start=date(2026, 7, 1), end=date(2026, 7, 31), label="Current")
      ],
      facts=[
        LiveStatementFactRow(qname="rs-gaap:Assets", name="Assets", values=[100.0])
      ],
      fact_count=1,
      validation=ValidationCheckResponse(
        passed=False,
        status="failed",
        checks=["accounting_equation"],
        failures=["Balance sheet does not balance: Assets (100.00) ≠ …"],
        warnings=[],
      ),
    )

    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__exit__.return_value = False

    with (
      patch(f"{MODULE}.extensions_session", return_value=cm),
      patch(
        f"{MODULE}.resolve_reporting_window",
        return_value=(date(2026, 7, 1), date(2026, 7, 31)),
      ),
      patch(f"{MODULE}.get_live_financial_statement", return_value=mock_response),
    ):
      result = await tool.execute({"statement_type": "balance_sheet"})

    assert result["validation"]["status"] == "failed"
    assert result["validation"]["passed"] is False
    assert result["validation"]["failures"] == [
      "Balance sheet does not balance: Assets (100.00) ≠ …"
    ]
    assert "tip" not in result

  @pytest.mark.unit
  async def test_default_limit_is_the_ceiling(self):
    """A real chart of accounts exceeds 50 non-zero rows routinely, and a
    cut statement's visible rows stop footing to its subtotals."""
    tool = LiveFinancialStatementTool(_make_client("kg_123"))
    from robosystems.models.api.extensions.reports import (
      LiveFinancialStatementResponse,
    )

    mock_response = LiveFinancialStatementResponse(
      graph_id="kg_123",
      statement_type="balance_sheet",
      periods=[],
      facts=[],
      fact_count=0,
    )
    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__exit__.return_value = False

    with (
      patch(f"{MODULE}.extensions_session", return_value=cm),
      patch(
        f"{MODULE}.resolve_reporting_window",
        return_value=(date(2026, 4, 1), date(2026, 4, 30)),
      ),
      patch(
        f"{MODULE}.get_live_financial_statement", return_value=mock_response
      ) as live,
    ):
      await tool.execute({"statement_type": "balance_sheet"})

    assert live.call_args.kwargs["limit"] == 1000

  @pytest.mark.unit
  async def test_equity_statement_is_refused_with_a_reason(self):
    tool = LiveFinancialStatementTool(_make_client())
    result = await tool.execute({"statement_type": "equity_statement"})
    assert "rollforward" in result["error"]
    assert "balance_sheet" in result["error"]

  @pytest.mark.unit
  async def test_missing_statement_type(self):
    tool = LiveFinancialStatementTool(_make_client())
    result = await tool.execute({})
    assert "statement_type" in result["error"].lower()

  @pytest.mark.unit
  async def test_invalid_statement_type(self):
    tool = LiveFinancialStatementTool(_make_client())
    result = await tool.execute({"statement_type": "bogus"})
    assert "Unknown statement_type" in result["error"]
    # The error message points users at the graph-backed analysis op as a
    # fall-through for statement types not covered by the OLTP path.
    assert "financial-statement-analysis" in result["error"]

  @pytest.mark.unit
  async def test_missing_mapping_returns_error_dict(self):
    tool = LiveFinancialStatementTool(_make_client())
    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__exit__.return_value = False
    with (
      patch(f"{MODULE}.extensions_session", return_value=cm),
      patch(
        f"{MODULE}.resolve_reporting_window",
        return_value=(date(2026, 4, 1), date(2026, 4, 30)),
      ),
      patch(
        f"{MODULE}.get_live_financial_statement",
        side_effect=CoaMappingNotFoundError("no mapping"),
      ),
    ):
      result = await tool.execute({"statement_type": "income_statement"})
    assert "no mapping" in result["error"]


# ──────────────────────────────────────────────────────────────────────────
# FinancialStatementAnalysisTool
# ──────────────────────────────────────────────────────────────────────────


class TestFinancialStatementAnalysisToolDefinition:
  @pytest.mark.unit
  def test_name_and_enum(self):
    tool = FinancialStatementAnalysisTool(_make_client())
    d = tool.get_tool_definition()
    assert d["name"] == "financial-statement-analysis"
    enum = d["inputSchema"]["properties"]["statement_type"]["enum"]
    assert "cash_flow_statement" in enum
    assert "income_statement" in enum


@pytest.mark.asyncio
class TestFinancialStatementAnalysisToolExecute:
  @pytest.mark.unit
  async def test_unresolvable_fiscal_year_errors_instead_of_answering(self):
    """Mirror of the REST view op's 404. An MCP caller asking for a fiscal
    year with no filing must get an error, not the newest filing — the
    unscoped ticker sweep never receives fiscal_year, so falling through
    silently answers about a different year."""
    tool = FinancialStatementAnalysisTool(_make_client("sec"))

    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new_callable=AsyncMock,
        return_value=None,
      ),
      patch(
        f"{MODULE}.query_financial_statement",
        new_callable=AsyncMock,
        return_value=[],
      ) as mock_query,
    ):
      result = await tool.execute(
        {
          "statement_type": "income_statement",
          "ticker": "NVDA",
          "fiscal_year": 2005,
        }
      )

    assert "error" in result
    assert "2005" in result["error"]
    mock_query.assert_not_called()

  @pytest.mark.unit
  async def test_shared_repo_ticker_autoresolves(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    resolved = {
      "identifier": "rpt_abc",
      "form": "10-K",
      "filing_date": "2025-06-30",
      "fiscal_year": 2025,
      "fiscal_period": "FY",
    }
    rows = [
      {
        "canonical_concept": "revenue",
        "qname": "us-gaap:Revenues",
        "name": "Revenues",
        "value": 100.0,
        "end_date": "2025-01-31",
        "period_type": "duration",
        "duration_type": "annual",
      }
    ]

    with (
      patch(
        f"{MODULE}.is_shared_repository_or_subgraph",
        return_value=True,
      ),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new_callable=AsyncMock,
        return_value=resolved,
      ),
      patch(
        f"{MODULE}.query_financial_statement",
        new_callable=AsyncMock,
        return_value=rows,
      ),
    ):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "NVDA"}
      )

    assert result["report_id"] == "rpt_abc"
    assert result["ticker"] == "NVDA"
    assert result["fact_count"] == 1
    assert result["resolved_report"]["form"] == "10-K"

  @pytest.mark.unit
  async def test_shared_repo_no_ticker_and_no_report_errors(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    with patch(
      f"{MODULE}.is_shared_repository_or_subgraph",
      return_value=True,
    ):
      result = await tool.execute({"statement_type": "income_statement"})
    assert "ticker is required" in result["error"].lower()

  @pytest.mark.unit
  async def test_tenant_graph_requires_report_id(self):
    tool = FinancialStatementAnalysisTool(_make_client("kg_abc"))
    with patch(
      f"{MODULE}.is_shared_repository_or_subgraph",
      return_value=False,
    ):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "NVDA"}
      )
    assert "report_id is required" in result["error"].lower()

  @pytest.mark.unit
  async def test_tenant_graph_with_report_id_skips_resolver(self):
    tool = FinancialStatementAnalysisTool(_make_client("kg_abc"))
    with (
      patch(
        f"{MODULE}.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new_callable=AsyncMock,
      ) as mock_resolve,
      patch(
        f"{MODULE}.query_financial_statement",
        new_callable=AsyncMock,
        return_value=[],
      ),
    ):
      result = await tool.execute(
        {"statement_type": "income_statement", "report_id": "rpt_xyz"}
      )
    assert result["report_id"] == "rpt_xyz"
    mock_resolve.assert_not_called()

  @pytest.mark.unit
  async def test_invalid_statement_type(self):
    tool = FinancialStatementAnalysisTool(_make_client("kg_abc"))
    result = await tool.execute({"statement_type": "bogus"})
    assert "Unknown statement_type" in result["error"]


# ──────────────────────────────────────────────────────────────────────────
# FinancialStatementAnalysisTool — the response economy half
# ──────────────────────────────────────────────────────────────────────────


def _row(qname: str, end: str, value: float = 1.0, **overrides):
  row = {
    "canonical_concept": None,
    "qname": qname,
    "name": qname.split(":")[-1],
    "value": value,
    "start_date": f"{end[:4]}-01-01",
    "end_date": end,
    "period_type": "duration",
    "duration_type": "annual",
  }
  row.update(overrides)
  return row


_RESOLVED_10K = {
  "identifier": "rpt_10k",
  "form": "10-K",
  "filing_date": "2025-02-05",
  "fiscal_year": 2024,
  "fiscal_period": "FY",
}


@contextmanager
def _shared_repo_patches(resolved, rows):
  """A shared-repo graph whose resolver and statement query are canned;
  yields the query mock so a test can read the period filter it was sent."""
  with (
    patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
    patch(
      "robosystems.adapters.sec.mcp.resolve_sec_report",
      new_callable=AsyncMock,
      return_value=resolved,
    ),
    patch(
      f"{MODULE}.query_financial_statement",
      new_callable=AsyncMock,
      return_value=rows,
    ) as mock_query,
  ):
    yield mock_query


class TestDefaultPeriodType:
  @pytest.mark.unit
  @pytest.mark.parametrize("form", ["10-K", "20-F", "40-F"])
  def test_annual_form_defaults_to_annual(self, form):
    assert default_period_type("income_statement", form) == "annual"

  @pytest.mark.unit
  def test_quarterly_form_is_not_filtered(self):
    assert default_period_type("income_statement", "10-Q") is None

  @pytest.mark.unit
  def test_balance_sheet_keeps_the_query_default(self):
    assert default_period_type("balance_sheet", "10-K") is None

  @pytest.mark.unit
  def test_unknown_form_is_not_filtered(self):
    assert default_period_type("cash_flow_statement", None) is None


class TestCapPeriods:
  @pytest.mark.unit
  def test_keeps_the_newest_end_dates(self):
    rows = [
      _row("us-gaap:Revenues", "2024-12-31"),
      _row("us-gaap:Revenues", "2023-12-31"),
      _row("us-gaap:Revenues", "2022-12-31"),
      _row("us-gaap:Revenues", "2021-12-31"),
      _row("us-gaap:Revenues", "2020-12-31"),
    ]
    kept, keys, omitted = cap_periods(rows, 3)
    assert [r["end_date"] for r in kept] == ["2024-12-31", "2023-12-31", "2022-12-31"]
    assert [k["end_date"] for k in keys] == ["2024-12-31", "2023-12-31", "2022-12-31"]
    assert omitted == 2

  @pytest.mark.unit
  def test_keys_sharing_an_end_date_count_once(self):
    """A 10-Q's quarter and year-to-date columns end on the same date: a cap
    of two end dates keeps all four columns."""
    rows = [
      _row(
        "us-gaap:Revenues",
        "2025-09-30",
        start_date="2025-07-01",
        duration_type="quarterly",
      ),
      _row(
        "us-gaap:Revenues", "2025-09-30", start_date="2025-01-01", duration_type=None
      ),
      _row(
        "us-gaap:Revenues",
        "2024-09-30",
        start_date="2024-07-01",
        duration_type="quarterly",
      ),
      _row(
        "us-gaap:Revenues", "2024-09-30", start_date="2024-01-01", duration_type=None
      ),
    ]
    kept, keys, omitted = cap_periods(rows, 2)
    assert len(kept) == 4
    assert len(keys) == 4
    assert omitted == 0
    assert keys[0] == {
      "start_date": "2025-07-01",
      "end_date": "2025-09-30",
      "period_type": "duration",
      "duration_type": "quarterly",
    }

  @pytest.mark.unit
  def test_empty_rows(self):
    assert cap_periods([], 3) == ([], [], 0)


class TestCompactFact:
  @pytest.mark.unit
  def test_keeps_a_label_name_on_a_tenant_row(self):
    """rs-gaap elements carry a readable label in ``name``; it says more
    than the qname and stays."""
    fact = compact_fact(
      _row(
        "rs-gaap:NonoperatingIncomeExpense",
        "2026-12-31",
        name="Nonoperating Income (Expense)",
      )
    )
    assert fact["name"] == "Nonoperating Income (Expense)"

  @pytest.mark.unit
  def test_drops_name_and_nulls(self):
    fact = compact_fact(
      _row(
        "us-gaap:Assets",
        "2024-12-31",
        start_date=None,
        period_type="instant",
        duration_type=None,
      )
    )
    assert fact == {
      "qname": "us-gaap:Assets",
      "value": 1.0,
      "end_date": "2024-12-31",
      "period_type": "instant",
    }

  @pytest.mark.unit
  def test_keeps_the_canonical_concept_when_mapped(self):
    fact = compact_fact(
      _row("us-gaap:Revenues", "2024-12-31", canonical_concept="revenue")
    )
    assert fact["canonical_concept"] == "revenue"
    assert "name" not in fact


class TestFinancialStatementAnalysisToolPeriods:
  @pytest.mark.unit
  def test_definition_exposes_periods(self):
    props = FinancialStatementAnalysisTool(_make_client("sec")).get_tool_definition()[
      "inputSchema"
    ]["properties"]
    assert "periods" in props

  @pytest.mark.unit
  async def test_annual_form_defaults_the_period_filter(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    with _shared_repo_patches(_RESOLVED_10K, []) as mock_query:
      await tool.execute({"statement_type": "income_statement", "ticker": "MMM"})
    assert mock_query.call_args.kwargs["period_type"] == "annual"

  @pytest.mark.unit
  async def test_explicit_period_type_wins(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    with _shared_repo_patches(_RESOLVED_10K, []) as mock_query:
      await tool.execute(
        {
          "statement_type": "income_statement",
          "ticker": "MMM",
          "period_type": "quarterly",
        }
      )
    assert mock_query.call_args.kwargs["period_type"] == "quarterly"

  @pytest.mark.unit
  async def test_balance_sheet_is_not_forced_annual(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    with _shared_repo_patches(_RESOLVED_10K, []) as mock_query:
      await tool.execute({"statement_type": "balance_sheet", "ticker": "MMM"})
    assert mock_query.call_args.kwargs["period_type"] is None

  @pytest.mark.unit
  async def test_flow_statement_keeps_three_periods_and_says_what_it_cut(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    rows = [_row("us-gaap:Revenues", f"{y}-12-31") for y in range(2024, 2018, -1)]
    with _shared_repo_patches(_RESOLVED_10K, rows):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "MMM"}
      )
    assert result["fact_count"] == PERIODS_DEFAULT_DURATION
    assert [k["end_date"] for k in result["periods"]] == [
      "2024-12-31",
      "2023-12-31",
      "2022-12-31",
    ]
    assert result["periods_omitted"] == 3
    assert "raise `periods`" in result["periods_tip"]
    assert "name" not in result["facts"][0]

  @pytest.mark.unit
  async def test_balance_sheet_keeps_two_periods(self):
    """The FY2024 3M balance sheet carried 2021 and 2022 instants from the
    equity roll-forward; a balance sheet presents two columns."""
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    rows = [
      _row(
        "us-gaap:Assets",
        f"{y}-12-31",
        start_date=None,
        period_type="instant",
        duration_type=None,
      )
      for y in (2024, 2023, 2022, 2021)
    ]
    with _shared_repo_patches(_RESOLVED_10K, rows):
      result = await tool.execute({"statement_type": "balance_sheet", "ticker": "MMM"})
    assert result["fact_count"] == PERIODS_DEFAULT_INSTANT
    assert result["periods_omitted"] == 2

  @pytest.mark.unit
  async def test_periods_argument_raises_the_cap_and_is_bounded(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    rows = [_row("us-gaap:Revenues", f"{y}-12-31") for y in range(2024, 2014, -1)]
    with _shared_repo_patches(_RESOLVED_10K, rows):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "MMM", "periods": 5}
      )
      assert result["fact_count"] == 5
      assert result["periods_omitted"] == 5
      result = await tool.execute(
        {
          "statement_type": "income_statement",
          "ticker": "MMM",
          "periods": PERIODS_MAX * 10,
        }
      )
      assert result["fact_count"] == 10
      assert "periods_omitted" not in result

  @pytest.mark.unit
  async def test_limit_applies_within_the_kept_periods(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    rows = [
      _row(f"us-gaap:Concept{i}", end)
      for end in ("2024-12-31", "2023-12-31", "2022-12-31", "2021-12-31")
      for i in range(4)
    ]
    with _shared_repo_patches(_RESOLVED_10K, rows):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "MMM", "limit": 5}
      )
    assert result["fact_count"] == 5
    assert all(f["end_date"] != "2021-12-31" for f in result["facts"])
    assert result["periods_omitted"] == 1

  @pytest.mark.unit
  async def test_periods_zero_is_floored_not_defaulted(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    rows = [_row("us-gaap:Revenues", f"{y}-12-31") for y in (2024, 2023, 2022)]
    with _shared_repo_patches(_RESOLVED_10K, rows):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "MMM", "periods": 0}
      )
    assert result["fact_count"] == 1
    assert result["periods_omitted"] == 2

  @pytest.mark.unit
  async def test_query_gets_the_full_row_budget_whatever_limit_is(self):
    """The fetch is newest-first and the cap runs after it, so a caller's
    small ``limit`` must not shrink what the cap can see."""
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    with _shared_repo_patches(_RESOLVED_10K, []) as mock_query:
      await tool.execute(
        {"statement_type": "income_statement", "ticker": "MMM", "limit": 5}
      )
    assert mock_query.call_args.kwargs["limit"] == QUERY_ROW_CEILING

  @pytest.mark.unit
  async def test_a_fetch_at_the_ceiling_is_flagged(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    rows = [_row(f"us-gaap:Concept{i}", "2024-12-31") for i in range(QUERY_ROW_CEILING)]
    with _shared_repo_patches(_RESOLVED_10K, rows):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "MMM"}
      )
    assert result["rows_truncated"] is True
    assert "ceiling" in result["rows_tip"]

  @pytest.mark.unit
  async def test_a_fetch_under_the_ceiling_is_not_flagged(self):
    tool = FinancialStatementAnalysisTool(_make_client("sec"))
    with _shared_repo_patches(_RESOLVED_10K, [_row("us-gaap:Revenues", "2024-12-31")]):
      result = await tool.execute(
        {"statement_type": "income_statement", "ticker": "MMM"}
      )
    assert "rows_truncated" not in result
