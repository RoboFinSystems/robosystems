"""Tests for the two financial-statement MCP tools.

Replaces the old `test_financial_statement.py` which covered the
pre-split `GetFinancialStatementTool`. Coverage here:
- tool definitions expose expected names and inputs
- LiveFinancialStatementTool delegates to ops-layer helpers
- FinancialStatementAnalysisTool dispatches shared-repo vs tenant correctly
- error paths: missing inputs, invalid types, CoaMappingNotFoundError
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.financial_statement_tools import (
  FinancialStatementAnalysisTool,
  LiveFinancialStatementTool,
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
