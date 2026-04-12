"""Tests for get-financial-statement MCP tool."""

from unittest.mock import AsyncMock

import pytest

from robosystems.middleware.mcp.tools.financial_statement_tool import (
  GetFinancialStatementTool,
)


@pytest.fixture
def mock_client():
  client = AsyncMock()
  client.graph_id = "sec"
  return client


@pytest.fixture
def tool(mock_client):
  return GetFinancialStatementTool(mock_client)


class TestGetFinancialStatementDefinition:
  def test_tool_definition_structure(self, tool):
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-financial-statement"
    assert "ticker" in defn["inputSchema"]["properties"]
    assert "statement_type" in defn["inputSchema"]["properties"]
    assert "report_id" in defn["inputSchema"]["properties"]
    assert "fiscal_year" in defn["inputSchema"]["properties"]
    assert "period_type" in defn["inputSchema"]["properties"]
    assert "limit" in defn["inputSchema"]["properties"]
    # Only statement_type is structurally required. Ticker is conditionally
    # required based on graph type (required in SEC shared repo, optional in
    # user graphs), but that's enforced at execute time, not in the schema.
    assert set(defn["inputSchema"]["required"]) == {"statement_type"}

  def test_statement_type_enum(self, tool):
    defn = tool.get_tool_definition()
    enum_values = defn["inputSchema"]["properties"]["statement_type"]["enum"]
    assert "income_statement" in enum_values
    assert "balance_sheet" in enum_values
    assert "cash_flow_statement" in enum_values
    assert "equity_statement" in enum_values


class TestGetFinancialStatementExecution:
  @pytest.mark.asyncio
  async def test_sec_repo_requires_ticker(self, tool):
    """In the SEC shared repository, ticker is required (no local OLTP)."""
    # Fixture sets graph_id='sec' which is the shared SEC repository.
    result = await tool.execute({"ticker": "", "statement_type": "income_statement"})
    assert "error" in result
    assert "ticker is required" in result["error"].lower()

  @pytest.mark.asyncio
  async def test_missing_statement_type(self, tool):
    result = await tool.execute({"ticker": "NVDA", "statement_type": ""})
    assert "error" in result
    assert "statement_type" in result["error"]

  @pytest.mark.asyncio
  async def test_unknown_statement_type(self, tool):
    result = await tool.execute({"ticker": "NVDA", "statement_type": "profit_and_loss"})
    assert "error" in result
    assert "Unknown statement_type" in result["error"]

  @pytest.mark.asyncio
  async def test_income_statement_with_report_id(self, tool, mock_client):
    """With explicit report_id, skips auto-resolve and queries directly."""
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "canonical_concept": "Revenues",
          "qname": "us-gaap:Revenues",
          "name": "Revenues",
          "value": 35082000000,
          "end_date": "2025-01-26",
          "period_type": "duration",
          "duration_type": "annual",
        },
        {
          "canonical_concept": "NetIncomeLoss",
          "qname": "us-gaap:NetIncomeLoss",
          "name": "Net Income",
          "value": 29760000000,
          "end_date": "2025-01-26",
          "period_type": "duration",
          "duration_type": "annual",
        },
      ]
    )

    result = await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "report_id": "0001045810-25-000023",
      }
    )
    assert result["ticker"] == "NVDA"
    assert result["statement_type"] == "income_statement"
    assert result["fact_count"] == 2
    assert result["facts"][0]["qname"] == "us-gaap:Revenues"
    assert result["facts"][1]["value"] == 29760000000
    # Only one query call (no auto-resolve)
    assert mock_client.execute_query.call_count == 1

  @pytest.mark.asyncio
  async def test_balance_sheet_instant_filter(self, tool, mock_client):
    """Balance sheet should default to instant period filter."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "balance_sheet",
        "report_id": "some-id",
      }
    )

    call_args = mock_client.execute_query.call_args[0][0]
    assert "period_type: 'instant'" in call_args

  @pytest.mark.asyncio
  async def test_period_type_annual_filter(self, tool, mock_client):
    # Auto-resolve returns a report, then main query uses annual filter
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [
          {
            "accession_number": "acc-1",
            "identifier": "id-1",
            "form": "10-K",
            "filing_date": "2025-10-31",
            "fiscal_year": 2025,
            "fiscal_period": "FY",
          }
        ],
        [],
      ]
    )

    await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "period_type": "annual",
      }
    )

    # Second call is the main query
    call_args = mock_client.execute_query.call_args_list[1][0][0]
    assert "duration_type: 'annual'" in call_args

  @pytest.mark.asyncio
  async def test_period_type_quarterly_filter(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [
          {
            "accession_number": "acc-1",
            "identifier": "id-1",
            "form": "10-Q",
            "filing_date": "2025-08-01",
            "fiscal_year": 2025,
            "fiscal_period": "Q3",
          }
        ],
        [],
      ]
    )

    await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "period_type": "quarterly",
      }
    )

    call_args = mock_client.execute_query.call_args_list[1][0][0]
    assert "duration_type: 'quarterly'" in call_args

  @pytest.mark.asyncio
  async def test_no_results_shows_tip(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute(
      {
        "ticker": "ZZZZ",
        "statement_type": "income_statement",
        "report_id": "some-id",
      }
    )
    assert result["fact_count"] == 0
    assert "tip" in result

  @pytest.mark.asyncio
  async def test_query_uses_parameters(self, tool, mock_client):
    """Verify query passes ticker and statement_type as parameters."""
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [
          {
            "accession_number": "acc-1",
            "identifier": "id-1",
            "form": "10-K",
            "filing_date": "2025-10-31",
            "fiscal_year": 2025,
            "fiscal_period": "FY",
          }
        ],
        [],
      ]
    )

    await tool.execute({"ticker": "aapl", "statement_type": "income_statement"})

    # Resolve query (first call) uses ticker
    resolve_params = mock_client.execute_query.call_args_list[0][1]["parameters"]
    assert resolve_params["ticker"] == "AAPL"

    # Main query (second call) uses statement_type and report_id
    main_params = mock_client.execute_query.call_args_list[1][1]["parameters"]
    assert main_params["statement_type"] == "income_statement"
    assert main_params["report_id"] == "id-1"

  @pytest.mark.asyncio
  async def test_report_id_filter(self, tool, mock_client):
    """report_id should add Report join matching by identifier."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "report_id": "0001045810-25-000023",
      }
    )

    call_args = mock_client.execute_query.call_args[0][0]
    assert "REPORT_HAS_FACT" in call_args
    assert "identifier: $report_id" in call_args
    assert "FACT_SET_CONTAINS_FACT" in call_args

    params = mock_client.execute_query.call_args[1].get("parameters", {})
    assert params["report_id"] == "0001045810-25-000023"

  @pytest.mark.asyncio
  async def test_query_error_handling(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(side_effect=Exception("connection refused"))

    result = await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "report_id": "some-id",
      }
    )
    assert "error" in result
    assert "connection refused" in result["error"]


class TestAutoResolve:
  @pytest.mark.asyncio
  async def test_annual_resolves_10k_forms(self, tool, mock_client):
    """Annual period_type should look for 10-K, 20-F, 40-F."""
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [
          {
            "accession_number": "acc-1",
            "identifier": "id-1",
            "form": "10-K",
            "filing_date": "2025-10-31",
            "fiscal_year": 2025,
            "fiscal_period": "FY",
          }
        ],
        [],
      ]
    )

    result = await tool.execute(
      {"ticker": "AAPL", "statement_type": "income_statement", "period_type": "annual"}
    )

    # Check resolve query used annual forms
    resolve_query = mock_client.execute_query.call_args_list[0][0][0]
    resolve_params = mock_client.execute_query.call_args_list[0][1]["parameters"]
    assert "r.form IN $forms" in resolve_query
    assert resolve_params["forms"] == ["10-K", "20-F", "40-F"]

    # Check resolved report is in result
    assert "resolved_report" in result
    assert result["resolved_report"]["form"] == "10-K"

  @pytest.mark.asyncio
  async def test_quarterly_resolves_all_forms(self, tool, mock_client):
    """Quarterly period_type should include 10-Q plus annual forms."""
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [
          {
            "accession_number": "acc-q1",
            "identifier": "id-q1",
            "form": "10-Q",
            "filing_date": "2025-08-01",
            "fiscal_year": 2025,
            "fiscal_period": "Q3",
          }
        ],
        [],
      ]
    )

    await tool.execute(
      {
        "ticker": "AAPL",
        "statement_type": "income_statement",
        "period_type": "quarterly",
      }
    )

    resolve_params = mock_client.execute_query.call_args_list[0][1]["parameters"]
    assert "10-Q" in resolve_params["forms"]
    assert "10-K" in resolve_params["forms"]
    assert "20-F" in resolve_params["forms"]

  @pytest.mark.asyncio
  async def test_fiscal_year_filter(self, tool, mock_client):
    """fiscal_year should be passed to the resolve query."""
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [
          {
            "accession_number": "acc-1",
            "identifier": "id-1",
            "form": "10-K",
            "filing_date": "2024-11-01",
            "fiscal_year": 2024,
            "fiscal_period": "FY",
          }
        ],
        [],
      ]
    )

    await tool.execute(
      {
        "ticker": "AAPL",
        "statement_type": "income_statement",
        "period_type": "annual",
        "fiscal_year": 2024,
      }
    )

    resolve_query = mock_client.execute_query.call_args_list[0][0][0]
    resolve_params = mock_client.execute_query.call_args_list[0][1]["parameters"]
    assert "r.fiscal_year_focus = $fiscal_year" in resolve_query
    assert resolve_params["fiscal_year"] == 2024

  @pytest.mark.asyncio
  async def test_no_report_found_still_queries(self, tool, mock_client):
    """When auto-resolve finds no report, query proceeds without report filter."""
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [],  # No report found
        [
          {
            "canonical_concept": "revenue",
            "qname": "us-gaap:Revenues",
            "name": "Revenues",
            "value": 1000,
            "end_date": "2025-01-01",
            "period_type": "duration",
            "duration_type": "annual",
          }
        ],
      ]
    )

    result = await tool.execute(
      {"ticker": "ZZZZ", "statement_type": "income_statement"}
    )

    # Main query should not have REPORT_HAS_FACT
    main_query = mock_client.execute_query.call_args_list[1][0][0]
    assert "REPORT_HAS_FACT" not in main_query
    assert "resolved_report" not in result

  @pytest.mark.asyncio
  async def test_default_period_type_resolves_annual(self, tool, mock_client):
    """No period_type defaults to annual forms for resolve."""
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [
          {
            "accession_number": "acc-1",
            "identifier": "id-1",
            "form": "10-K",
            "filing_date": "2025-10-31",
            "fiscal_year": 2025,
            "fiscal_period": "FY",
          }
        ],
        [],
      ]
    )

    await tool.execute({"ticker": "AAPL", "statement_type": "income_statement"})

    resolve_params = mock_client.execute_query.call_args_list[0][1]["parameters"]
    assert resolve_params["forms"] == ["10-K", "20-F", "40-F"]

  @pytest.mark.asyncio
  async def test_resolve_error_falls_through(self, tool, mock_client):
    """If resolve query fails, main query still runs without report filter."""
    mock_client.execute_query = AsyncMock(
      side_effect=[
        Exception("resolve timeout"),
        [],  # Main query succeeds
      ]
    )

    result = await tool.execute(
      {"ticker": "AAPL", "statement_type": "income_statement"}
    )

    assert "resolved_report" not in result
    assert result["fact_count"] == 0


class TestDeduplication:
  def test_dedup_by_qname_and_end_date(self, tool):
    rows = [
      {"qname": "us-gaap:Revenues", "end_date": "2025-09-27", "value": 416161000000},
      {"qname": "us-gaap:Revenues", "end_date": "2025-09-27", "value": 416161000000},
      {"qname": "us-gaap:Revenues", "end_date": "2024-09-28", "value": 391035000000},
      {
        "qname": "us-gaap:NetIncomeLoss",
        "end_date": "2025-09-27",
        "value": 112010000000,
      },
    ]
    deduped = tool._deduplicate_facts(rows)
    assert len(deduped) == 3
    assert deduped[0]["end_date"] == "2025-09-27"
    assert deduped[0]["qname"] == "us-gaap:Revenues"
    assert deduped[1]["end_date"] == "2024-09-28"
    assert deduped[2]["qname"] == "us-gaap:NetIncomeLoss"

  def test_dedup_keeps_first_occurrence(self, tool):
    """First occurrence wins — since results are ordered by end_date DESC,
    this keeps the most recent filing's value."""
    rows = [
      {"qname": "us-gaap:Revenues", "end_date": "2024-09-28", "value": 391035000000},
      {"qname": "us-gaap:Revenues", "end_date": "2024-09-28", "value": 999999999999},
    ]
    deduped = tool._deduplicate_facts(rows)
    assert len(deduped) == 1
    assert deduped[0]["value"] == 391035000000

  def test_dedup_empty_list(self, tool):
    assert tool._deduplicate_facts([]) == []

  def test_dedup_no_duplicates(self, tool):
    rows = [
      {"qname": "us-gaap:Revenues", "end_date": "2025-09-27", "value": 416161000000},
      {
        "qname": "us-gaap:NetIncomeLoss",
        "end_date": "2025-09-27",
        "value": 112010000000,
      },
    ]
    deduped = tool._deduplicate_facts(rows)
    assert len(deduped) == 2
