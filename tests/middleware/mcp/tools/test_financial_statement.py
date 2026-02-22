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
    assert "accession_number" in defn["inputSchema"]["properties"]
    assert "period_type" in defn["inputSchema"]["properties"]
    assert "limit" in defn["inputSchema"]["properties"]
    assert set(defn["inputSchema"]["required"]) == {"ticker", "statement_type"}

  def test_statement_type_enum(self, tool):
    defn = tool.get_tool_definition()
    enum_values = defn["inputSchema"]["properties"]["statement_type"]["enum"]
    assert "income_statement" in enum_values
    assert "balance_sheet" in enum_values
    assert "cash_flow_statement" in enum_values
    assert "equity_statement" in enum_values


class TestGetFinancialStatementExecution:
  @pytest.mark.asyncio
  async def test_missing_ticker(self, tool):
    result = await tool.execute({"ticker": "", "statement_type": "income_statement"})
    assert "error" in result
    assert "ticker" in result["error"]

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
  async def test_income_statement(self, tool, mock_client):
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
      {"ticker": "NVDA", "statement_type": "income_statement"}
    )
    assert result["ticker"] == "NVDA"
    assert result["statement_type"] == "income_statement"
    assert result["fact_count"] == 2
    assert result["facts"][0]["qname"] == "us-gaap:Revenues"
    assert result["facts"][1]["value"] == 29760000000

  @pytest.mark.asyncio
  async def test_balance_sheet_instant_filter(self, tool, mock_client):
    """Balance sheet should default to instant period filter."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute({"ticker": "NVDA", "statement_type": "balance_sheet"})

    call_args = mock_client.execute_query.call_args[0][0]
    assert "p.period_type = 'instant'" in call_args

  @pytest.mark.asyncio
  async def test_period_type_annual_filter(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "period_type": "annual",
      }
    )

    call_args = mock_client.execute_query.call_args[0][0]
    assert "p.duration_type = 'annual'" in call_args

  @pytest.mark.asyncio
  async def test_period_type_quarterly_filter(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "period_type": "quarterly",
      }
    )

    call_args = mock_client.execute_query.call_args[0][0]
    assert "p.duration_type = 'quarterly'" in call_args

  @pytest.mark.asyncio
  async def test_no_results_shows_tip(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute(
      {"ticker": "ZZZZ", "statement_type": "income_statement"}
    )
    assert result["fact_count"] == 0
    assert "tip" in result

  @pytest.mark.asyncio
  async def test_query_uses_parameters(self, tool, mock_client):
    """Verify query passes ticker and statement_type as parameters."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute({"ticker": "aapl", "statement_type": "income_statement"})

    call_kwargs = mock_client.execute_query.call_args
    params = call_kwargs[1].get("parameters", {})
    assert params["ticker"] == "AAPL"
    assert params["statement_type"] == "income_statement"

  @pytest.mark.asyncio
  async def test_accession_number_filter(self, tool, mock_client):
    """Accession number should add Report join."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "income_statement",
        "accession_number": "0001045810-25-000023",
      }
    )

    call_args = mock_client.execute_query.call_args[0][0]
    assert "REPORT_HAS_FACT" in call_args

    params = mock_client.execute_query.call_args[1].get("parameters", {})
    assert params["accession_number"] == "0001045810-25-000023"

  @pytest.mark.asyncio
  async def test_accession_number_in_result(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute(
      {
        "ticker": "NVDA",
        "statement_type": "balance_sheet",
        "accession_number": "0001045810-25-000023",
      }
    )
    assert result["accession_number"] == "0001045810-25-000023"

  @pytest.mark.asyncio
  async def test_no_accession_number_no_report_join(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute({"ticker": "NVDA", "statement_type": "income_statement"})

    call_args = mock_client.execute_query.call_args[0][0]
    assert "REPORT_HAS_FACT" not in call_args

  @pytest.mark.asyncio
  async def test_query_error_handling(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(side_effect=Exception("connection refused"))

    result = await tool.execute(
      {"ticker": "NVDA", "statement_type": "income_statement"}
    )
    assert "error" in result
    assert "connection refused" in result["error"]
