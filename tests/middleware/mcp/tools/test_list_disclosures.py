"""Tests for list-disclosures MCP tool."""

from unittest.mock import AsyncMock

import pytest

from robosystems.middleware.mcp.tools.list_disclosures_tool import ListDisclosuresTool


@pytest.fixture
def mock_client():
  client = AsyncMock()
  client.graph_id = "sec"
  return client


@pytest.fixture
def tool(mock_client):
  return ListDisclosuresTool(mock_client)


class TestListDisclosuresDefinition:
  def test_tool_definition_structure(self, tool):
    defn = tool.get_tool_definition()
    assert defn["name"] == "list-disclosures"
    assert "ticker" in defn["inputSchema"]["properties"]
    assert defn["inputSchema"]["required"] == []


class TestListDisclosuresExecution:
  @pytest.mark.asyncio
  async def test_without_ticker(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {"disclosure_type": "AssetsRollUp", "association_count": 42},
        {"disclosure_type": "RevenueBreakdown", "association_count": 18},
      ]
    )

    result = await tool.execute({})
    assert result["ticker"] is None
    assert result["total_disclosures"] == 2
    assert result["disclosures"][0]["type"] == "AssetsRollUp"
    assert result["disclosures"][0]["count"] == 42

    # Verify the query uses association count, not fact count
    call_args = mock_client.execute_query.call_args[0][0]
    assert "count(DISTINCT a)" in call_args

  @pytest.mark.asyncio
  async def test_with_ticker(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {"disclosure_type": "AssetsRollUp", "fact_count": 150},
        {"disclosure_type": "RevenueBreakdown", "fact_count": 30},
      ]
    )

    result = await tool.execute({"ticker": "NVDA"})
    assert result["ticker"] == "NVDA"
    assert result["total_disclosures"] == 2
    assert result["disclosures"][0]["count"] == 150

    # Verify the query joins through entity
    call_args = mock_client.execute_query.call_args[0][0]
    assert "ent.ticker" in call_args
    assert "count(DISTINCT f)" in call_args

  @pytest.mark.asyncio
  async def test_empty_result(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute({})
    assert result["total_disclosures"] == 0
    assert result["disclosures"] == []

  @pytest.mark.asyncio
  async def test_ticker_uppercased(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute({"ticker": "nvda"})
    assert result["ticker"] == "NVDA"

  @pytest.mark.asyncio
  async def test_query_error_handling(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(side_effect=Exception("timeout"))

    result = await tool.execute({})
    assert "error" in result
    assert "timeout" in result["error"]
