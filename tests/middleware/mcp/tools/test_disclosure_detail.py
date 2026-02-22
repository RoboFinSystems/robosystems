"""Tests for get-disclosure-detail MCP tool."""

from unittest.mock import AsyncMock

import pytest

from robosystems.middleware.mcp.tools.disclosure_detail_tool import (
  GetDisclosureDetailTool,
)


@pytest.fixture
def mock_client():
  client = AsyncMock()
  client.graph_id = "sec"
  return client


@pytest.fixture
def tool(mock_client):
  return GetDisclosureDetailTool(mock_client)


class TestGetDisclosureDetailDefinition:
  def test_tool_definition_structure(self, tool):
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-disclosure-detail"
    assert "disclosure_type" in defn["inputSchema"]["properties"]
    assert "ticker" in defn["inputSchema"]["properties"]
    assert "accession_number" in defn["inputSchema"]["properties"]
    assert "include_dimensions" in defn["inputSchema"]["properties"]
    assert "limit" in defn["inputSchema"]["properties"]
    assert defn["inputSchema"]["required"] == ["disclosure_type"]


class TestGetDisclosureDetailExecution:
  @pytest.mark.asyncio
  async def test_missing_disclosure_type(self, tool):
    result = await tool.execute({"disclosure_type": ""})
    assert "error" in result
    assert "disclosure_type" in result["error"]

  @pytest.mark.asyncio
  async def test_basic_query(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "canonical_concept": "Assets",
          "qname": "us-gaap:Assets",
          "name": "Assets",
          "value": 96013000000,
          "has_dimensions": False,
          "end_date": "2025-01-26",
          "period_type": "instant",
          "duration_type": None,
        },
        {
          "canonical_concept": "AssetsCurrent",
          "qname": "us-gaap:AssetsCurrent",
          "name": "Current Assets",
          "value": 55102000000,
          "has_dimensions": False,
          "end_date": "2025-01-26",
          "period_type": "instant",
          "duration_type": None,
        },
      ]
    )

    result = await tool.execute({"disclosure_type": "AssetsRollUp"})
    assert result["disclosure_type"] == "AssetsRollUp"
    assert result["ticker"] is None
    assert result["fact_count"] == 2
    assert result["facts"][0]["qname"] == "us-gaap:Assets"
    assert result["facts"][0]["value"] == 96013000000

  @pytest.mark.asyncio
  async def test_with_ticker_filter(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute({"disclosure_type": "AssetsRollUp", "ticker": "NVDA"})

    call_args = mock_client.execute_query.call_args[0][0]
    assert "FACT_HAS_ENTITY" in call_args
    assert "ent.ticker" in call_args

    params = mock_client.execute_query.call_args[1].get("parameters", {})
    assert params["ticker"] == "NVDA"

  @pytest.mark.asyncio
  async def test_dimensions_excluded_by_default(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute({"disclosure_type": "AssetsRollUp"})

    call_args = mock_client.execute_query.call_args[0][0]
    assert "f.has_dimensions = false" in call_args

  @pytest.mark.asyncio
  async def test_include_dimensions(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "canonical_concept": "Assets",
          "qname": "us-gaap:Assets",
          "name": "Assets",
          "value": 96013000000,
          "has_dimensions": True,
          "end_date": "2025-01-26",
          "period_type": "instant",
          "duration_type": None,
        },
      ]
    )

    result = await tool.execute(
      {"disclosure_type": "AssetsRollUp", "include_dimensions": True}
    )

    # When include_dimensions=True, has_dimensions should be in the output
    assert "has_dimensions" in result["facts"][0]

    # And the filter should NOT be in the query
    call_args = mock_client.execute_query.call_args[0][0]
    assert "f.has_dimensions = false" not in call_args

  @pytest.mark.asyncio
  async def test_empty_result(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute({"disclosure_type": "NonexistentType"})
    assert result["fact_count"] == 0
    assert result["facts"] == []

  @pytest.mark.asyncio
  async def test_query_error_handling(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(side_effect=Exception("db error"))

    result = await tool.execute({"disclosure_type": "AssetsRollUp"})
    assert "error" in result
    assert "db error" in result["error"]

  @pytest.mark.asyncio
  async def test_accession_number_filter(self, tool, mock_client):
    """Accession number should add Report join."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {
        "disclosure_type": "AssetsRollUp",
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
        "disclosure_type": "AssetsRollUp",
        "accession_number": "0001045810-25-000023",
      }
    )
    assert result["accession_number"] == "0001045810-25-000023"

  @pytest.mark.asyncio
  async def test_no_accession_number_no_report_join(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute({"disclosure_type": "AssetsRollUp"})

    call_args = mock_client.execute_query.call_args[0][0]
    assert "REPORT_HAS_FACT" not in call_args

  @pytest.mark.asyncio
  async def test_ticker_uppercased(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute({"disclosure_type": "AssetsRollUp", "ticker": "nvda"})
    assert result["ticker"] == "NVDA"
