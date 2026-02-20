"""Tests for resolve-structure MCP tool."""

from unittest.mock import AsyncMock

import pytest

from robosystems.middleware.mcp.tools.resolve_structure_tool import ResolveStructureTool


@pytest.fixture
def mock_client():
  client = AsyncMock()
  client.graph_id = "sec"
  return client


@pytest.fixture
def tool(mock_client):
  return ResolveStructureTool(mock_client)


class TestResolveStructureToolDefinition:
  def test_tool_definition_structure(self, tool):
    defn = tool.get_tool_definition()
    assert defn["name"] == "resolve-structure"
    assert "statement_type" in defn["inputSchema"]["properties"]
    assert "ticker" in defn["inputSchema"]["properties"]
    assert "accession_number" in defn["inputSchema"]["properties"]
    assert "include_parenthetical" in defn["inputSchema"]["properties"]
    assert defn["inputSchema"]["required"] == ["statement_type"]

  def test_statement_type_enum(self, tool):
    defn = tool.get_tool_definition()
    enum_values = defn["inputSchema"]["properties"]["statement_type"]["enum"]
    assert "income_statement" in enum_values
    assert "balance_sheet" in enum_values
    assert "cash_flow_statement" in enum_values


class TestResolveStructureExecution:
  @pytest.mark.asyncio
  async def test_empty_statement_type(self, tool):
    result = await tool.execute({"statement_type": ""})
    assert "error" in result

  @pytest.mark.asyncio
  async def test_balance_sheet_query(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "id": "struct-1",
          "name": "CONSOLIDATED BALANCE SHEETS",
          "type": "Statement",
          "definition": "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
          "canonical_type": "balance_sheet",
          "canonical_confidence": 0.85,
        }
      ]
    )

    result = await tool.execute({"statement_type": "balance_sheet"})
    assert result["statement_type"] == "balance_sheet"
    assert len(result["structures"]) == 1
    assert result["structures"][0]["canonical_type"] == "balance_sheet"

  @pytest.mark.asyncio
  async def test_with_ticker_filter(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "id": "struct-1",
          "name": "CONSOLIDATED BALANCE SHEETS",
          "type": "Statement",
          "definition": "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
          "canonical_type": "balance_sheet",
          "canonical_confidence": 0.85,
          "form": "10-K",
          "filing_date": "2024-02-15",
        }
      ]
    )

    result = await tool.execute({"statement_type": "balance_sheet", "ticker": "NVDA"})
    assert result["ticker"] == "NVDA"
    assert result["structures"][0]["form"] == "10-K"
    assert result["structures"][0]["filing_date"] == "2024-02-15"

  @pytest.mark.asyncio
  async def test_parenthetical_excluded_by_default(self, tool, mock_client):
    """Parenthetical filter should be applied by default."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute({"statement_type": "income_statement"})

    # Verify the query was called and includes the parenthetical filter
    call_args = mock_client.execute_query.call_args[0][0]
    assert "Parenthetical" in call_args

  @pytest.mark.asyncio
  async def test_include_parenthetical(self, tool, mock_client):
    """When include_parenthetical=True, filter should not be applied."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {"statement_type": "income_statement", "include_parenthetical": True}
    )

    call_args = mock_client.execute_query.call_args[0][0]
    assert "[Parenthetical]" not in call_args

  @pytest.mark.asyncio
  async def test_with_accession_number_filter(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "id": "struct-1",
          "name": "CONSOLIDATED BALANCE SHEETS",
          "type": "Statement",
          "definition": "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
          "canonical_type": "balance_sheet",
          "canonical_confidence": 0.85,
          "accession_number": "0001045810-25-000023",
          "form": "10-K",
          "filing_date": "2025-02-26",
        }
      ]
    )

    result = await tool.execute(
      {"statement_type": "balance_sheet", "accession_number": "0001045810-25-000023"}
    )
    assert result["accession_number"] == "0001045810-25-000023"
    assert len(result["structures"]) == 1
    assert result["structures"][0]["accession_number"] == "0001045810-25-000023"
    assert result["structures"][0]["form"] == "10-K"

    # Verify query includes accession_number filter
    call_args = mock_client.execute_query.call_args[0][0]
    assert "0001045810-25-000023" in call_args
    assert "REPORT_USES_TAXONOMY" in call_args

  @pytest.mark.asyncio
  async def test_with_ticker_and_accession_number(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "id": "struct-1",
          "name": "CONSOLIDATED BALANCE SHEETS",
          "type": "Statement",
          "definition": "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
          "canonical_type": "balance_sheet",
          "canonical_confidence": 0.85,
          "accession_number": "0001045810-25-000023",
          "form": "10-K",
          "filing_date": "2025-02-26",
        }
      ]
    )

    result = await tool.execute(
      {
        "statement_type": "balance_sheet",
        "ticker": "NVDA",
        "accession_number": "0001045810-25-000023",
      }
    )
    assert result["ticker"] == "NVDA"
    assert result["accession_number"] == "0001045810-25-000023"

    # Verify query includes both filters
    call_args = mock_client.execute_query.call_args[0][0]
    assert "NVDA" in call_args
    assert "0001045810-25-000023" in call_args

  @pytest.mark.asyncio
  async def test_no_results(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute({"statement_type": "equity_statement"})
    assert len(result["structures"]) == 0
