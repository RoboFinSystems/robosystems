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
    assert "query" in defn["inputSchema"]["properties"]
    assert "ticker" in defn["inputSchema"]["properties"]
    assert "report_id" in defn["inputSchema"]["properties"]
    assert "include_parenthetical" in defn["inputSchema"]["properties"]

  def test_no_required_fields(self, tool):
    """Neither statement_type nor query is required — but one must be provided."""
    defn = tool.get_tool_definition()
    assert "required" not in defn["inputSchema"]

  def test_statement_type_enum(self, tool):
    defn = tool.get_tool_definition()
    enum_values = defn["inputSchema"]["properties"]["statement_type"]["enum"]
    assert "income_statement" in enum_values
    assert "balance_sheet" in enum_values
    assert "cash_flow_statement" in enum_values


class TestResolveStructureCanonical:
  """Tests for the canonical (statement_type) lookup path."""

  @pytest.mark.asyncio
  async def test_empty_inputs(self, tool):
    result = await tool.execute({"statement_type": ""})
    assert "error" in result

  @pytest.mark.asyncio
  async def test_no_inputs(self, tool):
    result = await tool.execute({})
    assert "error" in result

  @pytest.mark.asyncio
  async def test_both_inputs_rejected(self, tool):
    result = await tool.execute(
      {"statement_type": "balance_sheet", "query": "cash flow"}
    )
    assert "error" in result
    assert "not both" in result["error"]

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
    call_args = mock_client.execute_query.call_args
    query = call_args[0][0]
    assert "Parenthetical" in query

  @pytest.mark.asyncio
  async def test_include_parenthetical(self, tool, mock_client):
    """When include_parenthetical=True, filter should not be applied."""
    mock_client.execute_query = AsyncMock(return_value=[])

    await tool.execute(
      {"statement_type": "income_statement", "include_parenthetical": True}
    )

    call_args = mock_client.execute_query.call_args
    query = call_args[0][0]
    assert "[Parenthetical]" not in query

  @pytest.mark.asyncio
  async def test_with_report_id_filter(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "id": "struct-1",
          "name": "CONSOLIDATED BALANCE SHEETS",
          "type": "Statement",
          "definition": "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
          "canonical_type": "balance_sheet",
          "canonical_confidence": 0.85,
          "report_id": "0001045810-25-000023",
          "form": "10-K",
          "filing_date": "2025-02-26",
        }
      ]
    )

    result = await tool.execute(
      {"statement_type": "balance_sheet", "report_id": "0001045810-25-000023"}
    )
    assert result["report_id"] == "0001045810-25-000023"
    assert len(result["structures"]) == 1
    assert result["structures"][0]["report_id"] == "0001045810-25-000023"
    assert result["structures"][0]["form"] == "10-K"

    # Verify query uses parameterized report_id filter
    call_args = mock_client.execute_query.call_args
    query = call_args[0][0]
    params = call_args[1]["parameters"]
    assert "$report_id" in query
    assert params["report_id"] == "0001045810-25-000023"
    assert "REPORT_USES_TAXONOMY" in query

  @pytest.mark.asyncio
  async def test_with_ticker_and_report_id(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(
      return_value=[
        {
          "id": "struct-1",
          "name": "CONSOLIDATED BALANCE SHEETS",
          "type": "Statement",
          "definition": "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
          "canonical_type": "balance_sheet",
          "canonical_confidence": 0.85,
          "report_id": "0001045810-25-000023",
          "form": "10-K",
          "filing_date": "2025-02-26",
        }
      ]
    )

    result = await tool.execute(
      {
        "statement_type": "balance_sheet",
        "ticker": "NVDA",
        "report_id": "0001045810-25-000023",
      }
    )
    assert result["ticker"] == "NVDA"
    assert result["report_id"] == "0001045810-25-000023"

    # Verify query uses parameterized filters
    call_args = mock_client.execute_query.call_args
    query = call_args[0][0]
    params = call_args[1]["parameters"]
    assert "$ticker" in query
    assert "$report_id" in query
    assert params["ticker"] == "NVDA"
    assert params["report_id"] == "0001045810-25-000023"

  @pytest.mark.asyncio
  async def test_no_results(self, tool, mock_client):
    mock_client.execute_query = AsyncMock(return_value=[])

    result = await tool.execute({"statement_type": "equity_statement"})
    assert len(result["structures"]) == 0


class TestResolveStructureVectorSearch:
  """Tests for the vector search (query) path."""

  @pytest.fixture
  def tool_with_enricher(self, mock_client):
    tool = ResolveStructureTool(mock_client)
    mock_enricher = AsyncMock()
    mock_enricher.embed_batch = lambda texts: [[0.1] * 384]
    tool._enricher = mock_enricher
    return tool

  @pytest.mark.asyncio
  async def test_vector_search_basic(self, tool_with_enricher, mock_client):
    """Vector search returns structures sorted by cosine similarity score."""
    mock_client.query_table = AsyncMock(
      return_value={
        "columns": [
          "identifier",
          "definition",
          "name",
          "type",
          "canonical_type",
          "canonical_confidence",
          "score",
        ],
        "rows": [
          [
            "struct-cf-1",
            "0001003 - Statement - CONSOLIDATED STATEMENTS OF CASH FLOWS",
            "CONSOLIDATED STATEMENTS OF CASH FLOWS",
            "Statement",
            "cash_flow_statement",
            0.72,
            0.91,
          ],
          [
            "struct-cf-2",
            "0001003 - Statement - CASH FLOWS FROM OPERATIONS",
            "CASH FLOWS FROM OPERATIONS",
            "Statement",
            None,
            None,
            0.85,
          ],
        ],
      }
    )

    result = await tool_with_enricher.execute({"query": "cash flow statement"})
    assert "query" in result
    assert result["query"] == "cash flow statement"
    assert len(result["structures"]) == 2
    assert result["structures"][0]["score"] == 0.91
    assert result["structures"][0]["identifier"] == "struct-cf-1"

  @pytest.mark.asyncio
  async def test_vector_search_with_ticker(self, tool_with_enricher, mock_client):
    """Vector search with ticker filters to structures from matching reports."""
    # DuckDB vector search returns candidates
    mock_client.query_table = AsyncMock(
      return_value={
        "columns": [
          "identifier",
          "definition",
          "name",
          "type",
          "canonical_type",
          "canonical_confidence",
          "score",
        ],
        "rows": [
          [
            "struct-1",
            "Statement - CASH FLOWS",
            "CASH FLOWS",
            "Statement",
            "cash_flow_statement",
            0.7,
            0.92,
          ],
          [
            "struct-2",
            "Statement - OTHER CASH FLOWS",
            "OTHER CASH FLOWS",
            "Statement",
            None,
            None,
            0.88,
          ],
        ],
      }
    )

    # Graph query to get valid structure IDs for ticker returns only struct-1
    # Then report metadata query returns filing info
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [{"id": "struct-1"}],  # _fetch_structure_ids_for_report
        [
          {
            "id": "struct-1",
            "report_id": "000-123",
            "form": "10-K",
            "filing_date": "2025-01-15",
          }
        ],  # _fetch_report_metadata
      ]
    )

    result = await tool_with_enricher.execute({"query": "cash flow", "ticker": "NVDA"})
    assert result["ticker"] == "NVDA"
    assert len(result["structures"]) == 1
    assert result["structures"][0]["identifier"] == "struct-1"
    assert result["structures"][0]["form"] == "10-K"
    assert result["structures"][0]["score"] == 0.92

  @pytest.mark.asyncio
  async def test_vector_search_with_report_id(self, tool_with_enricher, mock_client):
    """Vector search with report_id filters to matching filing."""
    mock_client.query_table = AsyncMock(
      return_value={
        "columns": [
          "identifier",
          "definition",
          "name",
          "type",
          "canonical_type",
          "canonical_confidence",
          "score",
        ],
        "rows": [
          [
            "struct-1",
            "Statement - BALANCE SHEETS",
            "BALANCE SHEETS",
            "Statement",
            "balance_sheet",
            0.8,
            0.95,
          ],
        ],
      }
    )

    mock_client.execute_query = AsyncMock(
      side_effect=[
        [{"id": "struct-1"}],  # _fetch_structure_ids_for_report
        [
          {
            "id": "struct-1",
            "report_id": "000-456",
            "form": "10-Q",
            "filing_date": "2025-03-01",
          }
        ],
      ]
    )

    result = await tool_with_enricher.execute(
      {"query": "balance sheet", "report_id": "000-456"}
    )
    assert result["report_id"] == "000-456"
    assert len(result["structures"]) == 1
    assert result["structures"][0]["report_id"] == "000-456"

  @pytest.mark.asyncio
  async def test_vector_search_excludes_parenthetical(
    self, tool_with_enricher, mock_client
  ):
    """Parenthetical structures are excluded by default in vector search."""
    mock_client.query_table = AsyncMock(
      return_value={
        "columns": [
          "identifier",
          "definition",
          "name",
          "type",
          "canonical_type",
          "canonical_confidence",
          "score",
        ],
        "rows": [
          [
            "struct-1",
            "Statement - BALANCE SHEETS",
            "BALANCE SHEETS",
            "Statement",
            "balance_sheet",
            0.8,
            0.95,
          ],
          [
            "struct-2",
            "Statement - BALANCE SHEETS [Parenthetical]",
            "BALANCE SHEETS (Parenthetical)",
            "Statement",
            "balance_sheet",
            0.75,
            0.90,
          ],
        ],
      }
    )

    result = await tool_with_enricher.execute({"query": "balance sheet"})
    assert len(result["structures"]) == 1
    assert result["structures"][0]["identifier"] == "struct-1"

  @pytest.mark.asyncio
  async def test_vector_search_includes_parenthetical(
    self, tool_with_enricher, mock_client
  ):
    """Parenthetical structures included when requested."""
    mock_client.query_table = AsyncMock(
      return_value={
        "columns": [
          "identifier",
          "definition",
          "name",
          "type",
          "canonical_type",
          "canonical_confidence",
          "score",
        ],
        "rows": [
          [
            "struct-1",
            "Statement - BALANCE SHEETS",
            "BALANCE SHEETS",
            "Statement",
            "balance_sheet",
            0.8,
            0.95,
          ],
          [
            "struct-2",
            "Statement - BALANCE SHEETS [Parenthetical]",
            "BALANCE SHEETS (Parenthetical)",
            "Statement",
            "balance_sheet",
            0.75,
            0.90,
          ],
        ],
      }
    )

    result = await tool_with_enricher.execute(
      {"query": "balance sheet", "include_parenthetical": True}
    )
    assert len(result["structures"]) == 2

  @pytest.mark.asyncio
  async def test_vector_search_embedding_failure(self, tool_with_enricher, mock_client):
    """Embedding failure returns error gracefully."""
    tool_with_enricher._enricher.embed_batch = lambda texts: (_ for _ in ()).throw(
      RuntimeError("Model load failed")
    )

    result = await tool_with_enricher.execute({"query": "cash flow"})
    assert "error" in result
    assert "Embedding failed" in result["error"]

  @pytest.mark.asyncio
  async def test_vector_search_duckdb_failure(self, tool_with_enricher, mock_client):
    """DuckDB query failure returns error gracefully."""
    mock_client.query_table = AsyncMock(side_effect=Exception("DuckDB unavailable"))

    result = await tool_with_enricher.execute({"query": "cash flow"})
    assert "error" in result
    assert "Vector search failed" in result["error"]

  @pytest.mark.asyncio
  async def test_vector_search_no_results(self, tool_with_enricher, mock_client):
    """Empty DuckDB results return empty structures list."""
    mock_client.query_table = AsyncMock(return_value={"columns": [], "rows": []})

    result = await tool_with_enricher.execute({"query": "nonexistent structure"})
    assert result["structures"] == []

  @pytest.mark.asyncio
  async def test_vector_search_ticker_no_matching_structures(
    self, tool_with_enricher, mock_client
  ):
    """When ticker filter yields no matching structure IDs, return empty."""
    mock_client.query_table = AsyncMock(
      return_value={
        "columns": [
          "identifier",
          "definition",
          "name",
          "type",
          "canonical_type",
          "canonical_confidence",
          "score",
        ],
        "rows": [
          [
            "struct-1",
            "Statement - INCOME",
            "INCOME",
            "Statement",
            "income_statement",
            0.8,
            0.90,
          ],
        ],
      }
    )
    # No structures match this ticker's reports
    mock_client.execute_query = AsyncMock(
      side_effect=[
        [],  # _fetch_structure_ids_for_report returns empty
        [],  # _fetch_report_metadata (won't be called but safe)
      ]
    )

    result = await tool_with_enricher.execute({"query": "income", "ticker": "ZZZZ"})
    assert result["structures"] == []


class TestHelpers:
  def test_is_parenthetical(self):
    assert ResolveStructureTool._is_parenthetical(
      "0001002 - Statement - BALANCE SHEETS [Parenthetical]"
    )
    assert ResolveStructureTool._is_parenthetical(
      "Statement - BALANCE SHEETS (Parenthetical)"
    )
    assert not ResolveStructureTool._is_parenthetical(
      "0001002 - Statement - CONSOLIDATED BALANCE SHEETS"
    )
    assert not ResolveStructureTool._is_parenthetical("")
    assert not ResolveStructureTool._is_parenthetical(None)

  def test_table_rows_to_dicts(self):
    response = {
      "columns": ["id", "name", "score"],
      "rows": [["s1", "Income", 0.9], ["s2", "Balance", 0.8]],
    }
    result = ResolveStructureTool._table_rows_to_dicts(response)
    assert len(result) == 2
    assert result[0] == {"id": "s1", "name": "Income", "score": 0.9}
