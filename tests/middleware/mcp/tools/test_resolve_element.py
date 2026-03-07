"""Tests for resolve-element MCP tool."""

from unittest.mock import AsyncMock, Mock

import pytest

from robosystems.middleware.mcp.tools.resolve_element_tool import ResolveElementTool


@pytest.fixture
def mock_client():
  client = AsyncMock()
  client.graph_id = "sec"
  return client


def _make_mock_enricher(canonical_concept=None):
  """Create a Mock enricher (sync methods, not async)."""
  enricher = Mock()
  enricher.embed_batch.return_value = [[0.1] * 384]
  enricher.match_canonical_from_query.return_value = canonical_concept
  return enricher


def _query_router(**responses):
  """Create a mock execute_query that returns different results based on query content.

  Key order matters — more specific patterns checked first.
  """
  defaults = {
    "CONTAINS": [],  # Text fallback (most specific, checked first)
    'canonical_concept = "': [],  # Canonical WHERE clause (not RETURN clause)
    "ELEMENT_HAS_LABEL": [],
    "FACT_HAS_ELEMENT": [],
  }
  defaults.update(responses)

  async def route(query, **kwargs):
    for key, result in defaults.items():
      if key in query:
        return result
    return []

  return route


@pytest.fixture
def tool(mock_client):
  t = ResolveElementTool(mock_client)
  t._enricher = _make_mock_enricher()
  return t


class TestResolveElementToolDefinition:
  def test_tool_definition_structure(self, tool):
    defn = tool.get_tool_definition()
    assert defn["name"] == "resolve-element"
    assert "concept" in defn["inputSchema"]["properties"]
    assert "ticker" in defn["inputSchema"]["properties"]
    assert "accession_number" in defn["inputSchema"]["properties"]
    assert defn["inputSchema"]["required"] == ["concept"]


class TestResolveElementExecution:
  @pytest.mark.asyncio
  async def test_empty_concept(self, tool):
    result = await tool.execute({"concept": ""})
    assert "error" in result

  @pytest.mark.asyncio
  async def test_with_canonical_match(self, mock_client):
    """When taxonomy matches, result includes canonical info and graph query results."""
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    mock_concept = CanonicalConcept(
      id="revenue",
      display_name="Revenue",
      category="income_statement",
      description="Total revenue",
      embedding=[0.1] * 384,
    )

    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher(canonical_concept=mock_concept)

    mock_client.execute_query = _query_router(
      **{
        'canonical_concept = "': [
          {
            "qname": "us-gaap:Revenues",
            "confidence": 0.95,
            "fact_count": 100,
          }
        ],
        "ELEMENT_HAS_LABEL": [{"qname": "us-gaap:Revenues", "label": "Revenue"}],
      }
    )

    result = await tool.execute({"concept": "revenue"})
    assert result["canonical_id"] == "revenue"
    assert result["canonical_name"] == "Revenue"
    assert len(result["matches"]) >= 1
    assert result["matches"][0]["qname"] == "us-gaap:Revenues"
    assert result["matches"][0]["fact_count"] == 100
    assert result["matches"][0]["label"] == "Revenue"
    assert result["query_hint"] is not None

  @pytest.mark.asyncio
  async def test_with_ticker_filter(self, mock_client):
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    mock_concept = CanonicalConcept(
      id="revenue",
      display_name="Revenue",
      category="income_statement",
      description="Total revenue",
      embedding=[0.1] * 384,
    )

    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher(canonical_concept=mock_concept)

    mock_client.execute_query = _query_router(
      **{
        'canonical_concept = "': [
          {
            "qname": "us-gaap:Revenues",
            "confidence": 0.95,
            "fact_count": 50,
          }
        ],
        "ELEMENT_HAS_LABEL": [{"qname": "us-gaap:Revenues", "label": "Revenue"}],
      }
    )

    result = await tool.execute({"concept": "revenue", "ticker": "NVDA"})
    assert result["ticker"] == "NVDA"
    assert "NVDA" in result["query_hint"]

  @pytest.mark.asyncio
  async def test_with_accession_number_filter(self, mock_client):
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    mock_concept = CanonicalConcept(
      id="revenue",
      display_name="Revenue",
      category="income_statement",
      description="Total revenue",
      embedding=[0.1] * 384,
    )

    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher(canonical_concept=mock_concept)

    mock_client.execute_query = _query_router(
      **{
        'canonical_concept = "': [
          {
            "qname": "us-gaap:Revenues",
            "confidence": 0.95,
            "fact_count": 12,
          }
        ],
        "ELEMENT_HAS_LABEL": [{"qname": "us-gaap:Revenues", "label": "Revenue"}],
      }
    )

    result = await tool.execute(
      {"concept": "revenue", "accession_number": "0001045810-25-000023"}
    )
    assert result["accession_number"] == "0001045810-25-000023"
    assert "0001045810-25-000023" in result["query_hint"]
    assert "REPORT_HAS_FACT" in result["query_hint"]
    assert result["matches"][0]["fact_count"] == 12

  @pytest.mark.asyncio
  async def test_no_canonical_match_falls_back_to_text(self, mock_client):
    """When no canonical match, falls back to text search on labels."""
    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher()  # No canonical match

    mock_client.execute_query = _query_router(
      **{
        "CONTAINS": [
          {
            "qname": "us-gaap:SomeObscureMetric",
            "label": "Some Obscure Metric",
            "concept": None,
            "confidence": None,
            "fact_count": 5,
          }
        ],
      }
    )

    result = await tool.execute({"concept": "some obscure metric"})
    assert result["canonical_id"] is None
    assert len(result["matches"]) >= 1
    assert result["matches"][0]["qname"] == "us-gaap:SomeObscureMetric"

  @pytest.mark.asyncio
  async def test_no_matches_at_all(self, mock_client):
    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher()

    mock_client.execute_query = _query_router()

    result = await tool.execute({"concept": "nonexistent metric"})
    assert result["canonical_id"] is None
    assert len(result["matches"]) == 0
    assert result["query_hint"] is None

  @pytest.mark.asyncio
  async def test_query_hint_generated(self, mock_client):
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    mock_concept = CanonicalConcept(
      id="total_assets",
      display_name="Total Assets",
      category="balance_sheet",
      description="Total assets",
      embedding=[0.1] * 384,
    )

    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher(canonical_concept=mock_concept)

    mock_client.execute_query = _query_router(
      **{
        'canonical_concept = "': [
          {"qname": "us-gaap:Assets", "confidence": 0.95, "fact_count": 200}
        ],
        "ELEMENT_HAS_LABEL": [{"qname": "us-gaap:Assets", "label": "Assets"}],
      }
    )

    result = await tool.execute({"concept": "total assets"})
    assert result["query_hint"] is not None
    assert "us-gaap:Assets" in result["query_hint"]
    assert "has_dimensions = false" in result["query_hint"]

  @pytest.mark.asyncio
  async def test_falls_back_to_canonical_when_duckdb_fails(self, mock_client):
    """When DuckDB vector search fails, falls back to canonical lookup."""
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    mock_concept = CanonicalConcept(
      id="revenue",
      display_name="Revenue",
      category="income_statement",
      description="Total revenue",
      embedding=[0.1] * 384,
    )

    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher(canonical_concept=mock_concept)

    # DuckDB query_table fails
    mock_client.query_table = AsyncMock(side_effect=Exception("DuckDB not available"))

    # But canonical lookup works
    mock_client.execute_query = _query_router(
      **{
        'canonical_concept = "': [
          {"qname": "us-gaap:Revenues", "confidence": 0.95, "fact_count": 100}
        ],
        "ELEMENT_HAS_LABEL": [{"qname": "us-gaap:Revenues", "label": "Revenue"}],
      }
    )

    result = await tool.execute({"concept": "revenue"})
    assert result["canonical_id"] == "revenue"
    assert len(result["matches"]) >= 1
    assert result["matches"][0]["qname"] == "us-gaap:Revenues"
