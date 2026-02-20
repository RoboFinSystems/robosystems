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

  Key order matters — more specific patterns are checked first so that queries
  containing multiple keywords (e.g. label fallback has both QUERY_VECTOR_INDEX
  and ELEMENT_HAS_LABEL) match the right handler.
  """
  defaults = {
    "QUERY_VECTOR_INDEX('Element'": [],
    "QUERY_VECTOR_INDEX('Label'": [],
    "FACT_HAS_ELEMENT": [],
    "ELEMENT_HAS_LABEL": [],
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
    """When taxonomy matches, result includes canonical info and vector search results."""
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

    # LadybugDB returns distance (cosine: 0=identical, lower=better)
    mock_client.execute_query = _query_router(
      **{
        "QUERY_VECTOR_INDEX('Element'": [
          {
            "id": "elem-001",
            "qname": "us-gaap:Revenues",
            "canonical": "revenue",
            "confidence": 0.95,
            "distance": 0.08,
          }
        ],
        "FACT_HAS_ELEMENT": [{"id": "elem-001", "fact_count": 100}],
        "ELEMENT_HAS_LABEL": [{"id": "elem-001", "label": "Revenue"}],
      }
    )

    result = await tool.execute({"concept": "revenue"})
    assert result["canonical_id"] == "revenue"
    assert result["canonical_name"] == "Revenue"
    assert len(result["matches"]) >= 1
    assert result["matches"][0]["qname"] == "us-gaap:Revenues"
    assert result["matches"][0]["fact_count"] == 100
    assert result["matches"][0]["label"] == "Revenue"
    # distance 0.08 → similarity 0.92
    assert result["matches"][0]["score"] == 0.92
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
        "QUERY_VECTOR_INDEX('Element'": [
          {
            "id": "elem-001",
            "qname": "us-gaap:Revenues",
            "canonical": "revenue",
            "confidence": 0.95,
            "distance": 0.08,
          },
          {
            "id": "elem-002",
            "qname": "us-gaap:SalesRevenueNet",
            "canonical": "revenue",
            "confidence": 0.90,
            "distance": 0.12,
          },
        ],
        "FACT_HAS_ELEMENT": [{"id": "elem-001", "fact_count": 50}],
        "ELEMENT_HAS_LABEL": [{"id": "elem-001", "label": "Revenue"}],
      }
    )

    result = await tool.execute({"concept": "revenue", "ticker": "NVDA"})
    assert result["ticker"] == "NVDA"
    assert "NVDA" in result["query_hint"]
    # elem-002 should be filtered out (fact_count=0 for this ticker)
    assert all(m["qname"] != "us-gaap:SalesRevenueNet" for m in result["matches"])

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
        "QUERY_VECTOR_INDEX('Element'": [
          {
            "id": "elem-001",
            "qname": "us-gaap:Revenues",
            "canonical": "revenue",
            "confidence": 0.95,
            "distance": 0.08,
          },
          {
            "id": "elem-002",
            "qname": "us-gaap:SalesRevenueNet",
            "canonical": "revenue",
            "confidence": 0.90,
            "distance": 0.12,
          },
        ],
        # Only elem-001 has facts in this specific filing
        "FACT_HAS_ELEMENT": [{"id": "elem-001", "fact_count": 12}],
        "ELEMENT_HAS_LABEL": [{"id": "elem-001", "label": "Revenue"}],
      }
    )

    result = await tool.execute(
      {"concept": "revenue", "accession_number": "0001045810-25-000023"}
    )
    assert result["accession_number"] == "0001045810-25-000023"
    assert "0001045810-25-000023" in result["query_hint"]
    assert "REPORT_HAS_FACT" in result["query_hint"]
    # elem-002 should be filtered out (no facts in this filing)
    assert all(m["qname"] != "us-gaap:SalesRevenueNet" for m in result["matches"])
    assert result["matches"][0]["fact_count"] == 12

  @pytest.mark.asyncio
  async def test_no_matches(self, mock_client):
    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher()

    mock_client.execute_query = _query_router()

    result = await tool.execute({"concept": "nonexistent metric"})
    assert result["canonical_id"] is None
    assert len(result["matches"]) == 0
    assert result["query_hint"] is None

  @pytest.mark.asyncio
  async def test_label_fallback_when_few_element_matches(self, mock_client):
    """When vector search returns < 3 results, label fallback triggers."""
    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher()

    mock_client.execute_query = _query_router(
      **{
        "QUERY_VECTOR_INDEX('Element'": [
          {
            "id": "elem-001",
            "qname": "us-gaap:Revenues",
            "canonical": "revenue",
            "confidence": 0.95,
            "distance": 0.08,
          }
        ],
        "FACT_HAS_ELEMENT": [{"id": "elem-001", "fact_count": 10}],
        "ELEMENT_HAS_LABEL": [{"id": "elem-001", "label": "Revenue"}],
        "QUERY_VECTOR_INDEX('Label'": [
          {
            "qname": "us-gaap:SalesRevenueNet",
            "label": "Net Sales Revenue",
            "distance": 0.15,
          }
        ],
      }
    )

    result = await tool.execute({"concept": "revenue"})
    qnames = [m["qname"] for m in result["matches"]]
    assert "us-gaap:Revenues" in qnames
    assert "us-gaap:SalesRevenueNet" in qnames

  @pytest.mark.asyncio
  async def test_deduplicates_qnames(self, mock_client):
    """Same qname from different filings should be deduplicated."""
    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher()

    mock_client.execute_query = _query_router(
      **{
        "QUERY_VECTOR_INDEX('Element'": [
          {
            "id": "elem-001",
            "qname": "us-gaap:Revenues",
            "canonical": "revenue",
            "confidence": 0.95,
            "distance": 0.08,
          },
          {
            "id": "elem-002",
            "qname": "us-gaap:Revenues",
            "canonical": "revenue",
            "confidence": 0.95,
            "distance": 0.10,
          },
          {
            "id": "elem-003",
            "qname": "us-gaap:SalesRevenueNet",
            "canonical": "revenue",
            "confidence": 0.90,
            "distance": 0.15,
          },
        ],
        "FACT_HAS_ELEMENT": [
          {"id": "elem-001", "fact_count": 100},
          {"id": "elem-003", "fact_count": 20},
        ],
        "ELEMENT_HAS_LABEL": [],
      }
    )

    result = await tool.execute({"concept": "revenue"})
    qnames = [m["qname"] for m in result["matches"]]
    assert qnames.count("us-gaap:Revenues") == 1

  @pytest.mark.asyncio
  async def test_sorts_by_fact_count_then_score(self, mock_client):
    """Results should be sorted by fact_count descending, then similarity score."""
    tool = ResolveElementTool(mock_client)
    tool._enricher = _make_mock_enricher()

    mock_client.execute_query = _query_router(
      **{
        "QUERY_VECTOR_INDEX('Element'": [
          {
            "id": "elem-001",
            "qname": "us-gaap:A",
            "canonical": None,
            "confidence": None,
            "distance": 0.05,
          },
          {
            "id": "elem-002",
            "qname": "us-gaap:B",
            "canonical": None,
            "confidence": None,
            "distance": 0.10,
          },
          {
            "id": "elem-003",
            "qname": "us-gaap:C",
            "canonical": None,
            "confidence": None,
            "distance": 0.15,
          },
        ],
        "FACT_HAS_ELEMENT": [
          {"id": "elem-001", "fact_count": 10},
          {"id": "elem-002", "fact_count": 100},
          {"id": "elem-003", "fact_count": 50},
        ],
        "ELEMENT_HAS_LABEL": [],
      }
    )

    result = await tool.execute({"concept": "something"})
    qnames = [m["qname"] for m in result["matches"]]
    assert qnames == ["us-gaap:B", "us-gaap:C", "us-gaap:A"]
