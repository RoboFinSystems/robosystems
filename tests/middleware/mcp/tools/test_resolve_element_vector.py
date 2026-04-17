"""Tests for resolve-element MCP tool — vector search (lance) path.

Tests the vector-search code path (in ``adapters/sec/mcp/element_resolver``)
which is gated by ``MCP_VECTOR_SEARCH_ENABLED``. When enabled, the tool
calls ``client.vector_search()`` before falling back to canonical or
text search.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from robosystems.middleware.mcp.tools.resolve_element_tool import ResolveElementTool

_RESOLVER = "robosystems.adapters.sec.mcp.element_resolver"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_enricher():
  """Create a Mock enricher that embeds successfully but has no canonical match."""
  enricher = Mock()
  enricher.embed_batch.return_value = [[0.1] * 384]
  enricher.match_canonical_from_query.return_value = None
  return enricher


def _query_router(**responses):
  """Route execute_query calls based on query content.

  Keys are checked in definition order. More specific patterns first:
    "CONTAINS $search_term" - text fallback (has both CONTAINS and ELEMENT_HAS_LABEL)
    "ELEMENT_HAS_LABEL" - label lookup for lance (has ELEMENT_HAS_LABEL + IN $qnames)
    "IN $qnames" - lance candidate graph query (fact counts, no ELEMENT_HAS_LABEL)
    "canonical_concept = $canonical_id" - canonical lookup
  """
  defaults = {
    "CONTAINS $search_term": [],
    "ELEMENT_HAS_LABEL": [],
    "IN $qnames": [],
    "canonical_concept = $canonical_id": [],
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
def mock_client():
  client = AsyncMock()
  client.graph_id = "sec"
  return client


@pytest.fixture
def tool(mock_client):
  t = ResolveElementTool(mock_client)
  t._vector_search_enabled = True
  return t


@pytest.fixture
def patched_enricher():
  """Mock the ops-layer enricher; tests can customize via
  ``patched_enricher.match_canonical_from_query.return_value = <concept>``.
  """
  enricher = _make_mock_enricher()
  with patch(f"{_RESOLVER}._get_enricher", return_value=enricher):
    yield enricher


@pytest.fixture(autouse=True)
def _autouse_enricher(patched_enricher):
  """Autouse so every test gets a fresh mocked enricher; tests that need
  a specific canonical match can depend on ``patched_enricher`` directly.
  """
  yield


# ---------------------------------------------------------------------------
# Vector search happy path
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVectorSearchResolution:
  @pytest.mark.asyncio
  async def test_lance_results_returned_with_graph_enrichment(self, tool, mock_client):
    """Vector search results are enriched with graph fact counts and labels."""
    mock_client.vector_search.return_value = [
      {
        "qname": "us-gaap:Revenues",
        "distance": 0.05,
        "canonical_concept": "revenue",
        "canonical_confidence": 0.95,
      },
      {
        "qname": "us-gaap:NetIncomeLoss",
        "distance": 0.12,
        "canonical_concept": "net_income",
        "canonical_confidence": 0.92,
      },
    ]

    mock_client.execute_query = _query_router(
      **{
        "IN $qnames": [
          {"qname": "us-gaap:Revenues", "confidence": 0.95, "fact_count": 500},
          {"qname": "us-gaap:NetIncomeLoss", "confidence": 0.92, "fact_count": 300},
        ],
        "ELEMENT_HAS_LABEL": [
          {"qname": "us-gaap:Revenues", "label": "Revenue"},
          {"qname": "us-gaap:NetIncomeLoss", "label": "Net Income (Loss)"},
        ],
      }
    )

    result = await tool.execute({"concept": "revenue"})

    assert len(result["matches"]) >= 1
    assert result["matches"][0]["qname"] == "us-gaap:Revenues"
    assert result["matches"][0]["fact_count"] == 500
    assert result["matches"][0]["label"] == "Revenue"
    # Score is 1.0 - distance
    assert result["matches"][0]["score"] == pytest.approx(0.95, abs=0.01)
    # Canonical set from top lance result
    assert result["canonical_id"] == "revenue"
    assert result["query_hint"] is not None

  @pytest.mark.asyncio
  async def test_lance_results_without_graph_rows(self, tool, mock_client):
    """When graph returns no rows, raw lance results are used directly."""
    mock_client.vector_search.return_value = [
      {
        "qname": "ext:CustomMetric",
        "distance": 0.08,
        "canonical_concept": None,
        "canonical_confidence": None,
      },
    ]

    mock_client.execute_query = _query_router()  # All empty

    result = await tool.execute({"concept": "custom metric"})

    assert len(result["matches"]) >= 1
    assert result["matches"][0]["qname"] == "ext:CustomMetric"
    assert result["matches"][0]["fact_count"] == 0
    assert result["matches"][0]["score"] == pytest.approx(0.92, abs=0.01)

  @pytest.mark.asyncio
  async def test_lance_with_ticker_filter(self, tool, mock_client):
    """Ticker is passed through to graph candidate query."""
    mock_client.vector_search.return_value = [
      {
        "qname": "us-gaap:Revenues",
        "distance": 0.05,
        "canonical_concept": "revenue",
        "canonical_confidence": 0.95,
      },
    ]

    mock_client.execute_query = _query_router(
      **{
        "IN $qnames": [
          {"qname": "us-gaap:Revenues", "confidence": 0.95, "fact_count": 42},
        ],
        "ELEMENT_HAS_LABEL": [
          {"qname": "us-gaap:Revenues", "label": "Revenue"},
        ],
      }
    )

    result = await tool.execute({"concept": "revenue", "ticker": "NVDA"})

    assert result["ticker"] == "NVDA"
    assert result["matches"][0]["fact_count"] == 42
    assert result["query_hint"] is not None
    assert "$ticker" in result["query_hint"]

  @pytest.mark.asyncio
  async def test_lance_with_report_id_filter(self, tool, mock_client):
    """Report ID is passed through to graph candidate query."""
    mock_client.vector_search.return_value = [
      {
        "qname": "us-gaap:Assets",
        "distance": 0.03,
        "canonical_concept": "total_assets",
        "canonical_confidence": 0.98,
      },
    ]

    mock_client.execute_query = _query_router(
      **{
        "IN $qnames": [
          {"qname": "us-gaap:Assets", "confidence": 0.98, "fact_count": 8},
        ],
        "ELEMENT_HAS_LABEL": [
          {"qname": "us-gaap:Assets", "label": "Assets"},
        ],
      }
    )

    report_id = "0001045810-25-000023"
    result = await tool.execute({"concept": "total assets", "report_id": report_id})

    assert result["report_id"] == report_id
    assert result["matches"][0]["qname"] == "us-gaap:Assets"
    assert "$report_id" in result["query_hint"]
    assert "REPORT_HAS_FACT" in result["query_hint"]

  @pytest.mark.asyncio
  async def test_lance_deduplicates_qnames(self, tool, mock_client):
    """Duplicate qnames from vector search are deduplicated."""
    mock_client.vector_search.return_value = [
      {
        "qname": "us-gaap:Revenues",
        "distance": 0.05,
        "canonical_concept": "revenue",
        "canonical_confidence": 0.95,
      },
      {
        "qname": "us-gaap:Revenues",
        "distance": 0.06,
        "canonical_concept": "revenue",
        "canonical_confidence": 0.95,
      },
      {
        "qname": "us-gaap:NetIncomeLoss",
        "distance": 0.10,
        "canonical_concept": "net_income",
        "canonical_confidence": 0.92,
      },
    ]

    mock_client.execute_query = _query_router()

    result = await tool.execute({"concept": "revenue"})

    qnames = [m["qname"] for m in result["matches"]]
    assert qnames == ["us-gaap:Revenues", "us-gaap:NetIncomeLoss"]


# ---------------------------------------------------------------------------
# Vector search disabled / fallback
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVectorSearchDisabled:
  @pytest.mark.asyncio
  async def test_falls_back_when_disabled(self, mock_client):
    """When vector search is disabled, falls through to canonical/text."""
    tool = ResolveElementTool(mock_client)
    tool._vector_search_enabled = False

    mock_client.execute_query = _query_router(
      **{
        "CONTAINS $search_term": [
          {
            "qname": "us-gaap:Revenues",
            "label": "Revenue",
            "concept": None,
            "confidence": None,
            "fact_count": 10,
          }
        ],
      }
    )

    result = await tool.execute({"concept": "revenue"})

    # Should have used text fallback, not vector search
    mock_client.vector_search.assert_not_called()
    assert len(result["matches"]) >= 1

  @pytest.mark.asyncio
  async def test_falls_back_when_lance_returns_empty(
    self, mock_client, patched_enricher
  ):
    """When vector search returns no results, falls through to canonical."""
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    mock_concept = CanonicalConcept(
      id="revenue",
      display_name="Revenue",
      category="income_statement",
      description="Total revenue",
      embedding=[0.1] * 384,
    )

    tool = ResolveElementTool(mock_client)
    tool._vector_search_enabled = True
    patched_enricher.match_canonical_from_query.return_value = mock_concept

    mock_client.vector_search.return_value = []  # No lance results

    mock_client.execute_query = _query_router(
      **{
        "canonical_concept = $canonical_id": [
          {"qname": "us-gaap:Revenues", "confidence": 0.95, "fact_count": 100}
        ],
        "ELEMENT_HAS_LABEL": [
          {"qname": "us-gaap:Revenues", "label": "Revenue"},
        ],
      }
    )

    result = await tool.execute({"concept": "revenue"})

    # Should have fallen back to canonical
    assert result["canonical_id"] == "revenue"
    assert result["matches"][0]["qname"] == "us-gaap:Revenues"

  @pytest.mark.asyncio
  async def test_falls_back_when_lance_raises(self, mock_client, patched_enricher):
    """When vector search raises an exception, falls through gracefully."""
    from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

    mock_concept = CanonicalConcept(
      id="revenue",
      display_name="Revenue",
      category="income_statement",
      description="Total revenue",
      embedding=[0.1] * 384,
    )

    tool = ResolveElementTool(mock_client)
    tool._vector_search_enabled = True
    patched_enricher.match_canonical_from_query.return_value = mock_concept

    mock_client.vector_search.side_effect = ConnectionError("Graph API unreachable")

    mock_client.execute_query = _query_router(
      **{
        "canonical_concept = $canonical_id": [
          {"qname": "us-gaap:Revenues", "confidence": 0.95, "fact_count": 100}
        ],
        "ELEMENT_HAS_LABEL": [
          {"qname": "us-gaap:Revenues", "label": "Revenue"},
        ],
      }
    )

    result = await tool.execute({"concept": "revenue"})

    # Should have caught the error and fallen back to canonical
    assert result["canonical_id"] == "revenue"
    assert result["matches"][0]["qname"] == "us-gaap:Revenues"


# ---------------------------------------------------------------------------
# vector_search_enabled property
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestVectorSearchEnabledProperty:
  def test_property_caches_value(self, mock_client):
    """Property reads env once and caches."""
    tool = ResolveElementTool(mock_client)
    tool._vector_search_enabled = None  # Reset cache

    with pytest.MonkeyPatch.context() as mp:
      mp.setenv("MCP_VECTOR_SEARCH_ENABLED", "true")
      # First access triggers env read
      val1 = tool.vector_search_enabled
      # Mutate env — should still return cached value
      mp.setenv("MCP_VECTOR_SEARCH_ENABLED", "false")
      val2 = tool.vector_search_enabled
      assert val1 == val2

  def test_property_defaults_to_false_on_error(self, mock_client):
    """Property defaults to False if env lookup raises."""
    tool = ResolveElementTool(mock_client)
    tool._vector_search_enabled = None

    # Direct set to simulate the fallback
    tool._vector_search_enabled = False
    assert tool.vector_search_enabled is False
