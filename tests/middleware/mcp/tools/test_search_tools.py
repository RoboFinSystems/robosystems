"""Tests for search MCP tools."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.search_tools import (
  SECTION_READ_DEFAULT,
  SECTION_READ_MAX,
  SNIPPET_CHARS_DEFAULT,
  SNIPPET_CHARS_MAX,
  SNIPPET_CHARS_MIN,
  GetDocumentSectionTool,
  SearchDocumentsTool,
  compact_search_response,
  window_section,
)
from robosystems.models.api.search import DocumentSection, SearchHit, SearchResponse


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "sec"
  return client


def _hit(document_id: str, **overrides) -> SearchHit:
  fields = {
    "document_id": document_id,
    "score": 9.1,
    "source_type": "ixbrl_disclosure",
    "entity_ticker": "MMM",
    "entity_name": "3M CO",
    "form_type": "10-K",
    "filing_date": "2025-02-05",
    "fiscal_year": 2024,
    "section_label": "Research, Development and Related Expenses",
    "section_id": "us-gaap:ResearchDevelopmentAndComputerSoftwareDisclosureTextBlock",
    "xbrl_elements": ["us-gaap:ResearchAndDevelopmentExpense"] * 20,
    "snippet": "... research and development ...",
    "content_length": 1655,
    "content_url": "https://cdn.example/sec/x.txt",
  }
  fields.update(overrides)
  return SearchHit(**fields)


def _response(*hits: SearchHit) -> SearchResponse:
  return SearchResponse(total=len(hits), hits=list(hits), query="r&d", graph_id="sec")


def _section(content: str, **overrides) -> DocumentSection:
  fields = {
    "document_id": "doc1",
    "graph_id": "sec",
    "source_type": "narrative_section",
    "entity_ticker": "MMM",
    "section_label": "MD&A (1/4)",
    "part": 1,
    "part_count": 4,
    "next_document_id": "doc2",
    "xbrl_elements": ["us-gaap:Revenues"],
    "content": content,
    "content_length": len(content),
  }
  fields.update(overrides)
  return DocumentSection(**fields)


class TestSearchDocumentsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = SearchDocumentsTool(mock_graph_client)
    defn = tool.get_tool_definition()

    assert defn["name"] == "search-documents"
    assert "inputSchema" in defn
    assert "query" in defn["inputSchema"]["properties"]
    assert "snippet_chars" in defn["inputSchema"]["properties"]
    assert defn["inputSchema"]["required"] == ["query"]

  def test_description_no_longer_promises_elements_on_a_hit(self, mock_graph_client):
    """The element list moved to get-document-section; the prompt must say so,
    or the model reads a hit expecting a field that is not there."""
    defn = SearchDocumentsTool(mock_graph_client).get_tool_definition()
    desc = " ".join(defn["description"].split())
    assert "get-document-section returns an iXBRL disclosure's xbrl_elements" in desc
    assert "A snippet is an excerpt around the match" in desc

  @pytest.mark.asyncio
  async def test_execute_calls_service(self, mock_graph_client):
    tool = SearchDocumentsTool(mock_graph_client)

    mock_service = MagicMock()
    mock_service.search_documents.return_value = _response(_hit("doc1"))

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      result = await tool.execute({"query": "tariff risk", "size": 5})
      assert result["total"] == 1
      assert result["hits"][0]["document_id"] == "doc1"
      mock_service.search_documents.assert_called_once()
      request = mock_service.search_documents.call_args[0][1]
      assert request.snippet_chars == SNIPPET_CHARS_DEFAULT

  @pytest.mark.asyncio
  @pytest.mark.parametrize(
    ("asked", "sent"),
    [(5000, SNIPPET_CHARS_MAX), (10, SNIPPET_CHARS_MIN), (800, 800)],
  )
  async def test_snippet_chars_is_clamped(self, mock_graph_client, asked, sent):
    tool = SearchDocumentsTool(mock_graph_client)
    mock_service = MagicMock()
    mock_service.search_documents.return_value = _response()

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      await tool.execute({"query": "x", "snippet_chars": asked})
    assert mock_service.search_documents.call_args[0][1].snippet_chars == sent

  @pytest.mark.asyncio
  async def test_returns_error_when_service_unavailable(self, mock_graph_client):
    tool = SearchDocumentsTool(mock_graph_client)

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=None,
    ):
      result = await tool.execute({"query": "test"})
      assert "error" in result


class TestGetDocumentSectionTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetDocumentSectionTool(mock_graph_client)
    defn = tool.get_tool_definition()

    assert defn["name"] == "get-document-section"
    props = defn["inputSchema"]["properties"]
    assert "document_id" in props
    assert props["offset"]["default"] == 0
    assert props["length"]["default"] == SECTION_READ_DEFAULT
    assert defn["inputSchema"]["required"] == ["document_id"]

  @pytest.mark.asyncio
  async def test_execute_returns_section(self, mock_graph_client):
    tool = GetDocumentSectionTool(mock_graph_client)

    mock_service = MagicMock()
    mock_service.get_document_section.return_value = _section("Full text...")

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      result = await tool.execute({"document_id": "doc1"})
      assert result["content"] == "Full text..."
      assert "next_offset" not in result

  @pytest.mark.asyncio
  async def test_execute_pages_with_offset_and_length(self, mock_graph_client):
    tool = GetDocumentSectionTool(mock_graph_client)
    text = "".join(f"{i % 10}" for i in range(20_000))

    mock_service = MagicMock()
    mock_service.get_document_section.return_value = _section(text)

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      first = await tool.execute({"document_id": "doc1"})
      assert first["content"] == text[:SECTION_READ_DEFAULT]
      assert first["content_length"] == 20_000
      assert first["next_offset"] == SECTION_READ_DEFAULT

      rest = await tool.execute(
        {"document_id": "doc1", "offset": first["next_offset"], "length": 50_000}
      )
      # length is capped, so the read stops short of the end and says so
      assert rest["content"] == text[SECTION_READ_DEFAULT:][:SECTION_READ_MAX]
      assert rest["next_offset"] == SECTION_READ_DEFAULT + SECTION_READ_MAX

  @pytest.mark.asyncio
  async def test_returns_error_when_not_found(self, mock_graph_client):
    tool = GetDocumentSectionTool(mock_graph_client)

    mock_service = MagicMock()
    mock_service.get_document_section.return_value = None

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      result = await tool.execute({"document_id": "nonexistent"})
      assert "error" in result


class TestResolveSearchGraphId:
  """Test _resolve_search_graph_id mixin for subgraph → parent resolution."""

  def test_shared_repo_parent_unchanged(self):
    """Parent shared repo graph_id passes through unchanged."""
    client = MagicMock()
    client.graph_id = "sec"
    tool = SearchDocumentsTool(client)
    assert tool._resolve_search_graph_id() == "sec"

  def test_shared_repo_subgraph_resolves_to_parent(self):
    """Subgraph of shared repo resolves to parent (sec_historical → sec)."""
    client = MagicMock()
    client.graph_id = "sec_historical"
    tool = SearchDocumentsTool(client)
    assert tool._resolve_search_graph_id() == "sec"

  def test_user_graph_unchanged(self):
    """User-owned graph passes through unchanged (ValueError fallback)."""
    client = MagicMock()
    client.graph_id = "kg123"
    tool = SearchDocumentsTool(client)
    assert tool._resolve_search_graph_id() == "kg123"

  def test_user_subgraph_unchanged(self):
    """Subgraph of user-owned graph passes through unchanged (ValueError fallback)."""
    client = MagicMock()
    client.graph_id = "kg123_dev"
    tool = SearchDocumentsTool(client)
    assert tool._resolve_search_graph_id() == "kg123_dev"

  def test_get_document_section_also_resolves(self):
    """GetDocumentSectionTool inherits the same resolution logic."""
    client = MagicMock()
    client.graph_id = "sec_historical"
    tool = GetDocumentSectionTool(client)
    assert tool._resolve_search_graph_id() == "sec"


class TestToolRegistration:
  @patch.dict("os.environ", {"SEMANTIC_SEARCH_ENABLED": "true"})
  def test_tools_registered_when_enabled(self):
    """Verify search tools appear in tool definitions when enabled."""
    # This tests the manager integration indirectly
    from robosystems.middleware.mcp.tools.search_tools import (
      GetDocumentSectionTool,
      SearchDocumentsTool,
    )

    client = MagicMock()
    search_tool = SearchDocumentsTool(client)
    section_tool = GetDocumentSectionTool(client)

    assert search_tool.get_tool_definition()["name"] == "search-documents"
    assert section_tool.get_tool_definition()["name"] == "get-document-section"


class TestSearchToolErrorSanitization:
  """opensearch-py exception text (endpoint hostnames, index internals) must
  not reach the LLM through a tool result."""

  @pytest.mark.asyncio
  async def test_backend_error_is_withheld_from_the_llm(self, mock_graph_client):
    tool = SearchDocumentsTool(mock_graph_client)

    class ConnectionTimeout(Exception):
      pass

    mock_service = MagicMock()
    mock_service.search_documents.side_effect = ConnectionTimeout(
      "ConnectionTimeout caused by vpc-os-abc.us-east-1.es.amazonaws.com:443"
    )
    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      result = await tool.execute({"query": "anything"})

    assert result["error"] == "search_failed"
    assert "amazonaws.com" not in result["message"]
    assert "see server logs" in result["message"]


class TestCompactSearchResponse:
  """A hit carries what a model needs to choose it; the rest is a follow-up."""

  def test_drops_element_list_and_null_fields(self):
    result = compact_search_response(_response(_hit("doc1")))
    hit = result["hits"][0]
    assert "xbrl_elements" not in hit
    assert "element_qname" not in hit  # None on the input
    assert hit["document_id"] == "doc1"
    assert hit["snippet"] == "... research and development ..."
    assert hit["section_label"] and hit["score"] and hit["content_url"]

  def test_hoists_filing_fields_shared_by_every_hit(self):
    result = compact_search_response(_response(_hit("doc1"), _hit("doc2")))
    assert result["entity_ticker"] == "MMM"
    assert result["entity_name"] == "3M CO"
    assert result["form_type"] == "10-K"
    assert result["filing_date"] == "2025-02-05"
    assert result["fiscal_year"] == 2024
    for hit in result["hits"]:
      for field in ("entity_ticker", "entity_name", "form_type", "filing_date"):
        assert field not in hit

  def test_keeps_filing_fields_on_hits_that_span_filings(self):
    result = compact_search_response(
      _response(_hit("doc1"), _hit("doc2", entity_ticker="NVDA", fiscal_year=2025))
    )
    assert "entity_ticker" not in result
    assert "fiscal_year" not in result
    # fields that still agree are hoisted independently of the ones that differ
    assert result["form_type"] == "10-K"
    assert [h["entity_ticker"] for h in result["hits"]] == ["MMM", "NVDA"]

  def test_a_field_missing_on_one_hit_is_not_hoisted(self):
    result = compact_search_response(
      _response(_hit("doc1"), _hit("doc2", filing_date=None))
    )
    assert "filing_date" not in result
    assert result["hits"][0]["filing_date"] == "2025-02-05"
    assert "filing_date" not in result["hits"][1]

  def test_empty_result_keeps_the_envelope(self):
    assert compact_search_response(_response()) == {
      "total": 0,
      "query": "r&d",
      "graph_id": "sec",
      "hits": [],
    }

  def test_long_section_part_fields_survive(self):
    hit = _hit(
      "p2", part=2, part_count=4, parent_document_id="p", next_document_id="p3"
    )
    compact = compact_search_response(_response(hit))["hits"][0]
    assert (compact["part"], compact["part_count"]) == (2, 4)
    assert compact["parent_document_id"] == "p"
    assert compact["next_document_id"] == "p3"

  def test_payload_shrinks_by_more_than_half_on_a_single_filing_search(self):
    """The measured shape: ten iXBRL hits from one 10-K, each carrying its
    section's element list."""
    import json

    response = _response(*(_hit(f"doc{i}") for i in range(10)))
    before = len(json.dumps(response.model_dump()))
    after = len(json.dumps(compact_search_response(response)))
    assert after < before / 2


class TestWindowSection:
  def test_short_content_is_returned_whole(self):
    result = window_section(_section("short"), 0, 4000)
    assert result["content"] == "short"
    assert result["content_length"] == 5
    assert result["offset"] == 0
    assert "next_offset" not in result

  def test_window_and_next_offset(self):
    text = "a" * 100
    result = window_section(_section(text), 0, 40)
    assert result["content"] == "a" * 40
    assert result["next_offset"] == 40
    tail = window_section(_section(text), 40, 100)
    assert tail["content"] == "a" * 60
    assert "next_offset" not in tail

  def test_offset_past_the_end_is_an_error(self):
    assert "error" in window_section(_section("abc"), 3, 10)
    assert "error" in window_section(_section("abc"), 99, 10)

  def test_empty_content_at_offset_zero_is_not_an_error(self):
    assert window_section(_section(""), 0, 10)["content"] == ""

  def test_keeps_elements_and_drops_null_fields(self):
    result = window_section(_section("x"), 0, 10)
    assert result["xbrl_elements"] == ["us-gaap:Revenues"]
    assert "document_title" not in result
    assert result["next_document_id"] == "doc2"
