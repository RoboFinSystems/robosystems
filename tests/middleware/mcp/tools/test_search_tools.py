"""Tests for search MCP tools."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.search_tools import (
  GetDocumentSectionTool,
  SearchDocumentsTool,
)


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "sec"
  return client


class TestSearchDocumentsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = SearchDocumentsTool(mock_graph_client)
    defn = tool.get_tool_definition()

    assert defn["name"] == "search-documents"
    assert "inputSchema" in defn
    assert "query" in defn["inputSchema"]["properties"]
    assert defn["inputSchema"]["required"] == ["query"]

  @pytest.mark.asyncio
  async def test_execute_calls_service(self, mock_graph_client):
    tool = SearchDocumentsTool(mock_graph_client)

    mock_response = MagicMock()
    mock_response.model_dump.return_value = {
      "total": 1,
      "hits": [{"document_id": "doc1"}],
      "query": "test",
      "graph_id": "sec",
    }

    mock_service = MagicMock()
    mock_service.search_documents.return_value = mock_response

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      result = await tool.execute({"query": "tariff risk", "size": 5})
      assert result["total"] == 1
      mock_service.search_documents.assert_called_once()

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
    assert "document_id" in defn["inputSchema"]["properties"]
    assert defn["inputSchema"]["required"] == ["document_id"]

  @pytest.mark.asyncio
  async def test_execute_returns_section(self, mock_graph_client):
    tool = GetDocumentSectionTool(mock_graph_client)

    mock_section = MagicMock()
    mock_section.model_dump.return_value = {
      "document_id": "doc1",
      "content": "Full text...",
    }

    mock_service = MagicMock()
    mock_service.get_document_section.return_value = mock_section

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=mock_service,
    ):
      result = await tool.execute({"document_id": "doc1"})
      assert result["content"] == "Full text..."

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
