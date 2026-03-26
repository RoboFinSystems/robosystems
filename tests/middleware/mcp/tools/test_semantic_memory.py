"""Tests for semantic memory MCP tools (remember-text, recall-text)."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.semantic_memory import (
  RecallTextTool,
  RememberTextTool,
)


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123_memory"  # subgraph
  return client


@pytest.fixture
def mock_graph_client_parent():
  client = MagicMock()
  client.graph_id = "kgtest123"  # parent graph (not subgraph)
  return client


class TestRememberTextTool:
  def test_tool_definition(self, mock_graph_client):
    tool = RememberTextTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "remember-text"
    assert "text" in defn["inputSchema"]["properties"]
    assert "text" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_rejects_parent_graph(self, mock_graph_client_parent):
    tool = RememberTextTool(mock_graph_client_parent)
    result = await tool.execute({"text": "test memory"})
    assert result["error"] == "subgraph_required"

  @pytest.mark.asyncio
  async def test_rejects_empty_text(self, mock_graph_client):
    tool = RememberTextTool(mock_graph_client)
    with patch(
      "robosystems.middleware.mcp.tools.semantic_memory._validate_subgraph_context",
      return_value=None,
    ):
      result = await tool.execute({"text": ""})
      assert result["error"] == "invalid_input"

  @pytest.mark.asyncio
  async def test_rejects_long_text(self, mock_graph_client):
    tool = RememberTextTool(mock_graph_client)
    with patch(
      "robosystems.middleware.mcp.tools.semantic_memory._validate_subgraph_context",
      return_value=None,
    ):
      result = await tool.execute({"text": "x" * 5001})
      assert result["error"] == "invalid_input"
      assert "5000" in result["message"]

  @pytest.mark.asyncio
  async def test_successful_store(self, mock_graph_client):
    tool = RememberTextTool(mock_graph_client)

    mock_service = MagicMock()
    mock_service.client.count_by_source_type.return_value = 5
    mock_service.client.index_document = MagicMock()

    mock_embedding_service = MagicMock()
    mock_embedding_service.embed_single.return_value = [0.1] * 384

    with (
      patch(
        "robosystems.middleware.mcp.tools.semantic_memory._validate_subgraph_context",
        return_value=None,
      ),
      patch(
        "robosystems.operations.search.get_search_service",
        return_value=mock_service,
      ),
      patch(
        "robosystems.operations.search.embeddings.get_embedding_service",
        return_value=mock_embedding_service,
      ),
      patch.object(RememberTextTool, "_resolve_tier", return_value="ladybug-standard"),
    ):
      result = await tool.execute(
        {"text": "test memory content", "category": "analysis", "tags": ["test"]}
      )

    assert result["success"] is True
    assert "document_id" in result
    assert result["document_id"].startswith("mem_kgtest123_memory_")

    # Verify the document was indexed
    mock_service.client.index_document.assert_called_once()
    indexed_doc = mock_service.client.index_document.call_args[0][0]
    assert indexed_doc["source_type"] == "memory"
    assert indexed_doc["content"] == "test memory content"
    assert indexed_doc["category"] == "analysis"
    assert indexed_doc["tags"] == ["test"]
    assert len(indexed_doc["embedding"]) == 384

  @pytest.mark.asyncio
  async def test_memory_limit_reached(self, mock_graph_client):
    tool = RememberTextTool(mock_graph_client)

    mock_service = MagicMock()
    mock_service.client.count_by_source_type.return_value = 1000  # At limit

    with (
      patch(
        "robosystems.middleware.mcp.tools.semantic_memory._validate_subgraph_context",
        return_value=None,
      ),
      patch(
        "robosystems.operations.search.get_search_service",
        return_value=mock_service,
      ),
      patch.object(RememberTextTool, "_resolve_tier", return_value="ladybug-standard"),
    ):
      result = await tool.execute({"text": "should be rejected"})

    assert result["error"] == "limit_reached"


class TestRecallTextTool:
  def test_tool_definition(self, mock_graph_client):
    tool = RecallTextTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "recall-text"
    assert "query" in defn["inputSchema"]["properties"]
    assert "query" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_rejects_empty_query(self, mock_graph_client):
    tool = RecallTextTool(mock_graph_client)
    result = await tool.execute({"query": ""})
    assert result["error"] == "invalid_input"

  @pytest.mark.asyncio
  async def test_successful_recall(self, mock_graph_client):
    tool = RecallTextTool(mock_graph_client)

    mock_service = MagicMock()
    mock_service.client.knn_search.return_value = {
      "hits": {
        "total": {"value": 1},
        "hits": [
          {
            "_id": "mem_kgtest123_memory_12345",
            "_score": 0.95,
            "_source": {
              "content": "NVDA supply chain risk",
              "category": "analysis",
              "tags": ["nvidia"],
              "indexed_at": "2026-03-20T00:00:00",
            },
          }
        ],
      }
    }

    mock_embedding_service = MagicMock()
    mock_embedding_service.embed_single.return_value = [0.1] * 384

    with (
      patch(
        "robosystems.operations.search.get_search_service",
        return_value=mock_service,
      ),
      patch(
        "robosystems.operations.search.embeddings.get_embedding_service",
        return_value=mock_embedding_service,
      ),
    ):
      result = await tool.execute({"query": "NVIDIA supply chain"})

    assert result["total"] == 1
    assert len(result["memories"]) == 1
    assert result["memories"][0]["text"] == "NVDA supply chain risk"
    assert result["memories"][0]["score"] == 0.95

  @pytest.mark.asyncio
  async def test_recall_with_category_filter(self, mock_graph_client):
    tool = RecallTextTool(mock_graph_client)

    mock_service = MagicMock()
    mock_service.client.knn_search.return_value = {
      "hits": {"total": {"value": 0}, "hits": []}
    }

    mock_embedding_service = MagicMock()
    mock_embedding_service.embed_single.return_value = [0.1] * 384

    with (
      patch(
        "robosystems.operations.search.get_search_service",
        return_value=mock_service,
      ),
      patch(
        "robosystems.operations.search.embeddings.get_embedding_service",
        return_value=mock_embedding_service,
      ),
    ):
      await tool.execute({"query": "test", "category": "observation", "size": 5})

    # Verify category was passed as filter
    call_kwargs = mock_service.client.knn_search.call_args
    filters = call_kwargs.kwargs.get("filters") or call_kwargs[1].get("filters")
    assert filters["category"] == "observation"
    assert filters["source_type"] == "memory"

  @pytest.mark.asyncio
  async def test_recall_service_unavailable(self, mock_graph_client):
    tool = RecallTextTool(mock_graph_client)

    with patch(
      "robosystems.operations.search.get_search_service",
      return_value=None,
    ):
      result = await tool.execute({"query": "test"})

    assert result["error"] == "unavailable"
