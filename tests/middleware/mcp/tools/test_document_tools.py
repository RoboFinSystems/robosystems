"""Tests for document management MCP tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.document_tools import (
  CreateDocumentTool,
  GetDocumentTool,
  ListDocumentsTool,
  UpdateDocumentTool,
)

DOC_MODULE = "robosystems.middleware.mcp.tools.document_tools"
DOC_SVC = "robosystems.operations.document_service.DocumentService"


@pytest.fixture
def mock_graph_client():
  client = MagicMock()
  client.graph_id = "kgtest123"
  return client


@pytest.fixture
def mock_shared_client():
  """Client for a shared repository graph (e.g., SEC)."""
  client = MagicMock()
  client.graph_id = "sec"
  return client


class TestCreateDocumentTool:
  def test_tool_definition(self, mock_graph_client):
    tool = CreateDocumentTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "create-document"
    assert "title" in defn["inputSchema"]["required"]
    assert "content" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_creates_document(self, mock_graph_client):
    mock_doc = MagicMock()
    mock_doc.id = "doc_01ABC"
    mock_doc.title = "Depreciation Policy"
    mock_doc.folder = "policies"

    mock_response = MagicMock()
    mock_response.sections_indexed = 3

    mock_service = MagicMock()
    mock_service.create_document.return_value = (mock_doc, mock_response)

    mock_session = MagicMock()

    tool = CreateDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._resolve_tier", return_value="ladybug-standard"),
      patch(f"{DOC_MODULE}._resolve_graph_owner", return_value="user_123"),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute(
        {
          "title": "Depreciation Policy",
          "content": "# Depreciation\n\nStraight-line for all assets.",
          "folder": "policies",
          "tags": ["depreciation"],
        }
      )

    assert result["success"] is True
    assert result["document_id"] == "doc_01ABC"
    assert result["sections_indexed"] == 3

  @pytest.mark.asyncio
  async def test_blocks_shared_repository(self, mock_shared_client):
    tool = CreateDocumentTool(mock_shared_client)
    with patch(
      f"{DOC_MODULE}._block_shared_repository",
      return_value={"error": "not_allowed", "message": "shared repo"},
    ):
      result = await tool.execute(
        {
          "title": "Test",
          "content": "Content",
        }
      )

    assert result["error"] == "not_allowed"

  @pytest.mark.asyncio
  async def test_rejects_empty_content(self, mock_graph_client):
    tool = CreateDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
    ):
      result = await tool.execute(
        {
          "title": "Empty",
          "content": "",
        }
      )

    assert result["error"] == "invalid_input"

  @pytest.mark.asyncio
  async def test_rejects_oversized_content(self, mock_graph_client):
    tool = CreateDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
    ):
      result = await tool.execute(
        {
          "title": "Huge",
          "content": "x" * 500_001,
        }
      )

    assert result["error"] == "invalid_input"


class TestUpdateDocumentTool:
  def test_tool_definition(self, mock_graph_client):
    tool = UpdateDocumentTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "update-document"
    assert "document_id" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_updates_document(self, mock_graph_client):
    mock_doc = MagicMock()
    mock_doc.id = "doc_01ABC"
    mock_doc.title = "Updated Title"

    mock_response = MagicMock()
    mock_response.sections_indexed = 2

    mock_service = MagicMock()
    mock_service.update_document.return_value = (mock_doc, mock_response)

    mock_session = MagicMock()

    tool = UpdateDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute(
        {
          "document_id": "doc_01ABC",
          "title": "Updated Title",
        }
      )

    assert result["success"] is True
    assert result["title"] == "Updated Title"

  @pytest.mark.asyncio
  async def test_rejects_no_fields(self, mock_graph_client):
    tool = UpdateDocumentTool(mock_graph_client)
    mock_session = MagicMock()
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
    ):
      result = await tool.execute({"document_id": "doc_01ABC"})

    assert result["error"] == "invalid_input"

  @pytest.mark.asyncio
  async def test_returns_not_found(self, mock_graph_client):
    mock_service = MagicMock()
    mock_service.update_document.side_effect = KeyError("not found")

    mock_session = MagicMock()

    tool = UpdateDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute(
        {
          "document_id": "doc_missing",
          "content": "new content",
        }
      )

    assert result["error"] == "not_found"


class TestGetDocumentTool:
  def test_tool_definition(self, mock_graph_client):
    tool = GetDocumentTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-document"
    assert "document_id" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_returns_document(self, mock_graph_client):
    mock_doc = MagicMock()
    mock_doc.id = "doc_01ABC"
    mock_doc.title = "Depreciation Policy"
    mock_doc.content = "# Depreciation\n\nStraight-line."
    mock_doc.folder = "policies"
    mock_doc.tags = ["depreciation"]
    mock_doc.source_type = "uploaded_doc"
    mock_doc.sections_indexed = 2
    mock_doc.created_at = "2026-04-01"
    mock_doc.updated_at = "2026-04-01"

    mock_service = MagicMock()
    mock_service.get_document.return_value = mock_doc

    mock_session = MagicMock()

    tool = GetDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute({"document_id": "doc_01ABC"})

    assert result["document_id"] == "doc_01ABC"
    assert result["title"] == "Depreciation Policy"
    assert "# Depreciation" in result["content"]

  @pytest.mark.asyncio
  async def test_returns_not_found(self, mock_graph_client):
    mock_service = MagicMock()
    mock_service.get_document.return_value = None

    mock_session = MagicMock()

    tool = GetDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute({"document_id": "doc_missing"})

    assert result["error"] == "not_found"


class TestListDocumentsTool:
  def test_tool_definition(self, mock_graph_client):
    tool = ListDocumentsTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "list-documents"

  @pytest.mark.asyncio
  async def test_returns_documents(self, mock_graph_client):
    doc1 = MagicMock()
    doc1.id = "doc_01"
    doc1.title = "Policy A"
    doc1.folder = "policies"
    doc1.tags = []
    doc1.source_type = "uploaded_doc"
    doc1.sections_indexed = 2
    doc1.created_at = "2026-04-01"
    doc1.updated_at = "2026-04-01"

    doc2 = MagicMock()
    doc2.id = "doc_02"
    doc2.title = "Memory Note"
    doc2.folder = "memory"
    doc2.tags = []
    doc2.source_type = "uploaded_doc"
    doc2.sections_indexed = 1
    doc2.created_at = "2026-04-01"
    doc2.updated_at = "2026-04-01"

    mock_service = MagicMock()
    mock_service.list_documents.return_value = [doc1, doc2]

    mock_session = MagicMock()

    tool = ListDocumentsTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute({})

    assert result["total"] == 2

  @pytest.mark.asyncio
  async def test_filters_by_folder(self, mock_graph_client):
    doc1 = MagicMock()
    doc1.id = "doc_01"
    doc1.title = "Policy"
    doc1.folder = "policies"
    doc1.tags = []
    doc1.source_type = "uploaded_doc"
    doc1.sections_indexed = 2
    doc1.created_at = "2026-04-01"
    doc1.updated_at = "2026-04-01"

    doc2 = MagicMock()
    doc2.id = "doc_02"
    doc2.title = "Note"
    doc2.folder = "memory"
    doc2.tags = []
    doc2.source_type = "uploaded_doc"
    doc2.sections_indexed = 1
    doc2.created_at = "2026-04-01"
    doc2.updated_at = "2026-04-01"

    mock_service = MagicMock()
    mock_service.list_documents.return_value = [doc1, doc2]

    mock_session = MagicMock()

    tool = ListDocumentsTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute({"folder": "policies"})

    assert result["total"] == 1
    assert result["documents"][0]["title"] == "Policy"
