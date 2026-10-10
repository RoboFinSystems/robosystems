"""Tests for document management MCP tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.document_tools import (
  CompleteDocumentUploadTool,
  CreateDocumentTool,
  CreateDocumentUploadTool,
  DeleteDocumentTool,
  GetDocumentTool,
  ListDocumentsTool,
  ReadDocumentFileTool,
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


class TestDeleteDocumentTool:
  def test_tool_definition(self, mock_graph_client):
    tool = DeleteDocumentTool(mock_graph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "delete-document"
    assert "document_id" in defn["inputSchema"]["required"]

  @pytest.mark.asyncio
  async def test_deletes_document(self, mock_graph_client):
    mock_service = MagicMock()
    mock_service.delete_document.return_value = True

    mock_session = MagicMock()

    tool = DeleteDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute({"document_id": "doc_01ABC"})

    assert result["success"] is True
    assert result["document_id"] == "doc_01ABC"
    mock_service.delete_document.assert_called_once_with("kgtest123", "doc_01ABC")

  @pytest.mark.asyncio
  async def test_returns_not_found(self, mock_graph_client):
    mock_service = MagicMock()
    mock_service.delete_document.return_value = False

    mock_session = MagicMock()

    tool = DeleteDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute({"document_id": "doc_missing"})

    assert result["error"] == "not_found"

  @pytest.mark.asyncio
  async def test_blocks_shared_repository(self, mock_shared_client):
    tool = DeleteDocumentTool(mock_shared_client)
    with patch(
      f"{DOC_MODULE}._block_shared_repository",
      return_value={"error": "not_allowed", "message": "shared repo"},
    ):
      result = await tool.execute({"document_id": "doc_01ABC"})

    assert result["error"] == "not_allowed"

  @pytest.mark.asyncio
  async def test_requires_write_access(self, mock_graph_client):
    tool = DeleteDocumentTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(
        f"{DOC_MODULE}._check_graph_access",
        return_value={"error": "access_denied", "message": "read-only"},
      ),
    ):
      result = await tool.execute({"document_id": "doc_01ABC"})

    assert result["error"] == "access_denied"


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
    """Folder filter is threaded into DocumentService.list_documents so the
    SQL query does the filtering — not Python post-processing."""
    doc1 = MagicMock()
    doc1.id = "doc_01"
    doc1.title = "Policy"
    doc1.folder = "policies"
    doc1.tags = []
    doc1.source_type = "uploaded_doc"
    doc1.sections_indexed = 2
    doc1.created_at = "2026-04-01"
    doc1.updated_at = "2026-04-01"

    mock_service = MagicMock()
    mock_service.list_documents.return_value = [doc1]

    mock_session = MagicMock()

    tool = ListDocumentsTool(mock_graph_client)
    with (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=mock_session),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    ):
      result = await tool.execute({"folder": "policies"})

    # Contract: MCP tool forwards the folder arg to DocumentService.
    mock_service.list_documents.assert_called_once()
    call_args = mock_service.list_documents.call_args
    # folder is the third positional arg (graph_id, source_type, folder).
    assert (
      call_args.args[2] == "policies" or call_args.kwargs.get("folder") == "policies"
    )
    assert result["total"] == 1
    assert result["documents"][0]["title"] == "Policy"


class TestDocumentFiles:
  """A stored file (a statement PDF) reads as its identity, never its bytes,
  and cannot be deleted while a recorded balance cites it."""

  def _patches(self, mock_service):
    return (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=MagicMock()),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    )

  @pytest.mark.asyncio
  async def test_get_document_names_the_stored_file(self, mock_graph_client):
    doc = MagicMock()
    doc.is_file = True
    doc.file_name = "sep-2026.pdf"
    doc.file_content_type = "application/pdf"
    doc.file_size_bytes = 2048
    doc.file_sha256 = "ef" * 32
    mock_service = MagicMock()
    mock_service.get_document.return_value = doc

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await GetDocumentTool(mock_graph_client).execute(
        {"document_id": "doc_1"}
      )

    assert result["file"] == {
      "file_name": "sep-2026.pdf",
      "content_type": "application/pdf",
      "size_bytes": 2048,
      "sha256": "ef" * 32,
    }

  @pytest.mark.asyncio
  async def test_a_text_document_has_no_file(self, mock_graph_client):
    doc = MagicMock()
    doc.is_file = False
    mock_service = MagicMock()
    mock_service.list_documents.return_value = [doc]

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await ListDocumentsTool(mock_graph_client).execute({})

    assert result["documents"][0]["file"] is None

  @pytest.mark.asyncio
  async def test_delete_is_refused_while_a_balance_cites_it(self, mock_graph_client):
    from robosystems.operations.document_service import DocumentInUseError

    mock_service = MagicMock()
    mock_service.delete_document.side_effect = DocumentInUseError("cited")

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await DeleteDocumentTool(mock_graph_client).execute(
        {"document_id": "doc_1"}
      )

    assert result == {"error": "in_use", "message": "cited"}

  @pytest.mark.asyncio
  async def test_editing_a_stored_files_content_is_refused(self, mock_graph_client):
    from robosystems.operations.document_service import DocumentFileError

    mock_service = MagicMock()
    mock_service.update_document.side_effect = DocumentFileError("cannot be edited")

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await UpdateDocumentTool(mock_graph_client).execute(
        {"document_id": "doc_1", "content": "# replaced"}
      )

    assert result == {"error": "invalid_input", "message": "cannot be edited"}


class TestDocumentFileTools:
  """An agent with a shell uploads the bytes itself, with the command the
  first tool hands back; a stored statement then reads as text."""

  def _patches(self, mock_service):
    return (
      patch(f"{DOC_MODULE}._get_platform_session", return_value=MagicMock()),
      patch(f"{DOC_MODULE}._block_shared_repository", return_value=None),
      patch(f"{DOC_MODULE}._check_graph_access", return_value=None),
      patch(DOC_SVC, return_value=mock_service),
    )

  @pytest.mark.asyncio
  async def test_create_upload_hands_back_the_command_to_run(self, mock_graph_client):
    mock_service = MagicMock()
    mock_service.begin_file_upload.return_value = (
      "upl_01M4HXRVFZ66AF7CYHHF85AFRR",
      "https://s3.example/put?X-Amz-Signature=abc&b=c",
    )

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await CreateDocumentUploadTool(mock_graph_client).execute(
        {"file_name": "sep.pdf", "file_path": "/Users/me/Bank Statements/sep.pdf"}
      )

    assert result["upload_id"] == "upl_01M4HXRVFZ66AF7CYHHF85AFRR"
    # Shell-quoted: a space in the path and the URL's & survive the shell.
    assert result["upload_command"] == (
      "curl -sSf -X PUT -H 'Content-Type: application/pdf' "
      "--data-binary @'/Users/me/Bank Statements/sep.pdf' "
      "'https://s3.example/put?X-Amz-Signature=abc&b=c'"
    )

  @pytest.mark.asyncio
  async def test_create_upload_without_a_path_leaves_a_placeholder(
    self, mock_graph_client
  ):
    mock_service = MagicMock()
    mock_service.begin_file_upload.return_value = ("upl_x", "https://s3.example/put")

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await CreateDocumentUploadTool(mock_graph_client).execute(
        {"file_name": "sep.pdf"}
      )

    assert "@<path-to-file>" in result["upload_command"]

  @pytest.mark.asyncio
  async def test_create_upload_takes_the_type_from_the_name(self, mock_graph_client):
    mock_service = MagicMock()
    mock_service.begin_file_upload.return_value = ("upl_x", "https://s3.example/put")

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await CreateDocumentUploadTool(mock_graph_client).execute(
        {"file_name": "receipt.jpeg"}
      )

    (_graph, request), _ = mock_service.begin_file_upload.call_args
    assert request.content_type == "image/jpeg"
    assert "Content-Type: image/jpeg" in result["upload_command"]

  @pytest.mark.asyncio
  async def test_create_upload_refuses_a_bad_name(self, mock_graph_client):

    mock_service = MagicMock()

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await CreateDocumentUploadTool(mock_graph_client).execute(
        {"file_name": "sep.txt"}
      )

    assert result["error"] == "invalid_input"
    assert "not a file that can be stored" in result["message"]
    mock_service.begin_file_upload.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.parametrize(
    ("raised", "code"),
    [("not-uploaded", "not_uploaded"), ("bad-file", "invalid_file")],
  )
  async def test_complete_upload_reports_why_it_did_not_store(
    self, mock_graph_client, raised, code
  ):
    from robosystems.operations.document_service import (
      DocumentFileError,
      DocumentFileNotUploadedError,
    )

    mock_service = MagicMock()
    mock_service.complete_file_upload.side_effect = {
      "not-uploaded": DocumentFileNotUploadedError("nothing yet"),
      "bad-file": DocumentFileError("not a pdf"),
    }[raised]

    p1, p2, p3, p4 = self._patches(mock_service)
    with (
      p1,
      p2,
      p3,
      p4,
      patch(f"{DOC_MODULE}._resolve_acting_user", return_value="usr_1"),
    ):
      result = await CompleteDocumentUploadTool(mock_graph_client).execute(
        {"upload_id": "upl_01M4HXRVFZ66AF7CYHHF85AFRR", "title": "Sept"}
      )

    assert result["error"] == code

  @pytest.mark.asyncio
  async def test_complete_upload_refuses_an_id_it_did_not_issue(
    self, mock_graph_client
  ):
    mock_service = MagicMock()
    p1, p2, p3, p4 = self._patches(mock_service)
    with (
      p1,
      p2,
      p3,
      p4,
      patch(f"{DOC_MODULE}._resolve_acting_user", return_value="usr_1"),
    ):
      result = await CompleteDocumentUploadTool(mock_graph_client).execute(
        {"upload_id": "../kgOther/upl_x", "title": "Sept"}
      )

    assert result["error"] == "invalid_input"
    mock_service.complete_file_upload.assert_not_called()

  @pytest.mark.asyncio
  async def test_read_returns_pages_and_says_when_more_remain(self, mock_graph_client):
    from robosystems.operations.document_service import FileText

    doc = MagicMock()
    doc.title = "Sept"
    doc.file_name = "sep.pdf"
    mock_service = MagicMock()
    mock_service.read_file_text.return_value = FileText(
      document=doc, page_count=4, pages=[(1, "Ending balance 3,204.88"), (2, "Lines")]
    )

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await ReadDocumentFileTool(mock_graph_client).execute(
        {"document_id": "doc_1", "max_pages": 50}
      )

    # The page cap holds whatever was asked.
    assert mock_service.read_file_text.call_args.kwargs == {
      "first_page": 1,
      "max_pages": 10,
    }
    assert result["page_count"] == 4
    assert result["pages"][0] == {"page": 1, "text": "Ending balance 3,204.88"}
    assert result["more_pages"] is True

  @pytest.mark.asyncio
  async def test_read_of_a_text_document_points_elsewhere(self, mock_graph_client):
    from robosystems.operations.document_service import DocumentFileError

    mock_service = MagicMock()
    mock_service.read_file_text.side_effect = DocumentFileError("has no stored file")

    p1, p2, p3, p4 = self._patches(mock_service)
    with p1, p2, p3, p4:
      result = await ReadDocumentFileTool(mock_graph_client).execute(
        {"document_id": "doc_1"}
      )

    assert result["error"] == "not_a_file"
