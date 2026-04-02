"""Tests for the documents router.

Covers: list, get, upload, bulk upload, update, delete endpoints.
All tests mock the DocumentService and SessionFactory.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.search import (
  DocumentUploadRequest,
  DocumentUploadResponse,
)
from robosystems.models.core.document import Document
from robosystems.routers.graphs.documents import (
  delete_document,
  get_document,
  list_documents,
  update_document,
  upload_document,
)

MODULE = "robosystems.routers.graphs.documents"

NOW = datetime(2026, 4, 1, tzinfo=UTC)


def _mock_user():
  user = MagicMock()
  user.id = "usr_1"
  return user


def _mock_document(**overrides):
  defaults = {
    "id": "doc_abc123",
    "graph_id": "kg_test",
    "user_id": "usr_1",
    "title": "Test Doc",
    "content": "# Hello\n\nWorld",
    "tags": ["tag1"],
    "folder": "reports",
    "external_id": None,
    "source_type": "uploaded_doc",
    "source_provider": None,
    "sections_indexed": 2,
    "created_at": NOW,
    "updated_at": NOW,
  }
  defaults.update(overrides)
  doc = MagicMock(spec=Document)
  for k, v in defaults.items():
    setattr(doc, k, v)
  return doc


def _mock_upload_response(**overrides):
  defaults = {
    "id": "doc_abc123",
    "document_id": "doc_kg_test_doc_abc123",
    "sections_indexed": 2,
    "total_content_length": 100,
    "section_ids": ["doc_kg_test_doc_abc123_0", "doc_kg_test_doc_abc123_1"],
  }
  defaults.update(overrides)
  return DocumentUploadResponse(**defaults)


@pytest.mark.unit
class TestListDocuments:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_lists_documents(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session
    docs = [_mock_document(), _mock_document(id="doc_def456", title="Doc 2")]

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.list_documents.return_value = docs
      result = await list_documents(graph_id="kg_test", current_user=_mock_user())

    assert result.total == 2
    assert len(result.documents) == 2
    assert result.graph_id == "kg_test"
    assert result.documents[0].id == "doc_abc123"
    session.close.assert_called_once()

  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_filters_by_source_type(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.list_documents.return_value = []
      result = await list_documents(
        graph_id="kg_test",
        source_type="memory",
        current_user=_mock_user(),
      )

    assert result.total == 0
    MockService.return_value.list_documents.assert_called_once_with("kg_test", "memory")

  @pytest.mark.asyncio
  @patch(f"{MODULE}._block_shared_repository")
  async def test_blocks_shared_repository(self, mock_block):
    mock_block.side_effect = HTTPException(403, "not allowed")
    with pytest.raises(HTTPException) as exc_info:
      await list_documents(graph_id="sec", current_user=_mock_user())
    assert exc_info.value.status_code == 403


@pytest.mark.unit
class TestGetDocument:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_returns_document_detail(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session
    doc = _mock_document()

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.get_document.return_value = doc
      result = await get_document(
        graph_id="kg_test",
        document_id="doc_abc123",
        current_user=_mock_user(),
      )

    assert result.id == "doc_abc123"
    assert result.title == "Test Doc"
    assert result.content == "# Hello\n\nWorld"
    session.close.assert_called_once()

  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_returns_404_when_not_found(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.get_document.return_value = None
      with pytest.raises(HTTPException) as exc_info:
        await get_document(
          graph_id="kg_test",
          document_id="doc_missing",
          current_user=_mock_user(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.unit
class TestUploadDocument:
  @pytest.mark.asyncio
  @patch(f"{MODULE}._resolve_tier", return_value="ladybug-standard")
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_uploads_document(self, mock_block, mock_sf, mock_tier):
    session = MagicMock()
    mock_sf.return_value = session
    doc = _mock_document()
    response = _mock_upload_response()

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.create_document.return_value = (doc, response)
      result = await upload_document(
        graph_id="kg_test",
        request=DocumentUploadRequest(title="Test", content="# Hello\n\nContent"),
        current_user=_mock_user(),
      )

    assert result.id == "doc_abc123"
    assert result.sections_indexed == 2
    session.close.assert_called_once()

  @pytest.mark.asyncio
  @patch(f"{MODULE}._resolve_tier", return_value="ladybug-standard")
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_returns_422_on_value_error(self, mock_block, mock_sf, mock_tier):
    session = MagicMock()
    mock_sf.return_value = session

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.create_document.side_effect = ValueError(
        "Document limit reached"
      )
      with pytest.raises(HTTPException) as exc_info:
        await upload_document(
          graph_id="kg_test",
          request=DocumentUploadRequest(title="Test", content="content"),
          current_user=_mock_user(),
        )

    assert exc_info.value.status_code == 422


@pytest.mark.unit
class TestUpdateDocument:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_updates_document(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session
    doc = _mock_document(title="Updated")
    response = _mock_upload_response()

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.update_document.return_value = (doc, response)

      from robosystems.models.api.search import DocumentUpdateRequest

      result = await update_document(
        graph_id="kg_test",
        document_id="doc_abc123",
        request=DocumentUpdateRequest(title="Updated"),
        current_user=_mock_user(),
      )

    assert result.id == "doc_abc123"
    session.close.assert_called_once()

  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_returns_404_when_not_found(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.update_document.side_effect = ValueError(
        "Document not found"
      )

      from robosystems.models.api.search import DocumentUpdateRequest

      with pytest.raises(HTTPException) as exc_info:
        await update_document(
          graph_id="kg_test",
          document_id="doc_missing",
          request=DocumentUpdateRequest(title="New"),
          current_user=_mock_user(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.unit
class TestDeleteDocument:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_deletes_document(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.delete_document.return_value = True
      result = await delete_document(
        graph_id="kg_test",
        document_id="doc_abc123",
        current_user=_mock_user(),
      )

    assert result is None
    session.close.assert_called_once()

  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_returns_404_when_not_found(self, mock_block, mock_sf):
    session = MagicMock()
    mock_sf.return_value = session

    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.delete_document.return_value = False
      with pytest.raises(HTTPException) as exc_info:
        await delete_document(
          graph_id="kg_test",
          document_id="doc_missing",
          current_user=_mock_user(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.unit
class TestBlockSharedRepository:
  @patch(
    f"{MODULE}.is_shared_repository_or_subgraph",
    create=True,
  )
  def test_raises_403_for_shared_repo(self, mock_is_shared):
    from robosystems.routers.graphs.documents import _block_shared_repository

    with patch(
      "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
      return_value=True,
    ):
      with pytest.raises(HTTPException) as exc_info:
        _block_shared_repository("sec")
      assert exc_info.value.status_code == 403

  def test_allows_user_graph(self):
    from robosystems.routers.graphs.documents import _block_shared_repository

    with patch(
      "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
      return_value=False,
    ):
      _block_shared_repository("kg_test")  # Should not raise
