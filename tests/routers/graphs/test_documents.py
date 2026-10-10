"""Tests for the documents router.

Covers: list + get endpoints (writes moved to content-ops).
All tests mock the DocumentService and SessionFactory.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.core.document import Document
from robosystems.routers.graphs.documents import (
  get_document,
  get_document_file,
  list_documents,
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
    "is_file": False,
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


@pytest.mark.unit
class TestListDocuments:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_lists_documents(self, mock_block, mock_enforce, mock_sf):
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
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_filters_by_source_type(self, mock_block, mock_enforce, mock_sf):
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
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_blocks_shared_repository(self, mock_block, mock_enforce):
    mock_block.side_effect = HTTPException(403, "not allowed")
    with pytest.raises(HTTPException) as exc_info:
      await list_documents(graph_id="sec", current_user=_mock_user())
    assert exc_info.value.status_code == 403


@pytest.mark.unit
class TestGetDocument:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_returns_document_detail(self, mock_block, mock_enforce, mock_sf):
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
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_returns_404_when_not_found(self, mock_block, mock_enforce, mock_sf):
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


def _mock_file(**overrides):
  return _mock_document(
    **{
      "content": "",
      "source_type": "uploaded_file",
      "sections_indexed": 0,
      "is_file": True,
      "file_name": "sep-2026.pdf",
      "file_content_type": "application/pdf",
      "file_size_bytes": 1024,
      "file_sha256": "ab" * 32,
      "file_status": "stored",
      **overrides,
    }
  )


@pytest.mark.unit
class TestDocumentFiles:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_a_file_document_reads_with_its_file(
    self, mock_block, mock_enforce, mock_sf
  ):
    mock_sf.return_value = MagicMock()
    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.get_document.return_value = _mock_file()
      MockService.return_value.list_documents.return_value = [
        _mock_document(),
        _mock_file(file_status="pending", file_sha256=None),
      ]
      detail = await get_document(
        graph_id="kg_test", document_id="doc_abc123", current_user=_mock_user()
      )
      listed = await list_documents(graph_id="kg_test", current_user=_mock_user())

    assert detail.file is not None
    assert (detail.file.file_name, detail.file.status, detail.file.size_bytes) == (
      "sep-2026.pdf",
      "stored",
      1024,
    )
    text_doc, pending = listed.documents
    assert text_doc.file is None
    # The declared size is not reported until the bytes are checked.
    assert pending.file is not None
    assert (pending.file.status, pending.file.size_bytes) == ("pending", None)

  @pytest.mark.asyncio
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_download_returns_a_short_lived_link(
    self, mock_block, mock_enforce, mock_sf
  ):
    mock_sf.return_value = MagicMock()
    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.file_download_url.return_value = (
        _mock_file(),
        "https://s3.example/signed",
      )
      result = await get_document_file(
        graph_id="kg_test", document_id="doc_abc123", current_user=_mock_user()
      )

    assert result.download_url == "https://s3.example/signed"
    assert result.expires_in == 300
    assert result.file.sha256 == "ab" * 32

  @pytest.mark.asyncio
  @pytest.mark.parametrize(
    "error",
    [KeyError("doc_abc123"), "no-file"],
    ids=["missing", "not-a-stored-file"],
  )
  @patch(f"{MODULE}.SessionFactory")
  @patch(f"{MODULE}._enforce_graph_access")
  @patch(f"{MODULE}._block_shared_repository")
  async def test_download_of_no_stored_file_is_404(
    self, mock_block, mock_enforce, mock_sf, error
  ):
    from robosystems.operations.document_service import DocumentFileError

    mock_sf.return_value = MagicMock()
    raised = DocumentFileError("no stored file") if error == "no-file" else error
    with patch(f"{MODULE}.DocumentService") as MockService:
      MockService.return_value.file_download_url.side_effect = raised
      with pytest.raises(HTTPException) as exc_info:
        await get_document_file(
          graph_id="kg_test", document_id="doc_abc123", current_user=_mock_user()
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
