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
