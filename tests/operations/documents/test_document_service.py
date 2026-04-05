"""Tests for DocumentService — PG CRUD with OpenSearch sync."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.search import (
  DocumentUploadRequest,
  DocumentUploadResponse,
)
from robosystems.models.core.document import Document
from robosystems.operations.documents.service import DocumentService

MODULE = "robosystems.operations.documents.service"
NOW = datetime(2026, 4, 1, tzinfo=UTC)


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
    "sections_indexed": 0,
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
    "id": "",
    "document_id": "doc_kg_test_hash",
    "sections_indexed": 2,
    "total_content_length": 100,
    "section_ids": ["doc_kg_test_hash_0", "doc_kg_test_hash_1"],
  }
  defaults.update(overrides)
  return DocumentUploadResponse(**defaults)


@pytest.mark.unit
class TestCreateDocument:
  @patch("robosystems.operations.search.get_search_service")
  @patch.object(Document, "create")
  def test_creates_in_pg_and_syncs(self, mock_create, mock_search):
    session = MagicMock()
    doc = _mock_document()
    mock_create.return_value = doc

    search_service = MagicMock()
    search_service.upload_document.return_value = _mock_upload_response()
    mock_search.return_value = search_service

    service = DocumentService(session)
    request = DocumentUploadRequest(title="Test Doc", content="# Hello\n\nWorld")
    result_doc, result_response = service.create_document("kg_test", "usr_1", request)

    assert result_doc is doc
    assert result_response.sections_indexed == 2
    assert result_response.id == "doc_abc123"
    mock_create.assert_called_once()
    doc.update.assert_called()

  @patch("robosystems.operations.search.get_search_service")
  @patch.object(Document, "create")
  def test_skips_opensearch_when_unavailable(self, mock_create, mock_search):
    session = MagicMock()
    doc = _mock_document()
    mock_create.return_value = doc
    mock_search.return_value = None

    service = DocumentService(session)
    request = DocumentUploadRequest(title="Test", content="content")
    result_doc, result_response = service.create_document("kg_test", "usr_1", request)

    assert result_doc is doc
    assert result_response.sections_indexed == 0

  @patch("robosystems.operations.search.get_search_service")
  @patch.object(Document, "get_by_external_id")
  @patch.object(Document, "create")
  def test_upserts_on_external_id(self, mock_create, mock_get_ext, mock_search):
    session = MagicMock()
    existing_doc = _mock_document(external_id="ext_1")
    mock_get_ext.return_value = existing_doc

    search_service = MagicMock()
    search_service.upload_document.return_value = _mock_upload_response()
    mock_search.return_value = search_service

    service = DocumentService(session)
    request = DocumentUploadRequest(
      title="Updated", content="new content", external_id="ext_1"
    )

    with patch.object(Document, "get_by_id_and_graph", return_value=existing_doc):
      _doc, _ = service.create_document("kg_test", "usr_1", request)

    # Should NOT call create since it upserts
    mock_create.assert_not_called()
    existing_doc.update.assert_called()

  @patch("robosystems.config.billing.core.get_tier_max_documents", return_value=5)
  @patch.object(Document, "count_by_graph", return_value=5)
  def test_raises_on_tier_limit(self, mock_count, mock_max):
    session = MagicMock()
    service = DocumentService(session)
    request = DocumentUploadRequest(title="Test", content="content")

    with pytest.raises(ValueError, match="Document limit reached"):
      service.create_document("kg_test", "usr_1", request, tier="ladybug-standard")


@pytest.mark.unit
class TestGetDocument:
  @patch.object(Document, "get_by_id_and_graph")
  def test_returns_document(self, mock_get):
    session = MagicMock()
    doc = _mock_document()
    mock_get.return_value = doc

    service = DocumentService(session)
    result = service.get_document("kg_test", "doc_abc123")
    assert result is doc

  @patch.object(Document, "get_by_id_and_graph", return_value=None)
  def test_returns_none_when_not_found(self, mock_get):
    session = MagicMock()
    service = DocumentService(session)
    result = service.get_document("kg_test", "doc_missing")
    assert result is None


@pytest.mark.unit
class TestListDocuments:
  @patch.object(Document, "get_by_graph")
  def test_lists_documents(self, mock_get):
    session = MagicMock()
    docs = [_mock_document(), _mock_document(id="doc_2")]
    mock_get.return_value = docs

    service = DocumentService(session)
    result = service.list_documents("kg_test")
    assert len(result) == 2

  @patch.object(Document, "get_by_graph")
  def test_filters_by_source_type(self, mock_get):
    session = MagicMock()
    mock_get.return_value = []

    service = DocumentService(session)
    service.list_documents("kg_test", source_type="memory")
    mock_get.assert_called_once_with("kg_test", session, "memory")


@pytest.mark.unit
class TestUpdateDocument:
  @patch("robosystems.operations.search.get_search_service")
  @patch.object(Document, "get_by_id_and_graph")
  def test_updates_and_resyncs(self, mock_get, mock_search):
    session = MagicMock()
    doc = _mock_document()
    mock_get.return_value = doc

    search_service = MagicMock()
    search_service.upload_document.return_value = _mock_upload_response()
    mock_search.return_value = search_service

    service = DocumentService(session)
    result_doc, response = service.update_document(
      "kg_test", "doc_abc123", title="Updated Title"
    )

    assert result_doc is doc
    doc.update.assert_called()

  @patch.object(Document, "get_by_id_and_graph", return_value=None)
  def test_raises_on_not_found(self, mock_get):
    session = MagicMock()
    service = DocumentService(session)

    with pytest.raises(KeyError, match="not found"):
      service.update_document("kg_test", "doc_missing", title="New")


@pytest.mark.unit
class TestDeleteDocument:
  @patch("robosystems.operations.search.get_search_service")
  @patch.object(Document, "get_by_id_and_graph")
  def test_deletes_from_pg_and_opensearch(self, mock_get, mock_search):
    session = MagicMock()
    doc = _mock_document()
    mock_get.return_value = doc
    mock_search.return_value = MagicMock()

    service = DocumentService(session)
    result = service.delete_document("kg_test", "doc_abc123")

    assert result is True
    doc.delete.assert_called_once_with(session)
    mock_search.return_value.delete_document.assert_called_once()

  @patch.object(Document, "get_by_id_and_graph", return_value=None)
  def test_returns_false_when_not_found(self, mock_get):
    session = MagicMock()
    service = DocumentService(session)
    result = service.delete_document("kg_test", "doc_missing")
    assert result is False


@pytest.mark.unit
class TestCountDocuments:
  @patch.object(Document, "count_by_graph", return_value=7)
  def test_counts_documents(self, mock_count):
    session = MagicMock()
    service = DocumentService(session)
    assert service.count_documents("kg_test") == 7
