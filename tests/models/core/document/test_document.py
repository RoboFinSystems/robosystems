# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportOperatorIssue=false, reportOptionalMemberAccess=false
"""Tests for Document model."""

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.core.document import Document


@pytest.mark.unit
class TestDocumentCreate:
  def test_creates_with_all_fields(self):
    session = MagicMock()
    doc = Document.create(
      graph_id="kg_test",
      user_id="usr_1",
      title="Test Document",
      content="# Hello\n\nWorld",
      session=session,
      tags=["finance", "q4"],
      folder="reports",
      external_id="ext_123",
      source_type="uploaded_doc",
      source_provider=None,
      sections_indexed=3,
    )

    assert doc.graph_id == "kg_test"
    assert doc.user_id == "usr_1"
    assert doc.title == "Test Document"
    assert doc.content == "# Hello\n\nWorld"
    assert doc.tags == ["finance", "q4"]
    assert doc.folder == "reports"
    assert doc.external_id == "ext_123"
    assert doc.source_type == "uploaded_doc"
    assert doc.sections_indexed == 3
    session.add.assert_called_once_with(doc)
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(doc)

  def test_creates_with_minimal_fields(self):
    session = MagicMock()
    doc = Document.create(
      graph_id="kg_test",
      user_id="usr_1",
      title="Minimal",
      content="Some content",
      session=session,
    )

    assert doc.graph_id == "kg_test"
    assert doc.title == "Minimal"
    assert doc.content == "Some content"
    assert doc.tags is None
    assert doc.folder is None
    assert doc.external_id is None
    assert doc.source_type == "uploaded_doc"
    assert doc.source_provider is None
    assert doc.sections_indexed == 0

  def test_rollback_on_sqlalchemy_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("db error")

    with pytest.raises(SQLAlchemyError):
      Document.create(
        graph_id="kg_test",
        user_id="usr_1",
        title="Fail",
        content="content",
        session=session,
      )

    session.rollback.assert_called_once()


@pytest.mark.unit
class TestDocumentGetByIdAndGraph:
  def test_returns_document_with_graph_match(self):
    session = MagicMock()
    mock_doc = MagicMock(spec=Document)
    session.query.return_value.filter.return_value.first.return_value = mock_doc

    result = Document.get_by_id_and_graph("doc_123", "kg_test", session)
    assert result is mock_doc

  def test_returns_none_on_graph_mismatch(self):
    session = MagicMock()
    session.query.return_value.filter.return_value.first.return_value = None

    result = Document.get_by_id_and_graph("doc_123", "kg_wrong", session)
    assert result is None


@pytest.mark.unit
class TestDocumentGetByExternalId:
  def test_returns_document(self):
    session = MagicMock()
    mock_doc = MagicMock(spec=Document)
    session.query.return_value.filter.return_value.first.return_value = mock_doc

    result = Document.get_by_external_id("kg_test", "ext_123", session)
    assert result is mock_doc


@pytest.mark.unit
class TestDocumentGetByGraph:
  def test_returns_all_for_graph(self):
    session = MagicMock()
    mock_docs = [MagicMock(spec=Document), MagicMock(spec=Document)]
    query = session.query.return_value
    query.filter.return_value.order_by.return_value.all.return_value = mock_docs

    result = Document.get_by_graph("kg_test", session)
    assert len(result) == 2

  def test_filters_by_source_type(self):
    session = MagicMock()
    query = session.query.return_value
    filtered = query.filter.return_value
    filtered.filter.return_value.order_by.return_value.all.return_value = []

    result = Document.get_by_graph("kg_test", session, source_type="memory")
    assert result == []


@pytest.mark.unit
class TestDocumentCountByGraph:
  def test_counts_all_for_graph(self):
    session = MagicMock()
    session.query.return_value.filter.return_value.count.return_value = 5

    result = Document.count_by_graph("kg_test", session)
    assert result == 5


@pytest.mark.unit
class TestDocumentUpdate:
  def test_updates_title(self):
    session = MagicMock()
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="Old",
      content="content",
    )

    doc.update(session, title="New Title")
    assert doc.title == "New Title"
    session.commit.assert_called_once()

  def test_updates_content(self):
    session = MagicMock()
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="Title",
      content="old content",
    )

    doc.update(session, content="new content")
    assert doc.content == "new content"

  def test_sets_tags_to_none(self):
    session = MagicMock()
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="Title",
      content="content",
      tags=["old"],
    )

    doc.update(session, tags=None)
    assert doc.tags is None

  def test_does_not_change_tags_when_sentinel(self):
    session = MagicMock()
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="Title",
      content="content",
      tags=["keep"],
    )

    # tags=... (sentinel) means "don't change"
    doc.update(session, title="New")
    assert doc.tags == ["keep"]

  def test_rollback_on_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("fail")
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="Title",
      content="content",
    )

    with pytest.raises(SQLAlchemyError):
      doc.update(session, title="Fail")
    session.rollback.assert_called_once()


@pytest.mark.unit
class TestDocumentDelete:
  def test_deletes_document(self):
    session = MagicMock()
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="Title",
      content="content",
    )

    doc.delete(session)
    session.delete.assert_called_once_with(doc)
    session.commit.assert_called_once()

  def test_rollback_on_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("fail")
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="Title",
      content="content",
    )

    with pytest.raises(SQLAlchemyError):
      doc.delete(session)
    session.rollback.assert_called_once()


@pytest.mark.unit
class TestDocumentRepr:
  def test_repr(self):
    doc = Document(
      id="doc_123",
      graph_id="kg_test",
      user_id="usr_1",
      title="My Doc",
      content="content",
    )
    assert "doc_123" in repr(doc)
    assert "My Doc" in repr(doc)
    assert "kg_test" in repr(doc)
