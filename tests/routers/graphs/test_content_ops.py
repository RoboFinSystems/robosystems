"""Tests for the document content-ops (index-document / delete-document).

These moved off the bespoke `/documents` write routes onto the operation
envelope; they mirror the memory remember/forget op shape.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.operations import DeleteDocumentOp, IndexDocumentOp
from robosystems.models.api.search import DocumentUploadResponse
from robosystems.routers.graphs.operations import delete_document_op, index_document_op

MODULE = "robosystems.routers.graphs.operations"


def _user():
  u = MagicMock()
  u.id = "usr_1"
  return u


def _upload_response():
  return DocumentUploadResponse(
    id="doc_1",
    document_id="udoc_1",
    sections_indexed=1,
    total_content_length=10,
    section_ids=["udoc_1_0"],
  )


def _guards():
  return (
    patch(f"{MODULE}._require_search_enabled"),
    patch(f"{MODULE}._block_shared_repo"),
    patch(f"{MODULE}._require_graph_write_access"),
  )


async def test_index_document_create():
  body = IndexDocumentOp(title="T", content="C", tags=["a"])
  g1, g2, g3 = _guards()
  with (
    g1,
    g2,
    g3,
    patch(f"{MODULE}._resolve_graph_tier", return_value="ladybug-standard"),
    patch("robosystems.database.SessionFactory", return_value=MagicMock()),
    patch("robosystems.operations.document_service.DocumentService") as svc,
  ):
    svc.return_value.create_document.return_value = (MagicMock(), _upload_response())
    env = await index_document_op(
      body, graph_id="kg_test", user=_user(), idempotency_key=None, cache=MagicMock()
    )
  assert env.status == "completed"
  assert env.result["document_id"] == "udoc_1"
  svc.return_value.create_document.assert_called_once()
  svc.return_value.update_document.assert_not_called()


async def test_index_document_update_when_id_present():
  body = IndexDocumentOp(document_id="doc_1", title="new title")
  g1, g2, g3 = _guards()
  with (
    g1,
    g2,
    g3,
    patch("robosystems.database.SessionFactory", return_value=MagicMock()),
    patch("robosystems.operations.document_service.DocumentService") as svc,
  ):
    svc.return_value.update_document.return_value = (MagicMock(), _upload_response())
    env = await index_document_op(
      body, graph_id="kg_test", user=_user(), idempotency_key=None, cache=MagicMock()
    )
  assert env.status == "completed"
  # only the provided field is forwarded (partial update), scoped to the graph
  _, kwargs = svc.return_value.update_document.call_args
  assert kwargs == {
    "graph_id": "kg_test",
    "document_id": "doc_1",
    "title": "new title",
  }
  svc.return_value.create_document.assert_not_called()


async def test_index_document_create_requires_title_and_content():
  body = IndexDocumentOp(title="only title")  # no content
  g1, g2, g3 = _guards()
  with (
    g1,
    g2,
    g3,
    patch("robosystems.database.SessionFactory", return_value=MagicMock()),
    patch("robosystems.operations.document_service.DocumentService"),
  ):
    with pytest.raises(HTTPException) as e:
      await index_document_op(
        body, graph_id="kg_test", user=_user(), idempotency_key=None, cache=MagicMock()
      )
  assert e.value.status_code == 422


async def test_delete_document():
  body = DeleteDocumentOp(document_id="doc_1")
  g1, g2, g3 = _guards()
  with (
    g1,
    g2,
    g3,
    patch("robosystems.database.SessionFactory", return_value=MagicMock()),
    patch("robosystems.operations.document_service.DocumentService") as svc,
  ):
    svc.return_value.delete_document.return_value = True
    env = await delete_document_op(
      body, graph_id="kg_test", user=_user(), idempotency_key=None, cache=MagicMock()
    )
  assert env.status == "completed"
  assert env.result == {"document_id": "doc_1", "deleted": True}


async def test_delete_document_404_when_missing():
  body = DeleteDocumentOp(document_id="doc_missing")
  g1, g2, g3 = _guards()
  with (
    g1,
    g2,
    g3,
    patch("robosystems.database.SessionFactory", return_value=MagicMock()),
    patch("robosystems.operations.document_service.DocumentService") as svc,
  ):
    svc.return_value.delete_document.return_value = False
    with pytest.raises(HTTPException) as e:
      await delete_document_op(
        body, graph_id="kg_test", user=_user(), idempotency_key=None, cache=MagicMock()
      )
  assert e.value.status_code == 404
