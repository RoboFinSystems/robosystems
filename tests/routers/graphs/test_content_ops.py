"""Tests for the document content-ops (index-document / delete-document).

These moved off the bespoke `/documents` write routes onto the operation
envelope; they mirror the memory remember/forget op shape.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.operations import (
  DeleteDocumentOp,
  DeleteFileOp,
  IndexDocumentOp,
  IngestFileOp,
  UpdateMemoryOp,
)
from robosystems.models.api.graphs.tables import DeleteFileResponse
from robosystems.models.api.search import DocumentUploadResponse
from robosystems.routers.graphs.operations import (
  delete_document_op,
  delete_file_op,
  index_document_op,
  ingest_file_op,
  update_memory_op,
)

MODULE = "robosystems.routers.graphs.operations"
INGEST_CMD = "robosystems.operations.graph.commands.ingest_file.ingest_file_cmd"
DELETE_FILE_CMD = "robosystems.operations.graph.commands.delete_file.delete_file_cmd"


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


def test_index_document_title_capped_at_500():
  # matches the documents.title String(500) DB column — over-long titles must
  # fail validation (clean 422), not slip through to a DB-layer error.
  from pydantic import ValidationError

  IndexDocumentOp(title="x" * 500, content="c")  # ok
  with pytest.raises(ValidationError):
    IndexDocumentOp(title="x" * 501, content="c")


async def test_ingest_file_async_when_operation_id():
  body = IngestFileOp(file_id="gf_1", ingest_to_graph=True)
  g1, g2, g3 = _guards()
  with (
    g1,
    g2,
    g3,
    patch(
      INGEST_CMD,
      new=AsyncMock(
        return_value={"status": "queued", "operation_id": "op_x", "file_id": "gf_1"}
      ),
    ),
  ):
    env = await ingest_file_op(
      body,
      background_tasks=MagicMock(),
      graph_id="kg_test",
      user=_user(),
      idempotency_key=None,
      cache=MagicMock(),
      db=MagicMock(),
    )
  assert env.status == "pending"
  assert env.operation_id == "op_x"


async def test_ingest_file_completed_when_no_operation_id():
  body = IngestFileOp(file_id="gf_1")
  g1, g2, g3 = _guards()
  with (
    g1,
    g2,
    g3,
    patch(
      INGEST_CMD,
      new=AsyncMock(
        return_value={"status": "success", "staged": True, "file_id": "gf_1"}
      ),
    ),
  ):
    env = await ingest_file_op(
      body,
      background_tasks=MagicMock(),
      graph_id="kg_test",
      user=_user(),
      idempotency_key=None,
      cache=MagicMock(),
      db=MagicMock(),
    )
  assert env.status == "completed"
  assert env.result["staged"] is True


async def test_delete_file():
  body = DeleteFileOp(file_id="gf_1", cascade=True)
  resp = DeleteFileResponse(
    status="deleted",
    file_id="gf_1",
    file_name="x.parquet",
    message="ok",
    cascade_deleted=True,
    tables_affected=["T"],
    graph_marked_stale=True,
  )
  g1, g2, g3 = _guards()
  with g1, g2, g3, patch(DELETE_FILE_CMD, new=AsyncMock(return_value=resp)):
    env = await delete_file_op(
      body,
      graph_id="kg_test",
      user=_user(),
      idempotency_key=None,
      cache=MagicMock(),
      db=MagicMock(),
    )
  assert env.status == "completed"
  assert env.result["file_id"] == "gf_1"
  assert env.result["cascade_deleted"] is True


async def test_update_memory_op():
  body = UpdateMemoryOp(memory_id="mem_" + "0" * 32, text="revised note")
  record = MagicMock()
  record.model_dump.return_value = {
    "memory_id": "mem_" + "0" * 32,
    "text": "revised note",
  }
  client = MagicMock()
  client.close = AsyncMock()
  service = MagicMock()
  service.update_memory = AsyncMock(return_value=record)
  with (
    patch(f"{MODULE}._require_memory_enabled"),
    patch(f"{MODULE}._block_shared_repo"),
    patch(f"{MODULE}._require_graph_write_access"),
    patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      new=AsyncMock(return_value=client),
    ),
    patch("robosystems.operations.memory.get_memory_service", return_value=service),
  ):
    env = await update_memory_op(
      body, graph_id="kg_test", user=_user(), idempotency_key=None, cache=MagicMock()
    )
  assert env.status == "completed"
  assert env.result["text"] == "revised note"
  service.update_memory.assert_awaited_once()


async def test_update_memory_op_404_when_missing():
  body = UpdateMemoryOp(memory_id="mem_" + "0" * 32, text="x")
  client = MagicMock()
  client.close = AsyncMock()
  service = MagicMock()
  service.update_memory = AsyncMock(return_value=None)
  with (
    patch(f"{MODULE}._require_memory_enabled"),
    patch(f"{MODULE}._block_shared_repo"),
    patch(f"{MODULE}._require_graph_write_access"),
    patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      new=AsyncMock(return_value=client),
    ),
    patch("robosystems.operations.memory.get_memory_service", return_value=service),
  ):
    with pytest.raises(HTTPException) as e:
      await update_memory_op(
        body, graph_id="kg_test", user=_user(), idempotency_key=None, cache=MagicMock()
      )
  assert e.value.status_code == 404
