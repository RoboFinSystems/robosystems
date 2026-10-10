"""Stored document files against the platform database and a mocked bucket:
a bank statement uploaded, checked, kept as evidence, and taken down with
the graph.
"""

from __future__ import annotations

import hashlib
from contextlib import nullcontext
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import boto3
import pytest
from moto import mock_aws

from robosystems.config import env
from robosystems.config.storage.graph import get_document_upload_key
from robosystems.models.api.graphs.operations import CreateDocumentUploadOp
from robosystems.models.core.document import FILE_PENDING, FILE_STORED, Document
from robosystems.operations.document_service import (
  DocumentFileError,
  DocumentFileNotUploadedError,
  DocumentInUseError,
  DocumentService,
)

pytestmark = pytest.mark.unit

BUCKET = "robosystems-user-test"
PDF = b"%PDF-1.7\n" + b"statement body " * 64
_SERVICE = "robosystems.operations.document_service"


@pytest.fixture()
def bucket(monkeypatch):
  # boto3 reads these itself, which would send every call past moto to a
  # running LocalStack.
  monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
  monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
  with (
    mock_aws(),
    patch.object(env, "USER_DATA_BUCKET", BUCKET),
    patch.object(env, "AWS_ENDPOINT_URL", ""),
    patch.object(env, "AWS_S3_PRESIGN_ENDPOINT_URL", ""),
  ):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    yield s3


@pytest.fixture()
def files(db_session, test_user, sample_graph, bucket):
  return DocumentService(db_session), sample_graph.graph_id, str(test_user.id), bucket


def _begin(service, graph_id, user_id, *, name="sep-2026.pdf", size=len(PDF)):
  return service.begin_file_upload(
    graph_id,
    user_id,
    CreateDocumentUploadOp(
      title="Checking statement, September 2026",
      file_name=name,
      file_size_bytes=size,
      folder="statements",
    ),
  )


def _objects(s3) -> list[str]:
  return [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])]


def _upload(s3, doc, body=PDF):
  """PUT to where the presigned URL points: the upload key."""
  key = get_document_upload_key(doc.graph_id, str(doc.id), doc.file_name)
  s3.put_object(Bucket=BUCKET, Key=key, Body=body)
  return key


def test_a_statement_is_uploaded_checked_and_stored(files):
  service, graph_id, user_id, s3 = files

  doc, url = _begin(service, graph_id, user_id)

  assert (doc.file_status, doc.source_type, doc.content) == (
    FILE_PENDING,
    "uploaded_file",
    "",
  )
  assert doc.file_s3_key == f"documents/{graph_id}/{doc.id}/sep-2026.pdf"
  # The URL reaches the upload key only, with type and size signed in.
  assert urlparse(url).path.endswith(
    f"/documents/{graph_id}/{doc.id}/incoming/sep-2026.pdf"
  )
  signed = parse_qs(urlparse(url).query)["X-Amz-SignedHeaders"][0].split(";")
  assert {"content-length", "content-type"} <= set(signed)

  _upload(s3, doc)
  stored = service.complete_file_upload(graph_id, str(doc.id))

  # The checked bytes moved to the stored key; the upload is gone.
  assert _objects(s3) == [doc.file_s3_key]
  assert s3.get_object(Bucket=BUCKET, Key=doc.file_s3_key)["Body"].read() == PDF

  assert stored.file_status == FILE_STORED
  assert stored.file_sha256 == hashlib.sha256(PDF).hexdigest()
  assert stored.file_size_bytes == len(PDF)
  # Completing again changes nothing.
  assert service.complete_file_upload(graph_id, str(doc.id)).file_sha256 == (
    stored.file_sha256
  )

  _doc, download = service.file_download_url(graph_id, str(doc.id))
  query = parse_qs(urlparse(download).query)
  assert query["response-content-disposition"] == [
    'attachment; filename="sep-2026.pdf"'
  ]


def test_completing_before_the_upload_leaves_it_pending(files):
  service, graph_id, user_id, _s3 = files
  doc, _url = _begin(service, graph_id, user_id)

  with pytest.raises(DocumentFileNotUploadedError):
    service.complete_file_upload(graph_id, str(doc.id))

  assert service.get_document(graph_id, str(doc.id)).file_status == FILE_PENDING
  with pytest.raises(DocumentFileError, match="no stored file"):
    service.file_download_url(graph_id, str(doc.id))


@pytest.mark.parametrize(
  ("body", "problem"),
  [
    (PDF[:-1], "not the"),
    (b"<html>" + PDF[6:], "not a application/pdf"),
  ],
  ids=["wrong-size", "not-a-pdf"],
)
def test_an_upload_that_is_not_the_declared_file_is_discarded(files, body, problem):
  service, graph_id, user_id, s3 = files
  doc, _url = _begin(service, graph_id, user_id, size=len(PDF))
  doc_id = str(doc.id)
  _upload(s3, doc, body)

  with pytest.raises(DocumentFileError, match=problem):
    service.complete_file_upload(graph_id, doc_id)

  assert service.get_document(graph_id, doc_id) is None
  assert _objects(s3) == []


@pytest.mark.parametrize(
  "name",
  [
    "../other/x.pdf",
    "dir/x.pdf",
    'quote".pdf',
    "back\\slash.pdf",
    "nl\n.pdf",
    ".pdf",
    "x.txt",
  ],
)
def test_a_name_that_is_not_a_plain_pdf_name_is_refused(files, name):
  service, graph_id, user_id, _s3 = files

  with pytest.raises(DocumentFileError):
    _begin(service, graph_id, user_id, name=name)

  assert Document.count_by_graph(graph_id, service.session) == 0


def test_a_stored_file_never_changes_and_is_never_indexed(files):
  service, graph_id, user_id, s3 = files
  doc, _url = _begin(service, graph_id, user_id)
  _upload(s3, doc)
  service.complete_file_upload(graph_id, str(doc.id))

  with pytest.raises(DocumentFileError, match="cannot be edited"):
    service.update_document(graph_id, str(doc.id), content="# Not a statement")

  with patch("robosystems.operations.search.get_search_service") as search:
    renamed, response = service.update_document(
      graph_id, str(doc.id), title="Checking, Sept 2026"
    )
    service.resync_document(renamed)
  search.assert_not_called()
  assert (renamed.title, response.sections_indexed) == ("Checking, Sept 2026", 0)


def test_a_file_cited_by_a_recorded_balance_cannot_be_deleted(files):
  service, graph_id, user_id, s3 = files
  doc, _url = _begin(service, graph_id, user_id)
  _upload(s3, doc)
  service.complete_file_upload(graph_id, str(doc.id))

  with (
    patch(f"{_SERVICE}._cited_as_evidence", return_value=True),
    pytest.raises(DocumentInUseError),
  ):
    service.delete_document(graph_id, str(doc.id))
  assert _objects(s3) == [doc.file_s3_key]

  with patch(f"{_SERVICE}._cited_as_evidence", return_value=False):
    assert service.delete_document(graph_id, str(doc.id)) is True
  assert _objects(s3) == []
  assert service.get_document(graph_id, str(doc.id)) is None


def test_files_do_not_count_toward_the_plans_document_limit(files):
  service, graph_id, user_id, _s3 = files
  _begin(service, graph_id, user_id)

  with patch("robosystems.config.billing.core.get_tier_max_documents", return_value=0):
    _begin(service, graph_id, user_id, name="oct-2026.pdf")

  assert Document.count_by_graph(graph_id, service.session, "uploaded_doc") == 0


def test_a_statement_balance_can_cite_only_a_stored_file(files):
  from robosystems.operations.roboledger.commands.reconciliations import (
    StatementDocumentPendingError,
    _check_statement_document,
  )

  service, graph_id, user_id, s3 = files
  doc, _url = _begin(service, graph_id, user_id)

  with patch(
    "robosystems.database.SessionFactory",
    return_value=nullcontext(service.session),
  ):
    with pytest.raises(StatementDocumentPendingError):
      _check_statement_document(graph_id, str(doc.id))

    _upload(s3, doc)
    service.complete_file_upload(graph_id, str(doc.id))
    _check_statement_document(graph_id, str(doc.id))


def test_an_upload_overwritten_after_completion_never_reaches_the_stored_file(files):
  """Whoever holds the upload URL can PUT again for its lifetime; the stored
  file, which only the server writes, stays the bytes that were hashed."""
  service, graph_id, user_id, s3 = files
  doc, _url = _begin(service, graph_id, user_id)
  _upload(s3, doc)
  stored = service.complete_file_upload(graph_id, str(doc.id))

  forged = b"%PDF-1.7\n" + b"x" * (len(PDF) - 9)
  _upload(s3, doc, forged)

  kept = s3.get_object(Bucket=BUCKET, Key=doc.file_s3_key)["Body"].read()
  assert kept == PDF
  assert hashlib.sha256(kept).hexdigest() == stored.file_sha256
  assert service.complete_file_upload(graph_id, str(doc.id)).file_sha256 == (
    stored.file_sha256
  )


def test_an_upload_that_changes_while_it_is_checked_is_not_stored(files):
  """The copy is conditional on the upload's ETag as read. Moto does not
  enforce the condition, so S3's refusal is raised here as S3 sends it
  (LocalStack enforces it, and the end-to-end check runs there)."""
  from botocore.exceptions import ClientError

  service, graph_id, user_id, s3 = files
  doc, _url = _begin(service, graph_id, user_id)
  _upload(s3, doc)
  refused = ClientError(
    {"Error": {"Code": "PreconditionFailed", "Message": "did not hold"}},
    "CopyObject",
  )

  with (
    patch("robosystems.operations.aws.s3.S3Client._build_client", return_value=s3),
    patch.object(s3, "copy_object", side_effect=refused) as copy,
    pytest.raises(DocumentFileNotUploadedError, match="changed while"),
  ):
    service.complete_file_upload(graph_id, str(doc.id))

  assert copy.call_args.kwargs["CopySourceIfMatch"]
  assert service.get_document(graph_id, str(doc.id)).file_status == FILE_PENDING
  assert doc.file_s3_key not in _objects(s3)


def test_a_graph_at_its_file_limit_takes_no_more(files):
  service, graph_id, user_id, _s3 = files
  _begin(service, graph_id, user_id)

  with (
    patch(f"{_SERVICE}.MAX_DOCUMENT_FILES_PER_GRAPH", 1),
    pytest.raises(DocumentFileError, match="the most it can"),
  ):
    _begin(service, graph_id, user_id, name="oct-2026.pdf")


def test_a_non_ascii_name_downloads_under_both_forms(files):
  service, graph_id, user_id, s3 = files
  doc, _url = _begin(service, graph_id, user_id, name="Relevé septembre.pdf")
  _upload(s3, doc)
  service.complete_file_upload(graph_id, str(doc.id))

  _doc, download = service.file_download_url(graph_id, str(doc.id))

  (disposition,) = parse_qs(urlparse(download).query)["response-content-disposition"]
  assert disposition == (
    'attachment; filename="Relev_ septembre.pdf"; '
    "filename*=UTF-8''Relev%C3%A9%20septembre.pdf"
  )


def test_a_text_document_cited_by_a_balance_cannot_be_deleted_either(files):
  from robosystems.models.api.search import DocumentUploadRequest

  service, graph_id, user_id, _s3 = files
  with patch("robosystems.operations.search.get_search_service", return_value=None):
    doc, _response = service.create_document(
      graph_id, user_id, DocumentUploadRequest(title="Statement", content="# Sept")
    )

  with (
    patch(f"{_SERVICE}._cited_as_evidence", return_value=True),
    pytest.raises(DocumentInUseError),
  ):
    service.delete_document(graph_id, str(doc.id))


def test_an_upload_abandoned_for_a_day_is_reaped(files):
  """A begun upload that never completes stops counting toward the file
  limit, and its bytes go with it; a recent one is still in flight."""
  from datetime import UTC, datetime, timedelta

  service, graph_id, user_id, s3 = files
  stale, _url = _begin(service, graph_id, user_id, name="stale.pdf")
  stale_upload = _upload(s3, stale)
  stale.created_at = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=2)
  recent, _url = _begin(service, graph_id, user_id, name="recent.pdf")
  service.session.commit()
  stale_id, recent_id = str(stale.id), str(recent.id)

  with patch(f"{_SERVICE}.MAX_DOCUMENT_FILES_PER_GRAPH", 2):
    _begin(service, graph_id, user_id, name="next.pdf")

  assert service.get_document(graph_id, stale_id) is None
  assert stale_upload not in _objects(s3)
  assert service.get_document(graph_id, recent_id).file_status == FILE_PENDING
