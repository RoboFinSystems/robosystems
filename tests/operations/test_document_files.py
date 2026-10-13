"""Stored document files against the platform database and a mocked bucket:
a bank statement uploaded, checked, kept as evidence, read, and taken down
with the graph.
"""

from __future__ import annotations

import hashlib
import sys
import tempfile
import threading
import time
import uuid
import zlib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import boto3
import psutil
import pytest
from moto import mock_aws
from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.constants import DOCUMENT_READS_PER_PROCESS
from robosystems.config.storage.graph import get_document_upload_key
from robosystems.models.api.graphs.operations import (
  CompleteDocumentUploadOp,
  CreateDocumentUploadOp,
)
from robosystems.models.core.document import FILE_STORED, Document
from robosystems.operations.document_service import (
  DocumentCitation,
  DocumentFileError,
  DocumentFileNotUploadedError,
  DocumentInUseError,
  DocumentReadBusyError,
  DocumentService,
)

pytestmark = pytest.mark.unit

BUCKET = "robosystems-user-test"
_SERVICE = "robosystems.operations.document_service"


def _pdf(*pages: str) -> bytes:
  """A minimal PDF whose pages carry these lines as a real text layer."""
  objects = [
    b"<< /Type /Catalog /Pages 2 0 R >>",
    b"<< /Type /Pages /Kids ["
    + b" ".join(f"{4 + 2 * i} 0 R".encode() for i in range(len(pages)))
    + b"] /Count "
    + str(len(pages)).encode()
    + b" >>",
    b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
  ]
  for i, line in enumerate(pages):
    stream = f"BT /F1 12 Tf 72 720 Td ({line}) Tj ET".encode()
    objects.append(
      b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
      b"/Resources << /Font << /F1 3 0 R >> >> /Contents "
      + f"{5 + 2 * i} 0 R".encode()
      + b" >>"
    )
    objects.append(
      b"<< /Length "
      + str(len(stream)).encode()
      + b" >>\nstream\n"
      + stream
      + b"\nendstream"
    )
  out = b"%PDF-1.4\n"
  offsets = []
  for number, body in enumerate(objects, 1):
    offsets.append(len(out))
    out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
  xref = len(out)
  out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
  out += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets)
  out += (
    f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
  ).encode()
  return out


PDF = _pdf("Operating Checking ending balance 3,204.88", "Page two")


def _pdf_with_shared_content(decoded_bytes: int, pages: int = 1) -> bytes:
  """A PDF whose pages all draw one compressed content stream."""
  op = b"BT /F1 1 Tf 0 0 Td (x) Tj ET\n"
  content = zlib.compress(op * (decoded_bytes // len(op)), 9)
  objects = [
    b"<< /Type /Catalog /Pages 2 0 R >>",
    f"<< /Type /Pages /Kids [{' '.join(f'{5 + i} 0 R' for i in range(pages))}] "
    f"/Count {pages} >>".encode(),
    b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    f"<< /Length {len(content)} /Filter /FlateDecode >>\nstream\n".encode()
    + content
    + b"\nendstream",
  ]
  objects += [
    b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
    b"/Resources << /Font << /F1 3 0 R >> >> /Contents 4 0 R >>"
  ] * pages
  out = b"%PDF-1.4\n"
  offsets = []
  for number, body in enumerate(objects, 1):
    offsets.append(len(out))
    out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
  xref = len(out)
  out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
  out += b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets)
  out += (
    f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
  ).encode()
  return out


class _PeakRss:
  """This process's peak resident memory above where it started."""

  def __init__(self) -> None:
    self._process = psutil.Process()
    self._stop = threading.Event()
    self.growth = 0

  def _sample(self, start: int) -> None:
    while not self._stop.wait(0.01):
      self.growth = max(self.growth, self._process.memory_info().rss - start)

  def __enter__(self) -> _PeakRss:
    start = self._process.memory_info().rss
    self._thread = threading.Thread(target=self._sample, args=(start,), daemon=True)
    self._thread.start()
    return self

  def __exit__(self, *exc) -> None:
    self._stop.set()
    self._thread.join()


@dataclass
class _Reads:
  """Reads that reached their parse step and are held there, and every
  download made."""

  held: threading.Semaphore = field(default_factory=lambda: threading.Semaphore(0))
  release: threading.Event = field(default_factory=threading.Event)
  downloads: list[str] = field(default_factory=list)


@contextmanager
def _held_reads(hold: set[str]) -> Iterator[_Reads]:
  """Hold the reads of the documents in ``hold`` at their parse step until
  released; read the others as usual."""
  from robosystems.operations import document_service

  reads = _Reads()
  lock = threading.Lock()
  real_download = document_service._download
  real_read = document_service._read_pdf_pages

  def download(key, into):
    with lock:
      reads.downloads.append(key)
    real_download(key, into)

  def read(document_id, pdf, **kwargs):
    if document_id not in hold:
      return real_read(document_id, pdf, **kwargs)
    reads.held.release()
    reads.release.wait(30)
    return 1, [(1, "held")]

  try:
    with (
      patch.object(document_service, "_download", download),
      patch.object(document_service, "_read_pdf_pages", read),
    ):
      yield reads
  finally:
    reads.release.set()


def _graph(session, org) -> str:
  from robosystems.config.graph_tier import GraphTier
  from robosystems.models.core import Graph

  unique = uuid.uuid4().hex[:8]
  return Graph.create(
    graph_id=f"kg{unique}{unique}",
    graph_name="Another graph",
    graph_type="entity",
    org_id=org.id,
    session=session,
    base_schema="base",
    schema_extensions=["roboledger"],
    graph_tier=GraphTier.LADYBUG_STANDARD,
  ).graph_id


def _read(engine, graph_id: str, document_id: str) -> tuple[str, float]:
  """One read on its own session, as a request makes it: how it ended, and
  how long it took."""
  service = DocumentService(Session(bind=engine))
  started = time.monotonic()
  try:
    service.read_file_text(graph_id, document_id, first_page=1, max_pages=10)
    outcome = "read"
  except Exception as exc:
    outcome = type(exc).__name__
  finally:
    service.session.close()
  return outcome, time.monotonic() - started


@pytest.fixture(autouse=True)
def no_memory_limit(tmp_path, monkeypatch):
  monkeypatch.setattr(f"{_SERVICE}._CGROUP", tmp_path / "no-cgroup")


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


def _begin(service, graph_id, *, name="sep-2026.pdf", size: int | None = len(PDF)):
  return service.begin_file_upload(
    graph_id, CreateDocumentUploadOp(file_name=name, file_size_bytes=size)
  )


def _upload(s3, graph_id, upload_id, *, name="sep-2026.pdf", body=PDF) -> str:
  """PUT to where the presigned URL points: the upload key."""
  key = get_document_upload_key(graph_id, upload_id, name)
  s3.put_object(Bucket=BUCKET, Key=key, Body=body)
  return key


def _complete(service, graph_id, user_id, upload_id) -> Document:
  return service.complete_file_upload(
    graph_id,
    user_id,
    CompleteDocumentUploadOp(
      upload_id=upload_id,
      title="Checking statement, September 2026",
      folder="statements",
    ),
  )


def _store(service, graph_id, user_id, s3, *, name="sep-2026.pdf", body=PDF):
  upload_id, _url = _begin(service, graph_id, name=name, size=len(body))
  _upload(s3, graph_id, upload_id, name=name, body=body)
  return _complete(service, graph_id, user_id, upload_id)


def _objects(s3) -> list[str]:
  return [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])]


def _files(service, graph_id) -> int:
  return Document.count_by_graph(graph_id, service.session, "uploaded_file")


def test_a_statement_is_uploaded_checked_and_stored(files):
  service, graph_id, user_id, s3 = files

  upload_id, url = _begin(service, graph_id)

  # Nothing is recorded until the upload completes.
  assert upload_id.startswith("upl_")
  assert _files(service, graph_id) == 0
  # The URL reaches the upload key only, with type and size signed in.
  assert urlparse(url).path.endswith(
    f"/documents-incoming/{graph_id}/{upload_id}/sep-2026.pdf"
  )
  signed = parse_qs(urlparse(url).query)["X-Amz-SignedHeaders"][0].split(";")
  assert {"content-length", "content-type"} <= set(signed)

  _upload(s3, graph_id, upload_id)
  doc = _complete(service, graph_id, user_id, upload_id)

  assert (doc.source_type, doc.content, doc.file_status, doc.folder) == (
    "uploaded_file",
    "",
    FILE_STORED,
    "statements",
  )
  assert doc.file_s3_key == f"documents/{graph_id}/{doc.id}/sep-2026.pdf"
  assert (doc.file_sha256, doc.file_size_bytes) == (
    hashlib.sha256(PDF).hexdigest(),
    len(PDF),
  )
  # The checked bytes moved to the stored key; the upload is gone.
  assert _objects(s3) == [doc.file_s3_key]
  assert s3.get_object(Bucket=BUCKET, Key=doc.file_s3_key)["Body"].read() == PDF
  # Completing the same upload again returns the same document.
  assert _complete(service, graph_id, user_id, upload_id).id == doc.id
  assert _files(service, graph_id) == 1

  _doc, download = service.file_download_url(graph_id, str(doc.id))
  query = parse_qs(urlparse(download).query)
  assert query["response-content-disposition"] == [
    'attachment; filename="sep-2026.pdf"'
  ]


def test_the_size_need_not_be_declared(files):
  """An agent uploading with curl may not know it; the check measures it."""
  service, graph_id, user_id, s3 = files

  upload_id, url = _begin(service, graph_id, size=None)

  signed = parse_qs(urlparse(url).query)["X-Amz-SignedHeaders"][0].split(";")
  assert "content-type" in signed
  assert "content-length" not in signed
  _upload(s3, graph_id, upload_id)
  assert _complete(service, graph_id, user_id, upload_id).file_size_bytes == len(PDF)


def test_completing_before_the_upload_records_nothing(files):
  service, graph_id, user_id, _s3 = files
  upload_id, _url = _begin(service, graph_id)

  with pytest.raises(DocumentFileNotUploadedError):
    _complete(service, graph_id, user_id, upload_id)

  assert _files(service, graph_id) == 0


def test_an_upload_that_is_not_a_pdf_is_discarded(files):
  service, graph_id, user_id, s3 = files
  upload_id, _url = _begin(service, graph_id)
  _upload(s3, graph_id, upload_id, body=b"<html>" + PDF[6:])

  with pytest.raises(DocumentFileError, match="not a application/pdf"):
    _complete(service, graph_id, user_id, upload_id)

  assert _files(service, graph_id) == 0
  assert _objects(s3) == []


def test_an_upload_over_the_cap_is_discarded_unread(files):
  service, graph_id, user_id, s3 = files
  upload_id, _url = _begin(service, graph_id, size=None)
  _upload(s3, graph_id, upload_id)

  with (
    patch(f"{_SERVICE}._MAX_FILE_BYTES", len(PDF) - 1),
    pytest.raises(DocumentFileError, match="larger than"),
  ):
    _complete(service, graph_id, user_id, upload_id)

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
  service, graph_id, _user_id, _s3 = files

  with pytest.raises(DocumentFileError):
    _begin(service, graph_id, name=name)


def test_a_stored_file_never_changes_and_is_never_indexed(files):
  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)

  with pytest.raises(DocumentFileError, match="cannot be edited"):
    service.update_document(graph_id, str(doc.id), content="# Not a statement")

  with patch("robosystems.operations.search.get_search_service") as search:
    renamed, response = service.update_document(
      graph_id, str(doc.id), title="Checking, Sept 2026"
    )
    service.resync_document(renamed)
  search.assert_not_called()
  assert (renamed.title, response.sections_indexed) == ("Checking, Sept 2026", 0)


def test_an_upload_overwritten_after_completion_never_reaches_the_stored_file(files):
  """Whoever holds the upload URL can PUT again for its lifetime; the stored
  file, which only the server writes, stays the bytes that were hashed."""
  service, graph_id, user_id, s3 = files
  upload_id, _url = _begin(service, graph_id)
  _upload(s3, graph_id, upload_id)
  doc = _complete(service, graph_id, user_id, upload_id)

  _upload(s3, graph_id, upload_id, body=b"%PDF-1.7\n" + b"x" * (len(PDF) - 9))

  kept = s3.get_object(Bucket=BUCKET, Key=doc.file_s3_key)["Body"].read()
  assert hashlib.sha256(kept).hexdigest() == doc.file_sha256
  assert _complete(service, graph_id, user_id, upload_id).file_sha256 == (
    doc.file_sha256
  )


def test_an_upload_that_changes_while_it_is_checked_is_not_stored(files):
  """The copy is conditional on the upload's ETag as read. Moto does not
  enforce the condition, so S3's refusal is raised here as S3 sends it
  (LocalStack enforces it, and the end-to-end check runs there)."""
  from botocore.exceptions import ClientError

  service, graph_id, user_id, s3 = files
  upload_id, _url = _begin(service, graph_id)
  _upload(s3, graph_id, upload_id)
  refused = ClientError(
    {"Error": {"Code": "PreconditionFailed", "Message": "did not hold"}},
    "CopyObject",
  )

  with (
    patch("robosystems.operations.aws.s3.S3Client._build_client", return_value=s3),
    patch.object(s3, "copy_object", side_effect=refused) as copy,
    pytest.raises(DocumentFileNotUploadedError, match="changed while"),
  ):
    _complete(service, graph_id, user_id, upload_id)

  assert copy.call_args.kwargs["CopySourceIfMatch"]
  assert _files(service, graph_id) == 0
  assert [k for k in _objects(s3) if k.startswith("documents/")] == []


def test_two_completions_of_one_upload_store_one_document(files):
  """The second finds no document when it looks, then loses the insert: it
  removes the copy it made and returns the first one's document."""
  service, graph_id, user_id, s3 = files
  upload_id, _url = _begin(service, graph_id)
  _upload(s3, graph_id, upload_id)
  first = _complete(service, graph_id, user_id, upload_id)
  first_id, first_key = str(first.id), str(first.file_s3_key)
  _upload(s3, graph_id, upload_id)

  real = Document.get_by_external_id
  with patch.object(
    Document,
    "get_by_external_id",
    side_effect=[None, real(graph_id, upload_id, service.session)],
  ):
    second = _complete(service, graph_id, user_id, upload_id)

  assert str(second.id) == first_id
  assert [k for k in _objects(s3) if k.startswith("documents/")] == [first_key]
  assert _files(service, graph_id) == 1


def test_a_graph_at_its_file_limit_takes_no_more(files):
  service, graph_id, user_id, s3 = files
  _store(service, graph_id, user_id, s3)

  with (
    patch(f"{_SERVICE}.MAX_DOCUMENT_FILES_PER_GRAPH", 1),
    pytest.raises(DocumentFileError, match="the most it can"),
  ):
    _begin(service, graph_id, name="oct-2026.pdf")


def test_files_do_not_count_toward_the_plans_document_limit(files):
  service, graph_id, user_id, s3 = files

  with patch("robosystems.config.billing.core.get_tier_max_documents", return_value=0):
    _store(service, graph_id, user_id, s3)
    _store(service, graph_id, user_id, s3, name="oct-2026.pdf")

  assert Document.count_by_graph(graph_id, service.session, "uploaded_doc") == 0
  assert _files(service, graph_id) == 2


def test_a_non_ascii_name_downloads_under_both_forms(files):
  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3, name="Relevé septembre.pdf")

  _doc, download = service.file_download_url(graph_id, str(doc.id))

  (disposition,) = parse_qs(urlparse(download).query)["response-content-disposition"]
  assert disposition == (
    'attachment; filename="Relev_ septembre.pdf"; '
    "filename*=UTF-8''Relev%C3%A9%20septembre.pdf"
  )


def test_a_stored_statement_reads_as_text_page_by_page(files):
  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)

  first = service.read_file_text(graph_id, str(doc.id), first_page=1, max_pages=1)
  rest = service.read_file_text(graph_id, str(doc.id), first_page=2, max_pages=5)

  assert first.page_count == 2
  ((number, text),) = first.pages
  assert number == 1
  assert "ending balance 3,204.88" in text
  assert [(n, t.strip()) for n, t in rest.pages] == [(2, "Page two")]


def test_only_a_readable_stored_file_reads_as_text(files):
  from robosystems.models.api.search import DocumentUploadRequest

  service, graph_id, user_id, s3 = files
  with patch("robosystems.operations.search.get_search_service", return_value=None):
    text_doc, _response = service.create_document(
      graph_id, user_id, DocumentUploadRequest(title="Policy", content="# Policy")
    )
  broken = _store(service, graph_id, user_id, s3, name="b.pdf", body=b"%PDF-1.4\nnot")

  with pytest.raises(DocumentFileError, match="no stored file"):
    service.read_file_text(graph_id, str(text_doc.id), first_page=1, max_pages=1)
  with pytest.raises(DocumentFileError, match="could not be read"):
    service.read_file_text(graph_id, str(broken.id), first_page=1, max_pages=1)


def test_reading_a_stored_pdf_bounds_extraction_work(files):
  service, graph_id, user_id, s3 = files
  doc = _store(
    service,
    graph_id,
    user_id,
    s3,
    name="dense.pdf",
    body=_pdf_with_shared_content(20 * 1024 * 1024),
  )

  started = time.monotonic()
  with (
    _PeakRss() as rss,
    pytest.raises(DocumentFileError, match="could not be read"),
  ):
    service.read_file_text(graph_id, str(doc.id), first_page=1, max_pages=1)

  assert time.monotonic() - started < 10
  assert rss.growth < 200 * 1024 * 1024


def test_a_document_cited_by_a_recorded_balance_cannot_be_deleted(files):
  from robosystems.models.api.search import DocumentUploadRequest

  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)
  with patch("robosystems.operations.search.get_search_service", return_value=None):
    text_doc, _response = service.create_document(
      graph_id, user_id, DocumentUploadRequest(title="Statement", content="# Sept")
    )

  cited = [DocumentCitation("evt_1", "invoice_issued", "committed", "2026-09-30")]
  with patch(f"{_SERVICE}.events_citing", return_value=cited):
    for cited in (doc, text_doc):
      with pytest.raises(DocumentInUseError):
        service.delete_document(graph_id, str(cited.id))
  assert _objects(s3) == [doc.file_s3_key]

  with patch(f"{_SERVICE}.events_citing", return_value=[]):
    assert service.delete_document(graph_id, str(doc.id)) is True
  assert _objects(s3) == []
  assert service.get_document(graph_id, str(doc.id)) is None


def test_a_statement_balance_can_cite_a_stored_file(files):
  from robosystems.operations.roboledger.commands.reconciliations import (
    StatementDocumentNotFoundError,
    _check_statement_document,
  )

  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)

  with patch(
    "robosystems.database.SessionFactory",
    return_value=nullcontext(service.session),
  ):
    _check_statement_document(graph_id, str(doc.id))
    with pytest.raises(StatementDocumentNotFoundError):
      _check_statement_document(graph_id, "doc_missing")


PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 64


@pytest.mark.parametrize(
  ("name", "body", "content_type"),
  [
    ("receipt.png", PNG, "image/png"),
    ("receipt.jpg", JPEG, "image/jpeg"),
    ("receipt.JPEG", JPEG, "image/jpeg"),
  ],
)
def test_a_receipt_photo_is_stored_as_its_type(files, name, body, content_type):
  service, graph_id, user_id, s3 = files
  upload_id, _url = service.begin_file_upload(
    graph_id,
    CreateDocumentUploadOp(
      file_name=name, content_type=content_type, file_size_bytes=len(body)
    ),
  )
  _upload(s3, graph_id, upload_id, name=name, body=body)

  doc = _complete(service, graph_id, user_id, upload_id)

  assert (doc.file_content_type, doc.file_size_bytes) == (content_type, len(body))
  _doc, download = service.file_download_url(graph_id, str(doc.id))
  assert parse_qs(urlparse(download).query)["response-content-type"] == [content_type]
  # A photo has no text layer; reading it is the extraction step's.
  with pytest.raises(DocumentFileError, match="no text layer"):
    service.read_file_text(graph_id, str(doc.id), first_page=1, max_pages=1)


def test_a_photo_must_be_the_type_its_name_says(files):
  service, graph_id, user_id, s3 = files
  upload_id, _url = service.begin_file_upload(
    graph_id,
    CreateDocumentUploadOp(file_name="receipt.png", content_type="image/png"),
  )
  _upload(s3, graph_id, upload_id, name="receipt.png", body=JPEG)

  with pytest.raises(DocumentFileError, match="not a image/png"):
    _complete(service, graph_id, user_id, upload_id)
  assert _files(service, graph_id) == 0


def test_a_name_must_match_its_declared_type(files):
  service, graph_id, _user_id, _s3 = files

  with pytest.raises(DocumentFileError, match=r"\.jpg or \.jpeg"):
    service.begin_file_upload(
      graph_id,
      CreateDocumentUploadOp(file_name="receipt.png", content_type="image/jpeg"),
    )


def _raced(service, graph_id, user_id, s3):
  """A first completion that finished, and an upload put back, as a second
  completion would find it if it started before the first one ended."""
  upload_id, _url = _begin(service, graph_id)
  _upload(s3, graph_id, upload_id)
  first = _complete(service, graph_id, user_id, upload_id)
  return upload_id, str(first.id)


def _unseen_first(graph_id, upload_id, session):
  """The second completion's opening check runs before the first commits."""
  real = Document.get_by_external_id(graph_id, upload_id, session)
  return patch.object(Document, "get_by_external_id", side_effect=[None, real])


def test_a_completion_that_finds_the_upload_gone_returns_the_winners_document(files):
  service, graph_id, user_id, s3 = files
  upload_id, first_id = _raced(service, graph_id, user_id, s3)

  with _unseen_first(graph_id, upload_id, service.session):
    second = _complete(service, graph_id, user_id, upload_id)

  assert str(second.id) == first_id


@pytest.mark.parametrize("step", ["read", "copy"])
def test_an_upload_removed_mid_completion_returns_the_winners_document(files, step):
  """Listed, then gone by the time it is read or copied: the winner removed it."""
  from botocore.exceptions import ClientError

  service, graph_id, user_id, s3 = files
  upload_id, first_id = _raced(service, graph_id, user_id, s3)
  _upload(s3, graph_id, upload_id)
  gone = ClientError({"Error": {"Code": "NoSuchKey", "Message": "gone"}}, "op")
  failing = (
    patch.object(
      s3, "get_object", side_effect=s3.exceptions.NoSuchKey(gone.response, "GetObject")
    )
    if step == "read"
    else patch.object(s3, "copy_object", side_effect=gone)
  )

  with (
    patch("robosystems.operations.aws.s3.S3Client._build_client", return_value=s3),
    failing,
    _unseen_first(graph_id, upload_id, service.session),
  ):
    second = _complete(service, graph_id, user_id, upload_id)

  assert str(second.id) == first_id
  assert _files(service, graph_id) == 1


def test_a_pdf_that_breaks_the_parser_any_way_is_unreadable_not_an_error():
  import io

  from robosystems.operations import pdf_text

  request = {
    "first_page": 1,
    "max_pages": 1,
    "stream_bytes": 1024 * 1024,
    "chars_per_page": 100,
    "chars_per_read": 100,
  }
  out = io.StringIO()
  with patch("pypdf.PdfReader", side_effect=RecursionError("cyclic object")):
    code = pdf_text.serve(request, io.BytesIO(PDF), out)

  assert (code, out.getvalue()) == (pdf_text.UNREADABLE, "")


def test_a_read_is_held_to_its_character_budget(files):
  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)

  with (
    patch(f"{_SERVICE}.DOCUMENT_READ_CHARS_PER_PAGE", 10),
    patch(f"{_SERVICE}.DOCUMENT_READ_CHARS_PER_CALL", 10),
  ):
    text = service.read_file_text(graph_id, str(doc.id), first_page=1, max_pages=2)

  # The budget is spent on page one; reading goes on from page two.
  assert text.page_count == 2
  assert text.pages == [(1, "Operating ")]


def test_a_read_that_runs_past_its_time_says_to_retry(files):
  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)

  with (
    patch(f"{_SERVICE}.DOCUMENT_READ_TIMEOUT_SECONDS", 0.001),
    pytest.raises(DocumentReadBusyError, match="try again"),
  ):
    service.read_file_text(graph_id, str(doc.id), first_page=1, max_pages=1)


def test_reads_are_held_to_their_slots(files, db_session, test_org):
  service, graph_id, user_id, s3 = files
  graphs = [graph_id] + [_graph(db_session, test_org) for _ in range(5)]
  docs = [str(_store(service, g, user_id, s3).id) for g in graphs]
  engine = db_session.get_bind()
  refusals = len(graphs) - DOCUMENT_READS_PER_PROCESS

  with _held_reads(set(docs)) as reads, ThreadPoolExecutor(len(graphs)) as pool:
    outcomes = [
      pool.submit(_read, engine, g, d) for g, d in zip(graphs, docs, strict=True)
    ]
    for _ in range(DOCUMENT_READS_PER_PROCESS):
      assert reads.held.acquire(timeout=30)
    deadline = time.monotonic() + 30
    while sum(o.done() for o in outcomes) < refusals and time.monotonic() < deadline:
      time.sleep(0.01)
    # Turned away while every slot is still held: a refusal never waits.
    refused = [o for o in outcomes if o.done()]
    downloads = list(reads.downloads)
    reads.release.set()
    turned_away = [o.result() for o in refused]
    admitted = [o.result(timeout=30)[0] for o in outcomes if o not in refused]

  assert [outcome for outcome, _took in turned_away] == [
    "DocumentReadBusyError"
  ] * refusals
  assert max(took for _outcome, took in turned_away) < 5
  assert len(downloads) == DOCUMENT_READS_PER_PROCESS
  assert admitted == ["read"] * DOCUMENT_READS_PER_PROCESS


def test_a_graph_reads_one_file_at_a_time(files, db_session, test_org):
  service, graph_id, user_id, s3 = files
  held = str(_store(service, graph_id, user_id, s3, name="first.pdf").id)
  same_graph = str(_store(service, graph_id, user_id, s3).id)
  other_graph = _graph(db_session, test_org)
  other = str(_store(service, other_graph, user_id, s3).id)
  engine = db_session.get_bind()

  with _held_reads({held}) as reads, ThreadPoolExecutor(1) as pool:
    first = pool.submit(_read, engine, graph_id, held)
    assert reads.held.acquire(timeout=30)
    again, _took = _read(engine, graph_id, same_graph)
    elsewhere, _took = _read(engine, other_graph, other)
    still_held = not first.done()
    reads.release.set()
    first_outcome, _took = first.result(timeout=30)

  assert still_held
  assert (again, elsewhere, first_outcome) == ("DocumentReadBusyError", "read", "read")


_MiB = 1024 * 1024


def _cgroup(root, files: dict[str, str]):
  for name, value in files.items():
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)
  return root


_FULL = {
  "cgroup v2": {
    "memory.max": f"{1024 * _MiB}\n",
    "memory.current": f"{900 * _MiB}\n",
    "memory.stat": f"anon {880 * _MiB}\ninactive_file {10 * _MiB}\n",
  },
  "cgroup v1, sized above the container": {
    "memory/memory.limit_in_bytes": "9223372036854771712\n",
    "memory/memory.usage_in_bytes": f"{900 * _MiB}\n",
    "memory/memory.stat": (
      f"hierarchical_memory_limit {1024 * _MiB}\ntotal_inactive_file {10 * _MiB}\n"
    ),
  },
}


@pytest.mark.parametrize("cgroup", list(_FULL.values()), ids=list(_FULL))
def test_a_read_starts_only_with_memory_to_spare(files, tmp_path, monkeypatch, cgroup):
  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)
  monkeypatch.setattr(f"{_SERVICE}._CGROUP", _cgroup(tmp_path / "cg", cgroup))

  started = time.monotonic()
  with (
    _held_reads(set()) as reads,
    patch(f"{_SERVICE}.subprocess.run") as spawn,
    pytest.raises(DocumentReadBusyError, match="try again"),
  ):
    service.read_file_text(graph_id, str(doc.id), first_page=1, max_pages=1)

  assert time.monotonic() - started < 5
  assert not spawn.called
  assert reads.downloads == []


_ROOMY = {
  "no limit to read": {},
  "cgroup v2, unlimited": {
    "memory.max": "max\n",
    "memory.current": f"{900 * _MiB}\n",
  },
  "cgroup v2, the rest is file cache": {
    "memory.max": f"{1024 * _MiB}\n",
    "memory.current": f"{900 * _MiB}\n",
    "memory.stat": f"anon {300 * _MiB}\ninactive_file {600 * _MiB}\n",
  },
  "cgroup v1, unlimited": {
    "memory/memory.limit_in_bytes": "9223372036854771712\n",
    "memory/memory.usage_in_bytes": f"{900 * _MiB}\n",
    "memory/memory.stat": "hierarchical_memory_limit 9223372036854771712\n",
  },
}


@pytest.mark.parametrize("cgroup", list(_ROOMY.values()), ids=list(_ROOMY))
def test_a_read_with_memory_to_spare_starts(files, tmp_path, monkeypatch, cgroup):
  service, graph_id, user_id, s3 = files
  doc = _store(service, graph_id, user_id, s3)
  monkeypatch.setattr(f"{_SERVICE}._CGROUP", _cgroup(tmp_path / "cg", cgroup))

  text = service.read_file_text(graph_id, str(doc.id), first_page=1, max_pages=1)

  assert text.pages[0][1].startswith("Operating Checking")


@pytest.mark.skipif(
  sys.platform != "linux",
  reason="the address-space limit is enforced by Linux only; macOS ignores it",
)
def test_the_reader_runs_within_its_memory():
  from robosystems.operations import document_service

  with (
    tempfile.TemporaryFile() as pdf,
    patch(f"{_SERVICE}.DOCUMENT_READ_STREAM_MB", 64),
    patch.object(document_service.logger, "warning") as warning,
    pytest.raises(DocumentFileError, match="could not be read"),
  ):
    pdf.write(_pdf_with_shared_content(20 * 1024 * 1024))
    pdf.seek(0)
    document_service._read_pdf_pages("doc", pdf, first_page=1, max_pages=1)

  assert "MemoryError" in str(warning.call_args)
