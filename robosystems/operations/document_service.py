"""Document CRUD: PostgreSQL is the source of truth, OpenSearch a derived index."""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import sys
import tempfile
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import IO
from urllib.parse import quote

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.config.constants import (
  DOCUMENT_DOWNLOAD_EXPIRY_SECONDS,
  DOCUMENT_READ_CHARS_PER_CALL,
  DOCUMENT_READ_CHARS_PER_PAGE,
  DOCUMENT_READ_MEMORY_MARGIN_MB,
  DOCUMENT_READ_MEMORY_MB,
  DOCUMENT_READ_STREAM_MB,
  DOCUMENT_READ_TIMEOUT_SECONDS,
  DOCUMENT_READS_PER_PROCESS,
  MAX_DOCUMENT_FILE_MB,
  MAX_DOCUMENT_FILES_PER_GRAPH,
  PRESIGNED_URL_EXPIRY_SECONDS,
)
from robosystems.models.api.graphs.operations import (
  CompleteDocumentUploadOp,
  CreateDocumentUploadOp,
)
from robosystems.models.api.search import (
  DocumentUploadRequest,
  DocumentUploadResponse,
)
from robosystems.models.core.document import (
  FILE_SOURCE_TYPE,
  FILE_STORED,
  Document,
)
from robosystems.operations.uploads import (
  UploadNameError,
  check_upload_file_name,
  presign_upload,
)
from robosystems.utils.ulid import generate_prefixed_ulid

logger = logging.getLogger(__name__)

# Media type -> (its extensions, the bytes every such file starts with).
# Statements arrive as PDFs; receipts and invoices as PDFs or photos.
_FILE_TYPES: dict[str, tuple[tuple[str, ...], bytes]] = {
  "application/pdf": ((".pdf",), b"%PDF-"),
  "image/png": ((".png",), b"\x89PNG\r\n\x1a\n"),
  "image/jpeg": ((".jpg", ".jpeg"), b"\xff\xd8\xff"),
}
PDF = "application/pdf"
_MAX_FILE_BYTES = MAX_DOCUMENT_FILE_MB * 1024 * 1024
_PDF_TEXT_READER = Path(__file__).with_name("pdf_text.py")
_DOWNLOAD_CHUNK_BYTES = 1024 * 1024
_CGROUP = Path("/sys/fs/cgroup")
_NO_LIMIT = 1 << 60


class DocumentFileError(ValueError):
  """The file cannot be stored as given."""


class DocumentFileNotUploadedError(Exception):
  """Nothing has been uploaded for the upload yet, or it changed while being
  checked."""


class DocumentInUseError(Exception):
  """The document is evidence for a recorded balance."""


class DocumentReadBusyError(Exception):
  """Every read of a stored file is taken, or this graph's is; try again."""


class _ReadSlots:
  """Reads of stored files running in this process: at most ``total``, one
  per graph (a subgraph shares its parent's), and only while the container
  has memory to spare for every read running plus this one."""

  def __init__(self, total: int) -> None:
    self._total = total
    self._lock = threading.Lock()
    self._graphs: set[str] = set()

  @contextmanager
  def take(self, graph_id: str) -> Iterator[None]:
    from robosystems.middleware.graph.utils import parse_subgraph_id

    subgraph = parse_subgraph_id(graph_id)
    key = subgraph.parent_graph_id if subgraph else graph_id
    with self._lock:
      if (
        key in self._graphs
        or len(self._graphs) >= self._total
        or not _has_room_for(len(self._graphs) + 1)
      ):
        raise DocumentReadBusyError(
          "No document read can start now; try again in a few seconds."
        )
      self._graphs.add(key)
    try:
      yield
    finally:
      with self._lock:
        self._graphs.discard(key)


def _has_room_for(reads: int) -> bool:
  spare = _memory_to_spare()
  if spare is None:
    return True
  needed = reads * DOCUMENT_READ_MEMORY_MB + DOCUMENT_READ_MEMORY_MARGIN_MB
  return spare >= needed * 1024 * 1024


def _memory_to_spare() -> int | None:
  """Bytes this container can still take before its memory limit, not
  counting file cache the kernel reclaims; None when it has no limit we can
  read."""
  usage = _cgroup_value(_CGROUP / "memory.current")
  if usage is not None:
    limit = _cgroup_value(_CGROUP / "memory.max")
    reclaimable = _cgroup_stat(_CGROUP / "memory.stat").get("inactive_file", 0)
  else:
    v1 = _CGROUP / "memory"
    usage = _cgroup_value(v1 / "memory.usage_in_bytes")
    stat = _cgroup_stat(v1 / "memory.stat")
    limits = [_cgroup_value(v1 / "memory.limit_in_bytes")]
    # The limit set above this cgroup, which is where the binding one sits
    # when only the enclosing group is sized.
    limits.append(stat.get("hierarchical_memory_limit"))
    limit = min((value for value in limits if value), default=None)
    reclaimable = stat.get("total_inactive_file", 0)
  if usage is None or limit is None or limit >= _NO_LIMIT:
    return None
  return limit - (usage - reclaimable)


def _cgroup_value(path: Path) -> int | None:
  try:
    value = path.read_text().strip()
  except OSError:
    return None
  if value == "max":
    return _NO_LIMIT
  try:
    return int(value)
  except ValueError:
    return None


def _cgroup_stat(path: Path) -> dict[str, int]:
  try:
    lines = path.read_text().splitlines()
  except OSError:
    return {}
  stat: dict[str, int] = {}
  for line in lines:
    name, _, value = line.partition(" ")
    if value.strip().isdigit():
      stat[name] = int(value)
  return stat


_READ_SLOTS = _ReadSlots(DOCUMENT_READS_PER_PROCESS)


def _apply_frontmatter(
  content: str,
  title: str | None,
  tags: list[str] | object | None,
  folder: str | object | None,
) -> tuple[str, str | None, list[str] | object | None, str | object | None]:
  """Strip YAML frontmatter from content and let it fill unset fields.

  "Unset" is Ellipsis (update_document's not-provided sentinel) or None;
  explicit request values always win over frontmatter.
  """
  if content is None:
    return content, title, tags, folder

  from robosystems.operations.search.markdown_parser import (
    frontmatter_folder,
    frontmatter_tags,
    frontmatter_title,
    parse_frontmatter,
  )

  metadata, body = parse_frontmatter(content)
  if not metadata:
    return content, title, tags, folder

  resolved_title = title if title else frontmatter_title(metadata.get("title")) or title

  resolved_tags = tags
  if resolved_tags is ... or resolved_tags is None:
    parsed = frontmatter_tags(metadata.get("tags"))
    if parsed:
      resolved_tags = parsed

  resolved_folder = folder
  if resolved_folder is ... or resolved_folder is None:
    fm_folder = frontmatter_folder(metadata.get("folder"))
    if fm_folder:
      resolved_folder = fm_folder

  return body, resolved_title, resolved_tags, resolved_folder


class DocumentService:
  """Document management with PG persistence and OpenSearch sync."""

  def __init__(self, session: Session) -> None:
    self.session = session

  def create_document(
    self,
    graph_id: str,
    user_id: str,
    request: DocumentUploadRequest,
    tier: str | None = None,
  ) -> tuple[Document, DocumentUploadResponse]:
    """Create a document and index it, returning ``(Document, response)``.

    Upserts on ``external_id`` when one is supplied. Raises ValueError when the
    tier's document limit is reached or the content yields no indexable
    sections.
    """
    content, title, tags, folder = _apply_frontmatter(
      request.content, request.title, request.tags, request.folder
    )

    # Upserts skip the tier limit.
    if request.external_id:
      existing = Document.get_by_external_id(
        graph_id, request.external_id, self.session
      )
      if existing:
        return self.update_document(
          graph_id=graph_id,
          document_id=existing.id,
          title=title,
          content=content,
          tags=tags,
          folder=folder,
        )

    if tier:
      self._check_tier_limit(graph_id, tier)

    doc = Document.create(
      graph_id=graph_id,
      user_id=user_id,
      title=title,
      content=content,
      session=self.session,
      tags=tags,
      folder=folder,
      external_id=request.external_id,
    )

    upload_response = self.resync_document(doc)

    return doc, upload_response

  def get_document(self, graph_id: str, document_id: str) -> Document | None:
    return Document.get_by_id_and_graph(document_id, graph_id, self.session)

  def list_documents(
    self,
    graph_id: str,
    source_type: str | None = None,
    folder: str | None = None,
  ) -> Sequence[Document]:
    """List documents for a graph, optionally filtered by source type and/or folder."""
    return Document.get_by_graph(graph_id, self.session, source_type, folder)

  def count_documents(
    self,
    graph_id: str,
    source_type: str | None = None,
    folder: str | None = None,
  ) -> int:
    """Count documents for a graph, optionally filtered by source type and/or folder."""
    return Document.count_by_graph(graph_id, self.session, source_type, folder)

  def begin_file_upload(
    self, graph_id: str, request: CreateDocumentUploadOp
  ) -> tuple[str, str]:
    """An upload id and the presigned URL to PUT a file's bytes to.

    Nothing is recorded: the document is created when the upload completes,
    and an upload never completed expires with its storage prefix. The URL
    reaches an upload key, never a stored file's. Raises `DocumentFileError`
    for a name or type that cannot be stored, or a graph at its file limit.
    """
    from robosystems.config.storage.graph import get_document_upload_key

    _check_file_name(request.file_name, request.content_type)
    self._check_file_limit(graph_id)
    upload_id = generate_prefixed_ulid("upl")
    url = presign_upload(
      get_document_upload_key(graph_id, upload_id, request.file_name),
      content_type=request.content_type,
      size_bytes=request.file_size_bytes,
      expires_in=PRESIGNED_URL_EXPIRY_SECONDS,
    )
    return upload_id, url

  def complete_file_upload(
    self, graph_id: str, user_id: str, request: CompleteDocumentUploadOp
  ) -> Document:
    """Check an uploaded file and store it as a document.

    The upload is read whole: it must fit the size cap and start with its
    type's bytes, and its SHA-256 is taken from it. The checked bytes are
    copied to the stored file's key, on the condition that the upload has not
    changed since it was read, and the upload is removed. A file that fails
    the check is discarded. Completing the same upload again returns its
    document. Nothing is indexed, and the plan's document limit does not
    apply: that limit counts the sections a search index pays for, and a
    stored file has none; files have a count of their own.

    Raises `DocumentFileNotUploadedError` before anything has been uploaded
    or when the upload changed while being checked, and `DocumentFileError`
    for bytes that are not the file named or a graph at its file limit.
    """
    from botocore.exceptions import ClientError

    from robosystems.config import env
    from robosystems.config.storage.graph import (
      get_document_file_key,
      get_document_upload_prefix,
    )
    from robosystems.operations.aws.s3 import S3Client

    done = self._completed(graph_id, request.upload_id)
    if done is not None:
      return done

    client = S3Client()
    s3 = client.s3_client
    bucket = env.USER_DATA_BUCKET
    prefix = get_document_upload_prefix(graph_id, request.upload_id)
    # The URL was signed for one key, so a PUT can land nowhere else.
    keys = list(client.iter_object_keys(bucket, prefix=prefix))
    if not keys:
      return self._completed_or_not_uploaded(graph_id, request.upload_id)
    if len(keys) > 1:
      raise DocumentFileError(
        f"The upload {request.upload_id} holds more than one file."
      )
    (upload_key,) = keys
    file_name = upload_key.removeprefix(prefix)
    content_type, magic = _file_type_of(file_name)

    # From here, a missing upload means a concurrent completion of the same
    # upload finished first and removed it.
    try:
      obj = s3.get_object(Bucket=bucket, Key=upload_key)
    except s3.exceptions.NoSuchKey:
      return self._completed_or_not_uploaded(graph_id, request.upload_id)
    problem = None
    body = b""
    try:
      if obj["ContentLength"] > _MAX_FILE_BYTES:
        problem = f"The upload is larger than {MAX_DOCUMENT_FILE_MB} MB."
      else:
        body = obj["Body"].read(_MAX_FILE_BYTES + 1)
        if len(body) != obj["ContentLength"]:
          problem = "The upload could not be read whole."
        elif not body.startswith(magic):
          problem = f"The upload is not a {content_type} file."
    finally:
      obj["Body"].close()
    if problem is not None:
      s3.delete_object(Bucket=bucket, Key=upload_key)
      raise DocumentFileError(f"{problem} It was discarded; upload it again.")
    self._check_file_limit(graph_id)

    document_id = generate_prefixed_ulid("doc")
    stored_key = get_document_file_key(graph_id, document_id, file_name)
    try:
      s3.copy_object(
        Bucket=bucket,
        Key=stored_key,
        CopySource={"Bucket": bucket, "Key": upload_key},
        CopySourceIfMatch=obj["ETag"],
      )
    except ClientError as exc:
      code = exc.response.get("Error", {}).get("Code")
      if code in ("NoSuchKey", "404"):
        return self._completed_or_not_uploaded(graph_id, request.upload_id)
      if code != "PreconditionFailed":
        raise
      raise DocumentFileNotUploadedError(
        f"The upload {request.upload_id} changed while it was being checked. "
        "Complete the upload again."
      ) from exc

    doc = Document(
      id=document_id,
      graph_id=graph_id,
      user_id=user_id,
      title=request.title,
      content="",
      tags=request.tags,
      folder=request.folder,
      # The upload it came from: completing that upload again finds it.
      external_id=request.upload_id,
      source_type=FILE_SOURCE_TYPE,
      sections_indexed=0,
      file_s3_key=stored_key,
      file_name=file_name,
      file_content_type=content_type,
      file_size_bytes=len(body),
      file_sha256=hashlib.sha256(body).hexdigest(),
      file_status=FILE_STORED,
    )
    self.session.add(doc)
    try:
      self.session.commit()
    except IntegrityError:
      # Another completion of the same upload stored it first.
      self.session.rollback()
      s3.delete_object(Bucket=bucket, Key=stored_key)
      done = self._completed(graph_id, request.upload_id)
      if done is None:
        raise
      return done
    s3.delete_object(Bucket=bucket, Key=upload_key)
    return doc

  def file_download_url(self, graph_id: str, document_id: str) -> tuple[Document, str]:
    """A short-lived link to a stored file.

    Raises KeyError when the document is not in this graph, and
    `DocumentFileError` when it is not a stored file.
    """
    from robosystems.config import env
    from robosystems.operations.aws.s3 import S3Client

    doc = self._stored_file(graph_id, document_id)
    url = S3Client().generate_presigned_url(
      bucket=env.USER_DATA_BUCKET,
      key=str(doc.file_s3_key),
      expires_in=DOCUMENT_DOWNLOAD_EXPIRY_SECONDS,
      response_content_type=str(doc.file_content_type),
      response_content_disposition=_attachment(str(doc.file_name)),
    )
    if url is None:
      raise RuntimeError(f"The file behind document {document_id} could not be signed.")
    return doc, url

  def read_file_text(
    self, graph_id: str, document_id: str, *, first_page: int, max_pages: int
  ) -> FileText:
    """The text of a stored PDF's pages, from ``first_page`` (1-based).

    The text layer only: a statement a bank generates carries one, a scan
    does not, and its pages come back empty. A photo has no text layer at
    all; reading one is the extraction step's (OCR), not this. Raises
    KeyError when the document is not in this graph, `DocumentFileError`
    when it is not a stored PDF or cannot be read as one, and
    `DocumentReadBusyError` when no read can start or finish now.

    Closes the session once the document is found: the read holds no
    database connection.
    """
    doc = self._stored_file(graph_id, document_id)
    if doc.file_content_type != PDF:
      raise DocumentFileError(
        f"Document {document_id} is a {doc.file_content_type} image, which has "
        "no text layer to read."
      )
    key = str(doc.file_s3_key)
    self.session.close()
    with _READ_SLOTS.take(graph_id), tempfile.TemporaryFile() as pdf:
      _download(key, pdf)
      page_count, pages = _read_pdf_pages(
        document_id, pdf, first_page=first_page, max_pages=max_pages
      )
    return FileText(document=doc, page_count=page_count, pages=pages)

  def _stored_file(self, graph_id: str, document_id: str) -> Document:
    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      raise KeyError(f"Document {document_id} not found in graph {graph_id}")
    if not doc.is_file:
      raise DocumentFileError(f"Document {document_id} has no stored file.")
    return doc

  def _completed(self, graph_id: str, upload_id: str) -> Document | None:
    """The document an upload already became, if it did."""
    done = Document.get_by_external_id(graph_id, upload_id, self.session)
    return done if done is not None and done.is_file else None

  def _completed_or_not_uploaded(self, graph_id: str, upload_id: str) -> Document:
    done = self._completed(graph_id, upload_id)
    if done is not None:
      return done
    raise DocumentFileNotUploadedError(
      f"Nothing has been uploaded for {upload_id} yet. PUT the file to the "
      "upload URL, then complete the upload."
    )

  def _check_file_limit(self, graph_id: str) -> None:
    # Advisory: concurrent completions can pass it together and overshoot
    # by a few files, which a storage bound tolerates.
    stored = Document.count_by_graph(graph_id, self.session, FILE_SOURCE_TYPE)
    if stored >= MAX_DOCUMENT_FILES_PER_GRAPH:
      raise DocumentFileError(
        f"This graph holds {stored} document files, the most it can. Delete "
        "files no longer needed first."
      )

  def update_document(
    self,
    graph_id: str,
    document_id: str,
    title: str | None = None,
    content: str | None = None,
    tags: list[str] | None = ...,  # type: ignore[assignment]
    folder: str | None = ...,  # type: ignore[assignment]
  ) -> tuple[Document, DocumentUploadResponse]:
    """Update a document and re-index it, returning ``(Document, response)``.

    Raises KeyError when the document is not in this graph, ValueError when the
    new content yields no indexable sections.
    """
    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      raise KeyError(f"Document {document_id} not found in graph {graph_id}")
    if doc.is_file and content is not None:
      raise DocumentFileError(
        "A stored file cannot be edited. Upload the new file as a new document."
      )

    # The MCP `update-document` tool bypasses the Pydantic cap, so enforce it here.
    if content is not None and len(content) > 500_000:
      raise ValueError(
        f"Content exceeds 500,000 character limit ({len(content):,} chars)"
      )

    if content is not None:
      content, title, tags, folder = _apply_frontmatter(  # type: ignore[assignment]
        content, title, tags, folder
      )

    doc.update(
      self.session,
      title=title,
      content=content,
      tags=tags,
      folder=folder,
    )

    upload_response = self.resync_document(doc)

    return doc, upload_response

  def delete_document(self, graph_id: str, document_id: str) -> bool:
    """Delete a document from PG and OpenSearch, and a stored file's bytes.

    Raises `DocumentInUseError` for a document a live event names as its
    evidence (an invoice, a bill, a statement balance): the event would be
    left pointing at nothing.
    """
    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      return False

    citing = events_citing(graph_id, str(doc.id), limit=1)
    if citing:
      raise DocumentInUseError(
        f"Document {document_id} is the evidence for live event "
        f"{citing[0].event_id} ({citing[0].event_type}), among any others "
        "get-document lists. Point the event at another document, or void "
        "it, first."
      )
    if not doc.is_file:
      self._delete_from_opensearch(graph_id, doc.id)
    doc.delete(self.session)
    # After the row: a failure here leaves bytes nothing points at, which
    # teardown's prefix purge removes, never a stored document with none.
    if doc.is_file:
      self._delete_file(doc)
    return True

  def _delete_file(self, doc: Document) -> None:
    """The stored file. Best effort: the row is already gone."""
    from robosystems.config import env
    from robosystems.operations.aws.s3 import S3Client

    if not S3Client().delete_object(env.USER_DATA_BUCKET, str(doc.file_s3_key)):
      logger.warning(f"Left the file behind deleted document {doc.id}")

  def _check_tier_limit(self, graph_id: str, tier: str) -> None:
    from robosystems.config.billing.core import get_tier_max_documents

    max_docs = get_tier_max_documents(tier)
    if max_docs is not None:
      current_count = Document.count_by_graph(
        graph_id, self.session, source_type="uploaded_doc"
      )
      if current_count >= max_docs:
        raise ValueError(
          f"Document limit reached ({current_count}/{max_docs}). "
          f"Upgrade your plan for more capacity."
        )

  def resync_document(self, doc: Document) -> DocumentUploadResponse:
    """Re-section, re-embed and re-index one document from its PostgreSQL row.

    The primitive behind create, update, and ``rebuild_documents_job``. A
    stored file has no text to index.
    """
    if doc.is_file:
      return DocumentUploadResponse(
        id=doc.id,
        document_id=f"udoc_{doc.id}",
        sections_indexed=0,
        total_content_length=0,
        section_ids=[],
      )
    upload_response = self._sync_to_opensearch(doc)
    doc.update(self.session, sections_indexed=upload_response.sections_indexed)
    return upload_response

  def _sync_to_opensearch(self, doc: Document) -> DocumentUploadResponse:
    """Sync a document to OpenSearch (section, embed, index)."""
    from robosystems.operations.search import get_search_service

    service = get_search_service()
    if service is None:
      logger.warning(f"Search service unavailable, skipping sync for {doc.id}")
      return DocumentUploadResponse(
        id=doc.id,
        document_id=f"udoc_{doc.id}",
        sections_indexed=0,
        total_content_length=len(doc.content),
        section_ids=[],
      )

    request = DocumentUploadRequest(
      title=doc.title,
      content=doc.content,
      tags=doc.tags,
      folder=doc.folder,
      external_id=doc.id,
    )
    response = service.upload_document(doc.graph_id, request)
    return DocumentUploadResponse(
      id=doc.id,
      document_id=response.document_id,
      sections_indexed=response.sections_indexed,
      total_content_length=response.total_content_length,
      section_ids=response.section_ids,
    )

  def _delete_from_opensearch(self, graph_id: str, document_id: str) -> None:
    """Delete a document's sections from OpenSearch."""
    from robosystems.operations.search import get_search_service

    service = get_search_service()
    if service is None:
      return

    os_doc_id = f"udoc_{document_id}"
    service.delete_document(graph_id, os_doc_id)


def _download(key: str, into: IO[bytes]) -> None:
  """A stored file's bytes, up to one past the size cap, written to ``into``."""
  from robosystems.config import env
  from robosystems.operations.aws.s3 import S3Client

  obj = S3Client().s3_client.get_object(Bucket=env.USER_DATA_BUCKET, Key=key)
  remaining = _MAX_FILE_BYTES + 1
  try:
    while remaining > 0:
      chunk = obj["Body"].read(min(_DOWNLOAD_CHUNK_BYTES, remaining))
      if not chunk:
        break
      into.write(chunk)
      remaining -= len(chunk)
  finally:
    obj["Body"].close()
  into.seek(0)


def _read_pdf_pages(
  document_id: str, pdf: IO[bytes], *, first_page: int, max_pages: int
) -> tuple[int, list[tuple[int, str]]]:
  """Pages of a PDF's text, read in a child process held to the read bounds.

  Raises `DocumentFileError` when the PDF cannot be read within them, and
  `DocumentReadBusyError` when the read runs out of time.
  """
  request = {
    "first_page": first_page,
    "max_pages": max_pages,
    "memory_bytes": DOCUMENT_READ_MEMORY_MB * 1024 * 1024,
    "stream_bytes": DOCUMENT_READ_STREAM_MB * 1024 * 1024,
    "chars_per_page": DOCUMENT_READ_CHARS_PER_PAGE,
    "chars_per_read": DOCUMENT_READ_CHARS_PER_CALL,
  }
  unreadable = DocumentFileError(f"Document {document_id} could not be read as a PDF.")
  try:
    done = subprocess.run(
      [sys.executable, "-I", str(_PDF_TEXT_READER), json.dumps(request)],
      stdin=pdf,
      capture_output=True,
      timeout=DOCUMENT_READ_TIMEOUT_SECONDS,
      check=False,
    )
  except subprocess.TimeoutExpired as exc:
    logger.warning(f"Document {document_id} timed out being read as a PDF")
    raise DocumentReadBusyError(
      f"Document {document_id} took too long to read; try again in a few seconds."
    ) from exc
  if done.returncode != 0:
    logger.warning(
      f"Document {document_id} could not be read as a PDF "
      f"(exit {done.returncode}): {done.stderr[:500]!r}"
    )
    raise unreadable
  try:
    result = json.loads(done.stdout)
    return result["page_count"], [(number, text) for number, text in result["pages"]]
  except (ValueError, KeyError, TypeError) as exc:
    logger.warning(f"Document {document_id} read back malformed text: {exc!r}")
    raise unreadable from exc


@dataclass(frozen=True)
class FileText:
  """Pages of a stored file's text, numbered from 1."""

  document: Document
  page_count: int
  pages: list[tuple[int, str]]


def _check_file_name(file_name: str, content_type: str) -> None:
  """A plain name ending in its type's extension. Raises `DocumentFileError`."""
  try:
    check_upload_file_name(file_name)
  except UploadNameError as exc:
    raise DocumentFileError(str(exc)) from exc
  extensions, _magic = _FILE_TYPES[content_type]
  if not file_name.lower().endswith(extensions):
    raise DocumentFileError(
      f"A {content_type} file's name must end in {' or '.join(extensions)}."
    )


def document_content_type(file_name: str) -> str:
  """The media type a file of this name is stored as. Raises
  `DocumentFileError` for a name no stored type has."""
  for content_type, (extensions, _magic) in _FILE_TYPES.items():
    if file_name.lower().endswith(extensions):
      return content_type
  kinds = ", ".join(e for extensions, _ in _FILE_TYPES.values() for e in extensions)
  raise DocumentFileError(f"{file_name!r} is not a file that can be stored ({kinds}).")


def _file_type_of(file_name: str) -> tuple[str, bytes]:
  """The media type an uploaded file was signed for, and its leading bytes."""
  content_type = document_content_type(file_name)
  return content_type, _FILE_TYPES[content_type][1]


def _attachment(file_name: str) -> str:
  """A Content-Disposition naming the file: plain when the name is ASCII,
  with an ASCII fallback beside the UTF-8 form when it is not."""
  if file_name.isascii():
    return f'attachment; filename="{file_name}"'
  fallback = file_name.encode("ascii", "replace").decode().replace("?", "_")
  return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(file_name)}"


# An event that still stands. A voided or superseded one no longer rests on
# anything, so its document can go.
_LIVE_EVENT_SQL = "status NOT IN ('voided', 'superseded')"
# What get-document lists of the events a document backs.
_MAX_CITATIONS = 50


@dataclass(frozen=True)
class DocumentCitation:
  """A live event that names a document as its evidence."""

  event_id: str
  event_type: str
  status: str
  occurred_at: str


def document_exists(graph_id: str, document_id: str) -> bool:
  """Whether the document is one of this graph's. The check a ledger write
  makes before naming a document: the two live in different databases, so
  nothing else holds the reference."""
  from robosystems.database import SessionFactory

  with SessionFactory() as platform_session:
    return (
      Document.get_by_id_and_graph(document_id, graph_id, platform_session) is not None
    )


def events_citing(
  graph_id: str, document_id: str, *, limit: int = _MAX_CITATIONS
) -> list[DocumentCitation]:
  """The live events on the graph's books that name the document, newest
  first; none on a graph with no ledger."""
  from robosystems.db.extensions import extensions_session, tenant_schema_exists

  if not tenant_schema_exists(graph_id):
    return []
  with extensions_session(graph_id) as session:
    rows = session.execute(
      text(
        "SELECT id, event_type, status, occurred_at FROM events "
        f"WHERE document_id = :doc AND {_LIVE_EVENT_SQL} "
        "ORDER BY occurred_at DESC, id DESC LIMIT :limit"
      ),
      {"doc": document_id, "limit": limit},
    ).all()
  return [
    DocumentCitation(
      event_id=str(row.id),
      event_type=str(row.event_type),
      status=str(row.status),
      occurred_at=row.occurred_at.isoformat(),
    )
    for row in rows
  ]
