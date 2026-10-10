"""Document CRUD: PostgreSQL is the source of truth, OpenSearch a derived index."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from urllib.parse import quote

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.config.constants import (
  DOCUMENT_DOWNLOAD_EXPIRY_SECONDS,
  MAX_DOCUMENT_FILE_MB,
  MAX_DOCUMENT_FILES_PER_GRAPH,
  PRESIGNED_URL_EXPIRY_SECONDS,
)
from robosystems.models.api.graphs.operations import CreateDocumentUploadOp
from robosystems.models.api.search import (
  DocumentUploadRequest,
  DocumentUploadResponse,
)
from robosystems.models.core.document import (
  FILE_PENDING,
  FILE_SOURCE_TYPE,
  FILE_STORED,
  Document,
)

logger = logging.getLogger(__name__)

# Media type -> (extension, the bytes every such file starts with).
_FILE_TYPES = {"application/pdf": (".pdf", b"%PDF-")}
_MAX_FILE_BYTES = MAX_DOCUMENT_FILE_MB * 1024 * 1024
# Far past the upload URL's expiry, so nothing still uploading is reaped.
_ABANDONED_AFTER = timedelta(days=1)


class DocumentFileError(ValueError):
  """The file cannot be stored as given."""


class DocumentFileNotUploadedError(Exception):
  """Nothing has been uploaded to the document's URL yet."""


class DocumentInUseError(Exception):
  """The document is evidence for a recorded balance."""


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
    self, graph_id: str, user_id: str, request: CreateDocumentUploadOp
  ) -> tuple[Document, str]:
    """A pending file document and the presigned URL to PUT its bytes to.

    The declared type and size are signed into the URL, which points at an
    upload key, never the stored file's. Nothing is indexed and the tier's
    document limit does not apply: that limit counts the sections a search
    index pays for, and a stored file has none; files have a count of their
    own. Raises `DocumentFileError` for a name or type that cannot be
    stored, or a graph at its file limit.
    """
    from robosystems.config import env
    from robosystems.config.storage.graph import (
      get_document_file_key,
      get_document_upload_key,
    )
    from robosystems.operations.aws.s3 import S3Client

    extension, _magic = _FILE_TYPES[request.content_type]
    name = request.file_name
    # It is a key segment and, on download, a quoted header value.
    if (
      PurePosixPath(name).name != name
      or name.startswith(".")
      or any(ch in '"\\' or ord(ch) < 32 or ord(ch) == 127 for ch in name)
    ):
      raise DocumentFileError(
        "The file name must be a plain name: no path, quotes, backslashes or "
        "control characters."
      )
    if not name.lower().endswith(extension):
      raise DocumentFileError(
        f"A {request.content_type} file's name must end in {extension}."
      )
    self._reap_abandoned_uploads(graph_id)
    stored = Document.count_by_graph(graph_id, self.session, FILE_SOURCE_TYPE)
    if stored >= MAX_DOCUMENT_FILES_PER_GRAPH:
      raise DocumentFileError(
        f"This graph holds {stored} document files, the most it can. Delete "
        "files no longer needed first."
      )

    doc = Document(
      graph_id=graph_id,
      user_id=user_id,
      title=request.title,
      content="",
      tags=request.tags,
      folder=request.folder,
      source_type=FILE_SOURCE_TYPE,
      sections_indexed=0,
      file_name=name,
      file_content_type=request.content_type,
      file_status=FILE_PENDING,
    )
    self.session.add(doc)
    self.session.flush()
    doc.file_s3_key = get_document_file_key(graph_id, str(doc.id), name)
    # Declared now so the object is checked against it; the stored size is
    # measured when the upload completes.
    doc.file_size_bytes = request.file_size_bytes
    self.session.commit()

    upload_url = S3Client().generate_presigned_put_url(
      env.USER_DATA_BUCKET,
      get_document_upload_key(graph_id, str(doc.id), name),
      content_type=request.content_type,
      content_length=request.file_size_bytes,
      expires_in=PRESIGNED_URL_EXPIRY_SECONDS,
    )
    return doc, upload_url

  def complete_file_upload(self, graph_id: str, document_id: str) -> Document:
    """Check the uploaded bytes and store them as the document's file.

    The upload is read whole: its size must be the declared one and its
    first bytes its type's, and its SHA-256 is taken from it. The checked
    bytes are then copied to the stored file's key, on the condition that
    the upload has not changed since it was read, and the upload is removed.
    A file that fails the check is discarded with its document. Completing a
    stored file again returns it unchanged.

    Raises KeyError when the document is not in this graph,
    `DocumentFileNotUploadedError` before anything has been uploaded or when
    the upload changed while being checked, and `DocumentFileError` for a
    document that is not a file or bytes that are not the file declared.
    """
    from botocore.exceptions import ClientError

    from robosystems.config import env
    from robosystems.config.storage.graph import get_document_upload_key
    from robosystems.operations.aws.s3 import S3Client

    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      raise KeyError(f"Document {document_id} not found in graph {graph_id}")
    if not doc.is_file:
      raise DocumentFileError(f"Document {document_id} is not a file upload.")
    if doc.file_status == FILE_STORED:
      return doc

    s3 = S3Client().s3_client
    bucket = env.USER_DATA_BUCKET
    upload_key = get_document_upload_key(graph_id, str(doc.id), str(doc.file_name))
    try:
      obj = s3.get_object(Bucket=bucket, Key=upload_key)
    except s3.exceptions.NoSuchKey as exc:
      raise DocumentFileNotUploadedError(
        f"Nothing has been uploaded for document {document_id} yet. PUT the "
        "file to the upload URL, then complete the upload."
      ) from exc

    _extension, magic = _FILE_TYPES[str(doc.file_content_type)]
    problem = None
    body = b""
    try:
      if obj["ContentLength"] != doc.file_size_bytes:
        problem = (
          f"The upload is {obj['ContentLength']} bytes, not the "
          f"{doc.file_size_bytes} declared."
        )
      else:
        body = obj["Body"].read(_MAX_FILE_BYTES + 1)
        if len(body) != doc.file_size_bytes:
          problem = f"The upload is not the {doc.file_size_bytes} bytes declared."
        elif not body.startswith(magic):
          problem = f"The upload is not a {doc.file_content_type} file."
    finally:
      obj["Body"].close()
    if problem is not None:
      s3.delete_object(Bucket=bucket, Key=upload_key)
      doc.delete(self.session)
      raise DocumentFileError(f"{problem} It was discarded; upload it again.")

    try:
      s3.copy_object(
        Bucket=bucket,
        Key=str(doc.file_s3_key),
        CopySource={"Bucket": bucket, "Key": upload_key},
        CopySourceIfMatch=obj["ETag"],
      )
    except ClientError as exc:
      if exc.response.get("Error", {}).get("Code") != "PreconditionFailed":
        raise
      raise DocumentFileNotUploadedError(
        f"The upload for document {document_id} changed while it was being "
        "checked. Complete the upload again."
      ) from exc
    s3.delete_object(Bucket=bucket, Key=upload_key)

    doc.file_sha256 = hashlib.sha256(body).hexdigest()
    doc.file_status = FILE_STORED
    doc.update(self.session)
    return doc

  def file_download_url(self, graph_id: str, document_id: str) -> tuple[Document, str]:
    """A short-lived link to a stored file.

    Raises KeyError when the document is not in this graph, and
    `DocumentFileError` when it is not a stored file.
    """
    from robosystems.config import env
    from robosystems.operations.aws.s3 import S3Client

    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      raise KeyError(f"Document {document_id} not found in graph {graph_id}")
    if not doc.is_file or doc.file_status != FILE_STORED:
      raise DocumentFileError(f"Document {document_id} has no stored file.")
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

    Raises `DocumentInUseError` for a document that a recorded statement
    balance cites as its evidence: the balance would be left pointing at
    nothing.
    """
    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      return False

    if _cited_as_evidence(graph_id, str(doc.id)):
      raise DocumentInUseError(
        f"Document {document_id} is the statement behind a recorded balance. "
        "Record that balance again with another document first."
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
    """The stored file and any upload left beside it. Best effort: the row
    is already gone."""
    from robosystems.config import env
    from robosystems.config.storage.graph import get_document_upload_key
    from robosystems.operations.aws.s3 import S3Client

    s3 = S3Client()
    upload_key = get_document_upload_key(
      str(doc.graph_id), str(doc.id), str(doc.file_name)
    )
    for key in (str(doc.file_s3_key), upload_key):
      if not s3.delete_object(env.USER_DATA_BUCKET, key):
        logger.warning(f"Left s3 object {key} behind deleted document {doc.id}")

  def _reap_abandoned_uploads(self, graph_id: str) -> None:
    """Delete the graph's uploads begun more than a day ago and never
    completed, with any bytes they left, so they stop counting toward the
    file limit. The upload URL expired long before."""
    cutoff = datetime.now(UTC).replace(tzinfo=None) - _ABANDONED_AFTER
    abandoned = (
      self.session.query(Document)
      .filter(
        Document.graph_id == graph_id,
        Document.source_type == FILE_SOURCE_TYPE,
        Document.file_status == FILE_PENDING,
        Document.created_at < cutoff,
      )
      .all()
    )
    for doc in abandoned:
      doc.delete(self.session)
      self._delete_file(doc)

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


def _attachment(file_name: str) -> str:
  """A Content-Disposition naming the file: plain when the name is ASCII,
  with an ASCII fallback beside the UTF-8 form when it is not."""
  if file_name.isascii():
    return f'attachment; filename="{file_name}"'
  fallback = file_name.encode("ascii", "replace").decode().replace("?", "_")
  return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(file_name)}"


def _cited_as_evidence(graph_id: str, document_id: str) -> bool:
  """Whether a live statement balance on the graph's books names the document."""
  from robosystems.db.extensions import extensions_session, tenant_schema_exists

  if not tenant_schema_exists(graph_id):
    return False
  with extensions_session(graph_id) as session:
    return (
      session.execute(
        text(
          "SELECT 1 FROM events WHERE event_type = 'balance_observed' "
          "AND status = 'committed' AND metadata->>'document_id' = :doc LIMIT 1"
        ),
        {"doc": document_id},
      ).first()
      is not None
    )
