"""Document service — PostgreSQL CRUD with OpenSearch sync.

Source of truth is PostgreSQL. On create/update, content is sectioned,
embedded, and indexed into OpenSearch for search. On delete, both PG
and OpenSearch records are removed.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from sqlalchemy.orm import Session

from robosystems.models.api.search import (
  DocumentUploadRequest,
  DocumentUploadResponse,
)
from robosystems.models.core.document import Document

logger = logging.getLogger(__name__)


def _apply_frontmatter(
  content: str,
  title: str | None,
  tags: list[str] | None | object,
  folder: str | None | object,
) -> tuple[str, str | None, list[str] | None | object, str | None | object]:
  """Strip YAML frontmatter from content and let it fill unset fields.

  An "unset" field is either Ellipsis (used by update_document as a sentinel
  for "not provided") or None (create path, where callers may not supply
  metadata). Explicit non-None request values always win over frontmatter.
  """
  if content is None:
    return content, title, tags, folder

  from robosystems.operations.search.markdown_parser import parse_frontmatter

  metadata, body = parse_frontmatter(content)
  if not metadata:
    return content, title, tags, folder

  resolved_title = title if title else metadata.get("title") or title

  resolved_tags = tags
  if resolved_tags is ... or resolved_tags is None:
    raw_tags = metadata.get("tags")
    if isinstance(raw_tags, str):
      parsed = [t.strip() for t in raw_tags.split(",") if t.strip()]
      if parsed:
        resolved_tags = parsed
    elif isinstance(raw_tags, list):
      parsed = [str(t).strip() for t in raw_tags if str(t).strip()]
      if parsed:
        resolved_tags = parsed

  resolved_folder = folder
  if resolved_folder is ... or resolved_folder is None:
    fm_folder = metadata.get("folder")
    if isinstance(fm_folder, str) and fm_folder.strip():
      resolved_folder = fm_folder.strip()

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

    # Upsert: if external_id exists, update instead (check before tier limit)
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

    # Check tier limits in PG (only for new documents, not upserts)
    if tier:
      self._check_tier_limit(graph_id, tier)

    # Create PG record
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

    # Sync to OpenSearch
    upload_response = self._sync_to_opensearch(doc)

    # Update sections count
    doc.update(self.session, sections_indexed=upload_response.sections_indexed)

    return doc, upload_response

  def get_document(self, graph_id: str, document_id: str) -> Document | None:
    """Get a document by ID with graph verification."""
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

    # Enforce the 500k content cap in the kernel. Create + the REST paths cap it
    # via the Pydantic request models, but the MCP `update-document` tool
    # forwards raw content with no length check — an uncapped multi-MB update
    # would bloat the PG row and the OpenSearch re-index.
    if content is not None and len(content) > 500_000:
      raise ValueError(
        f"Content exceeds 500,000 character limit ({len(content):,} chars)"
      )

    if content is not None:
      # _apply_frontmatter returns the Ellipsis sentinel through as `object`;
      # the caller intentionally re-accepts it. Narrow types match the
      # same `type: ignore` pattern on the function signature defaults.
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

    # Re-sync to OpenSearch if content or metadata changed
    upload_response = self._sync_to_opensearch(doc)
    doc.update(self.session, sections_indexed=upload_response.sections_indexed)

    return doc, upload_response

  def delete_document(self, graph_id: str, document_id: str) -> bool:
    """Delete a document from PG and OpenSearch."""
    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      return False

    # Delete from OpenSearch first
    self._delete_from_opensearch(graph_id, doc.id)

    # Delete from PG
    doc.delete(self.session)
    return True

  def _check_tier_limit(self, graph_id: str, tier: str) -> None:
    """Check if document count is within tier limit."""
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

    # The OpenSearch document_id prefix uses "udoc_" + PG document id
    os_doc_id = f"udoc_{document_id}"
    service.delete_document(graph_id, os_doc_id)
