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
    """Create a document in PG and sync to OpenSearch.

    Returns:
        Tuple of (Document model, DocumentUploadResponse).

    Raises:
        ValueError: If tier limit exceeded or no indexable sections.
    """
    # Upsert: if external_id exists, update instead (check before tier limit)
    if request.external_id:
      existing = Document.get_by_external_id(
        graph_id, request.external_id, self.session
      )
      if existing:
        return self.update_document(
          graph_id=graph_id,
          document_id=existing.id,
          title=request.title,
          content=request.content,
          tags=request.tags,
          folder=request.folder,
        )

    # Check tier limits in PG (only for new documents, not upserts)
    if tier:
      self._check_tier_limit(graph_id, tier)

    # Create PG record
    doc = Document.create(
      graph_id=graph_id,
      user_id=user_id,
      title=request.title,
      content=request.content,
      session=self.session,
      tags=request.tags,
      folder=request.folder,
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
  ) -> Sequence[Document]:
    """List documents for a graph."""
    return Document.get_by_graph(graph_id, self.session, source_type)

  def count_documents(
    self,
    graph_id: str,
    source_type: str | None = None,
  ) -> int:
    """Count documents for a graph."""
    return Document.count_by_graph(graph_id, self.session, source_type)

  def update_document(
    self,
    graph_id: str,
    document_id: str,
    title: str | None = None,
    content: str | None = None,
    tags: list[str] | None = ...,  # type: ignore[assignment]
    folder: str | None = ...,  # type: ignore[assignment]
  ) -> tuple[Document, DocumentUploadResponse]:
    """Update a document in PG and re-sync to OpenSearch.

    Returns:
        Tuple of (Document model, DocumentUploadResponse).

    Raises:
        ValueError: If document not found or no indexable sections.
    """
    doc = Document.get_by_id_and_graph(document_id, graph_id, self.session)
    if doc is None:
      raise KeyError(f"Document {document_id} not found in graph {graph_id}")

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
        document_id=f"doc_{doc.graph_id}_{doc.id}",
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

    # The OpenSearch document_id prefix uses the PG document id as external_id
    os_doc_id = f"doc_{graph_id}_{document_id}"
    service.delete_document(graph_id, os_doc_id)
