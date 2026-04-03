"""Document upload, listing, editing, and deletion for user graph documents.

Source of truth is PostgreSQL. Content is synced to OpenSearch for search.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from starlette import status as http_status

from robosystems.database import SessionFactory
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.api.search import (
  BulkDocumentUploadRequest,
  BulkDocumentUploadResponse,
  DocumentDetailResponse,
  DocumentListItem,
  DocumentListResponse,
  DocumentUpdateRequest,
  DocumentUploadRequest,
  DocumentUploadResponse,
)
from robosystems.models.core import User
from robosystems.models.core.document import Document
from robosystems.operations.documents import DocumentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["Documents"])


def _block_shared_repository(graph_id: str) -> None:
  """Block document operations on shared repository graphs."""
  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph,
  )

  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Document operations are not allowed on shared repository graphs",
    )


def _resolve_tier(graph_id: str, session) -> str:
  """Resolve the subscription tier for a graph using an existing session."""
  from robosystems.models.core.graph import Graph

  try:
    graph = Graph.get_by_id(graph_id, session)
    if graph is None or graph.graph_tier is None:
      raise HTTPException(500, "Unable to determine subscription tier")
    return graph.graph_tier
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Tier lookup failed for {graph_id}: {e}")
    raise HTTPException(500, "Unable to determine subscription tier")


def _document_to_list_item(doc: Document) -> DocumentListItem:
  """Convert a Document model to a DocumentListItem."""
  return DocumentListItem(
    id=doc.id,
    document_title=doc.title,
    section_count=doc.sections_indexed,
    source_type=doc.source_type,
    folder=doc.folder,
    tags=doc.tags,
    created_at=doc.created_at.isoformat() if doc.created_at else "",
    updated_at=doc.updated_at.isoformat() if doc.updated_at else "",
  )


def _document_to_detail(doc: Document) -> DocumentDetailResponse:
  """Convert a Document model to a DocumentDetailResponse."""
  return DocumentDetailResponse(
    id=doc.id,
    graph_id=doc.graph_id,
    user_id=doc.user_id,
    title=doc.title,
    content=doc.content,
    tags=doc.tags,
    folder=doc.folder,
    external_id=doc.external_id,
    source_type=doc.source_type,
    source_provider=doc.source_provider,
    sections_indexed=doc.sections_indexed,
    created_at=doc.created_at.isoformat() if doc.created_at else "",
    updated_at=doc.updated_at.isoformat() if doc.updated_at else "",
  )


@router.get("", operation_id="list_documents")
async def list_documents(
  graph_id: str,
  source_type: str | None = None,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentListResponse:
  """List documents for a graph from PostgreSQL."""
  _block_shared_repository(graph_id)
  session = SessionFactory()
  try:
    service = DocumentService(session)
    docs = service.list_documents(graph_id, source_type)
    return DocumentListResponse(
      total=len(docs),
      documents=[_document_to_list_item(d) for d in docs],
      graph_id=graph_id,
    )
  finally:
    session.close()


@router.get("/{document_id}", operation_id="get_document")
async def get_document(
  graph_id: str,
  document_id: str,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentDetailResponse:
  """Get a document with full content from PostgreSQL."""
  _block_shared_repository(graph_id)
  session = SessionFactory()
  try:
    service = DocumentService(session)
    doc = service.get_document(graph_id, document_id)
    if doc is None:
      raise HTTPException(status_code=404, detail="Document not found")
    return _document_to_detail(doc)
  finally:
    session.close()


@router.post("", operation_id="upload_document")
async def upload_document(
  graph_id: str,
  request: DocumentUploadRequest,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentUploadResponse:
  """Upload a markdown document. Stored in PostgreSQL, synced to OpenSearch."""
  _block_shared_repository(graph_id)
  session = SessionFactory()
  try:
    tier = _resolve_tier(graph_id, session)
    service = DocumentService(session)
    _doc, response = service.create_document(
      graph_id=graph_id,
      user_id=current_user.id,
      request=request,
      tier=tier,
    )
    return response
  except ValueError as e:
    raise HTTPException(
      status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
      detail=str(e),
    )
  finally:
    session.close()


@router.post("/bulk", operation_id="upload_documents_bulk")
async def upload_documents_bulk(
  graph_id: str,
  request: BulkDocumentUploadRequest,
  current_user: User = Depends(get_current_user_with_graph),
) -> BulkDocumentUploadResponse:
  """Upload multiple markdown documents (max 50)."""
  _block_shared_repository(graph_id)
  session = SessionFactory()
  try:
    tier = _resolve_tier(graph_id, session)
    service = DocumentService(session)
    results: list[DocumentUploadResponse] = []
    errors: list[dict] = []

    for i, doc_request in enumerate(request.documents):
      try:
        _doc, response = service.create_document(
          graph_id=graph_id,
          user_id=current_user.id,
          request=doc_request,
          tier=tier,
        )
        results.append(response)
      except Exception as e:
        errors.append({"index": i, "title": doc_request.title, "error": str(e)})

    return BulkDocumentUploadResponse(
      total_documents=len(results),
      total_sections_indexed=sum(r.sections_indexed for r in results),
      results=results,
      errors=errors if errors else None,
    )
  finally:
    session.close()


@router.put("/{document_id}", operation_id="update_document")
async def update_document(
  graph_id: str,
  document_id: str,
  request: DocumentUpdateRequest,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentUploadResponse:
  """Update a document's content and/or metadata. Re-syncs to OpenSearch."""
  _block_shared_repository(graph_id)
  session = SessionFactory()
  try:
    service = DocumentService(session)
    # Only pass fields that were explicitly provided in the request
    kwargs: dict = {}
    if "title" in request.model_fields_set:
      kwargs["title"] = request.title
    if "content" in request.model_fields_set:
      kwargs["content"] = request.content
    if "tags" in request.model_fields_set:
      kwargs["tags"] = request.tags
    if "folder" in request.model_fields_set:
      kwargs["folder"] = request.folder
    _doc, response = service.update_document(
      graph_id=graph_id,
      document_id=document_id,
      **kwargs,
    )
    return response
  except KeyError as e:
    raise HTTPException(status_code=404, detail=str(e))
  except ValueError as e:
    raise HTTPException(
      status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
      detail=str(e),
    )
  finally:
    session.close()


@router.delete(
  "/{document_id}",
  operation_id="delete_document",
  status_code=http_status.HTTP_204_NO_CONTENT,
)
async def delete_document(
  graph_id: str,
  document_id: str,
  current_user: User = Depends(get_current_user_with_graph),
) -> None:
  """Delete a document from PostgreSQL and OpenSearch."""
  _block_shared_repository(graph_id)
  session = SessionFactory()
  try:
    service = DocumentService(session)
    deleted = service.delete_document(graph_id, document_id)
    if not deleted:
      raise HTTPException(status_code=404, detail="Document not found")
  finally:
    session.close()
