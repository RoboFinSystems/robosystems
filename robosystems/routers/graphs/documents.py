"""Document upload, listing, and deletion for graph-scoped text search."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from starlette import status as http_status

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.api.search import (
  BulkDocumentUploadRequest,
  BulkDocumentUploadResponse,
  DocumentListResponse,
  DocumentUploadRequest,
  DocumentUploadResponse,
)
from robosystems.models.core import User
from robosystems.operations.search import get_search_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["Documents"])


def _require_search_service():
  """Get search service or raise 503."""
  service = get_search_service()
  if service is None:
    raise HTTPException(
      status_code=503,
      detail="Text search is not available",
    )
  return service


def _block_shared_repository(graph_id: str) -> None:
  """Block document uploads on shared repository graphs."""
  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph,
  )

  if is_shared_repository_or_subgraph(graph_id):
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail="Document uploads are not allowed on shared repository graphs",
    )


def _resolve_tier(graph_id: str) -> str:
  """Resolve the subscription tier for a graph. Raises 500 on failure."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.graph import Graph

  session = SessionFactory()
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
  finally:
    session.close()


@router.get("", operation_id="list_documents")
async def list_documents(
  graph_id: str,
  source_type: str | None = None,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentListResponse:
  """List indexed documents for a graph."""
  service = _require_search_service()
  return service.list_documents(graph_id, source_type)


@router.post("", operation_id="upload_document")
async def upload_document(
  graph_id: str,
  request: DocumentUploadRequest,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentUploadResponse:
  """Upload a markdown document for text indexing."""
  _block_shared_repository(graph_id)
  service = _require_search_service()
  tier = _resolve_tier(graph_id)

  try:
    return service.upload_document(graph_id, request, tier)
  except ValueError as e:
    raise HTTPException(
      status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
      detail=str(e),
    )


@router.post("/bulk", operation_id="upload_documents_bulk")
async def upload_documents_bulk(
  graph_id: str,
  request: BulkDocumentUploadRequest,
  current_user: User = Depends(get_current_user_with_graph),
) -> BulkDocumentUploadResponse:
  """Upload multiple markdown documents for text indexing (max 50)."""
  _block_shared_repository(graph_id)
  service = _require_search_service()
  tier = _resolve_tier(graph_id)

  results, errors = service.upload_documents_bulk(graph_id, request.documents, tier)

  return BulkDocumentUploadResponse(
    total_documents=len(results),
    total_sections_indexed=sum(r.sections_indexed for r in results),
    results=results,
    errors=errors if errors else None,
  )


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
  """Delete a document and all its sections."""
  _block_shared_repository(graph_id)
  service = _require_search_service()

  deleted = service.delete_document(graph_id, document_id)
  if not deleted:
    raise HTTPException(status_code=404, detail="Document not found")
