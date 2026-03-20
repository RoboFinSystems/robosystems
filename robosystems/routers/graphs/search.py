"""Full-text search router for graph-scoped document search."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.models.api.search import (
  DocumentSection,
  SearchRequest,
  SearchResponse,
)
from robosystems.models.iam import User
from robosystems.operations.search import get_search_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/search", tags=["Search"])


def _require_search_service():
  """Get search service or raise 503."""
  service = get_search_service()
  if service is None:
    raise HTTPException(
      status_code=503,
      detail="Text search is not available",
    )
  return service


@router.post("", operation_id="search_documents")
async def search_documents(
  graph_id: str,
  request: SearchRequest,
  req: Request,
  current_user: User = Depends(get_current_user_with_graph),
) -> SearchResponse:
  """Search filing narratives and text content within a graph."""
  service = _require_search_service()
  return service.search_documents(graph_id, request)


@router.get("/{document_id}", operation_id="get_document_section")
async def get_document_section(
  graph_id: str,
  document_id: str,
  req: Request,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentSection:
  """Retrieve the full text of a document section by ID."""
  service = _require_search_service()
  result = service.get_document_section(graph_id, document_id)
  if result is None:
    raise HTTPException(status_code=404, detail="Document not found")
  return result
