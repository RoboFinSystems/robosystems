"""Document upload, listing, editing, and deletion for user graph documents.

Source of truth is PostgreSQL. Content is synced to OpenSearch for search.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from starlette import status as http_status

from robosystems.database import SessionFactory
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.search import (
  DocumentDetailResponse,
  DocumentListItem,
  DocumentListResponse,
)
from robosystems.models.core import User
from robosystems.models.core.document import Document
from robosystems.operations.document_service import DocumentService

logger = logging.getLogger(__name__)

router = APIRouter(
  prefix="/documents",
  tags=["Documents"],
  dependencies=[Depends(subscription_aware_rate_limit_dependency)],
)


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


def _enforce_graph_access(graph_id: str, require_write: bool = False) -> None:
  """Check graph lifecycle and subscription status.

  Wraps require_graph_access with a fresh session. Raises HTTPException
  if the graph is suspended, deprovisioned, or subscription is non-active.
  """
  from robosystems.middleware.billing.enforcement import require_graph_access

  session = SessionFactory()
  try:
    require_graph_access(graph_id, session, require_write=require_write)
  finally:
    session.close()


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


@router.get(
  "",
  summary="List Documents",
  operation_id="list_documents",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_documents(
  graph_id: str,
  source_type: str | None = None,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentListResponse:
  _block_shared_repository(graph_id)
  _enforce_graph_access(graph_id)
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


@router.get(
  "/{document_id}",
  summary="Get Document",
  operation_id="get_document",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_document(
  graph_id: str,
  document_id: str,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentDetailResponse:
  _block_shared_repository(graph_id)
  _enforce_graph_access(graph_id)
  session = SessionFactory()
  try:
    service = DocumentService(session)
    doc = service.get_document(graph_id, document_id)
    if doc is None:
      raise HTTPException(status_code=404, detail="Document not found")
    return _document_to_detail(doc)
  finally:
    session.close()
