"""Full-text search router for graph-scoped document search."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from starlette import status as http_status

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import RESOURCE_ERROR_RESPONSES
from robosystems.models.api.search import (
  DocumentSection,
  SearchRequest,
  SearchResponse,
)
from robosystems.models.core import User
from robosystems.operations.search import get_search_service

logger = logging.getLogger(__name__)

# Per-tier burst limiting (GRAPH_SEARCH) applies to ALL graphs, shared or
# user-owned — OpenSearch is a shared resource. The shared-repo per-plan
# volume caps in _check_search_rate_limit run on top for SEC et al.
router = APIRouter(
  prefix="/search",
  tags=["Search"],
  dependencies=[Depends(subscription_aware_rate_limit_dependency)],
)


def _require_search_service():
  """Get search service or raise 503."""
  service = get_search_service()
  if service is None:
    raise HTTPException(
      status_code=503,
      detail="Text search is not available",
    )
  return service


async def _check_search_rate_limit(
  graph_id: str, current_user: User, endpoint: str
) -> None:
  """Apply dual-layer rate limiting for search on shared repositories."""
  from robosystems.config import env
  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph,
    resolve_shared_repository_parent,
  )

  if not is_shared_repository_or_subgraph(graph_id):
    return

  if not env.RATE_LIMIT_ENABLED:
    return

  from robosystems.config.valkey_registry import (
    ValkeyDatabase,
    create_async_redis_client,
  )

  # Check user has access to the shared repo (subscriptions are on the parent)
  from robosystems.database import SessionFactory
  from robosystems.middleware.rate_limits import DualLayerRateLimiter
  from robosystems.models.core.user.user_repository import UserRepository

  parent_repo_id = resolve_shared_repository_parent(graph_id)
  session = SessionFactory()
  try:
    repo_access = UserRepository.get_by_user_and_repository(
      current_user.id, parent_repo_id, session
    )
  finally:
    session.close()

  if not repo_access:
    raise HTTPException(
      status_code=http_status.HTTP_403_FORBIDDEN,
      detail=f"Access to {graph_id.upper()} repository requires a subscription. "
      f"Visit {env.ROBOSYSTEMS_URL}/billing",
    )

  redis_client = create_async_redis_client(ValkeyDatabase.RATE_LIMITS)

  try:
    limiter = DualLayerRateLimiter(redis_client)

    limit_check = await limiter.check_limits(
      user_id=str(current_user.id),
      graph_id=graph_id,
      operation="search",
      endpoint=endpoint,
      repository_plan=repo_access.repository_plan,
    )

    if not limit_check["allowed"]:
      reason = limit_check.get("reason", "unknown")
      message = limit_check.get("message", "Rate limit exceeded")

      if reason == "no_access" or reason == "endpoint_not_allowed":
        raise HTTPException(
          status_code=http_status.HTTP_403_FORBIDDEN,
          detail=message,
        )
      else:
        retry_after = str(limit_check.get("detail", {}).get("reset_in", 60))
        raise HTTPException(
          status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
          detail=message,
          headers={"Retry-After": retry_after},
        )
  finally:
    await redis_client.aclose()


@router.post(
  "",
  summary="Search Graph Documents",
  description="Shared repositories require a subscription; subscription plan determines search rate limits.",
  operation_id="search_documents",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    503: {"description": "Text search not available"},
  },
)
async def search_documents(
  graph_id: str,
  request: SearchRequest,
  current_user: User = Depends(get_current_user_with_graph),
) -> SearchResponse:
  from robosystems.database import SessionFactory
  from robosystems.middleware.billing.enforcement import require_graph_access

  session = SessionFactory()
  try:
    require_graph_access(graph_id, session)
  finally:
    session.close()
  await _check_search_rate_limit(graph_id, current_user, "search")
  service = _require_search_service()
  return service.search_documents(graph_id, request)


@router.get(
  "/{document_id}",
  summary="Get Document Section",
  operation_id="get_document_section",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    503: {"description": "Text search not available"},
  },
)
async def get_document_section(
  graph_id: str,
  document_id: str,
  current_user: User = Depends(get_current_user_with_graph),
) -> DocumentSection:
  from robosystems.database import SessionFactory
  from robosystems.middleware.billing.enforcement import require_graph_access

  session = SessionFactory()
  try:
    require_graph_access(graph_id, session)
  finally:
    session.close()
  await _check_search_rate_limit(graph_id, current_user, "search")
  service = _require_search_service()
  result = service.get_document_section(graph_id, document_id)
  if result is None:
    raise HTTPException(status_code=404, detail="Document not found")
  return result
