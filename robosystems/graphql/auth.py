"""GraphQL-side authorization helpers.

The extensions GraphQL endpoint is mounted at
`/extensions/{graph_id}/graphql`, so `graph_id` is a URL path parameter,
not a query argument. `get_context` reads it via FastAPI's `Path(...)`
dependency before Strawberry parses the query, then calls
`check_graph_access` as part of building the context. Access is enforced
by the time any resolver runs, so resolvers never call this helper.

This mirrors the post-authentication portion of
`get_current_user_with_graph` in
`robosystems/middleware/auth/dependencies.py`.
"""

from __future__ import annotations

from fastapi import HTTPException, Request, status

from robosystems.db.extensions import LIBRARY_GRAPH_ID
from robosystems.models.core import User


def check_graph_access(
  user: User, graph_id: str, request: Request | None = None
) -> None:
  """Verify the authenticated user has read access to `graph_id`, else 403.

  Covers both shared repositories (SEC, etc.) and user graphs. The `library`
  sentinel routes to the taxonomy library, shared reference material that
  any authenticated user may read, so no per-graph ACL applies to it.

  A user-graph denial is audited (`AuthorizationDenied`) the way the REST
  and MCP gates audit theirs: an authenticated account querying graphs it
  does not belong to is the enumeration signal the detective control is
  for, and a bare 403 leaves no trail. Repository denials are already
  audited inside `validate_repository_access`.
  """
  if graph_id == LIBRARY_GRAPH_ID:
    return

  from robosystems.config.shared_repositories import (
    is_shared_repository_or_subgraph,
  )
  from robosystems.middleware.auth.dependencies import _db_check_graph_access
  from robosystems.middleware.graph.utils import MultiTenantUtils

  user_id = str(user.id)

  if is_shared_repository_or_subgraph(graph_id):
    has_access = MultiTenantUtils.validate_repository_access(
      graph_id,
      user_id,
      "read",
    )
  else:
    has_access = _db_check_graph_access(user_id, graph_id)

  if not has_access:
    if not is_shared_repository_or_subgraph(graph_id):
      from robosystems.security import SecurityAuditLogger

      SecurityAuditLogger.log_authorization_denied(
        user_id=user_id,
        resource=f"graph_database:{graph_id}",
        action="read",
        ip_address=(
          request.client.host if request is not None and request.client else None
        ),
        endpoint=f"/extensions/{graph_id}/graphql",
      )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Access denied to graph: {graph_id}",
    )
