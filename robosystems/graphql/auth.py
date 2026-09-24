"""GraphQL graph-access check, called by `get_context` (never by resolvers).

Mirrors the post-authentication part of `get_current_user_with_graph`.
"""

from __future__ import annotations

from fastapi import HTTPException, Request, status

from robosystems.db.extensions import LIBRARY_GRAPH_ID
from robosystems.models.core import User


def check_graph_access(
  user: User, graph_id: str, request: Request | None = None
) -> None:
  """403 unless the user can read `graph_id`; `library` is open to any user.

  User-graph denials are audited (the enumeration signal); repository
  denials are audited inside `validate_repository_access`.
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
