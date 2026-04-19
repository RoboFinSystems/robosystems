"""GraphQL-side authorization helpers.

The extensions GraphQL endpoint is mounted at
`/extensions/{graph_id}/graphql`, so `graph_id` is a **URL path
parameter**, not a query argument. `get_context` reads it via FastAPI's
`Path(...)` dependency before Strawberry parses the query, then calls
`check_graph_access` eagerly as part of building the context. By the
time any resolver runs, access has already been enforced — resolvers
never need to call this helper themselves.

This mirrors the access logic in `get_current_user_with_graph`
(`robosystems/middleware/auth/dependencies.py`) — specifically the
portion that runs after the user has already been authenticated.
"""

from __future__ import annotations

from fastapi import HTTPException, status

from robosystems.models.core import User


def check_graph_access(user: User, graph_id: str) -> None:
  """Verify the authenticated user has read access to the given graph_id.

  Raises:
      HTTPException: 403 if the user lacks access to the graph.

  Handles both shared repositories (SEC, etc.) and personal user graphs,
  mirroring the logic in `get_current_user_with_graph`.

  The `library` sentinel routes to the taxonomy library (shared reference
  material). Any authenticated user has read access — no per-graph ACL
  applies to library content.
  """
  # Library sentinel: accessible to any authenticated user. No ACL check.
  if graph_id == "library":
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
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Access denied to graph: {graph_id}",
    )
