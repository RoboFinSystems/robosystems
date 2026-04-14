"""Strawberry context builder for the extensions GraphQL endpoint.

The endpoint is graph-scoped at `/extensions/{graph_id}/graphql`, so
`graph_id` is read from the URL path and put on the context dict
alongside the authenticated user. Resolvers then access both via
`info.context["graph_id"]` and `info.context["user"]` instead of
having to take `graph_id` as a query argument.

**Auth model:**

- `get_current_user` is called eagerly. If it fails (no API key, bad
  token, etc.) the user is left as `None` so unauthenticated
  **introspection** queries still work (used by SDK codegen).
- If a user IS authenticated, `check_graph_access` runs immediately.
  An access failure raises a 403 at the FastAPI layer, before
  Strawberry parses the query. This means resolvers never need to
  call `check_graph_access` themselves — it's already enforced.
- For real data queries, `require_user(info)` still raises a GraphQL
  error if no user is set (i.e. the request was unauthenticated).
"""

from __future__ import annotations

from typing import TypedDict

import strawberry
from fastapi import HTTPException, Path, Request, Security
from strawberry.types import Info

from robosystems.graphql.auth import check_graph_access
from robosystems.middleware.auth.dependencies import (
  API_KEY_HEADER,
  get_current_user,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.models.core import User


class GraphQLContext(TypedDict):
  """Typed context passed to every Strawberry resolver.

  `user` is `None` for unauthenticated requests (permitted only for
  introspection queries); every data resolver must call `require_user`
  before reading it. `graph_id` always matches the URL path
  parameter — auth+access are enforced before the context is built,
  so resolvers can trust it.
  """

  request: Request
  user: User | None
  graph_id: str


async def get_context(
  request: Request,
  api_key: str | None = Security(API_KEY_HEADER),
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
) -> GraphQLContext:
  """Strawberry `context_getter`. Passed directly to `GraphQLRouter`.

  - Reads `graph_id` from the URL path parameter.
  - Tries to authenticate the request. Auth failures leave `user=None`
    so unauthenticated introspection still works.
  - For authenticated users, validates per-graph access immediately —
    a 403 here short-circuits the request before Strawberry runs.
  """
  try:
    user = await get_current_user(request, api_key or "")
  except HTTPException:
    user = None

  if user is not None:
    check_graph_access(user, graph_id)

  return {"request": request, "user": user, "graph_id": graph_id}


def require_user(info: Info[GraphQLContext, None]) -> User:
  """Return the authenticated user, or raise a GraphQL error.

  Call this at the top of any resolver that returns real data.
  Unauthenticated errors surface in the GraphQL `errors[]` array
  rather than as an HTTP 401 — the 401 semantics are reserved for
  introspection bypass at the context-getter layer.
  """
  user = info.context["user"]
  if user is None:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message="Authentication required",
      extensions={"code": "UNAUTHENTICATED"},
    )
  return user


def require_graph_id(info: Info[GraphQLContext, None]) -> str:
  """Return the `graph_id` from the request URL.

  Always set by `get_context` from the path parameter; this helper
  exists so resolvers don't sprinkle `info.context["graph_id"]`
  literals everywhere.
  """
  return info.context["graph_id"]
