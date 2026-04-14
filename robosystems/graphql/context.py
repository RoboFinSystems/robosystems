"""Strawberry context builder for the extensions GraphQL endpoint.

Resolves the authenticated `User` from the FastAPI Request using the same
`get_current_user` helper the REST routers use. If auth fails, `user` is
set to None in the context — unauthenticated introspection queries are
allowed (so SDK codegen can fetch the schema without a key), but any real
resolver must call `require_user(info)` to enforce auth.

The per-graph access check does NOT happen here; it happens inside each
resolver via `check_graph_access(user, graph_id)`, because `graph_id`
comes from a GraphQL variable rather than a URL path parameter.
"""

from __future__ import annotations

from typing import TypedDict

import strawberry
from fastapi import HTTPException, Request, Security
from strawberry.types import Info

from robosystems.middleware.auth.dependencies import (
  API_KEY_HEADER,
  get_current_user,
)
from robosystems.models.core import User


class GraphQLContext(TypedDict):
  """Typed context passed to every Strawberry resolver.

  `user` is None for unauthenticated requests (permitted only for
  introspection queries); every data resolver must call `require_user`
  before reading it.
  """

  request: Request
  user: User | None


async def get_context(
  request: Request,
  api_key: str | None = Security(API_KEY_HEADER),
) -> GraphQLContext:
  """Strawberry `context_getter`. Passed directly to `GraphQLRouter`.

  Tries to authenticate the request; if that fails, returns the context
  with `user=None`. This lets unauthenticated introspection queries
  succeed (because they don't hit any resolver that calls `require_user`)
  while keeping real data queries behind auth.
  """
  try:
    user = await get_current_user(request, api_key or "")
  except HTTPException:
    user = None
  return {"request": request, "user": user}


def require_user(info: Info[GraphQLContext, None]) -> User:
  """Return the authenticated user, or raise a GraphQL error.

  Call this at the top of any resolver that returns real data. Unauth
  errors surface in the GraphQL `errors[]` array as normal, not as an
  HTTP 401 — the 401 semantics are reserved for introspection bypass.
  """
  user = info.context["user"]
  if user is None:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message="Authentication required",
      extensions={"code": "UNAUTHENTICATED"},
    )
  return user
