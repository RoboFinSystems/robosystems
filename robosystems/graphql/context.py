"""Strawberry context builder for the extensions GraphQL endpoint.

The endpoint is graph-scoped at `/extensions/{graph_id}/graphql`, so
`graph_id` comes from the URL path and lands on the context dict alongside
the authenticated user. Resolvers read both from `info.context` instead of
taking `graph_id` as a query argument.

Auth model:

- `get_current_user` runs eagerly. When no credentials are presented at
  all, the user is left as `None` so unauthenticated introspection still
  works.
- When credentials are presented but fail to validate, the underlying
  `HTTPException(401)` is re-raised — an expired token must not silently
  downgrade to anonymous.
- With a valid user, `check_graph_access` runs before Strawberry parses
  the query, so resolvers never call it themselves.
- Data resolvers call `require_user(info)`, which raises an
  `UNAUTHENTICATED` GraphQL error when the request was anonymous.

Introspection is open in every environment, production included: schema
shape is public, data is private. That keeps SDK codegen and
schema-walking tooling working against any deployment URL without
provisioning build-pipeline credentials, so don't encode secrets in type
or field names. Per-domain feature flags gate schema composition, so
introspection reflects only what a given deployment has enabled. To close
introspection off, either set `EXTENSIONS_GRAPHQL_ENABLED=false` to drop
the endpoint entirely, or add a validator extension in `schema.py` that
rejects `__schema` / `__type` for unauthenticated requests.
"""

from __future__ import annotations

from typing import TypedDict

import strawberry
from fastapi import Depends, HTTPException, Path, Request, Security
from sqlalchemy.orm import Session
from strawberry.types import Info

from robosystems.database import get_db_session
from robosystems.db.extensions import LIBRARY_GRAPH_ID
from robosystems.graphql.auth import check_graph_access
from robosystems.middleware.auth.dependencies import (
  API_KEY_HEADER,
  get_current_user,
)
from robosystems.middleware.extensions import load_graph_metadata
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils.subgraph import is_subgraph
from robosystems.models.core import User


class GraphQLContext(TypedDict):
  """Typed context passed to every Strawberry resolver.

  `user` is `None` for unauthenticated requests (introspection only); data
  resolvers must call `require_user` before reading it. `graph_id` always
  matches the URL path parameter, and access is enforced before the context
  is built, so resolvers can trust it.

  `schema_extensions` and `graph_type` are loaded from the platform DB once
  per request after auth passes, and stay empty for anonymous introspection
  traffic. Resolvers gate on them through `require_extension(info, ...)`,
  which raises `EXTENSION_NOT_PROVISIONED`.
  """

  request: Request
  user: User | None
  graph_id: str
  schema_extensions: tuple[str, ...]
  graph_type: str


async def get_context(
  request: Request,
  api_key: str | None = Security(API_KEY_HEADER),
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  db: Session = Depends(get_db_session),
) -> GraphQLContext:
  """Strawberry `context_getter`, passed directly to `GraphQLRouter`.

  Auth contract:

  1. No credentials (neither `X-API-Key` nor `Authorization`) → `user=None`.
     Introspection works; data resolvers fail at `require_user(info)` with
     an `UNAUTHENTICATED` GraphQL error.
  2. Credentials present but invalid → re-raise `HTTPException(401)` so the
     caller sees a transport-level 401 rather than a 200 carrying a GraphQL
     error. Downgrading invalid credentials to anonymous would let expired
     tokens lose access while still appearing to work.
  3. Valid credentials → `check_graph_access` short-circuits with 403 on
     denial, then the graph row is loaded once so resolvers can gate on
     `schema_extensions` / `graph_type` without a second lookup.

  Subgraph IDs are rejected with 403 regardless of auth: this is the
  extensions domain-app surface, not a subgraph modality.
  """
  has_credentials = bool(api_key) or bool(request.headers.get("Authorization"))

  try:
    user = await get_current_user(request, api_key or "")
  except HTTPException:
    if has_credentials:
      # Bad/expired credentials → real transport-layer auth failure.
      raise
    # No credentials at all → anonymous fallthrough for introspection.
    user = None

  # Subgraph guard: a subgraph is a modality container (scratch/knowledge),
  # not an extensions domain target — a `{parent}_{name}` graph_id has no
  # extensions schema, so reject it outright rather than resolve
  # subgraph→parent and dead-end in schema resolution. Runs after auth (bad
  # credentials still 401, and denials stay attributable) and before
  # `check_graph_access`, which would resolve to the parent. Uses
  # `is_subgraph` rather than a non-`kg` prefix test, which would break the
  # `library` sentinel and shared repositories.
  if is_subgraph(graph_id):
    if user is not None:
      from robosystems.security import SecurityAuditLogger

      SecurityAuditLogger.log_authorization_denied(
        user_id=str(user.id),
        resource=graph_id,
        action="graphql",
        ip_address=request.client.host if request.client else None,
        endpoint=f"/extensions/{graph_id}/graphql",
      )
    raise HTTPException(
      status_code=403,
      detail=(
        f"Subgraph '{graph_id}' is not addressable via the extensions "
        "GraphQL endpoint; target the parent graph."
      ),
    )

  schema_extensions: tuple[str, ...] = ()
  graph_type: str = ""
  if user is not None:
    check_graph_access(user, graph_id)
    # Library sentinel — no graph row to load, no per-graph metadata.
    # The `library` extension is always "enabled" for this sentinel.
    if graph_id == LIBRARY_GRAPH_ID:
      schema_extensions = (LIBRARY_GRAPH_ID,)
      graph_type = LIBRARY_GRAPH_ID
    else:
      # Load once per request; resolvers read from context, never re-hit the
      # DB. Same 403 semantics as check_graph_access: missing graph rows
      # surface as "access denied" to avoid enumeration.
      meta = load_graph_metadata(graph_id, db)
      schema_extensions = meta.schema_extensions
      graph_type = meta.graph_type

  return {
    "request": request,
    "user": user,
    "graph_id": graph_id,
    "schema_extensions": schema_extensions,
    "graph_type": graph_type,
  }


def require_user(info: Info[GraphQLContext, None]) -> User:
  """Return the authenticated user, or raise a GraphQL error.

  Call this at the top of any resolver that returns real data.
  Unauthenticated errors surface in the GraphQL `errors[]` array rather
  than as an HTTP 401; 401 is reserved for invalid credentials at the
  context-getter layer.
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

  Always set by `get_context` from the path parameter; this helper keeps
  `info.context["graph_id"]` literals out of resolvers.
  """
  return info.context["graph_id"]
