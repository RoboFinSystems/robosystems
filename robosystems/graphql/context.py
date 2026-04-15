"""Strawberry context builder for the extensions GraphQL endpoint.

The endpoint is graph-scoped at `/extensions/{graph_id}/graphql`, so
`graph_id` is read from the URL path and put on the context dict
alongside the authenticated user. Resolvers then access both via
`info.context["graph_id"]` and `info.context["user"]` instead of
having to take `graph_id` as a query argument.

**Auth model:**

- `get_current_user` is called eagerly. If it fails because no
  credentials were presented at all, the user is left as `None` so
  unauthenticated **introspection** queries still work (see
  "Introspection policy" below).
- If credentials WERE presented but turned out to be invalid (bad API
  key, expired JWT, etc.), the underlying `HTTPException(401)` is
  re-raised so the caller sees a transport-layer 401 instead of a
  silent downgrade to anonymous.
- If a user IS authenticated, `check_graph_access` runs immediately.
  An access failure raises a 403 at the FastAPI layer, before
  Strawberry parses the query. This means resolvers never need to
  call `check_graph_access` themselves — it's already enforced.
- For real data queries, `require_user(info)` still raises a GraphQL
  error if no user is set (i.e. the request was unauthenticated).

**Introspection policy (deliberate decision):**

The schema is fully introspectable in **all environments** —
production included — without credentials. This is a deliberate
choice, not an oversight, and the trade-off is:

- ✅ SDK codegen (`graphql-codegen`, Strawberry's TypeScript
  generator, etc.) works out of the box against any deployment URL
  without provisioning credentials for the build pipeline.
- ✅ Tooling that walks the schema (GraphiQL, GraphQL playground,
  IDE plugins) Just Works for any developer hitting the public URL.
- ✅ The `hello` probe in `schema.py` gives unauthenticated callers
  a clean three-way liveness check (HTTP 200 + data, HTTP 200 +
  UNAUTHENTICATED error, HTTP 401) without leaking business data.
- ⚠️  Schema shape is **public information**. Field names, argument
  names, type names, and union members are all visible to anyone
  who can reach the endpoint. Don't bake secrets into type names.
- ⚠️  Per-domain feature flags (`ROBOLEDGER_ENABLED`,
  `ROBOINVESTOR_ENABLED`) gate the schema composition itself, so
  introspection on a ledger-only deployment exposes only ledger
  fields. The runtime schema honestly reflects what's enabled.

If you ever need to lock introspection down for a specific
deployment (e.g. an air-gapped install), the right places to add the
gate are:

1. Set `EXTENSIONS_GRAPHQL_ENABLED=false` to drop the entire
   GraphQL endpoint (the kill switch is exactly for this).
2. Or add an `extensions=[...]` argument to `strawberry.Schema(...)`
   in `schema.py` with a custom validator that rejects
   `__schema` / `__type` queries from unauthenticated requests.

Don't make introspection auth-gated by default — it breaks SDK
codegen and surprises tooling. The current "schema is public, data
is private" stance is the standard GraphQL posture.
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
  - **Auth contract**:

    1. **No credentials presented** (no `X-API-Key` header AND no
       `Authorization` header) → `user=None`. Unauthenticated
       introspection queries still work; data resolvers will fail at
       `require_user(info)` with a `UNAUTHENTICATED` GraphQL error.

    2. **Credentials presented but invalid** (bad API key, expired
       JWT, etc.) → re-raise the underlying `HTTPException(401)` so
       the caller sees a transport-level 401, NOT a 200 with a GraphQL
       error. Downgrading invalid credentials to anonymous would let
       expired tokens silently lose access while still appearing to
       work — a security regression vs. the REST endpoints.

    3. **Valid credentials** → `user` populated, `check_graph_access`
       enforced immediately (403 short-circuit), then the request
       reaches Strawberry.
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
