"""Strawberry context builder for the extensions GraphQL endpoint.

Introspection is open in every environment (schema shape is public, data is
private), so never encode secrets in type or field names.
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
  _stash_api_key_identity,
  get_current_user,
)
from robosystems.middleware.auth.utils import validate_api_key_with_graph
from robosystems.middleware.billing.enforcement import require_graph_access
from robosystems.middleware.extensions import load_graph_metadata
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils.subgraph import is_subgraph
from robosystems.models.core import User


class GraphQLContext(TypedDict):
  """Per-request resolver context.

  `user` is None for anonymous introspection, where `schema_extensions` and
  `graph_type` are also empty. Graph access is enforced before it is built.
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
  """Strawberry `context_getter`.

  No credentials → `user=None` (introspection only). Invalid credentials →
  a real 401, never a downgrade to anonymous. Valid → graph access checked
  (403) and graph metadata loaded once. Subgraph IDs are always 403.
  """
  has_credentials = bool(api_key) or bool(request.headers.get("Authorization"))

  try:
    user = await get_current_user(request, api_key or "")
  except HTTPException:
    if not has_credentials:
      user = None
    elif api_key and not request.headers.get("Authorization"):
      # A graph-scoped key is refused by `get_current_user` by design; the URL
      # scopes this endpoint to one graph, so validate it against that graph
      # as the REST operations and MCP surfaces do.
      user = validate_api_key_with_graph(api_key, graph_id, db)
      if user is None:
        raise
      # Publish the principal, which `get_current_user` would have done.
      _stash_api_key_identity(request, api_key, user)
    else:
      raise

  # A subgraph has no extensions schema. Checked after auth (bad credentials
  # still 401) and before `check_graph_access`, which would resolve to the
  # parent. A non-`kg` prefix test would wrongly catch `library` and shared
  # repositories.
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
    check_graph_access(user, graph_id, request)
    # Library sentinel: no graph row to load.
    if graph_id == LIBRARY_GRAPH_ID:
      schema_extensions = (LIBRARY_GRAPH_ID,)
      graph_type = LIBRARY_GRAPH_ID
    else:
      # A missing graph row surfaces as access denied, to avoid enumeration.
      meta = load_graph_metadata(graph_id, db)
      schema_extensions = meta.schema_extensions
      graph_type = meta.graph_type
      # Suspended or expired graphs are unreadable here too, as on REST.
      require_graph_access(graph_id, db, require_write=False)

  return {
    "request": request,
    "user": user,
    "graph_id": graph_id,
    "schema_extensions": schema_extensions,
    "graph_type": graph_type,
  }


def require_user(info: Info[GraphQLContext, None]) -> User:
  """Return the user or raise `UNAUTHENTICATED` in `errors[]` (HTTP 401 is
  reserved for invalid credentials).
  """
  user = info.context["user"]
  if user is None:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message="Authentication required",
      extensions={"code": "UNAUTHENTICATED"},
    )
  return user


def require_graph_id(info: Info[GraphQLContext, None]) -> str:
  """Return the `graph_id` from the request URL."""
  return info.context["graph_id"]
