"""Shared helpers for GraphQL resolvers.

Lifted out of `ledger.py` and `investor.py` where they were duplicated
verbatim. Put them in one place so the bounds and behavior can't drift
between domains as more resolvers get added.

What lives here:

- **Pagination guards** (`_MIN_LIMIT`, `_MAX_LIMIT`, `_MIN_OFFSET`,
  `validate_pagination`) — Strawberry doesn't have a `Field(ge=…, le=…)`
  equivalent, so the bounds the retired REST endpoints enforced via
  FastAPI's `Query(..., ge=N, le=M)` get reasserted at the resolver
  boundary.

- **`open_extensions_session`** — the shared auth-then-open-session
  prelude every data resolver runs. Calls `require_user` to enforce
  the introspection bypass contract, reads `graph_id` from the URL via
  `require_graph_id`, then returns an `extensions_session(graph_id)`
  context manager.
"""

from __future__ import annotations

import strawberry
from strawberry.types import Info

from robosystems.graphql.context import (
  GraphQLContext,
  require_graph_id,
  require_user,
)

# Pagination bounds — mirror the retired REST `Query(..., ge=N, le=M)`.
_MIN_LIMIT = 1
_MAX_LIMIT = 1000
_MIN_OFFSET = 0


def resolve_pagination(
  limit: int | None, offset: int | None, *, default_limit: int
) -> tuple[int, int]:
  """Default null pagination args, then bounds-check.

  Generated SDK clients (graphql-codegen) pass explicit ``null`` for
  every omitted variable, and GraphQL rejects explicit null for a
  non-null argument even when it declares a default — so pagination
  args are declared nullable in the schema (``Int`` rather than
  ``Int! = N``) and defaulted here instead.
  """
  resolved_limit = default_limit if limit is None else limit
  resolved_offset = 0 if offset is None else offset
  validate_pagination(resolved_limit, resolved_offset)
  return resolved_limit, resolved_offset


def validate_pagination(limit: int, offset: int) -> None:
  """Reject out-of-range pagination args at the resolver boundary.

  Strawberry doesn't have a `Field(ge=…, le=…)` equivalent, so the
  bounds the retired REST endpoints enforced via FastAPI's
  `Query(..., ge=N, le=M)` are reasserted here. Raising a
  `StrawberryGraphQLError` surfaces a clean GraphQL error rather than
  a 500 — same shape as authentication failures.
  """
  if not _MIN_LIMIT <= limit <= _MAX_LIMIT:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=f"limit must be between {_MIN_LIMIT} and {_MAX_LIMIT}",
      extensions={"code": "INVALID_PAGINATION"},
    )
  if offset < _MIN_OFFSET:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=f"offset must be >= {_MIN_OFFSET}",
      extensions={"code": "INVALID_PAGINATION"},
    )


def require_extension(info: Info[GraphQLContext, None], extension: str) -> None:
  """Raise a GraphQL error if the graph hasn't been provisioned for this extension.

  Complements `require_user`: `get_context` has already validated auth
  and graph access, but it does NOT 403 graphs that lack a given
  extension (doing so would break the `hello` probe and schema
  introspection on under-provisioned graphs). Domain resolvers call
  this at the top of their shared session opener so the first data
  field surfaces a clean `EXTENSION_NOT_PROVISIONED` error instead of
  a confusing schema-missing fallthrough.

  The context's `schema_extensions` is an empty tuple for
  unauthenticated introspection traffic; combined with `require_user`
  running first, this branch only fires for authenticated calls on
  graphs where the extension genuinely isn't enabled.

  **Deliberate asymmetry with the REST gate** (`require_graph_extension`
  in `middleware/extensions.py`): the REST gate rejects
  `graph_type == "repository"` outright because command writes must
  never land in a shared tenant schema. This GraphQL gate does NOT
  apply that check — repository graphs (e.g. SEC) declare
  `schema_extensions=["roboledger"]` in their manifest precisely so
  ledger-shaped reads work against the shared data. Removing that
  asymmetry here would break SEC GraphQL queries for every
  subscriber. If a future "fix" adds a `graph_type` check, SEC read
  access dies with it.
  """
  if extension not in info.context["schema_extensions"]:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=f"{extension} is not provisioned for this graph",
      extensions={"code": "EXTENSION_NOT_PROVISIONED"},
    )


def open_extensions_session(info: Info[GraphQLContext, None], extension: str):
  """Shared auth + extension-gate + extensions-session prelude.

  Auth + graph access were enforced by `get_context` before this is
  ever reached — `require_user` here only catches the introspection
  bypass case (no API key supplied at all). `require_extension`
  enforces the per-domain feature gate so resolvers never open a
  session on a graph that lacks the extension. `graph_id` is read
  from the request URL via `require_graph_id`.
  """
  require_user(info)
  require_extension(info, extension)
  graph_id = require_graph_id(info)
  # Local import keeps this module importable without a running extensions DB.
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)


def open_library_session(info: Info[GraphQLContext, None]):
  """Open an extensions session for library reads on any graph_id.

  Unlike `open_extensions_session`, this does NOT gate on a per-graph
  extension flag. Library reads are safe on any authenticated graph
  because the returned rows are driven by the session's `search_path`:

  - `graph_id == "library"` → `search_path = public` → canonical library.
  - `graph_id == "kg…"`     → `search_path = {schema}, public` →
    tenant's library copy + any tenant extensions of library tables
    (e.g. the tenant's own CoA elements and anchor associations).

  Access control is already enforced by `check_graph_access` in
  `get_context`. The extension gate was defensive but blocked the
  roboledger-app `/library` page (entity-graph scope) from resolving
  library fields without adding security.
  """
  require_user(info)
  graph_id = require_graph_id(info)
  # Local import keeps this module importable without a running extensions DB.
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)
