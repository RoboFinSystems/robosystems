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


def open_extensions_session(info: Info[GraphQLContext, None]):
  """Shared auth + extensions-session prelude for every data resolver.

  Auth + graph access were enforced by `get_context` before this is
  ever reached — `require_user` here only catches the introspection
  bypass case (no API key supplied at all). `graph_id` is read from
  the request URL via `require_graph_id`.
  """
  require_user(info)
  graph_id = require_graph_id(info)
  # Local import keeps this module importable without a running extensions DB.
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)
