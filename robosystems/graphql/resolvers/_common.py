"""Shared helpers for GraphQL resolvers, kept in one place so bounds and
behavior can't drift between domains.

- Pagination guards (`resolve_pagination`, `validate_pagination`).
  Strawberry has no `Field(ge=…, le=…)` equivalent, so the bounds are
  asserted at the resolver boundary.
- `open_extensions_session` / `open_library_session` — the auth, extension
  gate, and session-open prelude every data resolver runs.
"""

from __future__ import annotations

import strawberry
from strawberry.types import Info

from robosystems.graphql.context import (
  GraphQLContext,
  require_graph_id,
  require_user,
)

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

  Raises `StrawberryGraphQLError` so callers get an `INVALID_PAGINATION`
  entry in `errors[]` rather than a 500 — the same shape as an
  authentication failure.
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
  """Raise `EXTENSION_NOT_PROVISIONED` if the graph lacks this extension.

  `get_context` validates auth and graph access but deliberately does not
  403 graphs that lack an extension — that would break the `hello` probe
  and introspection. Domain resolvers call this from their session opener
  so the first data field fails cleanly instead of falling through to a
  missing schema.

  `schema_extensions` is empty for anonymous introspection traffic, and
  `require_user` runs first, so this only fires for authenticated calls.

  This gate is intentionally weaker than the REST gate
  (`require_graph_extension` in `middleware/extensions.py`), which also
  rejects `graph_type == "repository"` so command writes can never land in
  a shared tenant schema. Repository graphs such as SEC declare
  `schema_extensions=["roboledger"]` so ledger-shaped *reads* work against
  shared data; adding a `graph_type` check here would break SEC GraphQL
  reads for every subscriber.
  """
  if extension not in info.context["schema_extensions"]:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=f"{extension} is not provisioned for this graph",
      extensions={"code": "EXTENSION_NOT_PROVISIONED"},
    )


def require_any_extension(
  info: Info[GraphQLContext, None], extensions: tuple[str, ...]
) -> None:
  """Like :func:`require_extension`, but any one of several will do.

  For reads whose data can arrive on a graph that never provisioned the
  extension that *owns* the table. A cross-graph report share writes ledger
  rows into the recipient's schema, so an investor-only tenant legitimately
  holds reports it can never author — gating those reads on `roboledger`
  would hide data the platform delivered on purpose. The tenant schema always
  has the tables (`provision_tenant_schema` creates all of them regardless of
  the graph's extensions), so this is a policy gate, not a structural one.
  """
  provisioned = info.context["schema_extensions"]
  if not any(extension in provisioned for extension in extensions):
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=(f"none of {', '.join(extensions)} is provisioned for this graph"),
      extensions={"code": "EXTENSION_NOT_PROVISIONED"},
    )


def open_extensions_session(info: Info[GraphQLContext, None], extension: str):
  """Shared auth, extension-gate, and extensions-session prelude.

  `get_context` already enforced auth and graph access, so `require_user`
  here only catches the anonymous-introspection case. `require_extension`
  keeps resolvers from opening a session on a graph that lacks the
  extension.
  """
  require_user(info)
  require_extension(info, extension)
  graph_id = require_graph_id(info)
  # Local import keeps this module importable without a running extensions DB.
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)


def open_extensions_session_for_any(
  info: Info[GraphQLContext, None], extensions: tuple[str, ...]
):
  """`open_extensions_session` for reads served to more than one domain.

  See :func:`require_any_extension` — the received-report surface is the case
  this exists for.
  """
  require_user(info)
  require_any_extension(info, extensions)
  graph_id = require_graph_id(info)
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)


def open_library_session(info: Info[GraphQLContext, None]):
  """Open an extensions session for library reads on any graph_id.

  Unlike `open_extensions_session`, this does not gate on a per-graph
  extension flag: library reads are safe on any authenticated graph because
  the rows returned are driven by the session's `search_path`.

  - `graph_id == "library"` → `search_path = public` → canonical library.
  - `graph_id == "kg…"` → `search_path = {schema}, public` → the tenant's
    library copy plus any tenant extensions of library tables (its own CoA
    elements and anchor associations).

  Access control is enforced by `check_graph_access` in `get_context`.
  """
  require_user(info)
  graph_id = require_graph_id(info)
  # Local import keeps this module importable without a running extensions DB.
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)
