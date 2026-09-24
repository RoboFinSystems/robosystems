"""Shared resolver helpers: pagination guards and the auth/extension-gate/
session prelude every data resolver runs.
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

  The args are nullable in the schema (not `Int! = N`) because codegen clients
  send explicit `null` for omitted variables, which a non-null arg rejects.
  """
  resolved_limit = default_limit if limit is None else limit
  resolved_offset = 0 if offset is None else offset
  validate_pagination(resolved_limit, resolved_offset)
  return resolved_limit, resolved_offset


def validate_pagination(limit: int, offset: int) -> None:
  """Raise `INVALID_PAGINATION` (in `errors[]`, not a 500) when out of range."""
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

  Not done in `get_context`, which would break the `hello` probe and
  introspection. Deliberately weaker than the REST gate: no
  `graph_type == "repository"` check, because SEC declares `roboledger` so
  ledger-shaped reads work against shared data.
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

  For reads whose rows can arrive via cross-graph share on a graph that never
  provisioned the owning extension. A policy gate only: every tenant schema
  has all the tables.
  """
  provisioned = info.context["schema_extensions"]
  if not any(extension in provisioned for extension in extensions):
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=(f"none of {', '.join(extensions)} is provisioned for this graph"),
      extensions={"code": "EXTENSION_NOT_PROVISIONED"},
    )


def open_extensions_session(info: Info[GraphQLContext, None], extension: str):
  """Auth, extension-gate, and extensions-session prelude.

  `require_user` here only catches anonymous introspection; `get_context`
  already enforced graph access.
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
  """`open_extensions_session` gated by :func:`require_any_extension`."""
  require_user(info)
  require_any_extension(info, extensions)
  graph_id = require_graph_id(info)
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)


def open_library_session(info: Info[GraphQLContext, None]):
  """Extensions session for library reads, with no extension gate.

  Rows follow the `search_path`: `library` sees `public`, a tenant sees its
  own schema then `public`.
  """
  require_user(info)
  graph_id = require_graph_id(info)
  # Local import keeps this module importable without a running extensions DB.
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)
