"""Extensions GraphQL schema.

Assembles the Strawberry Schema served at `/extensions/{graph_id}/graphql`.
The top-level `Query` root composes the per-domain resolver classes
(`LedgerQuery`, `InvestorQuery`) only when their feature flags are enabled,
plus a `hello` auth probe. A ledger-only deployment therefore exposes only
ledger fields rather than fields that fail at runtime. The router mounts on
`ROBOLEDGER_ENABLED OR ROBOINVESTOR_ENABLED`, so with both off the schema is
never served.

The endpoint is graph-scoped at the URL level: `graph_id` is a path
parameter, not a query argument, and resolvers read it via
`info.context["graph_id"]`. Auth and per-graph access are validated by
`get_context` before any resolver runs.

Strawberry's `auto_camel_case=True` default exposes Python snake_case fields
as camelCase on the wire, matching what the TypeScript and Python SDK
facades expect.
"""

from __future__ import annotations

import inspect

import strawberry
from strawberry.extensions import (
  MaxAliasesLimiter,
  MaxTokensLimiter,
  QueryDepthLimiter,
)
from strawberry.extensions.tracing.opentelemetry import OpenTelemetryExtensionSync
from strawberry.types import Info

from robosystems.config import env
from robosystems.graphql.context import GraphQLContext, require_user
from robosystems.graphql.execution import MaskUnexpectedErrors, OffloadSyncResolvers
from robosystems.graphql.resolvers.information_block import InformationBlockQuery
from robosystems.graphql.resolvers.investor import InvestorQuery
from robosystems.graphql.resolvers.ledger import LedgerQuery
from robosystems.graphql.resolvers.library import LibraryQuery
from robosystems.graphql.resolvers.taxonomy_block import TaxonomyBlockQuery


@strawberry.type
class _BaseQuery:
  """Always-on probe field shared by every Query composition.

  Lives on its own type so the dynamically-built `Query` class can inherit
  it alongside whichever domain mixins are enabled. Mounted in all
  environments, not just dev.
  """

  @strawberry.field
  def hello(self, info: Info[GraphQLContext, None]) -> str:
    """Auth probe: returns `hello, {user.email}` for a valid request.

    Opens no database session and touches no domain, so it gives clients a
    three-way liveness check:

    - HTTP 200 with `data.hello` → endpoint up, credentials valid
    - HTTP 200 with an `UNAUTHENTICATED` error → endpoint up, no
      credentials presented (anonymous introspection is still allowed)
    - HTTP 401 → endpoint up, credentials presented but invalid
    """
    user = require_user(info)
    return f"hello, {user.email}"


def _describe_from_docstrings(cls: type) -> type:
  """Publish each resolver's docstring as its GraphQL field description.

  Strawberry reads a description only from an explicit
  ``@strawberry.field(description=...)``; it ignores ``__doc__``. Every
  resolver here already carries a docstring written as API copy, so without
  this pass the 66 entry points ship with no description at all while the
  types they return are richly documented — the index into the schema blank
  and the shapes it points at full.

  Running it once over the composed root means a resolver is documented the
  moment someone writes an ordinary docstring, on every surface that reads
  the schema: GraphiQL, introspection, the SDK snapshot, the
  ``get-graphql-schema`` MCP tool and the published reference. An explicit
  ``description=`` still wins, so a field whose public wording should differ
  from its docstring can say so.

  **A resolver docstring is therefore public API copy.** Implementation
  notes, retired endpoints and app-internal call sites belong in comments
  inside the function, not in the docstring.
  """
  for field in cls.__strawberry_definition__.fields:
    if field.description:
      continue
    resolver = field.base_resolver
    func = getattr(resolver, "wrapped_func", None) if resolver is not None else None
    doc = inspect.getdoc(func) if func is not None else None
    if doc:
      field.description = doc
  return cls


def _build_query_type() -> type:
  """Build the Query root from whichever domain mixins are enabled.

  Dropping a disabled domain entirely, rather than exposing fields that
  fail with `*_NOT_INITIALIZED` at runtime, keeps introspection honest so
  clients can branch on the schema shape.

  `LibraryQuery`, `InformationBlockQuery`, and `TaxonomyBlockQuery` are
  always composed: they are cross-domain and not gated by a per-graph
  extension flag. Their data visibility is driven by the session
  `search_path`, which follows from the URL's `graph_id` — the `library`
  sentinel browses the public schema, a tenant graph_id sees tenant rows
  with public fallback.
  """
  bases: tuple[type, ...] = (
    InformationBlockQuery,
    TaxonomyBlockQuery,
    LibraryQuery,
    _BaseQuery,
  )
  if env.ROBOLEDGER_ENABLED:
    bases = (LedgerQuery, *bases)
  if env.ROBOINVESTOR_ENABLED:
    bases = (InvestorQuery, *bases)
  return _describe_from_docstrings(strawberry.type(type("Query", bases, {})))


Query = _build_query_type()

# The limiters bound query cost against the small extensions OLTP pool, where
# each resolved field can open a session. They are passed as factory callables
# (not instances) per Strawberry's per-request construction contract.
# Introspection is unaffected — the depth limiter does not count introspection
# fields — so SDK codegen still works.
#
# OpenTelemetry spans land in the same pipeline as the REST routes (tracer
# provider set up in `middleware/otel/setup.py`). The `Sync` variant works on
# both sync and async execution paths; the async variant breaks
# `schema.execute_sync(...)`, which cannot host async context managers.
#
# `OffloadSyncResolvers` must come after the OpenTelemetry extension: the
# resolve hooks compose with the last extension outermost, and the resolver
# span has to open inside the worker thread around the actual work rather
# than around a coroutine handle. `MaskUnexpectedErrors` scrubs what reaches
# `errors[]` — see `graphql/execution.py` for both.
schema = strawberry.Schema(
  query=Query,
  extensions=[
    lambda: QueryDepthLimiter(max_depth=env.EXTENSIONS_GRAPHQL_MAX_DEPTH),
    lambda: MaxAliasesLimiter(max_alias_count=env.EXTENSIONS_GRAPHQL_MAX_ALIASES),
    lambda: MaxTokensLimiter(max_token_count=env.EXTENSIONS_GRAPHQL_MAX_TOKENS),
    OpenTelemetryExtensionSync,
    OffloadSyncResolvers,
    MaskUnexpectedErrors,
  ],
)
