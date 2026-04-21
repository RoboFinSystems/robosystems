"""Extensions GraphQL schema.

Assembles the Strawberry Schema served at
`/extensions/{graph_id}/graphql`. The top-level `Query` root is
composed from per-domain resolver classes (`LedgerQuery`,
`InvestorQuery`) **only when their respective feature flags are
enabled**, plus a `hello` probe for auth smoke-testing.

Per-domain gating means a ledger-only deployment exposes only
ledger fields (no `INVESTOR_NOT_INITIALIZED` runtime errors), and an
investor-only deployment exposes only investor fields. The router
itself is mounted on `ROBOLEDGER_ENABLED OR ROBOINVESTOR_ENABLED`, so if
both are off the schema is never served at all.

The endpoint is **graph-scoped at the URL level** — `graph_id` is a
path parameter, not a query argument. Resolvers read it via
`info.context["graph_id"]`. Auth + per-graph access are validated by
`get_context` before any resolver runs.

Strawberry defaults (`auto_camel_case=True`) mean Python snake_case
fields are exposed as camelCase on the wire, matching what the
TypeScript and Python SDK facades expect without manual reshape.
"""

from __future__ import annotations

import strawberry
from strawberry.extensions.tracing.opentelemetry import OpenTelemetryExtensionSync
from strawberry.types import Info

from robosystems.config import env
from robosystems.graphql.context import GraphQLContext, require_user
from robosystems.graphql.resolvers.information_block import InformationBlockQuery
from robosystems.graphql.resolvers.investor import InvestorQuery
from robosystems.graphql.resolvers.ledger import LedgerQuery
from robosystems.graphql.resolvers.library import LibraryQuery


@strawberry.type
class _BaseQuery:
  """Always-on probe field shared by every Query composition.

  Lives on its own type so the dynamically-built `Query` class can
  inherit it alongside whichever domain mixins are enabled. The probe
  is mounted in **all environments** (not dev-only) — see the `hello`
  resolver docstring for the rationale.
  """

  @strawberry.field
  def hello(self, info: Info[GraphQLContext, None]) -> str:
    """Permanent auth probe — verifies the endpoint is live and authenticated.

    Returns `"hello, {user.email}"` when the request carries valid
    credentials. Raises `UNAUTHENTICATED` (via `require_user`) when
    the request has no credentials. Combined with the auth contract
    in `get_context`, this gives clients a three-way liveness check:

    - **HTTP 200, `{ data: { hello: "hello, ..." } }`** → endpoint up,
      auth valid, user can reach the schema
    - **HTTP 200, `{ errors: [{ extensions: { code: "UNAUTHENTICATED" } }] }`**
      → endpoint up, no credentials presented (anonymous introspection
      is still allowed)
    - **HTTP 401** → endpoint up, credentials presented but invalid
      (expired token, bad API key)

    Kept as a permanent fixture rather than a dev-only scaffold so SDK
    codegen workflows, healthcheck integrations, and agent loops can
    rely on it. It has no business value beyond auth probing — it
    doesn't open a database session or touch any domain — so the
    runtime cost is negligible.
    """
    user = require_user(info)
    return f"hello, {user.email}"


def _build_query_type() -> type:
  """Build the Query root from whichever domain mixins are enabled.

  Skipping a disabled domain entirely (rather than exposing fields
  that would fail with `*_NOT_INITIALIZED` at runtime) keeps the
  introspection result honest and lets clients branch on the actual
  schema shape instead of trial-and-error against runtime errors.

  The library mixin (`LibraryQuery`) is always composed — it's the
  read surface for both the `graph_id="library"` sentinel (canonical
  browse of the public schema) and any tenant graph_id (tenant schema
  + public fallback via search_path). Library fields are not gated
  by a per-graph extension flag; data visibility is driven by the
  session's search_path, which is implicit in the URL's graph_id.

  `InformationBlockQuery` is always composed for the same reason — the
  Information Block construct is cross-domain (see
  `local/docs/specs/information-block.md`). Schedule blocks surface
  only on tenant graphs because their atoms live in the tenant schema;
  on the library sentinel only block types with
  `surfaces_in_library=True` surface (currently none — Schedule is
  tenant-only).
  """
  bases: tuple[type, ...] = (InformationBlockQuery, LibraryQuery, _BaseQuery)
  if env.ROBOLEDGER_ENABLED:
    bases = (LedgerQuery, *bases)
  if env.ROBOINVESTOR_ENABLED:
    bases = (InvestorQuery, *bases)
  return strawberry.type(type("Query", bases, {}))


Query = _build_query_type()

# OpenTelemetry instrumentation — Strawberry's built-in extension emits
# one span per GraphQL operation plus one child span per resolved field.
# Spans surface in the same OTel pipeline as the REST routes (the global
# tracer provider is set up in `middleware/otel/setup.py`), so GraphQL
# reads get the same Jaeger/Tempo coverage as `/extensions/*/operations/*`
# writes without any per-resolver decorator boilerplate.
#
# We use the `Sync` variant of the extension because (a) it works in both
# sync and async execution paths (important: tests run
# `schema.execute_sync(...)` which can't host async context managers), and
# (b) the sync path has no meaningful overhead in our mostly-I/O-bound
# resolvers. The async variant would force every test and every sync
# introspection call to fail with "GraphQL execution failed to complete
# synchronously."
schema = strawberry.Schema(
  query=Query,
  extensions=[OpenTelemetryExtensionSync],
)
