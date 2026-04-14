"""Extensions GraphQL schema.

Assembles the Strawberry Schema served at
`/extensions/{graph_id}/graphql`. The top-level `Query` root is
composed from per-domain resolver classes (`LedgerQuery`,
`InvestorQuery`) via multiple inheritance, plus a `hello` probe
for auth smoke-testing.

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
from strawberry.types import Info

from robosystems.graphql.context import GraphQLContext, require_user
from robosystems.graphql.resolvers.investor import InvestorQuery
from robosystems.graphql.resolvers.ledger import LedgerQuery


@strawberry.type
class Query(LedgerQuery, InvestorQuery):
  """Root query type for the extensions GraphQL API.

  Composed via multiple inheritance from per-domain query classes so
  each domain owns its own resolver file but the wire schema presents
  a single flat namespace (`entity`, `portfolios`, `trialBalance`, …).
  """

  @strawberry.field
  def hello(self, info: Info[GraphQLContext, None]) -> str:
    """Scaffolding query used to verify auth and the endpoint are live."""
    user = require_user(info)
    return f"hello, {user.email}"


schema = strawberry.Schema(query=Query)
