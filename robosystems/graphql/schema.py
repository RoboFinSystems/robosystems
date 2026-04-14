"""Extensions GraphQL schema.

Assembles the Strawberry Schema served at /extensions/graphql. Read-heavy
extensions queries (ledger, investor, ...) live here; mutations stay on the
/v1/* REST surface.

Strawberry defaults (auto_camel_case=True) mean Python snake_case fields are
exposed as camelCase on the wire, matching what the TypeScript SDK expects
without any manual reshape.
"""

from __future__ import annotations

import strawberry
from sqlalchemy.exc import ProgrammingError
from strawberry.types import Info

from robosystems.graphql.auth import check_graph_access
from robosystems.graphql.context import GraphQLContext, require_user
from robosystems.graphql.types.ledger import LedgerEntity
from robosystems.models.api.extensions.entity import LedgerEntityResponse


@strawberry.type
class Query:
  """Root query type for the extensions GraphQL API."""

  @strawberry.field
  def hello(self, info: Info[GraphQLContext, None]) -> str:
    """Scaffolding query used to verify auth and the endpoint are live."""
    user = require_user(info)
    return f"hello, {user.email}"

  @strawberry.field
  def entity(
    self, info: Info[GraphQLContext, None], graph_id: strawberry.ID
  ) -> LedgerEntity | None:
    """Return the ledger entity (company/organization) for a graph.

    Mirrors `GET /v1/ledger/{graph_id}/entity`. Returns `null` when the
    ledger exists but has no entity yet, matching the REST 404 behavior at
    the typed-response level.
    """
    user = require_user(info)
    check_graph_access(user, str(graph_id))

    # Local imports keep this module importable without a running
    # extensions database.
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions import Entity

    try:
      with extensions_session(str(graph_id)) as session:
        entity_row = (
          session.query(Entity)
          .filter(Entity.is_parent.is_(True), Entity.source != "linked")
          .first()
        )
        if entity_row is None:
          return None
        return LedgerEntity.from_pydantic(_entity_to_response(entity_row))
    except (ValueError, ProgrammingError):
      # Ledger not initialized / session creation failed — mirror REST 404
      # semantics by returning null rather than raising.
      return None


def _entity_to_response(entity) -> LedgerEntityResponse:
  """Mirror of `routers/ledger/entity.py:_entity_to_response`.

  Kept locally so the GraphQL resolver has no import-time dependency on the
  router module. If this mapping ever drifts from REST, extract both into a
  shared helper under `operations/`.
  """
  return LedgerEntityResponse(
    id=entity.id,
    name=entity.name,
    legal_name=entity.legal_name,
    uri=entity.uri,
    cik=entity.cik,
    ticker=entity.ticker,
    exchange=entity.exchange,
    sic=entity.sic,
    sic_description=entity.sic_description,
    category=entity.category,
    state_of_incorporation=entity.state_of_incorporation,
    fiscal_year_end=entity.fiscal_year_end,
    tax_id=entity.tax_id,
    lei=entity.lei,
    industry=entity.industry,
    entity_type=entity.entity_type,
    phone=entity.phone,
    website=entity.website,
    status=entity.status,
    is_parent=entity.is_parent,
    parent_entity_id=entity.parent_entity_id,
    source=entity.source,
    source_id=entity.source_id,
    source_graph_id=(entity.metadata_ or {}).get("source_graph_id"),
    connection_id=entity.connection_id,
    address_line1=entity.address_line1,
    address_city=entity.address_city,
    address_state=entity.address_state,
    address_postal_code=entity.address_postal_code,
    address_country=entity.address_country,
    created_at=entity.created_at.isoformat() if entity.created_at else None,
    updated_at=entity.updated_at.isoformat() if entity.updated_at else None,
  )


schema = strawberry.Schema(query=Query)
