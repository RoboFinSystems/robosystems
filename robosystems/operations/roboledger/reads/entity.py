"""Read operations for the ledger entity (company/organization)."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.entity import LedgerEntityResponse
from robosystems.models.extensions import Entity


def entity_to_response(entity: Entity) -> LedgerEntityResponse:
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
    reporting_style_id=entity.reporting_style_id,
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


def resolve_parent_entity(session: Session) -> Entity | None:
  """The ledger's own entity: what every writer means by "the entity".

  Filtered by predicate because a tenant that received a shared report also
  holds the sender's entity as a linked row, and an unfiltered ``LIMIT 1`` can
  return it.
  """
  return (
    session.query(Entity)
    .filter(Entity.is_parent.is_(True), Entity.source != "linked")
    .order_by(Entity.created_at.asc())
    .first()
  )


def get_parent_entity(session: Session) -> LedgerEntityResponse | None:
  """The parent (non-linked) entity, or None if there is none yet."""
  entity = resolve_parent_entity(session)
  if entity is None:
    return None
  return entity_to_response(entity)


def list_entities(
  session: Session, source: str | None = None
) -> list[LedgerEntityResponse]:
  """List entities ordered by name, optionally filtered by source."""
  query = select(Entity)
  if source is not None:
    query = query.where(Entity.source == source)
  query = query.order_by(Entity.name)
  entities = session.execute(query).scalars().all()
  return [entity_to_response(e) for e in entities]
