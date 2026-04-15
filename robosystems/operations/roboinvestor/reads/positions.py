"""Position read operations."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.investor import (
  PositionListResponse,
  PositionResponse,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Position, Security


def position_to_response(
  row: Position,
  security_name: str | None = None,
  entity_name: str | None = None,
) -> PositionResponse:
  """Map a Position row to the wire-facing PositionResponse."""
  return PositionResponse(
    id=row.id,
    portfolio_id=row.portfolio_id,
    security_id=row.security_id,
    security_name=security_name,
    entity_name=entity_name,
    quantity=row.quantity,
    quantity_type=row.quantity_type,
    cost_basis=row.cost_basis,
    cost_basis_dollars=float(row.cost_basis) / 100.0,
    currency=row.currency,
    current_value=row.current_value,
    current_value_dollars=(
      float(row.current_value) / 100.0 if row.current_value is not None else None
    ),
    valuation_date=row.valuation_date,
    valuation_source=row.valuation_source,
    acquisition_date=row.acquisition_date,
    disposition_date=row.disposition_date,
    status=row.status,
    notes=row.notes,
    created_at=row.created_at,
    updated_at=row.updated_at,
  )


def enrich_positions(session: Session, rows: list[Position]) -> list[PositionResponse]:
  """Batch-load security and entity names for a list of positions."""
  security_ids = {r.security_id for r in rows}
  sec_map: dict[str, Security] = {}
  entity_map: dict[str, str] = {}

  if security_ids:
    securities = (
      session.execute(select(Security).where(Security.id.in_(security_ids)))
      .scalars()
      .all()
    )
    sec_map = {str(s.id): s for s in securities}

    entity_ids = {s.entity_id for s in securities if s.entity_id}
    if entity_ids:
      entities = (
        session.execute(select(Entity).where(Entity.id.in_(entity_ids))).scalars().all()
      )
      entity_map = {str(e.id): str(e.name) for e in entities}

  results: list[PositionResponse] = []
  for r in rows:
    sec = sec_map.get(r.security_id)
    results.append(
      position_to_response(
        r,
        security_name=sec.name if sec else None,
        entity_name=entity_map.get(sec.entity_id) if sec else None,
      )
    )
  return results


def list_positions(
  session: Session,
  *,
  portfolio_id: str | None = None,
  security_id: str | None = None,
  status: str | None = None,
  limit: int = 100,
  offset: int = 0,
) -> PositionListResponse:
  """List positions filtered by portfolio / security / status, paginated."""
  query = select(Position)
  count_query = select(func.count()).select_from(Position)

  if portfolio_id is not None:
    query = query.where(Position.portfolio_id == portfolio_id)
    count_query = count_query.where(Position.portfolio_id == portfolio_id)
  if security_id is not None:
    query = query.where(Position.security_id == security_id)
    count_query = count_query.where(Position.security_id == security_id)
  if status is not None:
    query = query.where(Position.status == status)
    count_query = count_query.where(Position.status == status)

  total = session.execute(count_query).scalar() or 0
  rows = (
    session.execute(
      query.order_by(Position.created_at.desc()).offset(offset).limit(limit)
    )
    .scalars()
    .all()
  )

  return PositionListResponse(
    positions=enrich_positions(session, rows),
    pagination=create_pagination_info(total, limit, offset),
  )


def get_position(session: Session, position_id: str) -> PositionResponse | None:
  """Return a single enriched position by id, or None if not found."""
  row = session.execute(
    select(Position).where(Position.id == position_id)
  ).scalar_one_or_none()
  if row is None:
    return None
  enriched = enrich_positions(session, [row])
  return enriched[0]
