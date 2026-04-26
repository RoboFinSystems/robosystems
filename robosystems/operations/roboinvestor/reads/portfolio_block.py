"""Portfolio Block read operation — portfolio-centric molecule envelope."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  EntityLite,
  PortfolioBlockEnvelope,
  PositionBlock,
  SecurityLite,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Portfolio, Position, Security
from robosystems.operations.roboinvestor.reads.holdings import PortfolioNotFoundError


def _entity_to_lite(entity: Entity | None) -> EntityLite | None:
  if entity is None:
    return None
  meta = entity.metadata_ if isinstance(entity.metadata_, dict) else {}
  return EntityLite(
    id=entity.id,
    name=entity.name,
    source_graph_id=meta.get("source_graph_id"),
  )


def get_portfolio_block(session: Session, portfolio_id: str) -> PortfolioBlockEnvelope:
  """Assemble a Portfolio Block envelope.

  Raises `PortfolioNotFoundError` when the portfolio id is unknown so the
  caller can return a 404. Active positions only.
  """
  portfolio = session.execute(
    select(Portfolio).where(Portfolio.id == portfolio_id)
  ).scalar_one_or_none()
  if portfolio is None:
    raise PortfolioNotFoundError(portfolio_id)

  owner: Entity | None = None
  if portfolio.entity_id:
    owner = session.execute(
      select(Entity).where(Entity.id == portfolio.entity_id)
    ).scalar_one_or_none()

  positions = (
    session.execute(
      select(Position)
      .where(Position.portfolio_id == portfolio_id)
      .where(Position.status == "active")
    )
    .scalars()
    .all()
  )

  sec_map: dict = {}
  entity_map: dict = {}
  if positions:
    security_ids = {p.security_id for p in positions}
    securities = (
      session.execute(select(Security).where(Security.id.in_(security_ids)))
      .scalars()
      .all()
    )
    sec_map = {s.id: s for s in securities}

    issuer_ids = {s.entity_id for s in securities if s.entity_id}
    if issuer_ids:
      issuers = (
        session.execute(select(Entity).where(Entity.id.in_(issuer_ids))).scalars().all()
      )
      entity_map = {e.id: e for e in issuers}

  position_blocks: list[PositionBlock] = []
  total_cost = 0.0
  total_value = 0.0
  has_value = False

  for p in positions:
    sec = sec_map.get(p.security_id)
    issuer = entity_map.get(sec.entity_id) if sec and sec.entity_id else None

    cost_dollars = float(p.cost_basis) / 100.0
    value_dollars = (
      float(p.current_value) / 100.0 if p.current_value is not None else None
    )
    total_cost += cost_dollars
    if value_dollars is not None:
      total_value += value_dollars
      has_value = True

    if sec is None:
      continue

    position_blocks.append(
      PositionBlock(
        id=p.id,
        quantity=p.quantity,
        quantity_type=p.quantity_type,
        cost_basis_dollars=cost_dollars,
        current_value_dollars=value_dollars,
        valuation_date=p.valuation_date,
        valuation_source=p.valuation_source,
        acquisition_date=p.acquisition_date,
        status=p.status,
        notes=p.notes,
        security=SecurityLite(
          id=sec.id,
          name=sec.name,
          security_type=sec.security_type,
          security_subtype=sec.security_subtype,
          is_active=sec.is_active,
          issuer=_entity_to_lite(issuer),
          source_graph_id=sec.source_graph_id,
        ),
      )
    )

  return PortfolioBlockEnvelope(
    id=portfolio.id,
    name=portfolio.name,
    description=portfolio.description,
    strategy=portfolio.strategy,
    inception_date=portfolio.inception_date,
    base_currency=portfolio.base_currency,
    owner=_entity_to_lite(owner),
    positions=position_blocks,
    total_cost_basis_dollars=total_cost,
    total_current_value_dollars=total_value if has_value else None,
    active_position_count=len(position_blocks),
    created_at=portfolio.created_at,
    updated_at=portfolio.updated_at,
  )
