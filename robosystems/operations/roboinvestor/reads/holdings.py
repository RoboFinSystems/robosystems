"""Holdings read operation — positions grouped by entity."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  HoldingResponse,
  HoldingSecuritySummary,
  HoldingsListResponse,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Portfolio, Position, Security


class PortfolioNotFoundError(LookupError):
  """Raised when the portfolio does not exist."""


def list_holdings(session: Session, portfolio_id: str) -> HoldingsListResponse:
  """Aggregate a portfolio's active positions into per-entity holdings.

  Raises `PortfolioNotFoundError` if the portfolio id is unknown so the
  caller can return a 404. Returns an empty `HoldingsListResponse` when
  the portfolio exists but has no active positions.
  """
  # Verify portfolio exists
  portfolio = session.execute(
    select(Portfolio).where(Portfolio.id == portfolio_id)
  ).scalar_one_or_none()
  if portfolio is None:
    raise PortfolioNotFoundError(portfolio_id)

  # Get all active positions for this portfolio
  positions = (
    session.execute(
      select(Position)
      .where(Position.portfolio_id == portfolio_id)
      .where(Position.status == "active")
    )
    .scalars()
    .all()
  )

  if not positions:
    return HoldingsListResponse(holdings=[], total_entities=0, total_positions=0)

  # Load securities
  security_ids = {p.security_id for p in positions}
  securities = (
    session.execute(select(Security).where(Security.id.in_(security_ids)))
    .scalars()
    .all()
  )
  sec_map = {s.id: s for s in securities}

  # Load entities (filter out None for unlinked securities)
  entity_ids = {s.entity_id for s in securities if s.entity_id}
  entities = (
    session.execute(select(Entity).where(Entity.id.in_(entity_ids))).scalars().all()
  )
  entity_map = {e.id: e for e in entities}

  # Group positions by entity (use "unlinked" key for securities without entity)
  entity_holdings: dict[str | None, list[Position]] = {}
  for p in positions:
    sec = sec_map.get(p.security_id)
    if sec:
      entity_holdings.setdefault(sec.entity_id, []).append(p)

  # Build response
  holdings: list[HoldingResponse] = []
  for entity_id, entity_positions in entity_holdings.items():
    entity = entity_map.get(entity_id) if entity_id else None
    meta = entity.metadata_ if entity and isinstance(entity.metadata_, dict) else {}

    security_summaries: list[HoldingSecuritySummary] = []
    total_cost = 0.0
    total_value = 0.0
    has_value = False

    for p in entity_positions:
      sec = sec_map.get(p.security_id)
      cost_dollars = float(p.cost_basis) / 100.0
      value_dollars = (
        float(p.current_value) / 100.0 if p.current_value is not None else None
      )
      total_cost += cost_dollars
      if value_dollars is not None:
        total_value += value_dollars
        has_value = True

      security_summaries.append(
        HoldingSecuritySummary(
          security_id=p.security_id,
          security_name=sec.name if sec else "Unknown",
          security_type=sec.security_type if sec else "unknown",
          quantity=p.quantity,
          quantity_type=p.quantity_type,
          cost_basis_dollars=cost_dollars,
          current_value_dollars=value_dollars,
        )
      )

    holdings.append(
      HoldingResponse(
        entity_id=entity_id or "unlinked",
        entity_name=entity.name if entity else "Unlinked Securities",
        source_graph_id=meta.get("source_graph_id"),
        securities=security_summaries,
        total_cost_basis_dollars=total_cost,
        total_current_value_dollars=total_value if has_value else None,
        position_count=len(entity_positions),
      )
    )

  # Sort by entity name
  holdings.sort(key=lambda h: h.entity_name)

  return HoldingsListResponse(
    holdings=holdings,
    total_entities=len(holdings),
    total_positions=len(positions),
  )
