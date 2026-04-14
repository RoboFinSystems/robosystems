"""Position write operations."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  CreatePositionRequest,
  PositionResponse,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Portfolio, Position, Security
from robosystems.operations.roboinvestor.reads.positions import (
  enrich_positions,
  position_to_response,
)


class PortfolioNotFoundError(LookupError):
  """Raised when the position's target portfolio does not exist."""


class SecurityNotFoundError(LookupError):
  """Raised when the position's target security does not exist."""


class DuplicateActivePositionError(Exception):
  """Raised when an active position for this (portfolio, security) exists."""


def create_position(
  session: Session, body: CreatePositionRequest, created_by: str
) -> PositionResponse:
  """Create a position, validating portfolio and security existence.

  Raises `PortfolioNotFoundError`, `SecurityNotFoundError`, or
  `DuplicateActivePositionError` — caller maps to 404 / 409.
  """
  portfolio = session.execute(
    select(Portfolio).where(Portfolio.id == body.portfolio_id)
  ).scalar_one_or_none()
  if portfolio is None:
    raise PortfolioNotFoundError(body.portfolio_id)

  security = session.execute(
    select(Security).where(Security.id == body.security_id)
  ).scalar_one_or_none()
  if security is None:
    raise SecurityNotFoundError(body.security_id)

  entity = session.execute(
    select(Entity).where(Entity.id == security.entity_id)
  ).scalar_one_or_none()

  position = Position(
    portfolio_id=body.portfolio_id,
    security_id=body.security_id,
    quantity=body.quantity,
    quantity_type=body.quantity_type,
    cost_basis=body.cost_basis,
    currency=body.currency,
    current_value=body.current_value,
    valuation_date=body.valuation_date,
    valuation_source=body.valuation_source,
    acquisition_date=body.acquisition_date,
    notes=body.notes,
    created_by=created_by,
  )
  session.add(position)
  try:
    session.flush()
  except IntegrityError as exc:
    raise DuplicateActivePositionError(
      "An active position already exists for this security in this portfolio."
    ) from exc

  return position_to_response(
    position,
    security_name=security.name,
    entity_name=entity.name if entity else None,
  )


def update_position(
  session: Session, position_id: str, updates: dict[str, Any]
) -> PositionResponse | None:
  """Apply updates to a position. Returns None if the position does not exist."""
  row = session.execute(
    select(Position).where(Position.id == position_id)
  ).scalar_one_or_none()
  if row is None:
    return None

  for field, value in updates.items():
    setattr(row, field, value)

  session.flush()
  enriched = enrich_positions(session, [row])
  return enriched[0]


def soft_delete_position(session: Session, position_id: str) -> bool:
  """Soft-delete a position by setting its `status` to `"disposed"`.

  Returns `True` if the position existed, `False` otherwise.
  """
  row = session.execute(
    select(Position).where(Position.id == position_id)
  ).scalar_one_or_none()
  if row is None:
    return False
  row.status = "disposed"
  session.flush()
  return True
