"""Position write operations.

Registrar-compatible signatures `(session, body, created_by=...)`;
update/delete declare `requires_created_by=False`. Missing rows raise
`PositionNotFoundError` so the registrar error map can surface 404.
`PortfolioNotFoundError` / `SecurityNotFoundError` are re-used from
their owning command modules to keep FK-miss translations unified.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  CreatePositionRequest,
  DeletePositionOperation,
  DeleteResult,
  PositionResponse,
  UpdatePositionOperation,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Portfolio, Position, Security
from robosystems.operations.roboinvestor.commands.portfolios import (
  PortfolioNotFoundError,
)
from robosystems.operations.roboinvestor.commands.securities import (
  SecurityNotFoundError,
)
from robosystems.operations.roboinvestor.reads.positions import (
  enrich_positions,
  position_to_response,
)

# Re-export FK-miss errors so callers (router, tests) can import them
# from a single location even though the types live on the owning module.
__all__ = [
  "DuplicateActivePositionError",
  "PortfolioNotFoundError",
  "PositionNotFoundError",
  "SecurityNotFoundError",
  "create_position",
  "soft_delete_position",
  "update_position",
]


class PositionNotFoundError(LookupError):
  """Raised when a referenced position does not exist."""


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
  session: Session, body: UpdatePositionOperation
) -> PositionResponse:
  """Apply updates to a position. Raises `PositionNotFoundError` if missing."""
  row = session.execute(
    select(Position).where(Position.id == body.position_id)
  ).scalar_one_or_none()
  if row is None:
    raise PositionNotFoundError(body.position_id)

  for field, value in body.model_dump(
    exclude_unset=True, exclude={"position_id"}
  ).items():
    setattr(row, field, value)

  session.flush()
  enriched = enrich_positions(session, [row])
  return enriched[0]


def soft_delete_position(
  session: Session, body: DeletePositionOperation
) -> DeleteResult:
  """Soft-delete a position (`status='disposed'`).

  Raises `PositionNotFoundError` if the row doesn't exist (registrar → 404).
  Historical holding records referencing it remain valid.
  """
  row = session.execute(
    select(Position).where(Position.id == body.position_id)
  ).scalar_one_or_none()
  if row is None:
    raise PositionNotFoundError(body.position_id)
  row.status = "disposed"
  session.flush()
  return DeleteResult(deleted=True)
