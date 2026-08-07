"""Portfolio Block write operations — molecule-level command surface.

:func:`create_portfolio_block`, :func:`update_portfolio_block` and
:func:`delete_portfolio_block` each validate the entire envelope before any
DB write and apply all changes atomically — partial failures roll back via
the caller's session.

`PortfolioNotFoundError` is imported from
``robosystems.operations.roboinvestor.reads.holdings`` so the read and write
surfaces share one 404 sentinel.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  CreatePortfolioBlockRequest,
  DeletePortfolioBlockOperation,
  DeletePortfolioBlockResponse,
  PortfolioBlockEnvelope,
  PortfolioBlockPositionAdd,
  UpdatePortfolioBlockOperation,
)
from robosystems.models.extensions.roboinvestor import Portfolio, Position, Security
from robosystems.operations.roboinvestor.reads.holdings import PortfolioNotFoundError
from robosystems.operations.roboinvestor.reads.portfolio_block import (
  get_portfolio_block,
)


class SecurityNotFoundError(LookupError):
  """Raised when an `add` delta references an unknown security_id."""


class PositionNotFoundError(LookupError):
  """Raised when an `update` or `dispose` delta references an unknown position id."""


class DuplicateActivePositionError(Exception):
  """Raised when an `add` would violate the unique active (portfolio,security) index."""


class PositionPortfolioMismatchError(Exception):
  """Raised when an `update`/`dispose` targets a position outside this portfolio."""


class ActivePositionsRequireConfirmationError(Exception):
  """Raised when delete-portfolio-block has active positions but no confirmation flag."""

  def __init__(self, active_count: int) -> None:
    super().__init__(
      f"Portfolio has {active_count} active position(s); set "
      f"confirm_active_positions=true to cascade-delete."
    )
    self.active_count = active_count


def _validate_securities_exist(session: Session, security_ids: list[str]) -> None:
  if not security_ids:
    return
  unique_ids = set(security_ids)
  found = (
    session.execute(select(Security.id).where(Security.id.in_(unique_ids)))
    .scalars()
    .all()
  )
  missing = unique_ids - set(found)
  if missing:
    raise SecurityNotFoundError(sorted(missing))


def _add_position(
  session: Session,
  portfolio_id: str,
  spec: PortfolioBlockPositionAdd,
  created_by: str,
) -> Position:
  position = Position(
    portfolio_id=portfolio_id,
    security_id=spec.security_id,
    quantity=spec.quantity,
    quantity_type=spec.quantity_type,
    cost_basis=spec.cost_basis,
    currency=spec.currency,
    current_value=spec.current_value,
    valuation_date=spec.valuation_date,
    valuation_source=spec.valuation_source,
    acquisition_date=spec.acquisition_date,
    notes=spec.notes,
    created_by=created_by,
  )
  session.add(position)
  try:
    session.flush()
  except IntegrityError as exc:
    if getattr(getattr(exc, "orig", None), "pgcode", None) == "23505":
      raise DuplicateActivePositionError(
        f"An active position already exists for security {spec.security_id} "
        f"in portfolio {portfolio_id}."
      ) from exc
    raise
  return position


def create_portfolio_block(
  session: Session,
  body: CreatePortfolioBlockRequest,
  created_by: str,
) -> PortfolioBlockEnvelope:
  """Create a portfolio with optional initial positions, return its envelope.

  Validates every referenced security up front so we never partially
  write a portfolio with bad position data; on validation failure no
  rows are added.
  """
  _validate_securities_exist(session, [p.security_id for p in body.positions])

  portfolio = Portfolio(
    name=body.portfolio.name,
    description=body.portfolio.description,
    strategy=body.portfolio.strategy,
    inception_date=body.portfolio.inception_date,
    base_currency=body.portfolio.base_currency,
    entity_id=body.portfolio.entity_id,
    created_by=created_by,
  )
  session.add(portfolio)
  session.flush()

  for spec in body.positions:
    _add_position(session, portfolio.id, spec, created_by)

  return get_portfolio_block(session, portfolio.id)


def update_portfolio_block(
  session: Session,
  body: UpdatePortfolioBlockOperation,
  created_by: str,
) -> PortfolioBlockEnvelope:
  """Patch portfolio fields and apply position deltas atomically.

  `add` items mint new positions; `update` patches by position id;
  `dispose` flips status to `disposed`. Partial failures abort the
  whole envelope — the caller's session boundary owns the rollback.
  """
  portfolio = session.execute(
    select(Portfolio).where(Portfolio.id == body.portfolio_id)
  ).scalar_one_or_none()
  if portfolio is None:
    raise PortfolioNotFoundError(body.portfolio_id)

  _validate_securities_exist(session, [a.security_id for a in body.positions.add])

  update_ids = [u.id for u in body.positions.update]
  dispose_ids = [d.id for d in body.positions.dispose]
  position_ids_to_load = list({*update_ids, *dispose_ids})

  position_map: dict[str, Position] = {}
  if position_ids_to_load:
    rows = (
      session.execute(select(Position).where(Position.id.in_(position_ids_to_load)))
      .scalars()
      .all()
    )
    position_map = {str(p.id): p for p in rows}

    for pid in position_ids_to_load:
      row = position_map.get(pid)
      if row is None:
        raise PositionNotFoundError(pid)
      if row.portfolio_id != body.portfolio_id:
        raise PositionPortfolioMismatchError(
          f"Position {pid} does not belong to portfolio {body.portfolio_id}."
        )

  portfolio_patch = body.portfolio.model_dump(exclude_unset=True)
  for field, value in portfolio_patch.items():
    setattr(portfolio, field, value)

  for spec in body.positions.add:
    _add_position(session, body.portfolio_id, spec, created_by)

  for patch in body.positions.update:
    row = position_map[patch.id]
    for field, value in patch.model_dump(exclude_unset=True, exclude={"id"}).items():
      setattr(row, field, value)

  for spec in body.positions.dispose:
    row = position_map[spec.id]
    row.status = "disposed"
    row.disposition_date = datetime.now(UTC).date()
    if spec.disposition_reason is not None:
      meta = dict(row.metadata_) if isinstance(row.metadata_, dict) else {}
      meta["disposition_reason"] = spec.disposition_reason
      row.metadata_ = meta

  # Bump updated_at on the portfolio so the envelope reflects this write
  # even when only positions changed.
  portfolio.updated_at = datetime.now(UTC)

  session.flush()
  return get_portfolio_block(session, body.portfolio_id)


def delete_portfolio_block(
  session: Session,
  body: DeletePortfolioBlockOperation,
) -> DeletePortfolioBlockResponse:
  """Cascade-delete a portfolio plus all of its positions.

  Active positions trigger `ActivePositionsRequireConfirmationError`
  unless `confirm_active_positions=true`; disposed-only portfolios
  delete without the flag.
  """
  portfolio = session.execute(
    select(Portfolio).where(Portfolio.id == body.portfolio_id)
  ).scalar_one_or_none()
  if portfolio is None:
    raise PortfolioNotFoundError(body.portfolio_id)

  active_count = int(
    session.execute(
      select(func.count())
      .select_from(Position)
      .where(Position.portfolio_id == body.portfolio_id)
      .where(Position.status == "active")
    ).scalar()
    or 0
  )
  if active_count and not body.confirm_active_positions:
    raise ActivePositionsRequireConfirmationError(active_count)

  positions_to_delete = (
    session.execute(select(Position).where(Position.portfolio_id == body.portfolio_id))
    .scalars()
    .all()
  )
  for row in positions_to_delete:
    session.delete(row)

  session.delete(portfolio)
  session.flush()

  return DeletePortfolioBlockResponse(
    deleted=True,
    portfolio_id=body.portfolio_id,
    positions_deleted=len(positions_to_delete),
  )


__all__ = [
  "ActivePositionsRequireConfirmationError",
  "DuplicateActivePositionError",
  "PortfolioNotFoundError",
  "PositionNotFoundError",
  "PositionPortfolioMismatchError",
  "SecurityNotFoundError",
  "create_portfolio_block",
  "delete_portfolio_block",
  "update_portfolio_block",
]
