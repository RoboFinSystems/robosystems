"""Portfolio write operations.

All four commands share the registrar-compatible signature
`(session, body, created_by=...)` (update/delete drop `created_by` and
are declared with `requires_created_by=False` in the router). Missing
rows raise `PortfolioNotFoundError` so the registrar error map can
surface a clean 404.
"""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  CreatePortfolioRequest,
  DeletePortfolioOperation,
  DeleteResult,
  PortfolioResponse,
  UpdatePortfolioOperation,
)
from robosystems.models.extensions.roboinvestor import Portfolio, Position
from robosystems.operations.roboinvestor.reads.portfolios import portfolio_to_response


class PortfolioNotFoundError(LookupError):
  """Raised when a referenced portfolio does not exist."""


class PortfolioHasActivePositionsError(Exception):
  """Raised when trying to delete a portfolio with active positions."""

  def __init__(self, active_count: int) -> None:
    super().__init__(f"Portfolio has {active_count} active position(s).")
    self.active_count = active_count


def create_portfolio(
  session: Session, body: CreatePortfolioRequest, created_by: str
) -> PortfolioResponse:
  """Create a portfolio row and return its response representation.

  The caller is expected to `session.commit()` after — this function
  only `flush()`es so the generated id is available.
  """
  portfolio = Portfolio(
    name=body.name,
    description=body.description,
    strategy=body.strategy,
    inception_date=body.inception_date,
    base_currency=body.base_currency,
    created_by=created_by,
  )
  session.add(portfolio)
  session.flush()
  return portfolio_to_response(portfolio)


def update_portfolio(
  session: Session, body: UpdatePortfolioOperation
) -> PortfolioResponse:
  """Apply updates to a portfolio. Raises `PortfolioNotFoundError` if missing."""
  row = session.execute(
    select(Portfolio).where(Portfolio.id == body.portfolio_id)
  ).scalar_one_or_none()
  if row is None:
    raise PortfolioNotFoundError(body.portfolio_id)

  for field, value in body.model_dump(
    exclude_unset=True, exclude={"portfolio_id"}
  ).items():
    setattr(row, field, value)

  session.flush()
  return portfolio_to_response(row)


def delete_portfolio(session: Session, body: DeletePortfolioOperation) -> DeleteResult:
  """Delete a portfolio.

  Raises `PortfolioNotFoundError` on missing rows (registrar → 404) and
  `PortfolioHasActivePositionsError` when active positions remain
  (registrar → 409). Returns `DeleteResult(deleted=True)` on success.
  """
  row = session.execute(
    select(Portfolio).where(Portfolio.id == body.portfolio_id)
  ).scalar_one_or_none()
  if row is None:
    raise PortfolioNotFoundError(body.portfolio_id)

  active_count = session.execute(
    select(func.count())
    .select_from(Position)
    .where(Position.portfolio_id == body.portfolio_id)
    .where(Position.status == "active")
  ).scalar()
  if active_count:
    raise PortfolioHasActivePositionsError(int(active_count))

  session.delete(row)
  return DeleteResult(deleted=True)
