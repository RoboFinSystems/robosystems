"""Portfolio write operations."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  CreatePortfolioRequest,
  PortfolioResponse,
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
  session: Session, portfolio_id: str, updates: dict[str, Any]
) -> PortfolioResponse | None:
  """Apply updates to a portfolio. Returns None if the portfolio does not exist."""
  row = session.execute(
    select(Portfolio).where(Portfolio.id == portfolio_id)
  ).scalar_one_or_none()
  if row is None:
    return None

  for field, value in updates.items():
    setattr(row, field, value)

  session.flush()
  return portfolio_to_response(row)


def delete_portfolio(session: Session, portfolio_id: str) -> bool:
  """Delete a portfolio.

  Returns `True` if the row was deleted, `False` if it did not exist.
  Raises `PortfolioHasActivePositionsError` if the portfolio still has
  active positions — the caller is expected to translate this into a
  409 Conflict.
  """
  row = session.execute(
    select(Portfolio).where(Portfolio.id == portfolio_id)
  ).scalar_one_or_none()
  if row is None:
    return False

  active_count = session.execute(
    select(func.count())
    .select_from(Position)
    .where(Position.portfolio_id == portfolio_id)
    .where(Position.status == "active")
  ).scalar()
  if active_count:
    raise PortfolioHasActivePositionsError(int(active_count))

  session.delete(row)
  return True
