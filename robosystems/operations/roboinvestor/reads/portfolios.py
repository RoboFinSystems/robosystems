"""Portfolio read operations."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.investor import (
  PortfolioListResponse,
  PortfolioResponse,
)
from robosystems.models.extensions.roboinvestor import Portfolio


def portfolio_to_response(row: Portfolio) -> PortfolioResponse:
  """Map a Portfolio row to the wire-facing PortfolioResponse."""
  return PortfolioResponse(
    id=row.id,
    name=row.name,
    description=row.description,
    strategy=row.strategy,
    inception_date=row.inception_date,
    base_currency=row.base_currency,
    created_at=row.created_at,
    updated_at=row.updated_at,
  )


def list_portfolios(
  session: Session, *, limit: int = 100, offset: int = 0
) -> PortfolioListResponse:
  """List portfolios ordered by name, paginated."""
  total = session.execute(select(func.count()).select_from(Portfolio)).scalar() or 0
  rows = (
    session.execute(
      select(Portfolio).order_by(Portfolio.name).offset(offset).limit(limit)
    )
    .scalars()
    .all()
  )
  return PortfolioListResponse(
    portfolios=[portfolio_to_response(r) for r in rows],
    pagination=create_pagination_info(total, limit, offset),
  )
