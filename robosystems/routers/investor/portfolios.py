"""Investor portfolio endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.investor import (
  CreatePortfolioRequest,
  PortfolioListResponse,
  PortfolioResponse,
  UpdatePortfolioRequest,
)
from robosystems.models.core import User
from robosystems.models.extensions.roboinvestor import Portfolio

router = APIRouter()


def _portfolio_to_response(row: Portfolio) -> PortfolioResponse:
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


@router.post(
  "/portfolios",
  response_model=PortfolioResponse,
  operation_id="createPortfolio",
  summary="Create Portfolio",
  tags=["Investor"],
  status_code=201,
)
async def create_portfolio(
  body: CreatePortfolioRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      portfolio = Portfolio(
        name=body.name,
        description=body.description,
        strategy=body.strategy,
        inception_date=body.inception_date,
        base_currency=body.base_currency,
        created_by=current_user.id,
      )
      session.add(portfolio)
      session.flush()
      return _portfolio_to_response(portfolio)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.get(
  "/portfolios",
  response_model=PortfolioListResponse,
  operation_id="listPortfolios",
  summary="List Portfolios",
  tags=["Investor"],
)
async def list_portfolios(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      total = session.execute(select(func.count()).select_from(Portfolio)).scalar() or 0
      rows = (
        session.execute(
          select(Portfolio).order_by(Portfolio.name).offset(offset).limit(limit)
        )
        .scalars()
        .all()
      )
      return PortfolioListResponse(
        portfolios=[_portfolio_to_response(r) for r in rows],
        pagination=create_pagination_info(total, limit, offset),
      )
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.get(
  "/portfolios/{portfolio_id}",
  response_model=PortfolioResponse,
  operation_id="getPortfolio",
  summary="Get Portfolio",
  tags=["Investor"],
)
async def get_portfolio(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  portfolio_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Portfolio).where(Portfolio.id == portfolio_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Portfolio not found.")
      return _portfolio_to_response(row)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.patch(
  "/portfolios/{portfolio_id}",
  response_model=PortfolioResponse,
  operation_id="updatePortfolio",
  summary="Update Portfolio",
  tags=["Investor"],
)
async def update_portfolio(
  body: UpdatePortfolioRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  portfolio_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Portfolio).where(Portfolio.id == portfolio_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Portfolio not found.")

      updates = body.model_dump(exclude_unset=True)
      for field, value in updates.items():
        setattr(row, field, value)

      session.flush()
      return _portfolio_to_response(row)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.delete(
  "/portfolios/{portfolio_id}",
  status_code=204,
  operation_id="deletePortfolio",
  summary="Delete Portfolio",
  tags=["Investor"],
)
async def delete_portfolio(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  portfolio_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Portfolio).where(Portfolio.id == portfolio_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Portfolio not found.")
      session.delete(row)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")
