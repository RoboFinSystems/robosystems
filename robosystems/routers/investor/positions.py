"""Investor position endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.investor import (
  CreatePositionRequest,
  PositionListResponse,
  PositionResponse,
  UpdatePositionRequest,
)
from robosystems.models.core import User
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Portfolio, Position, Security

router = APIRouter()


def _position_to_response(
  row: Position,
  security_name: str | None = None,
  entity_name: str | None = None,
) -> PositionResponse:
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
    current_value_dollars=float(row.current_value) / 100.0
    if row.current_value is not None
    else None,
    valuation_date=row.valuation_date,
    valuation_source=row.valuation_source,
    acquisition_date=row.acquisition_date,
    disposition_date=row.disposition_date,
    status=row.status,
    notes=row.notes,
    created_at=row.created_at,
    updated_at=row.updated_at,
  )


def _enrich_positions(session, rows: list[Position]) -> list[PositionResponse]:
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
    sec_map = {s.id: s for s in securities}

    entity_ids = {s.entity_id for s in securities if s.entity_id}
    if entity_ids:
      entities = (
        session.execute(select(Entity).where(Entity.id.in_(entity_ids))).scalars().all()
      )
      entity_map = {e.id: e.name for e in entities}

  results = []
  for r in rows:
    sec = sec_map.get(r.security_id)
    results.append(
      _position_to_response(
        r,
        security_name=sec.name if sec else None,
        entity_name=entity_map.get(sec.entity_id) if sec else None,
      )
    )
  return results


@router.post(
  "/positions",
  response_model=PositionResponse,
  operation_id="createPosition",
  summary="Create Position",
  tags=["Investor"],
  status_code=201,
)
async def create_position(
  body: CreatePositionRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      # Verify portfolio exists
      portfolio = session.execute(
        select(Portfolio).where(Portfolio.id == body.portfolio_id)
      ).scalar_one_or_none()
      if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found.")

      # Verify security exists
      security = session.execute(
        select(Security).where(Security.id == body.security_id)
      ).scalar_one_or_none()
      if not security:
        raise HTTPException(status_code=404, detail="Security not found.")

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
        created_by=current_user.id,
      )
      session.add(position)
      try:
        session.flush()
      except IntegrityError:
        raise HTTPException(
          status_code=409,
          detail="An active position already exists for this security in this portfolio.",
        )

      return _position_to_response(
        position,
        security_name=security.name,
        entity_name=entity.name if entity else None,
      )
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.get(
  "/positions",
  response_model=PositionListResponse,
  operation_id="listPositions",
  summary="List Positions",
  tags=["Investor"],
)
async def list_positions(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  portfolio_id: str | None = Query(None, description="Filter by portfolio"),
  security_id: str | None = Query(None, description="Filter by security"),
  status: str | None = Query(None, description="Filter by status"),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
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
        positions=_enrich_positions(session, rows),
        pagination=create_pagination_info(total, limit, offset),
      )
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.get(
  "/positions/{position_id}",
  response_model=PositionResponse,
  operation_id="getPosition",
  summary="Get Position",
  tags=["Investor"],
)
async def get_position(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  position_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Position).where(Position.id == position_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Position not found.")

      enriched = _enrich_positions(session, [row])
      return enriched[0]
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.patch(
  "/positions/{position_id}",
  response_model=PositionResponse,
  operation_id="updatePosition",
  summary="Update Position",
  tags=["Investor"],
)
async def update_position(
  body: UpdatePositionRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  position_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Position).where(Position.id == position_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Position not found.")

      updates = body.model_dump(exclude_unset=True)
      for field, value in updates.items():
        setattr(row, field, value)

      session.flush()
      enriched = _enrich_positions(session, [row])
      return enriched[0]
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.delete(
  "/positions/{position_id}",
  status_code=204,
  operation_id="deletePosition",
  summary="Delete Position",
  tags=["Investor"],
)
async def delete_position(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  position_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Position).where(Position.id == position_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Position not found.")
      # Soft delete
      row.status = "disposed"
      session.flush()
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")
