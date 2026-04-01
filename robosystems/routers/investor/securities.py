"""Investor security endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.investor import (
  CreateSecurityRequest,
  SecurityListResponse,
  SecurityResponse,
  UpdateSecurityRequest,
)
from robosystems.models.core import User
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Security

router = APIRouter()


def _ensure_linked_entity_from_graph(
  session, source_graph_id: str, created_by: str
) -> tuple[str | None, str | None]:
  """Find or create a linked entity for a source graph. Returns (entity_id, entity_name)."""
  from sqlalchemy import text

  from robosystems.utils.ulid import generate_prefixed_ulid

  # Check if a linked entity for this source graph already exists
  existing = session.execute(
    text(
      "SELECT id, name FROM entities WHERE metadata->>'source_graph_id' = :sgid LIMIT 1"
    ),
    {"sgid": source_graph_id},
  ).first()

  if existing:
    return existing.id, existing.name

  # Read the source graph's parent entity
  try:
    with extensions_session(source_graph_id) as source_session:
      source_entity = source_session.execute(
        select(Entity).where(Entity.is_parent.is_(True)).limit(1)
      ).scalar_one_or_none()

      if not source_entity:
        return None, None

      entity_data = {
        "name": source_entity.name,
        "legal_name": source_entity.legal_name,
        "entity_type": source_entity.entity_type,
        "industry": source_entity.industry,
        "cik": source_entity.cik,
        "ticker": source_entity.ticker,
      }
  except Exception:
    entity_data = {"name": f"Entity ({source_graph_id})"}

  linked = Entity(
    id=generate_prefixed_ulid("ent"),
    name=entity_data["name"],
    legal_name=entity_data.get("legal_name"),
    entity_type=entity_data.get("entity_type"),
    industry=entity_data.get("industry"),
    cik=entity_data.get("cik"),
    ticker=entity_data.get("ticker"),
    source="linked",
    is_parent=True,
    status="active",
    address_country="US",
    metadata_={"source_graph_id": source_graph_id},
    created_by=created_by,
  )
  session.add(linked)
  session.flush()
  return linked.id, linked.name


def _security_to_response(
  row: Security, entity_name: str | None = None
) -> SecurityResponse:
  return SecurityResponse(
    id=row.id,
    entity_id=row.entity_id,
    entity_name=entity_name,
    name=row.name,
    security_type=row.security_type,
    security_subtype=row.security_subtype,
    terms=row.terms or {},
    is_active=row.is_active,
    authorized_shares=row.authorized_shares,
    outstanding_shares=row.outstanding_shares,
    created_at=row.created_at,
    updated_at=row.updated_at,
  )


@router.post(
  "/securities",
  response_model=SecurityResponse,
  operation_id="createSecurity",
  summary="Create Security",
  tags=["Investor"],
  status_code=201,
)
async def create_security(
  body: CreateSecurityRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      entity_id = body.entity_id
      entity_name = None

      # POC: auto-link entity from another graph
      if body.source_graph_id and not entity_id:
        entity_id, entity_name = _ensure_linked_entity_from_graph(
          session, body.source_graph_id, current_user.id
        )
      elif entity_id:
        entity = session.execute(
          select(Entity).where(Entity.id == entity_id)
        ).scalar_one_or_none()
        if not entity:
          raise HTTPException(status_code=404, detail="Entity not found.")
        entity_name = entity.name

      security = Security(
        entity_id=entity_id,
        name=body.name,
        security_type=body.security_type,
        security_subtype=body.security_subtype,
        terms=body.terms,
        authorized_shares=body.authorized_shares,
        outstanding_shares=body.outstanding_shares,
        created_by=current_user.id,
      )
      session.add(security)
      session.flush()
      return _security_to_response(security, entity_name=entity_name)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.get(
  "/securities",
  response_model=SecurityListResponse,
  operation_id="listSecurities",
  summary="List Securities",
  tags=["Investor"],
)
async def list_securities(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  entity_id: str | None = Query(None, description="Filter by entity"),
  security_type: str | None = Query(None, description="Filter by security type"),
  is_active: bool | None = Query(None, description="Filter by active status"),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      query = select(Security)
      count_query = select(func.count()).select_from(Security)

      if entity_id is not None:
        query = query.where(Security.entity_id == entity_id)
        count_query = count_query.where(Security.entity_id == entity_id)
      if security_type is not None:
        query = query.where(Security.security_type == security_type)
        count_query = count_query.where(Security.security_type == security_type)
      if is_active is not None:
        query = query.where(Security.is_active == is_active)
        count_query = count_query.where(Security.is_active == is_active)

      total = session.execute(count_query).scalar() or 0
      rows = (
        session.execute(query.order_by(Security.name).offset(offset).limit(limit))
        .scalars()
        .all()
      )

      # Batch-load entity names
      entity_ids = {r.entity_id for r in rows}
      entity_map: dict[str, str] = {}
      if entity_ids:
        entities = (
          session.execute(select(Entity).where(Entity.id.in_(entity_ids)))
          .scalars()
          .all()
        )
        entity_map = {str(e.id): str(e.name) for e in entities}

      return SecurityListResponse(
        securities=[
          _security_to_response(r, entity_name=entity_map.get(r.entity_id))
          for r in rows
        ],
        pagination=create_pagination_info(total, limit, offset),
      )
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.get(
  "/securities/{security_id}",
  response_model=SecurityResponse,
  operation_id="getSecurity",
  summary="Get Security",
  tags=["Investor"],
)
async def get_security(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  security_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Security).where(Security.id == security_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Security not found.")

      entity = session.execute(
        select(Entity).where(Entity.id == row.entity_id)
      ).scalar_one_or_none()

      return _security_to_response(row, entity_name=entity.name if entity else None)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.patch(
  "/securities/{security_id}",
  response_model=SecurityResponse,
  operation_id="updateSecurity",
  summary="Update Security",
  tags=["Investor"],
)
async def update_security(
  body: UpdateSecurityRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  security_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Security).where(Security.id == security_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Security not found.")

      updates = body.model_dump(exclude_unset=True)
      for field, value in updates.items():
        setattr(row, field, value)

      session.flush()

      entity = session.execute(
        select(Entity).where(Entity.id == row.entity_id)
      ).scalar_one_or_none()

      return _security_to_response(row, entity_name=entity.name if entity else None)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")


@router.delete(
  "/securities/{security_id}",
  status_code=204,
  operation_id="deleteSecurity",
  summary="Delete Security",
  tags=["Investor"],
)
async def delete_security(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  security_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(Security).where(Security.id == security_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Security not found.")
      # Soft delete
      row.is_active = False
      session.flush()
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Investor module not initialized.")
