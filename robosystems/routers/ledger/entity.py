"""Ledger entity endpoints."""

from datetime import UTC, datetime

from fastapi import APIRouter, Body, Depends, HTTPException, Path
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.entity import (
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.models.extensions import Entity
from robosystems.models.iam import User

router = APIRouter()


def _entity_to_response(entity: Entity) -> LedgerEntityResponse:
  return LedgerEntityResponse(
    id=entity.id,
    name=entity.name,
    legal_name=entity.legal_name,
    uri=entity.uri,
    cik=entity.cik,
    ticker=entity.ticker,
    exchange=entity.exchange,
    sic=entity.sic,
    sic_description=entity.sic_description,
    category=entity.category,
    state_of_incorporation=entity.state_of_incorporation,
    fiscal_year_end=entity.fiscal_year_end,
    tax_id=entity.tax_id,
    lei=entity.lei,
    industry=entity.industry,
    entity_type=entity.entity_type,
    phone=entity.phone,
    website=entity.website,
    status=entity.status,
    is_parent=entity.is_parent,
    parent_entity_id=entity.parent_entity_id,
    source=entity.source,
    source_id=entity.source_id,
    connection_id=entity.connection_id,
    address_line1=entity.address_line1,
    address_city=entity.address_city,
    address_state=entity.address_state,
    address_postal_code=entity.address_postal_code,
    address_country=entity.address_country,
    created_at=entity.created_at.isoformat() if entity.created_at else None,
    updated_at=entity.updated_at.isoformat() if entity.updated_at else None,
  )


@router.get(
  "/entity",
  response_model=LedgerEntityResponse,
  operation_id="getLedgerEntity",
  dependencies=[Depends(subscription_aware_rate_limit_dependency)],
)
async def get_entity(
  graph_id: str = Path(
    ...,
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
    description="Graph database identifier",
  ),
  user: User = Depends(get_current_user_with_graph),
) -> LedgerEntityResponse:
  """Get the entity for this ledger graph."""
  try:
    with extensions_session(graph_id) as session:
      entity = session.query(Entity).filter(Entity.is_parent.is_(True)).first()
      if not entity:
        raise HTTPException(
          status_code=404,
          detail="No entity found. Create an entity graph first.",
        )
      return _entity_to_response(entity)
  except HTTPException:
    raise
  except (ValueError, ProgrammingError):
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )


@router.put(
  "/entity",
  response_model=LedgerEntityResponse,
  operation_id="updateLedgerEntity",
  dependencies=[Depends(subscription_aware_rate_limit_dependency)],
)
async def update_entity(
  graph_id: str = Path(
    ...,
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
    description="Graph database identifier",
  ),
  body: UpdateEntityRequest = Body(...),
  user: User = Depends(get_current_user_with_graph),
) -> LedgerEntityResponse:
  """Update entity details. Only provided (non-null) fields are updated."""
  try:
    with extensions_session(graph_id) as session:
      entity = session.query(Entity).filter(Entity.is_parent.is_(True)).first()
      if not entity:
        raise HTTPException(
          status_code=404,
          detail="No entity found. Create an entity graph first.",
        )

      # Apply only provided fields
      update_data = body.model_dump(exclude_none=True)
      if not update_data:
        raise HTTPException(
          status_code=400,
          detail="No fields provided for update.",
        )

      for field_name, value in update_data.items():
        setattr(entity, field_name, value)

      entity.updated_at = datetime.now(UTC)
      session.commit()
      session.refresh(entity)

      return _entity_to_response(entity)
  except HTTPException:
    raise
  except (ValueError, ProgrammingError):
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )
