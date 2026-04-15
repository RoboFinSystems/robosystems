"""Security read operations."""

from __future__ import annotations

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.investor import (
  SecurityListResponse,
  SecurityResponse,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Security


def find_linked_entity(
  session: Session, source_graph_id: str
) -> tuple[str | None, str | None]:
  """Find an existing linked entity for a source graph.

  Returns `(entity_id, entity_name)` — both `None` when no linked
  entity has been auto-created yet (report sharing is what creates
  them). This function only looks up existing entities; it never
  reaches into the source graph.
  """
  existing = session.execute(
    text(
      "SELECT id, name FROM entities WHERE metadata->>'source_graph_id' = :sgid LIMIT 1"
    ),
    {"sgid": source_graph_id},
  ).first()

  if existing:
    return existing.id, existing.name
  return None, None


def security_to_response(
  row: Security, entity_name: str | None = None
) -> SecurityResponse:
  """Map a Security row to the wire-facing SecurityResponse."""
  return SecurityResponse(
    id=row.id,
    entity_id=row.entity_id,
    entity_name=entity_name,
    source_graph_id=row.source_graph_id,
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


def list_securities(
  session: Session,
  *,
  entity_id: str | None = None,
  security_type: str | None = None,
  is_active: bool | None = None,
  limit: int = 100,
  offset: int = 0,
) -> SecurityListResponse:
  """List securities with optional filters, paginated."""
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
  entity_ids = {r.entity_id for r in rows if r.entity_id}
  entity_map: dict[str, str] = {}
  if entity_ids:
    entities = (
      session.execute(select(Entity).where(Entity.id.in_(entity_ids))).scalars().all()
    )
    entity_map = {str(e.id): str(e.name) for e in entities}

  return SecurityListResponse(
    securities=[
      security_to_response(r, entity_name=entity_map.get(r.entity_id)) for r in rows
    ],
    pagination=create_pagination_info(total, limit, offset),
  )


def get_security(session: Session, security_id: str) -> SecurityResponse | None:
  """Return a single security with its entity name, or None if not found."""
  row = session.execute(
    select(Security).where(Security.id == security_id)
  ).scalar_one_or_none()
  if row is None:
    return None

  entity = session.execute(
    select(Entity).where(Entity.id == row.entity_id)
  ).scalar_one_or_none()

  return security_to_response(row, entity_name=entity.name if entity else None)
