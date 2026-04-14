"""Security write operations."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  CreateSecurityRequest,
  SecurityResponse,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Security
from robosystems.operations.roboinvestor.reads.securities import (
  find_linked_entity,
  security_to_response,
)


class EntityNotFoundError(LookupError):
  """Raised when `entity_id` is supplied but no matching entity exists."""


def create_security(
  session: Session, body: CreateSecurityRequest, created_by: str
) -> SecurityResponse:
  """Create a security row, auto-linking the entity when possible.

  Behavior mirrors the existing REST endpoint:
  - If `body.entity_id` is provided, verify the entity exists (raise
    `EntityNotFoundError` if not).
  - Else if `body.source_graph_id` is provided, look up an existing
    linked entity in this graph; if none yet, `entity_id` stays null
    and will be auto-linked later when a report arrives.
  - Otherwise the security is created unlinked.
  """
  entity_id = body.entity_id
  entity_name: str | None = None

  if entity_id:
    entity = session.execute(
      select(Entity).where(Entity.id == entity_id)
    ).scalar_one_or_none()
    if entity is None:
      raise EntityNotFoundError(entity_id)
    entity_name = str(entity.name)
  elif body.source_graph_id:
    entity_id, entity_name = find_linked_entity(session, body.source_graph_id)

  security = Security(
    entity_id=entity_id,
    source_graph_id=body.source_graph_id,
    name=body.name,
    security_type=body.security_type,
    security_subtype=body.security_subtype,
    terms=body.terms,
    authorized_shares=body.authorized_shares,
    outstanding_shares=body.outstanding_shares,
    created_by=created_by,
  )
  session.add(security)
  session.flush()
  return security_to_response(security, entity_name=entity_name)


def update_security(
  session: Session, security_id: str, updates: dict[str, Any]
) -> SecurityResponse | None:
  """Apply updates to a security. Returns None if the security does not exist."""
  row = session.execute(
    select(Security).where(Security.id == security_id)
  ).scalar_one_or_none()
  if row is None:
    return None

  for field, value in updates.items():
    setattr(row, field, value)

  session.flush()

  entity = session.execute(
    select(Entity).where(Entity.id == row.entity_id)
  ).scalar_one_or_none()

  return security_to_response(row, entity_name=entity.name if entity else None)


def soft_delete_security(session: Session, security_id: str) -> bool:
  """Soft-delete a security by setting `is_active=False`.

  Returns `True` if a row was flipped, `False` if the security did not
  exist. Matches the existing REST endpoint's soft-delete semantics.
  """
  row = session.execute(
    select(Security).where(Security.id == security_id)
  ).scalar_one_or_none()
  if row is None:
    return False
  row.is_active = False
  session.flush()
  return True
