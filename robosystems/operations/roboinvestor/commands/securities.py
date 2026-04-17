"""Security write operations.

Registrar-compatible signatures `(session, body, created_by=...)`;
update/delete declare `requires_created_by=False`. Missing rows raise
`SecurityNotFoundError` for the registrar's error map to translate.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.investor import (
  CreateSecurityRequest,
  DeleteResult,
  DeleteSecurityOperation,
  SecurityResponse,
  UpdateSecurityOperation,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboinvestor import Security
from robosystems.operations.roboinvestor.reads.securities import (
  find_linked_entity,
  security_to_response,
)


class EntityNotFoundError(LookupError):
  """Raised when `entity_id` is supplied but no matching entity exists."""


class SecurityNotFoundError(LookupError):
  """Raised when a referenced security does not exist."""


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
  session: Session, body: UpdateSecurityOperation
) -> SecurityResponse:
  """Apply updates to a security. Raises `SecurityNotFoundError` if missing."""
  row = session.execute(
    select(Security).where(Security.id == body.security_id)
  ).scalar_one_or_none()
  if row is None:
    raise SecurityNotFoundError(body.security_id)

  for field, value in body.model_dump(
    exclude_unset=True, exclude={"security_id"}
  ).items():
    setattr(row, field, value)

  session.flush()

  entity = session.execute(
    select(Entity).where(Entity.id == row.entity_id)
  ).scalar_one_or_none()

  return security_to_response(row, entity_name=entity.name if entity else None)


def soft_delete_security(
  session: Session, body: DeleteSecurityOperation
) -> DeleteResult:
  """Soft-delete a security by setting `is_active=False`.

  Raises `SecurityNotFoundError` if the row doesn't exist (registrar
  translates to 404). Historical positions referencing the security
  stay valid.
  """
  row = session.execute(
    select(Security).where(Security.id == body.security_id)
  ).scalar_one_or_none()
  if row is None:
    raise SecurityNotFoundError(body.security_id)
  row.is_active = False
  session.flush()
  return DeleteResult(deleted=True)
