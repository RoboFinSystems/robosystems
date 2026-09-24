"""Write operations for the ledger entity."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.entity import (
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.operations.roboledger.reads.entity import (
  entity_to_response,
  resolve_parent_entity,
)

__all__ = [
  "ParentEntityNotFoundError",
  "update_entity",
  "update_parent_entity",
]


class ParentEntityNotFoundError(LookupError):
  """The graph has no primary entity to update."""

  def __init__(self) -> None:
    super().__init__("No entity found. Create an entity graph first.")


def update_parent_entity(
  session: Session, updates: dict[str, Any]
) -> LedgerEntityResponse | None:
  """Apply `updates` to the parent entity and commit.

  Returns `None` if there is no parent entity. Commits whatever it is given;
  the caller validates that `updates` is non-empty.
  """
  entity = resolve_parent_entity(session)
  if entity is None:
    return None

  for field_name, value in updates.items():
    setattr(entity, field_name, value)

  entity.updated_at = datetime.now(UTC)
  # Build the response before committing: commit expires the instance, and a
  # refresh would run on a pooled connection whose search_path was reset.
  session.flush()
  response = entity_to_response(entity)
  session.commit()

  return response


def update_entity(
  session: Session,
  body: UpdateEntityRequest,
  created_by: str,
) -> LedgerEntityResponse:
  """Update the graph's primary entity from a validated request body.

  Only non-null fields are applied. Raises `ValueError` for an empty body and
  `ParentEntityNotFoundError`. ``created_by`` is unused (registrar signature).
  """
  del created_by

  updates = body.model_dump(exclude_none=True)
  if not updates:
    raise ValueError("No fields provided for update.")

  result = update_parent_entity(session, updates)
  if result is None:
    raise ParentEntityNotFoundError()
  return result
