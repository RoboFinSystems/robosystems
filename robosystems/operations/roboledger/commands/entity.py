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

  Returns the refreshed response, or `None` if no parent entity exists
  for the ledger. The caller is expected to have already validated that
  `updates` is non-empty — this function commits whatever it is given.
  """
  entity = resolve_parent_entity(session)
  if entity is None:
    return None

  for field_name, value in updates.items():
    setattr(entity, field_name, value)

  entity.updated_at = datetime.now(UTC)
  # Build the response off the flush, before committing: `commit()` expires
  # every instance, so reading the entity afterwards would issue a refresh
  # SELECT on whichever pooled connection comes back — one whose search_path
  # `extensions_session` has already reset. See
  # tests/operations/information_block/test_no_post_commit_reads.py.
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

  Only provided (non-null) fields are applied. Raises :class:`ValueError`
  when the body carries no updates and :class:`ParentEntityNotFoundError`
  when the graph has no primary entity; the registrar maps both.

  ``created_by`` is accepted for registrar uniformity — the entity row
  tracks `updated_at` rather than a per-edit author.
  """
  del created_by

  updates = body.model_dump(exclude_none=True)
  if not updates:
    raise ValueError("No fields provided for update.")

  result = update_parent_entity(session, updates)
  if result is None:
    raise ParentEntityNotFoundError()
  return result
