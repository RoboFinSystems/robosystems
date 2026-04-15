"""Write operations for the ledger entity."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.entity import LedgerEntityResponse
from robosystems.models.extensions import Entity
from robosystems.operations.roboledger.reads.entity import entity_to_response


def update_parent_entity(
  session: Session, updates: dict[str, Any]
) -> LedgerEntityResponse | None:
  """Apply `updates` to the parent entity and commit.

  Returns the refreshed response, or `None` if no parent entity exists
  for the ledger. The caller is expected to have already validated that
  `updates` is non-empty — this function commits whatever it is given.
  """
  entity = (
    session.query(Entity)
    .filter(Entity.is_parent.is_(True), Entity.source != "linked")
    .first()
  )
  if entity is None:
    return None

  for field_name, value in updates.items():
    setattr(entity, field_name, value)

  entity.updated_at = datetime.now(UTC)
  session.commit()
  session.refresh(entity)

  return entity_to_response(entity)
