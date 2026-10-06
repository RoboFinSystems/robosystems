"""The group parent a test ledger's rows are anchored to.

Every ledger row belongs to an entity, so a fixture that builds a tenant
schema seeds one with `seed_parent_entity` and stamps the rows it inserts by
hand with `PARENT_ENTITY_ID`.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.extensions import Entity

PARENT_ENTITY_ID = "ent_test_parent"


def seed_parent_entity(
  session: Session, *, entity_id: str = PARENT_ENTITY_ID, name: str = "Test Co"
) -> str:
  """Insert the group parent and flush. Returns its id."""
  session.add(
    Entity(
      id=entity_id,
      name=name,
      is_parent=True,
      source="native",
      created_by="test",
    )
  )
  session.flush()
  return entity_id
