"""Read operations for library structures (extended link roles)."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.library import LibraryStructureResponse
from robosystems.models.extensions import Structure


def _structure_to_response(structure: Structure) -> LibraryStructureResponse:
  role_uri = (structure.metadata_ or {}).get("role_uri")
  return LibraryStructureResponse(
    id=structure.id,
    name=structure.name,
    block_type=structure.block_type,
    taxonomy_id=structure.taxonomy_id,
    role_uri=role_uri,
    is_active=structure.is_active,
  )


def list_structures(
  session: Session,
  taxonomy_id: str | None = None,
  block_type: str | None = None,
) -> list[LibraryStructureResponse]:
  """List structures, optionally filtered by taxonomy or type."""
  query = select(Structure)
  if taxonomy_id is not None:
    query = query.where(Structure.taxonomy_id == taxonomy_id)
  if block_type is not None:
    query = query.where(Structure.block_type == block_type)
  query = query.order_by(Structure.block_type, Structure.name)

  structures = session.execute(query).scalars().all()
  return [_structure_to_response(s) for s in structures]


def get_structure(
  session: Session, structure_id: str
) -> LibraryStructureResponse | None:
  structure = session.execute(
    select(Structure).where(Structure.id == structure_id)
  ).scalar_one_or_none()
  if structure is None:
    return None
  return _structure_to_response(structure)
