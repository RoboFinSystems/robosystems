"""Handlers for ``taxonomy_type='schedule'`` — container for Schedule info blocks.

Schedule taxonomies are thin grouping containers: they carry a ``name``
and a ``description``, nothing else. The actual Schedule artifacts live
as ``Information Blocks`` of type ``schedule``, each linked to a parent
taxonomy row via ``structures.taxonomy_id``.

This handler is deliberately minimal — no elements, no structures, no
associations. The envelope validation caps still apply, but the handler
ignores any atoms that slip through.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  TaxonomyBlockEnvelope,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Structure, Taxonomy

SCHEDULE_BLOCK_TYPE = "schedule"
SCHEDULE_DISPLAY_NAME = "Schedule Container"
SCHEDULE_DISPLAY_PLURAL = "Schedule Containers"
SCHEDULE_CATEGORY = "Close"


def create(
  session: Session,
  payload: CreateTaxonomyBlockRequest,
  created_by: str,
) -> str:
  if payload.taxonomy_type != SCHEDULE_BLOCK_TYPE:
    raise ValueError(
      f"schedule container handler received payload with taxonomy_type="
      f"{payload.taxonomy_type!r}"
    )
  taxonomy = Taxonomy(
    name=payload.name,
    description=payload.description,
    taxonomy_type=SCHEDULE_BLOCK_TYPE,
    version=payload.version,
    standard=payload.standard,
    namespace_uri=payload.namespace_uri,
    is_shared=False,
    is_active=True,
    is_locked=False,
    metadata_=dict(payload.metadata),
    created_by=created_by,
  )
  session.add(taxonomy)
  session.flush()
  return taxonomy.id


_ADMIN_ONLY_MESSAGE = (
  "schedule-container update/delete are intentionally not exposed on the "
  "public envelope; admin-only via library_creator. The schedule artifacts "
  "themselves live as Schedule Information Blocks and are mutated through "
  "that surface."
)


def update(
  session: Session,
  payload: UpdateTaxonomyBlockRequest,
  updated_by: str,
) -> str:
  """Intentionally not exposed; admin-only via library_creator."""
  raise NotImplementedError(_ADMIN_ONLY_MESSAGE)


def delete(
  session: Session,
  payload: DeleteTaxonomyBlockRequest,
  deleted_by: str,
) -> str:
  """Intentionally not exposed; admin-only via library_creator."""
  raise NotImplementedError(_ADMIN_ONLY_MESSAGE)


def build_envelope(session: Session, taxonomy_id: str) -> TaxonomyBlockEnvelope | None:
  taxonomy = session.get(Taxonomy, taxonomy_id)
  if taxonomy is None or taxonomy.taxonomy_type != SCHEDULE_BLOCK_TYPE:
    return None

  structures_rows = (
    session.execute(
      select(Structure)
      .where(Structure.taxonomy_id == taxonomy_id)
      .order_by(Structure.name)
    )
    .scalars()
    .all()
  )

  return TaxonomyBlockEnvelope(
    id=taxonomy.id,
    name=taxonomy.name,
    taxonomy_type=taxonomy.taxonomy_type,
    display_name=SCHEDULE_DISPLAY_NAME,
    category=SCHEDULE_CATEGORY,
    parent_taxonomy_id=taxonomy.parent_taxonomy_id,
    parent_taxonomy_name=None,
    version=taxonomy.version,
    standard=taxonomy.standard,
    namespace_uri=taxonomy.namespace_uri,
    is_locked=taxonomy.is_locked,
    elements=[],
    structures=[],
    associations=[],
    rules=[],
    verification_results=[],
    element_count=0,
    structure_count=len(structures_rows),
    association_count=0,
  )
