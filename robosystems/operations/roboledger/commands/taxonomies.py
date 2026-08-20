"""Write operations for mapping associations and entity↔taxonomy links.

These commands are pure functions: take an open extensions `Session`
and validated Pydantic request bodies, return Pydantic response models.
Callers own session lifetime and translate domain exceptions into
transport errors.

**Blocks, not atoms.** Row-level writers for taxonomies and structures
(`create_taxonomy`, `update_structure`, `delete_taxonomy`, …) are
deliberately absent: tenant taxonomy writes go through the taxonomy
*block* surface — `create-taxonomy-block` / `update-taxonomy-block` /
`delete-taxonomy-block` in `operations/taxonomy_block/` — which mutates
a whole envelope (taxonomy + structures + elements + associations +
rules) in one transaction and runs the block validators. An atomic
row writer bypasses those validators, so don't reintroduce one; extend
the block handlers instead.

Removed 2026-08-20: seven such atoms existed here, unreferenced by any
route, tool, or test, while their docstrings claimed the block surface
invoked them indirectly. It never did.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.db.integrity import violates
from robosystems.models.api.common import DeleteResult
from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse,
  CreateMappingAssociationOperation,
  DeleteAssociationRequest,
  DeleteMappingAssociationOperation,
  EntityTaxonomyResponse,
  LinkEntityTaxonomyRequest,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  Element,
  EntityTaxonomy,
  Rule,
  Structure,
  Taxonomy,
  VerificationResult,
)
from robosystems.operations.roboledger.commands._guards import (
  LibraryImmutableError,
  assert_not_library_origin,
)
from robosystems.operations.roboledger.reads.entity import resolve_parent_entity
from robosystems.utils.ulid import generate_prefixed_ulid

__all__ = [
  "AssociationNotFoundError",
  "ElementNotFoundError",
  "LibraryImmutableError",
  "MappingAssociationExistsError",
  "MappingStructureNotFoundError",
  "TaxonomyNotFoundError",
  "create_mapping_association",
  "delete_association",
  "delete_mapping_association",
  "link_entity_taxonomy",
]


class MappingStructureNotFoundError(LookupError):
  """Raised when a referenced mapping structure does not exist."""

  def __init__(self, mapping_id: str) -> None:
    super().__init__(f"Mapping structure not found: {mapping_id}")
    self.mapping_id = mapping_id


class TaxonomyNotFoundError(LookupError):
  """Raised when a taxonomy is not found by id."""

  def __init__(self, taxonomy_id: str) -> None:
    super().__init__(f"Taxonomy not found: {taxonomy_id}")
    self.taxonomy_id = taxonomy_id


class AssociationNotFoundError(LookupError):
  """Raised when an association is not found by id."""

  def __init__(self, association_id: str) -> None:
    super().__init__(f"Association not found: {association_id}")
    self.association_id = association_id


class ElementNotFoundError(LookupError):
  """Raised when the from/to element in an association does not exist."""

  def __init__(self, side: str, element_id: str) -> None:
    super().__init__(f"{side} element not found: {element_id}")
    self.side = side  # "source" or "target"
    self.element_id = element_id


class EntityTaxonomyConflictError(ValueError):
  """An entity↔taxonomy adoption collided with a concurrent identical link."""


class MappingAssociationExistsError(ValueError):
  """Raised when the association already exists on the mapping structure.

  `uq_association_structure_elements_type` makes the pair unique per structure
  and type. Without this check the insert reached the database and surfaced the
  IntegrityError as an opaque 500, which a caller cannot tell apart from a real
  fault — so re-running a seeding script had no safe way to skip what it had
  already created.
  """

  def __init__(self, mapping_id: str, from_element_id: str, to_element_id: str) -> None:
    super().__init__(
      f"Mapping association already exists on {mapping_id}: "
      f"{from_element_id} → {to_element_id}"
    )
    self.mapping_id = mapping_id
    self.from_element_id = from_element_id
    self.to_element_id = to_element_id


def create_mapping_association(
  session: Session,
  body: CreateMappingAssociationOperation,
  created_by: str,
) -> AssociationResponse:
  """Add a mapping association (CoA element → reporting concept).

  Raises `MappingStructureNotFoundError` if the mapping structure is
  missing, `ElementNotFoundError` with `side="source"` / `"target"` if
  either element is missing, or `MappingAssociationExistsError` if the pair
  is already mapped. The caller translates these to HTTP status codes.
  """
  structure = session.execute(
    select(Structure).where(Structure.id == body.mapping_id)
  ).scalar_one_or_none()
  if structure is None:
    raise MappingStructureNotFoundError(body.mapping_id)
  # Block arc insertion into library-seeded structures (fac-presentation,
  # fac-to-rs-gaap, rs-gaap-hierarchy, etc). The DB-level trigger
  # catches the same case as defense-in-depth; this path gives a clean
  # 403 instead of a ProgrammingError.
  assert_not_library_origin(structure)

  from_elem = session.execute(
    select(Element).where(Element.id == body.from_element_id)
  ).scalar_one_or_none()
  if from_elem is None:
    raise ElementNotFoundError("source", body.from_element_id)
  # The from-element must be tenant-authored (CoA side); the target is
  # allowed to be a library row — that's the whole point of mapping.
  assert_not_library_origin(from_elem)

  to_elem = session.execute(
    select(Element).where(Element.id == body.to_element_id)
  ).scalar_one_or_none()
  if to_elem is None:
    raise ElementNotFoundError("target", body.to_element_id)

  # Pre-check the uniqueness the DB enforces, so a repeat gets a clean 409
  # instead of an IntegrityError escaping as a 500.
  existing = session.execute(
    select(Association).where(
      Association.structure_id == body.mapping_id,
      Association.from_element_id == body.from_element_id,
      Association.to_element_id == body.to_element_id,
      Association.association_type == body.association_type,
    )
  ).scalar_one_or_none()
  if existing is not None:
    raise MappingAssociationExistsError(
      body.mapping_id, body.from_element_id, body.to_element_id
    )

  assoc = Association(
    id=generate_prefixed_ulid("assoc"),
    structure_id=body.mapping_id,
    from_element_id=body.from_element_id,
    to_element_id=body.to_element_id,
    association_type=body.association_type,
    order_value=body.order_value,
    weight=body.weight,
    confidence=body.confidence,
    suggested_by=body.suggested_by,
    created_by=created_by,
  )
  session.add(assoc)
  try:
    session.flush()
  except IntegrityError as exc:
    # The pre-check above lost a race with a concurrent identical insert; the
    # unique key is the truth. Same answer as the check. Any other constraint
    # is a real fault and keeps its identity.
    if not violates(exc, "uq_association_structure_elements_type"):
      raise
    raise MappingAssociationExistsError(
      body.mapping_id, body.from_element_id, body.to_element_id
    ) from exc

  return AssociationResponse(
    id=assoc.id,
    structure_id=assoc.structure_id,
    from_element_id=assoc.from_element_id,
    from_element_name=from_elem.name,
    from_element_qname=from_elem.qname,
    to_element_id=assoc.to_element_id,
    to_element_name=to_elem.name,
    to_element_qname=to_elem.qname,
    association_type=assoc.association_type,
    order_value=assoc.order_value,
    weight=assoc.weight,
    confidence=assoc.confidence,
    suggested_by=assoc.suggested_by,
    approved_by=assoc.approved_by,
  )


def delete_mapping_association(
  session: Session, body: DeleteMappingAssociationOperation
) -> DeleteResult:
  """Delete a mapping association edge (the inverse of create).

  Raises ``AssociationNotFoundError`` (→ 404) when no edge matches, or
  ``LibraryImmutableError`` (→ 403) for library-seeded rows. Used to
  correct a wrong mapping: delete the bad edge, then re-create the right
  one with ``create_mapping_association``.
  """
  assoc = session.execute(
    select(Association).where(
      Association.id == body.association_id,
      Association.structure_id == body.mapping_id,
    )
  ).scalar_one_or_none()
  if assoc is None:
    raise AssociationNotFoundError(body.association_id)
  # Match update_association / delete_association — reject library-seeded
  # rows at the service layer so the caller sees LibraryImmutableError
  # (→ 403) instead of a bare DB trigger ProgrammingError (→ 500).
  assert_not_library_origin(assoc)
  _delete_association_dependents(session, [body.association_id])
  session.query(Association).filter(
    Association.id == body.association_id,
    Association.structure_id == body.mapping_id,
  ).delete(synchronize_session=False)
  return DeleteResult(deleted=True)


def _delete_association_dependents(
  session: Session, association_ids: list[str]
) -> None:
  """Delete rows that FK association ids before hard-deleting associations."""
  if not association_ids:
    return

  rule_ids = (
    session.execute(
      select(Rule.id).where(Rule.target_association_id.in_(association_ids))
    )
    .scalars()
    .all()
  )
  if rule_ids:
    session.query(VerificationResult).filter(
      VerificationResult.rule_id.in_(rule_ids)
    ).delete(synchronize_session=False)
    session.query(Rule).filter(Rule.id.in_(rule_ids)).delete(synchronize_session=False)

  session.query(AssociationClassification).filter(
    AssociationClassification.association_id.in_(association_ids)
  ).delete(synchronize_session=False)


# ─── Taxonomy update / delete ─────────────────────────────────────────────


# ─── Structure update / delete ────────────────────────────────────────────


# ─── Association bulk create / update / delete ───────────────────────────


def delete_association(session: Session, body: DeleteAssociationRequest) -> dict:
  """Hard delete an association.

  Associations are cheap edges; we don't soft-delete them. If you need
  to remove many at once, use `update-taxonomy-block` with
  `associations_to_remove`.

  Raises `AssociationNotFoundError` if the association does not exist.
  Returns `{"deleted": True}` on success so the route layer has a
  consistent response shape with other delete ops.
  """
  assoc = session.execute(
    select(Association).where(Association.id == body.association_id)
  ).scalar_one_or_none()
  if assoc is None:
    raise AssociationNotFoundError(body.association_id)
  assert_not_library_origin(assoc)
  _delete_association_dependents(session, [body.association_id])

  deleted = (
    session.query(Association)
    .filter(Association.id == body.association_id)
    .delete(synchronize_session=False)
  )
  if not deleted:
    raise AssociationNotFoundError(body.association_id)
  return {"deleted": True}


# ─── Entity ↔ Taxonomy linkage ────────────────────────────────────────────


class EntityNotFoundError(LookupError):
  """Raised when no entity exists in the graph."""


def link_entity_taxonomy(
  session: Session, body: LinkEntityTaxonomyRequest
) -> EntityTaxonomyResponse:
  """Link the graph's entity to a taxonomy (creates ENTITY_HAS_TAXONOMY edge).

  Idempotent: if the exact (entity, taxonomy, basis) combination already
  exists, returns the existing row without error.

  Raises `EntityNotFoundError` if no entity exists in the graph, or
  `TaxonomyNotFoundError` if the taxonomy doesn't exist.
  """
  entity = resolve_parent_entity(session)
  if entity is None:
    raise EntityNotFoundError("No entity found in this graph")

  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.id == body.taxonomy_id)
  ).scalar_one_or_none()
  if taxonomy is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)

  # Check for existing adoption at this (entity, taxonomy, basis) combo
  existing = session.execute(
    select(EntityTaxonomy).where(
      EntityTaxonomy.entity_id == entity.id,
      EntityTaxonomy.taxonomy_id == body.taxonomy_id,
      EntityTaxonomy.basis == body.basis,
    )
  ).scalar_one_or_none()

  if existing:
    return EntityTaxonomyResponse(
      entity_id=existing.entity_id,
      taxonomy_id=existing.taxonomy_id,
      basis=existing.basis,
      is_primary=existing.is_primary,
      adoption_context=existing.adoption_context,
    )

  # If requesting is_primary=true, clear any existing primary for this
  # (entity, basis) pair. The partial unique index
  # idx_entity_taxonomies_primary enforces at most one primary per basis.
  if body.is_primary:
    session.query(EntityTaxonomy).filter(
      EntityTaxonomy.entity_id == entity.id,
      EntityTaxonomy.basis == body.basis,
      EntityTaxonomy.is_primary.is_(True),
    ).update({"is_primary": False}, synchronize_session=False)
    session.flush()

  adoption = EntityTaxonomy(
    entity_id=entity.id,
    taxonomy_id=body.taxonomy_id,
    is_primary=body.is_primary,
    basis=body.basis,
    adoption_context=body.adoption_context,
  )
  session.add(adoption)
  try:
    session.flush()
  except IntegrityError as exc:
    # Concurrent identical adoption, or a concurrent primary for the same
    # basis (`idx_entity_taxonomies_primary`) landing between the clear above
    # and this insert. Both are "already linked", not a fault.
    if not violates(exc, "uq_entity_taxonomy_combo", "idx_entity_taxonomies_primary"):
      raise
    raise EntityTaxonomyConflictError(
      f"Entity {entity.id} already has a {body.basis!r} link to taxonomy "
      f"{body.taxonomy_id!r} (or another primary for that basis landed "
      "concurrently). Retry to read the current state."
    ) from exc

  return EntityTaxonomyResponse(
    entity_id=adoption.entity_id,
    taxonomy_id=adoption.taxonomy_id,
    basis=adoption.basis,
    is_primary=adoption.is_primary,
    adoption_context=adoption.adoption_context,
  )
