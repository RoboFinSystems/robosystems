"""Write operations for taxonomies, structures, and mappings.

These commands are pure functions: take an open extensions `Session`
and validated Pydantic request bodies, return Pydantic response models.
Callers own session lifetime and translate domain exceptions into
transport errors.

They are the single source of truth for taxonomy-layer writes. The
REST operation surface, MCP tools, agents, and seeders (roboledger_demo)
all delegate here.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.common import DeleteResult
from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse,
  CreateMappingAssociationOperation,
  CreateStructureRequest,
  CreateTaxonomyRequest,
  DeleteAssociationRequest,
  DeleteMappingAssociationOperation,
  DeleteStructureRequest,
  DeleteTaxonomyRequest,
  EntityTaxonomyResponse,
  LinkEntityTaxonomyRequest,
  StructureResponse,
  TaxonomyResponse,
  UpdateAssociationRequest,
  UpdateStructureRequest,
  UpdateTaxonomyRequest,
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
from robosystems.models.extensions.entity import Entity
from robosystems.operations.roboledger.commands._guards import (
  LibraryImmutableError,
  assert_not_library_origin,
  assert_tenant_taxonomy,
)
from robosystems.utils.ulid import generate_prefixed_ulid

__all__ = [
  "AssociationNotFoundError",
  "ElementNotFoundError",
  "LibraryImmutableError",
  "MappingStructureNotFoundError",
  "StructureNotFoundError",
  "TaxonomyNotFoundError",
  "create_mapping_association",
  "create_structure",
  "create_taxonomy",
  "delete_association",
  "delete_mapping_association",
  "delete_structure",
  "delete_taxonomy",
  "link_entity_taxonomy",
  "update_association",
  "update_structure",
  "update_taxonomy",
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


class StructureNotFoundError(LookupError):
  """Raised when a structure is not found by id."""

  def __init__(self, structure_id: str) -> None:
    super().__init__(f"Structure not found: {structure_id}")
    self.structure_id = structure_id


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


def _taxonomy_to_response(row: Taxonomy) -> TaxonomyResponse:
  return TaxonomyResponse(
    id=row.id,
    name=row.name,
    description=row.description,
    taxonomy_type=row.taxonomy_type,
    version=row.version,
    standard=row.standard,
    namespace_uri=row.namespace_uri,
    is_shared=row.is_shared,
    is_active=row.is_active,
    is_locked=row.is_locked,
    source_taxonomy_id=row.source_taxonomy_id,
    target_taxonomy_id=row.target_taxonomy_id,
  )


def _structure_to_response(row: Structure) -> StructureResponse:
  return StructureResponse(
    id=row.id,
    name=row.name,
    description=row.description,
    block_type=row.block_type,
    taxonomy_id=row.taxonomy_id,
    is_active=row.is_active,
  )


def create_taxonomy(
  session: Session, body: CreateTaxonomyRequest, created_by: str
) -> TaxonomyResponse:
  """Insert a new taxonomy row and return its response representation.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``create-taxonomy-block`` / ``update-taxonomy-block``); this is
  invoked indirectly by those operations.
  """
  taxonomy = Taxonomy(
    id=generate_prefixed_ulid("tax"),
    name=body.name,
    description=body.description,
    taxonomy_type=body.taxonomy_type,
    version=body.version,
    source_taxonomy_id=body.source_taxonomy_id,
    target_taxonomy_id=body.target_taxonomy_id,
    created_by=created_by,
  )
  session.add(taxonomy)
  session.flush()
  return _taxonomy_to_response(taxonomy)


def create_structure(
  session: Session, body: CreateStructureRequest, created_by: str
) -> StructureResponse:
  """Insert a new structure row and return its response representation.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``create-taxonomy-block`` / ``update-taxonomy-block``); this is
  invoked indirectly by those operations.
  """
  # Tenants can only add structures to tenant-origin taxonomies.
  assert_tenant_taxonomy(session, body.taxonomy_id)
  structure = Structure(
    id=generate_prefixed_ulid("struct"),
    name=body.name,
    description=body.description,
    block_type=body.block_type,
    taxonomy_id=body.taxonomy_id,
    created_by=created_by,
  )
  session.add(structure)
  session.flush()
  return _structure_to_response(structure)


def create_mapping_association(
  session: Session,
  body: CreateMappingAssociationOperation,
  created_by: str,
) -> AssociationResponse:
  """Add a mapping association (CoA element → reporting concept).

  Raises `MappingStructureNotFoundError` if the mapping structure is
  missing, or `ElementNotFoundError` with `side="source"` / `"target"`
  if either element is missing. The caller translates these to HTTP
  status codes.
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
  session.flush()

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


def update_taxonomy(session: Session, body: UpdateTaxonomyRequest) -> TaxonomyResponse:
  """Update mutable fields on a taxonomy.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``create-taxonomy-block`` / ``update-taxonomy-block``); this is
  invoked indirectly by those operations.

  Uses `model_dump(exclude_unset=True)` — omitted fields are left
  unchanged, explicit nulls are applied. `taxonomy_type` is immutable
  and is not in the request model.

  Raises `TaxonomyNotFoundError` if the taxonomy does not exist.
  """
  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.id == body.taxonomy_id)
  ).scalar_one_or_none()
  if taxonomy is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)
  assert_not_library_origin(taxonomy)

  updates = body.model_dump(exclude_unset=True)
  updates.pop("taxonomy_id", None)
  for key, value in updates.items():
    setattr(taxonomy, key, value)
  session.flush()
  return _taxonomy_to_response(taxonomy)


def delete_taxonomy(session: Session, body: DeleteTaxonomyRequest) -> TaxonomyResponse:
  """Soft delete — sets `is_active=false`.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``delete-taxonomy-block``); this is invoked indirectly by that
  operation.

  Historical references (structures, elements, associations) remain
  valid. The taxonomy is simply no longer offered for new writes.

  Raises `TaxonomyNotFoundError` if the taxonomy does not exist.
  """
  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.id == body.taxonomy_id)
  ).scalar_one_or_none()
  if taxonomy is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)
  assert_not_library_origin(taxonomy)
  taxonomy.is_active = False
  session.flush()
  return _taxonomy_to_response(taxonomy)


# ─── Structure update / delete ────────────────────────────────────────────


def update_structure(
  session: Session, body: UpdateStructureRequest
) -> StructureResponse:
  """Update mutable fields on a structure.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``create-taxonomy-block`` / ``update-taxonomy-block``); this is
  invoked indirectly by those operations.

  `block_type` and `taxonomy_id` are immutable.
  Raises `StructureNotFoundError` if the structure does not exist.
  """
  structure = session.execute(
    select(Structure).where(Structure.id == body.structure_id)
  ).scalar_one_or_none()
  if structure is None:
    raise StructureNotFoundError(body.structure_id)
  assert_not_library_origin(structure)

  updates = body.model_dump(exclude_unset=True)
  updates.pop("structure_id", None)
  for key, value in updates.items():
    setattr(structure, key, value)
  session.flush()
  return _structure_to_response(structure)


def delete_structure(
  session: Session, body: DeleteStructureRequest
) -> StructureResponse:
  """Soft delete — sets `is_active=false`.

  Internal helper. Tenant writes go through the taxonomy block surface
  (``delete-taxonomy-block``); this is invoked indirectly by that
  operation.

  Raises `StructureNotFoundError` if the structure does not exist.
  """
  structure = session.execute(
    select(Structure).where(Structure.id == body.structure_id)
  ).scalar_one_or_none()
  if structure is None:
    raise StructureNotFoundError(body.structure_id)
  assert_not_library_origin(structure)
  structure.is_active = False
  session.flush()
  return _structure_to_response(structure)


# ─── Association bulk create / update / delete ───────────────────────────


def _association_to_response(
  row: Association,
  from_elem: Element | None = None,
  to_elem: Element | None = None,
) -> AssociationResponse:
  return AssociationResponse(
    id=row.id,
    structure_id=row.structure_id,
    from_element_id=row.from_element_id,
    from_element_name=from_elem.name if from_elem else None,
    from_element_qname=from_elem.qname if from_elem else None,
    to_element_id=row.to_element_id,
    to_element_name=to_elem.name if to_elem else None,
    to_element_qname=to_elem.qname if to_elem else None,
    association_type=row.association_type,
    order_value=row.order_value,
    weight=row.weight,
    confidence=row.confidence,
    suggested_by=row.suggested_by,
    approved_by=row.approved_by,
  )


def update_association(
  session: Session, body: UpdateAssociationRequest
) -> AssociationResponse:
  """Update mutable fields on an association. `from_element_id`,
  `to_element_id`, `association_type`, and `structure_id` are
  immutable — delete and recreate instead.

  Raises `AssociationNotFoundError` if the association does not exist.
  """
  assoc = session.execute(
    select(Association).where(Association.id == body.association_id)
  ).scalar_one_or_none()
  if assoc is None:
    raise AssociationNotFoundError(body.association_id)
  assert_not_library_origin(assoc)

  updates = body.model_dump(exclude_unset=True)
  updates.pop("association_id", None)
  for key, value in updates.items():
    setattr(assoc, key, value)
  session.flush()

  from_elem = session.get(Element, assoc.from_element_id)
  to_elem = session.get(Element, assoc.to_element_id)
  return _association_to_response(assoc, from_elem, to_elem)


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
  entity = session.execute(select(Entity).limit(1)).scalar_one_or_none()
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
  session.flush()

  return EntityTaxonomyResponse(
    entity_id=adoption.entity_id,
    taxonomy_id=adoption.taxonomy_id,
    basis=adoption.basis,
    is_primary=adoption.is_primary,
    adoption_context=adoption.adoption_context,
  )
