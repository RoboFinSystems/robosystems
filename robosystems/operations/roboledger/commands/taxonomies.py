"""Write operations for mapping associations and entity↔taxonomy links.

There are deliberately no row-level writers for taxonomies or structures:
tenant taxonomy writes go through the taxonomy block operations
(`operations/taxonomy_block/`), which apply a whole envelope in one
transaction and run the block validators. Extend those rather than adding a
row writer that bypasses them.
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
from robosystems.operations.taxonomy_block.immutability import (
  assert_history_undisturbed,
)
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
  def __init__(self, mapping_id: str) -> None:
    super().__init__(f"Mapping structure not found: {mapping_id}")
    self.mapping_id = mapping_id


class TaxonomyNotFoundError(LookupError):
  def __init__(self, taxonomy_id: str) -> None:
    super().__init__(f"Taxonomy not found: {taxonomy_id}")
    self.taxonomy_id = taxonomy_id


class AssociationNotFoundError(LookupError):
  def __init__(self, association_id: str) -> None:
    super().__init__(f"Association not found: {association_id}")
    self.association_id = association_id


class ElementNotFoundError(LookupError):
  def __init__(self, side: str, element_id: str) -> None:
    super().__init__(f"{side} element not found: {element_id}")
    self.side = side  # "source" or "target"
    self.element_id = element_id


class EntityTaxonomyConflictError(ValueError):
  """An entity↔taxonomy adoption collided with a concurrent identical link."""


class MappingTargetIsRollupError(ValueError):
  """The target is a subtotal the renderer sums from its children."""

  def __init__(self, qname: str | None) -> None:
    super().__init__(
      f"{qname} is a subtotal rendered from its children; map the account to "
      "the leaf concept beneath it (suggest-mapping lists the leaves)."
    )
    self.qname = qname


class MappingTargetNotRenderedError(MappingTargetIsRollupError):
  """The target is off the entity's statements, so its amount lands on the
  subtotal above it."""

  def __init__(self, qname: str | None) -> None:
    ValueError.__init__(
      self,
      f"{qname} is not on this entity's statements, so its amount would land "
      "on the subtotal above it; map the account to a concept the statements "
      "show (suggest-mapping lists them).",
    )
    self.qname = qname


def _assert_leaf_target(session: Session, target: Element) -> None:
  """Refuse a mapping target the graph's statements compute from children, or
  an rs-gaap concept they don't show (it would roll up into a subtotal).

  A direct fact on a computed concept overrides it at render, so the
  statement no longer articulates. Same rule as the suggester. A graph with
  no entity yet is judged on the default Style's subtotals only, since its
  legal form, and so its Style, is not known.
  """
  from robosystems.operations.operators.implementations.mapping.constants import (
    RS_GAAP_SYNTHESIZED_DETAIL_ALLOW,
  )
  from robosystems.operations.roboledger.reads.taxonomies import (
    _load_renderable_concepts,
    is_subtotal_target,
    load_subtotal_concepts,
  )
  from robosystems.operations.roboledger.reports.network_picker import (
    DEFAULT_STYLE_ID,
    load_primary_reporting_style,
  )

  try:
    style_id: str | None = load_primary_reporting_style(session)
  except LookupError:
    style_id = None
  if is_subtotal_target(
    target, load_subtotal_concepts(session, style_id or DEFAULT_STYLE_ID)
  ):
    raise MappingTargetIsRollupError(target.qname)
  if style_id is None or not (target.qname or "").startswith("rs-gaap:"):
    return
  renderable = _load_renderable_concepts(session, style_id)
  if (
    renderable
    and target.id not in renderable
    and target.qname not in RS_GAAP_SYNTHESIZED_DETAIL_ALLOW
  ):
    raise MappingTargetNotRenderedError(target.qname)


class MappingAssociationExistsError(ValueError):
  """The pair is already mapped on this structure (a clean 409, so a re-run
  can skip what exists)."""

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

  Raises `MappingStructureNotFoundError`, `ElementNotFoundError` (with
  ``side``), `MappingTargetIsRollupError`, or `MappingAssociationExistsError`.
  """
  structure = session.execute(
    select(Structure).where(Structure.id == body.mapping_id)
  ).scalar_one_or_none()
  if structure is None:
    raise MappingStructureNotFoundError(body.mapping_id)
  # A DB trigger also refuses library structures; this gives a clean 403.
  assert_not_library_origin(structure)

  from_elem = session.execute(
    select(Element).where(Element.id == body.from_element_id)
  ).scalar_one_or_none()
  if from_elem is None:
    raise ElementNotFoundError("source", body.from_element_id)
  # The source must be tenant-authored; the target may be a library row.
  assert_not_library_origin(from_elem)
  # A closed month's stamped statements went through the arcs as they stood;
  # a new arc on an account with history there would restate it.
  assert_history_undisturbed(session, account_ids=[body.from_element_id])

  to_elem = session.execute(
    select(Element).where(Element.id == body.to_element_id)
  ).scalar_one_or_none()
  if to_elem is None:
    raise ElementNotFoundError("target", body.to_element_id)
  if body.association_type == "mapping":
    _assert_leaf_target(session, to_elem)

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
    # Lost a race with a concurrent identical insert.
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
  """Delete a mapping association edge.

  Raises ``AssociationNotFoundError``, ``LibraryImmutableError``, or
  ``ProtectedFactsError`` when the account has landed history in a closed
  month.
  """
  assoc = session.execute(
    select(Association).where(
      Association.id == body.association_id,
      Association.structure_id == body.mapping_id,
    )
  ).scalar_one_or_none()
  if assoc is None:
    raise AssociationNotFoundError(body.association_id)
  assert_not_library_origin(assoc)
  assert_history_undisturbed(session, account_ids=[str(assoc.from_element_id)])
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


# ─── Association bulk create / update / delete ───────────────────────────


def delete_association(session: Session, body: DeleteAssociationRequest) -> dict:
  """Hard delete an association. Raises `AssociationNotFoundError`."""
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
  """No entity exists in the graph."""


def link_entity_taxonomy(
  session: Session, body: LinkEntityTaxonomyRequest
) -> EntityTaxonomyResponse:
  """Link the graph's entity to a taxonomy; idempotent per (entity, taxonomy,
  basis).

  Raises `EntityNotFoundError` or `TaxonomyNotFoundError`.
  """
  entity = resolve_parent_entity(session)
  if entity is None:
    raise EntityNotFoundError("No entity found in this graph")

  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.id == body.taxonomy_id)
  ).scalar_one_or_none()
  if taxonomy is None:
    raise TaxonomyNotFoundError(body.taxonomy_id)

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

  # At most one primary per basis (idx_entity_taxonomies_primary).
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
    # A concurrent identical adoption or primary for the same basis.
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
