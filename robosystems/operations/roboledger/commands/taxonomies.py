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
from robosystems.operations.roboledger.entity_scope import (
  resolve_entity,
  resolve_entity_id,
)
from robosystems.operations.taxonomy_block.coa_mappings import COA_MAPPING_BLOCK_TYPE
from robosystems.operations.taxonomy_block.immutability import (
  assert_history_undisturbed,
)
from robosystems.operations.taxonomy_block.reclassification import (
  Reclassification,
  judge_remap,
  reclassification_record,
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


def assert_leaf_target(
  session: Session, target: Element, mapping_id: str | None = None
) -> None:
  """Refuse a mapping target the statements compute from children.

  A direct fact on a computed concept overrides it at render, so the
  statement no longer articulates. Same rule as the suggester. The
  statements are those of the entity whose chart ``mapping_id`` maps from,
  under its own Reporting Style (the group parent's without one). A graph
  with no entity yet is judged on the default Style.
  """
  from robosystems.operations.roboledger.reads.taxonomies import (
    is_subtotal_target,
    load_subtotal_concepts,
  )
  from robosystems.operations.roboledger.reports.network_picker import (
    DEFAULT_STYLE_ID,
    load_entity_reporting_style,
  )
  from robosystems.operations.taxonomy_block.coa_mappings import mapping_owner_id

  owner = mapping_owner_id(session, mapping_id) if mapping_id else None
  try:
    style_id = load_entity_reporting_style(session, resolve_entity_id(session, owner))
  except LookupError:
    style_id = DEFAULT_STYLE_ID
  if is_subtotal_target(target, load_subtotal_concepts(session, style_id)):
    raise MappingTargetIsRollupError(target.qname)


def assert_mapping_leaf(
  session: Session, to_element_id: str, mapping_id: str | None = None
) -> None:
  """``assert_leaf_target`` for a write path that holds only the target's id."""
  target = session.get(Element, to_element_id)
  if target is not None:
    assert_leaf_target(session, target, mapping_id)


class ReplacedArcMismatchError(ValueError):
  """``replaces_association_id`` names an arc that is not this account's
  mapping arc in this structure."""

  def __init__(self, association_id: str, from_element_id: str) -> None:
    super().__init__(
      f"association {association_id!r} is not a mapping arc of account "
      f"{from_element_id!r} in this structure, so it cannot be replaced here."
    )


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
  """Add a mapping association (CoA element → reporting concept), replacing
  the account's current arc in the same write when ``replaces_association_id``
  names it.

  Raises `MappingStructureNotFoundError`, `ElementNotFoundError` (with
  ``side``), `AssociationNotFoundError` / `ReplacedArcMismatchError` for a
  bad ``replaces_association_id``, `MappingTargetIsRollupError`,
  `MappingAssociationExistsError`, or `ProtectedFactsError` when the account
  has landed history in a closed month and the write would restate it: a
  plain new arc, or a replacement that crosses a primary statement.
  """
  structure = session.execute(
    select(Structure).where(
      Structure.id == body.mapping_id,
      Structure.block_type == COA_MAPPING_BLOCK_TYPE,
    )
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

  to_elem = session.execute(
    select(Element).where(Element.id == body.to_element_id)
  ).scalar_one_or_none()
  if to_elem is None:
    raise ElementNotFoundError("target", body.to_element_id)

  replaced: Association | None = None
  reclassified: Reclassification | None = None
  if body.replaces_association_id:
    replaced = session.execute(
      select(Association).where(
        Association.id == body.replaces_association_id,
        Association.structure_id == body.mapping_id,
        Association.association_type == body.association_type,
      )
    ).scalar_one_or_none()
    if replaced is None:
      raise AssociationNotFoundError(body.replaces_association_id)
    if str(replaced.from_element_id) != body.from_element_id:
      raise ReplacedArcMismatchError(body.replaces_association_id, body.from_element_id)
    assert_not_library_origin(replaced)
    if str(replaced.to_element_id) == body.to_element_id:
      raise MappingAssociationExistsError(
        body.mapping_id, body.from_element_id, body.to_element_id
      )
    # A closed month's stamped statements went through the arcs as they
    # stood. Moving the account within its statement keeps every total the
    # close asserted, so it is a reclassification; across one, a restatement.
    reclassified = judge_remap(
      session,
      account_id=body.from_element_id,
      account_taxonomy_id=str(from_elem.taxonomy_id),
      account_qname=from_elem.qname,
      old_target_id=str(replaced.to_element_id),
      new_target_id=body.to_element_id,
    )
  else:
    # A new arc on an account with history there would restate it.
    assert_history_undisturbed(session, account_ids=[body.from_element_id])

  if body.association_type == "mapping":
    assert_leaf_target(session, to_elem, body.mapping_id)

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

  previous_target_qname: str | None = None
  if replaced is not None:
    previous = session.get(Element, str(replaced.to_element_id))
    if previous is not None and previous.qname:
      previous_target_qname = str(previous.qname)
    _delete_association_dependents(session, [str(replaced.id)])
    session.query(Association).filter(Association.id == replaced.id).delete(
      synchronize_session=False
    )
    session.flush()

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
    metadata_=(
      reclassification_record(
        reclassified,
        replaces_association_id=str(replaced.id),
        previous_target_qname=previous_target_qname,
        reason=body.reason,
      )
      if reclassified is not None and replaced is not None
      else {}
    ),
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
    replaced_association_id=str(replaced.id) if replaced is not None else None,
    reclassified_periods=(
      list(reclassified.closed_periods) if reclassified is not None else []
    ),
  )


def delete_mapping_association(
  session: Session, body: DeleteMappingAssociationOperation
) -> DeleteResult:
  """Delete a mapping association edge.

  Raises ``AssociationNotFoundError``, ``LibraryImmutableError``, or
  ``ProtectedFactsError`` when the account has landed history in a closed
  month: removing its arc alone would drop its balance off the statement.
  Move such an account with ``create_mapping_association`` and
  ``replaces_association_id`` instead.
  """
  assoc = session.execute(
    select(Association)
    .join(Structure, Structure.id == Association.structure_id)
    .where(
      Association.id == body.association_id,
      Association.structure_id == body.mapping_id,
      Structure.block_type == COA_MAPPING_BLOCK_TYPE,
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
  """Link an entity to a taxonomy, default the group parent; idempotent per
  (entity, taxonomy, basis).

  Raises `EntityNotFoundError` or `TaxonomyNotFoundError`.
  """
  try:
    entity = resolve_entity(session, body.entity_id)
  except LookupError as exc:
    raise EntityNotFoundError(str(exc)) from exc

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
