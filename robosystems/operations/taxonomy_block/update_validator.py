"""Update-path validator — projects the post-delta state and reuses create phases.

Phase 2.4 update scope covers only additive deltas (``elements_to_add``,
``structures_to_add``, ``associations_to_add``, ``rules_to_add``,
``rules_to_remove``) plus top-level field updates. This validator
synthesizes a virtual :class:`CreateTaxonomyBlockRequest` representing
the union of the current DB state + the additions, then runs the six
create-time phases against that projection.

Removal/update deltas (``elements_to_update``, ``elements_to_remove``,
etc.) are rejected at the handler layer with a "Phase 2.4.1" message
before this validator runs.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  TaxonomyBlockAssociationRequest,
  TaxonomyBlockElementRequest,
  TaxonomyBlockStructureRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
  Taxonomy,
)
from robosystems.operations.taxonomy_block.validators import (
  ValidationIssue,
  validate_create_envelope,
)


def validate_update_envelope(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
) -> list[ValidationIssue]:
  """Project post-update state + run create phases against it."""
  issues: list[ValidationIssue] = []

  current_elements = (
    session.execute(select(Element).where(Element.taxonomy_id == taxonomy.id))
    .scalars()
    .all()
  )
  current_structures = (
    session.execute(select(Structure).where(Structure.taxonomy_id == taxonomy.id))
    .scalars()
    .all()
  )
  current_structure_ids = [s.id for s in current_structures]
  current_associations: list[Association] = []
  if current_structure_ids:
    current_associations = list(
      session.execute(
        select(Association).where(Association.structure_id.in_(current_structure_ids))
      )
      .scalars()
      .all()
    )

  structure_name_by_id = {s.id: s.name for s in current_structures}
  qname_by_element_id = {e.id: e.qname for e in current_elements}

  virtual_elements: list[TaxonomyBlockElementRequest] = [
    TaxonomyBlockElementRequest(
      qname=str(e.qname) if e.qname else "",
      name=str(e.name),
      classification=_element_classification_hint(e),
      balance_type=str(e.balance_type) if e.balance_type else None,
      period_type=str(e.period_type) if e.period_type else None,
      element_type=str(e.element_type) if e.element_type else "concept",
      is_monetary=bool(e.is_monetary),
      description=str(e.description) if e.description else None,
      code=str(e.code) if e.code else None,
      parent_ref=(qname_by_element_id.get(e.parent_id) if e.parent_id else None),
      metadata={},
    )
    for e in current_elements
  ]
  virtual_elements.extend(payload.elements_to_add)

  virtual_structures: list[TaxonomyBlockStructureRequest] = [
    TaxonomyBlockStructureRequest(
      name=str(s.name),
      structure_type=str(s.structure_type),
      description=str(s.description) if s.description else None,
    )
    for s in current_structures
  ]
  virtual_structures.extend(payload.structures_to_add)

  virtual_associations: list[TaxonomyBlockAssociationRequest] = [
    TaxonomyBlockAssociationRequest(
      structure_ref=structure_name_by_id.get(a.structure_id, ""),
      from_ref=qname_by_element_id.get(a.from_element_id, ""),
      to_ref=qname_by_element_id.get(a.to_element_id, ""),
      association_type=str(a.association_type),
      order_value=float(a.order_value) if a.order_value is not None else None,
      arcrole=str(a.arcrole) if a.arcrole else None,
      weight=float(a.weight) if a.weight is not None else None,
    )
    for a in current_associations
  ]
  virtual_associations.extend(payload.associations_to_add)

  projection = CreateTaxonomyBlockRequest(
    name=str(taxonomy.name),
    taxonomy_type=str(taxonomy.taxonomy_type),
    parent_taxonomy_id=(
      str(taxonomy.parent_taxonomy_id) if taxonomy.parent_taxonomy_id else None
    ),
    version=str(taxonomy.version) if taxonomy.version else None,
    description=str(taxonomy.description) if taxonomy.description else None,
    standard=str(taxonomy.standard) if taxonomy.standard else None,
    namespace_uri=str(taxonomy.namespace_uri) if taxonomy.namespace_uri else None,
    elements=virtual_elements,
    structures=virtual_structures,
    associations=virtual_associations,
    rules=payload.rules_to_add,
    metadata={},
  )

  issues.extend(validate_create_envelope(projection, session))

  issues.extend(_validate_rules_to_remove(session, taxonomy, payload))

  return issues


def _element_classification_hint(element: Element) -> str | None:
  """Best-effort classification for projection — None if not carried on the row.

  The live DB row doesn't always carry a classification (it lives in
  the element_classifications junction), but for update validation we
  don't need to re-check classifications of rows that already
  validated at create time. Return None so the CoA type-specific phase
  skips them (it only rejects *new* rows with missing classification
  when the payload explicitly adds them).
  """
  return None


def _validate_rules_to_remove(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  if not payload.rules_to_remove:
    return issues

  rows = (
    session.execute(select(Rule).where(Rule.id.in_(payload.rules_to_remove)))
    .scalars()
    .all()
  )
  found_by_id = {r.id: r for r in rows}

  for rule_id in payload.rules_to_remove:
    rule = found_by_id.get(rule_id)
    if rule is None:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="unknown_rule_id",
          message=f"rule {rule_id!r} does not exist.",
          context={"rule_id": rule_id},
        )
      )
      continue
    if rule.taxonomy_id != taxonomy.id and rule.target_taxonomy_id != taxonomy.id:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="rule_not_in_taxonomy",
          message=(
            f"rule {rule_id!r} does not belong to taxonomy {taxonomy.id!r} "
            f"and cannot be removed from this envelope."
          ),
          context={"rule_id": rule_id},
        )
      )
      continue
    if rule.rule_origin == "auto":
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="auto_rule_immutable",
          message=(
            f"rule {rule_id!r} was auto-generated (rule_origin='auto') "
            f"and cannot be removed via update."
          ),
          context={"rule_id": rule_id},
        )
      )

  return issues


def reject_unsupported_deltas(payload: UpdateTaxonomyBlockRequest) -> None:
  """Raise ValueError for delta kinds not yet in scope.

  Phase 2.4 ships `_to_add` for all atom types and rules_to_remove
  by id. Everything else (updates + removals of elements/structures/
  associations) is a Phase 2.4.1 concern and rejected here so tenants
  get a clear error instead of silent no-op.
  """
  unsupported: list[str] = []
  if payload.elements_to_update:
    unsupported.append("elements_to_update")
  if payload.elements_to_remove:
    unsupported.append("elements_to_remove")
  if payload.structures_to_update:
    unsupported.append("structures_to_update")
  if payload.structures_to_remove:
    unsupported.append("structures_to_remove")
  if payload.associations_to_remove:
    unsupported.append("associations_to_remove")

  if unsupported:
    raise ValueError(
      f"update-taxonomy-block delta kinds not yet supported (Phase 2.4.1): "
      f"{', '.join(unsupported)}"
    )


__all__ = [
  "reject_unsupported_deltas",
  "validate_update_envelope",
]
