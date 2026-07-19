"""Update-path validator — projects the post-delta state and reuses create phases.

Update scope covers all deltas: ``_to_add``, ``_to_update``, and
``_to_remove`` for every atom type, plus ``rules_to_add`` /
``rules_to_remove`` and top-level field updates. This validator
synthesizes a virtual :class:`CreateTaxonomyBlockRequest` representing
the post-delta state of the taxonomy (DB rows minus removals, with
mutations applied, plus additions), then runs the create-time phases
against that projection. Delta-specific guards (library origin
immutability, live fact/line-item dependencies, cross-taxonomy
mappings, orphan children) run after the projection check.
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
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.taxonomy_block.validators import (
  ValidationIssue,
  validate_create_envelope,
)

_LIBRARY_SEEDER = "library-seeder"


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
  taxonomy_structure_id_set = set(current_structure_ids)
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
  element_id_by_qname = {e.qname: e.id for e in current_elements if e.qname}

  _resolve_foreign_element_qnames(
    session, qname_by_element_id, current_elements, current_associations
  )

  structures_to_remove = set(payload.structures_to_remove or [])
  elements_to_remove_qnames = set(payload.elements_to_remove or [])
  associations_to_remove_ids = set(payload.associations_to_remove or [])

  element_patches_by_qname = {p.qname: p for p in payload.elements_to_update}
  structure_patches_by_id = {p.structure_id: p for p in payload.structures_to_update}

  for sid, patch in structure_patches_by_id.items():
    if sid in structure_name_by_id and patch.name is not None:
      structure_name_by_id[sid] = patch.name

  virtual_elements: list[TaxonomyBlockElementRequest] = []
  for e in current_elements:
    qname = str(e.qname) if e.qname else ""
    if qname in elements_to_remove_qnames:
      continue
    patch = element_patches_by_qname.get(qname)
    if patch is None:
      virtual_elements.append(
        TaxonomyBlockElementRequest(
          qname=qname,
          name=str(e.name),
          trait=_element_classification_hint(e),
          balance_type=str(e.balance_type) if e.balance_type else None,
          period_type=str(e.period_type) if e.period_type else None,
          element_type=str(e.element_type) if e.element_type else "concept",
          is_monetary=bool(e.is_monetary),
          description=str(e.description) if e.description else None,
          code=str(e.code) if e.code else None,
          parent_ref=(qname_by_element_id.get(e.parent_id) if e.parent_id else None),
          metadata={},
        )
      )
      continue
    current_parent_ref = qname_by_element_id.get(e.parent_id) if e.parent_id else None
    virtual_elements.append(
      TaxonomyBlockElementRequest(
        qname=qname,
        name=patch.name if patch.name is not None else str(e.name),
        trait=(
          patch.trait if patch.trait is not None else _element_classification_hint(e)
        ),
        balance_type=(
          patch.balance_type
          if patch.balance_type is not None
          else (str(e.balance_type) if e.balance_type else None)
        ),
        period_type=(
          patch.period_type
          if patch.period_type is not None
          else (str(e.period_type) if e.period_type else None)
        ),
        element_type=str(e.element_type) if e.element_type else "concept",
        is_monetary=(
          patch.is_monetary if patch.is_monetary is not None else bool(e.is_monetary)
        ),
        description=(
          patch.description
          if patch.description is not None
          else (str(e.description) if e.description else None)
        ),
        code=(
          patch.code if patch.code is not None else (str(e.code) if e.code else None)
        ),
        parent_ref=(
          patch.parent_ref if patch.parent_ref is not None else current_parent_ref
        ),
        metadata={},
      )
    )
  virtual_elements.extend(payload.elements_to_add)

  virtual_structures: list[TaxonomyBlockStructureRequest] = []
  for s in current_structures:
    if s.id in structures_to_remove:
      continue
    patch = structure_patches_by_id.get(s.id)
    virtual_structures.append(
      TaxonomyBlockStructureRequest(
        name=patch.name if patch and patch.name is not None else str(s.name),
        block_type=str(s.block_type),
        description=(
          patch.description
          if patch and patch.description is not None
          else (str(s.description) if s.description else None)
        ),
      )
    )
  virtual_structures.extend(payload.structures_to_add)

  virtual_associations: list[TaxonomyBlockAssociationRequest] = []
  for a in current_associations:
    if a.id in associations_to_remove_ids:
      continue
    if a.structure_id in structures_to_remove:
      continue
    from_qname = qname_by_element_id.get(a.from_element_id, "")
    to_qname = qname_by_element_id.get(a.to_element_id, "")
    if from_qname in elements_to_remove_qnames:
      continue
    if to_qname in elements_to_remove_qnames:
      continue
    virtual_associations.append(
      TaxonomyBlockAssociationRequest(
        structure_ref=structure_name_by_id.get(a.structure_id, ""),
        from_ref=from_qname,
        to_ref=to_qname,
        association_type=str(a.association_type),
        order_value=float(a.order_value) if a.order_value is not None else None,
        arcrole=str(a.arcrole) if a.arcrole else None,
        weight=float(a.weight) if a.weight is not None else None,
      )
    )
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
  issues.extend(
    _validate_elements_to_update(session, taxonomy, payload, element_id_by_qname)
  )
  issues.extend(
    _validate_elements_to_remove(session, taxonomy, payload, element_id_by_qname)
  )
  issues.extend(
    _validate_structures_to_update(
      session, taxonomy, payload, taxonomy_structure_id_set
    )
  )
  issues.extend(
    _validate_structures_to_remove(
      session, taxonomy, payload, taxonomy_structure_id_set
    )
  )
  issues.extend(
    _validate_associations_to_remove(
      session, taxonomy, payload, taxonomy_structure_id_set
    )
  )

  return issues


def _resolve_foreign_element_qnames(
  session: Session,
  qname_by_element_id: dict,
  current_elements: list[Element],
  current_associations: list[Association],
) -> None:
  """Fold library-element qnames into the projection's id→qname map.

  Extend-mode taxonomies arc from / parent under LIBRARY concepts, whose
  Element rows live in the parent taxonomy — without resolving those
  foreign ids the projection stringifies them to ``""`` and every update
  fails reference resolution (``phantom_from_ref``), even though the
  create validator resolves the same refs via ``_load_library_qnames``.
  Mutates ``qname_by_element_id`` in place.
  """
  foreign_ids = {
    eid
    for a in current_associations
    for eid in (a.from_element_id, a.to_element_id)
    if eid and eid not in qname_by_element_id
  } | {
    e.parent_id
    for e in current_elements
    if e.parent_id and e.parent_id not in qname_by_element_id
  }
  if not foreign_ids:
    return
  for eid, qname in session.execute(
    select(Element.id, Element.qname).where(Element.id.in_(foreign_ids))
  ).all():
    if qname:
      qname_by_element_id[eid] = qname


def _element_classification_hint(element: Element) -> str | None:
  """Best-effort EFS trait for projection — None if not carried on the row.

  The live DB row doesn't always carry a trait (it lives in the
  element_traits junction), but for update validation we don't need to
  re-check traits of rows that already validated at create time. Return
  None so the CoA type-specific phase skips them (it only rejects *new*
  rows with missing trait when the payload explicitly adds them).
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


def _validate_elements_to_update(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  element_id_by_qname: dict[str, str],
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  if not payload.elements_to_update:
    return issues

  qnames = [p.qname for p in payload.elements_to_update]
  rows = session.execute(
    select(Element.qname, Element.created_by).where(
      Element.taxonomy_id == taxonomy.id, Element.qname.in_(qnames)
    )
  ).all()
  seeder_qnames = {qname for qname, created_by in rows if created_by == _LIBRARY_SEEDER}

  for patch in payload.elements_to_update:
    if patch.qname not in element_id_by_qname:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="unknown_element_qname",
          message=(
            f"element {patch.qname!r} is not in taxonomy {taxonomy.id!r} "
            f"and cannot be updated."
          ),
          context={"qname": patch.qname},
        )
      )
      continue
    if patch.qname in seeder_qnames:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="library_element_immutable",
          message=(
            f"element {patch.qname!r} was seeded by the library "
            f"(created_by='library-seeder') and cannot be updated via envelope."
          ),
          context={"qname": patch.qname},
        )
      )

  return issues


def _validate_elements_to_remove(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  element_id_by_qname: dict[str, str],
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  if not payload.elements_to_remove:
    return issues

  removal_qnames = list(payload.elements_to_remove)
  rows = session.execute(
    select(Element.qname, Element.id, Element.created_by).where(
      Element.taxonomy_id == taxonomy.id, Element.qname.in_(removal_qnames)
    )
  ).all()
  found_by_qname = {qname: (eid, created_by) for qname, eid, created_by in rows}
  removal_ids = [found_by_qname[q][0] for q in removal_qnames if q in found_by_qname]

  for qname in removal_qnames:
    found = found_by_qname.get(qname)
    if found is None:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="unknown_element_qname",
          message=(
            f"element {qname!r} is not in taxonomy {taxonomy.id!r} and "
            f"cannot be removed."
          ),
          context={"qname": qname},
        )
      )
      continue
    element_id, created_by = found
    if created_by == _LIBRARY_SEEDER:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="library_element_immutable",
          message=(
            f"element {qname!r} was seeded by the library "
            f"(created_by='library-seeder') and cannot be removed via envelope."
          ),
          context={"qname": qname},
        )
      )
      continue

    fact_count = len(
      session.execute(select(Fact.id).where(Fact.element_id == element_id))
      .scalars()
      .all()
    )
    if fact_count > 0:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="element_has_facts",
          message=(
            f"element {qname!r} has {fact_count} live fact(s); clear them "
            f"before removing the element."
          ),
          context={"qname": qname, "fact_count": fact_count},
        )
      )
      continue

    line_item_count = len(
      session.execute(select(LineItem.id).where(LineItem.element_id == element_id))
      .scalars()
      .all()
    )
    if line_item_count > 0:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="element_has_line_items",
          message=(
            f"element {qname!r} has {line_item_count} journal line item(s) "
            f"referencing it; clear them before removing the element."
          ),
          context={"qname": qname, "line_item_count": line_item_count},
        )
      )
      continue

    cross_mapping_count = len(
      session.execute(
        select(Association.id)
        .join(Structure, Association.structure_id == Structure.id)
        .where(
          Structure.taxonomy_id != taxonomy.id,
          (
            (Association.from_element_id == element_id)
            | (Association.to_element_id == element_id)
          ),
        )
      )
      .scalars()
      .all()
    )
    if cross_mapping_count > 0:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="element_has_cross_taxonomy_mappings",
          message=(
            f"element {qname!r} is referenced by {cross_mapping_count} "
            f"mapping association(s) from other taxonomies; clear those "
            f"before removing the element."
          ),
          context={"qname": qname, "mapping_count": cross_mapping_count},
        )
      )
      continue

    orphan_child_ids = (
      session.execute(
        select(Element.id).where(
          Element.parent_id == element_id,
          ~Element.id.in_(removal_ids),
        )
      )
      .scalars()
      .all()
    )
    if orphan_child_ids:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="element_has_children",
          message=(
            f"element {qname!r} has {len(orphan_child_ids)} child element(s) "
            f"not included in the removal set; include them or re-parent them "
            f"first."
          ),
          context={"qname": qname, "child_count": len(orphan_child_ids)},
        )
      )

  return issues


def _validate_structures_to_update(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  taxonomy_structure_ids: set[str],
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  if not payload.structures_to_update:
    return issues

  for patch in payload.structures_to_update:
    if patch.structure_id not in taxonomy_structure_ids:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="unknown_structure_id",
          message=(
            f"structure {patch.structure_id!r} is not in taxonomy "
            f"{taxonomy.id!r} and cannot be updated."
          ),
          context={"structure_id": patch.structure_id},
        )
      )

  return issues


def _validate_structures_to_remove(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  taxonomy_structure_ids: set[str],
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  if not payload.structures_to_remove:
    return issues

  for sid in payload.structures_to_remove:
    if sid not in taxonomy_structure_ids:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="unknown_structure_id",
          message=(
            f"structure {sid!r} is not in taxonomy {taxonomy.id!r} and "
            f"cannot be removed."
          ),
          context={"structure_id": sid},
        )
      )

  return issues


def _validate_associations_to_remove(
  session: Session,
  taxonomy: Taxonomy,
  payload: UpdateTaxonomyBlockRequest,
  taxonomy_structure_ids: set[str],
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  if not payload.associations_to_remove:
    return issues

  rows = session.execute(
    select(Association.id, Association.structure_id).where(
      Association.id.in_(payload.associations_to_remove)
    )
  ).all()
  found_by_id = dict(rows)

  for aid in payload.associations_to_remove:
    sid = found_by_id.get(aid)
    if sid is None:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="unknown_association_id",
          message=f"association {aid!r} does not exist.",
          context={"association_id": aid},
        )
      )
      continue
    if sid not in taxonomy_structure_ids:
      issues.append(
        ValidationIssue(
          phase="delta_validation",
          code="association_not_in_taxonomy",
          message=(
            f"association {aid!r} belongs to a structure outside taxonomy "
            f"{taxonomy.id!r} and cannot be removed from this envelope."
          ),
          context={"association_id": aid},
        )
      )

  return issues


__all__ = [
  "validate_update_envelope",
]
