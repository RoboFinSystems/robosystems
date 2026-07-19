"""Handlers for ``taxonomy_type='reporting_extension'`` — tenant extension of a library.

A reporting extension is a tenant taxonomy that extends a library
``reporting_standard`` (us-gaap, ifrs, ...). Its elements can reference
library parents by qname; the handler resolves ``parent_ref`` /
``from_ref`` / ``to_ref`` against a merged symbol table:

1. Envelope-local qnames (just inserted in pass 1) win.
2. Library fallback — a scoped lookup in the parent taxonomy
   (``taxonomy_id == parent_taxonomy_id``) resolves qnames the tenant
   pulled in from the library.

Any qname that resolves neither locally nor in the parent library is a
ValueError. No global search — ambiguity is an error, not a convenience.

``build_envelope`` projects the extension's own atoms. When an element's
``parent_id`` points outside this taxonomy (i.e., at a library element),
the CoA pattern of ``element_qname_by_id`` from in-taxonomy rows only
misses it; a scoped second query resolves just those cross-taxonomy
parent ids to qnames.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  TaxonomyBlockAssociation,
  TaxonomyBlockElement,
  TaxonomyBlockEnvelope,
  TaxonomyBlockStructure,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import (
  Association,
  Element,
  ElementTrait,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.taxonomy_block._helpers import (
  qname_for,
  structure_from_request,
)
from robosystems.operations.taxonomy_block.auto_rules import (
  emit_auto_rules,
  refresh_auto_rules,
)
from robosystems.operations.taxonomy_block.cascade import (
  cascade_delete_taxonomy,
  preflight_delete,
)
from robosystems.operations.taxonomy_block.rule_persistence import (
  persist_tenant_rules,
)
from robosystems.operations.taxonomy_block.rule_reads import project_rules
from robosystems.operations.taxonomy_block.update_apply import (
  apply_associations_to_add,
  apply_associations_to_remove,
  apply_elements_to_add,
  apply_elements_to_remove,
  apply_elements_to_update,
  apply_rules_to_add,
  apply_rules_to_remove,
  apply_structures_to_add,
  apply_structures_to_remove,
  apply_structures_to_update,
  apply_top_level_fields,
)
from robosystems.operations.taxonomy_block.update_validator import (
  validate_update_envelope,
)
from robosystems.operations.taxonomy_block.validators import (
  TaxonomyBlockValidationError,
  validate_create_envelope,
)

REPORTING_EXTENSION_BLOCK_TYPE = "reporting_extension"
DISPLAY_NAME = "Reporting Extension"
DISPLAY_PLURAL = "Reporting Extensions"
CATEGORY = "Reporting"


def _library_qname_lookup(
  session: Session, parent_taxonomy_id: str, qnames: set[str]
) -> dict[str, str]:
  """Resolve a set of qnames to element ids scoped to the parent library."""
  if not qnames:
    return {}
  rows = session.execute(
    select(Element.qname, Element.id).where(
      Element.taxonomy_id == parent_taxonomy_id,
      Element.qname.in_(qnames),
    )
  ).all()
  return dict(rows)


def create(
  session: Session,
  payload: CreateTaxonomyBlockRequest,
  created_by: str,
) -> str:
  """Create a reporting_extension taxonomy that references a library parent.

  Returns the new taxonomy_id. Two-pass insert with local-first /
  library-fallback parent resolution.
  """
  if payload.taxonomy_type != REPORTING_EXTENSION_BLOCK_TYPE:
    raise ValueError(
      f"reporting_extension handler received payload with taxonomy_type="
      f"{payload.taxonomy_type!r}"
    )
  if not payload.parent_taxonomy_id:
    raise ValueError(
      "reporting_extension requires parent_taxonomy_id — the library "
      "reporting_standard being extended."
    )

  issues = validate_create_envelope(payload, session)
  if issues:
    raise TaxonomyBlockValidationError(issues)

  parent_taxonomy = session.get(Taxonomy, payload.parent_taxonomy_id)
  if parent_taxonomy is None:
    raise ValueError(
      f"parent_taxonomy_id {payload.parent_taxonomy_id!r} does not exist."
    )
  if parent_taxonomy.taxonomy_type != "reporting_standard":
    raise ValueError(
      f"parent_taxonomy_id {payload.parent_taxonomy_id!r} has type "
      f"{parent_taxonomy.taxonomy_type!r}; reporting_extension must extend "
      f"a reporting_standard taxonomy."
    )

  taxonomy = Taxonomy(
    name=payload.name,
    description=payload.description,
    taxonomy_type=REPORTING_EXTENSION_BLOCK_TYPE,
    parent_taxonomy_id=payload.parent_taxonomy_id,
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

  elements_by_qname: dict[str, Element] = {}
  parent_refs: dict[str, str | None] = {}

  for req in payload.elements:
    qname = req.qname or qname_for(payload.standard, "ext", req.code, req.name)
    element = Element(
      code=req.code,
      name=req.name,
      description=req.description,
      qname=qname,
      balance_type=req.balance_type or "debit",
      period_type=req.period_type or "duration",
      element_type=req.element_type,
      is_monetary=req.is_monetary,
      taxonomy_id=taxonomy.id,
      source="native",
      is_active=True,
      metadata_=dict(req.metadata),
      created_by=created_by,
    )
    session.add(element)
    elements_by_qname[qname] = element
    parent_refs[qname] = req.parent_ref

  session.flush()

  library_qnames_needed: set[str] = set()
  for qname, parent_ref in parent_refs.items():
    if parent_ref and parent_ref not in elements_by_qname:
      library_qnames_needed.add(parent_ref)
  for req in payload.associations:
    if req.from_ref not in elements_by_qname:
      library_qnames_needed.add(req.from_ref)
    if req.to_ref not in elements_by_qname:
      library_qnames_needed.add(req.to_ref)

  library_qname_to_id = _library_qname_lookup(
    session, payload.parent_taxonomy_id, library_qnames_needed
  )

  for qname, parent_ref in parent_refs.items():
    if not parent_ref:
      continue
    local = elements_by_qname.get(parent_ref)
    if local is not None:
      elements_by_qname[qname].parent_id = local.id
      continue
    library_id = library_qname_to_id.get(parent_ref)
    if library_id is None:
      raise ValueError(
        f"element {qname!r} references parent_ref {parent_ref!r} but no "
        f"such element exists in this envelope or in parent_taxonomy_id "
        f"{payload.parent_taxonomy_id!r}."
      )
    elements_by_qname[qname].parent_id = library_id

  structures_by_name: dict[str, Structure] = {}
  for req in payload.structures:
    structure = structure_from_request(
      req, taxonomy_id=taxonomy.id, created_by=created_by
    )
    session.add(structure)
    structures_by_name[req.name] = structure

  session.flush()

  for req in payload.associations:
    structure = structures_by_name.get(req.structure_ref)
    if structure is None:
      raise ValueError(
        f"association references structure {req.structure_ref!r} which "
        f"was not declared in this envelope."
      )
    from_local = elements_by_qname.get(req.from_ref)
    to_local = elements_by_qname.get(req.to_ref)
    from_id = from_local.id if from_local else library_qname_to_id.get(req.from_ref)
    to_id = to_local.id if to_local else library_qname_to_id.get(req.to_ref)
    if from_id is None:
      raise ValueError(
        f"association from_ref {req.from_ref!r} does not resolve in this "
        f"envelope or in parent_taxonomy_id {payload.parent_taxonomy_id!r}."
      )
    if to_id is None:
      raise ValueError(
        f"association to_ref {req.to_ref!r} does not resolve in this "
        f"envelope or in parent_taxonomy_id {payload.parent_taxonomy_id!r}."
      )
    association = Association(
      structure_id=structure.id,
      from_element_id=from_id,
      to_element_id=to_id,
      association_type=req.association_type,
      order_value=req.order_value,
      arcrole=req.arcrole,
      weight=req.weight,
      metadata_=dict(req.metadata),
      created_by=created_by,
    )
    session.add(association)

  session.flush()

  emit_auto_rules(
    session,
    taxonomy,
    list(structures_by_name.values()),
    created_by=created_by,
  )

  if payload.rules:
    persist_tenant_rules(
      session,
      taxonomy,
      payload.rules,
      elements_by_qname,
      structures_by_name,
      created_by,
    )
    session.flush()

  return taxonomy.id


def _element_factory(req, taxonomy: Taxonomy, updated_by: str) -> tuple[Element, str]:
  qname = req.qname or qname_for(taxonomy.standard, "ext", req.code, req.name)
  element = Element(
    code=req.code,
    name=req.name,
    description=req.description,
    qname=qname,
    balance_type=req.balance_type or "debit",
    period_type=req.period_type or "duration",
    element_type=req.element_type,
    is_monetary=req.is_monetary,
    taxonomy_id=taxonomy.id,
    source="native",
    is_active=True,
    metadata_=dict(req.metadata),
    created_by=updated_by,
  )
  return element, qname


def update(
  session: Session,
  payload: UpdateTaxonomyBlockRequest,
  updated_by: str,
) -> str:
  taxonomy = session.get(Taxonomy, payload.taxonomy_id)
  if taxonomy is None or taxonomy.taxonomy_type != REPORTING_EXTENSION_BLOCK_TYPE:
    raise ValueError(
      f"taxonomy {payload.taxonomy_id!r} is not a reporting_extension taxonomy."
    )

  issues = validate_update_envelope(session, taxonomy, payload)
  if issues:
    raise TaxonomyBlockValidationError(issues)

  parent_taxonomy_id = str(taxonomy.parent_taxonomy_id)

  def _library_lookup(qnames: set[str]) -> dict[str, str]:
    return _library_qname_lookup(session, parent_taxonomy_id, qnames)

  element_id_by_qname: dict[str, str] = {
    str(qname): str(eid)
    for eid, qname in session.execute(
      select(Element.id, Element.qname).where(Element.taxonomy_id == taxonomy.id)
    ).all()
    if qname
  }

  apply_top_level_fields(taxonomy, payload)
  apply_elements_to_update(
    session,
    taxonomy,
    payload,
    updated_by,
    library_parent_lookup=_library_lookup,
  )
  new_elements = apply_elements_to_add(
    session,
    taxonomy,
    payload,
    updated_by,
    element_factory=_element_factory,
    library_parent_lookup=_library_lookup,
  )
  apply_structures_to_update(session, taxonomy, payload, updated_by)
  new_structures = apply_structures_to_add(session, taxonomy, payload, updated_by)
  session.flush()
  apply_associations_to_add(
    session,
    taxonomy,
    new_elements,
    new_structures,
    payload,
    updated_by,
    library_ref_lookup=_library_lookup,
  )
  apply_associations_to_remove(session, taxonomy, payload)
  apply_structures_to_remove(session, taxonomy, payload)
  apply_elements_to_remove(session, taxonomy, payload, element_id_by_qname)
  apply_rules_to_remove(session, taxonomy, payload)
  apply_rules_to_add(
    session, taxonomy, payload, new_elements, new_structures, updated_by
  )
  refresh_auto_rules(session, taxonomy, payload, updated_by=updated_by)
  session.flush()
  return taxonomy.id


def delete(
  session: Session,
  payload: DeleteTaxonomyBlockRequest,
  deleted_by: str,
) -> int:
  taxonomy = session.get(Taxonomy, payload.taxonomy_id)
  if taxonomy is None or taxonomy.taxonomy_type != REPORTING_EXTENSION_BLOCK_TYPE:
    raise ValueError(
      f"taxonomy {payload.taxonomy_id!r} is not a reporting_extension taxonomy."
    )
  if taxonomy.is_locked:
    raise ValueError("library taxonomies cannot be deleted via the envelope.")

  preflight = preflight_delete(session, str(taxonomy.id))
  if preflight.fact_count > 0 and not payload.cascade_facts:
    raise ValueError(
      f"taxonomy {taxonomy.id!r} has {preflight.fact_count} live facts; "
      f"pass cascade_facts=true to delete them, or clear them first."
    )
  if preflight.line_item_count > 0:
    raise ValueError(
      f"taxonomy {taxonomy.id!r} has {preflight.line_item_count} journal "
      f"line items referencing its elements; clear them before deleting."
    )
  if preflight.cross_taxonomy_mapping_count > 0:
    raise ValueError(
      f"taxonomy {taxonomy.id!r} is referenced by "
      f"{preflight.cross_taxonomy_mapping_count} mapping associations "
      f"from other taxonomies; clear those before deleting."
    )

  return cascade_delete_taxonomy(
    session, str(taxonomy.id), cascade_facts=payload.cascade_facts
  )


def build_envelope(session: Session, taxonomy_id: str) -> TaxonomyBlockEnvelope | None:
  """Project a reporting_extension taxonomy as an envelope.

  Resolves cross-taxonomy parent qnames (elements whose ``parent_id``
  points into the extended library) with a scoped second query.
  """
  taxonomy = session.get(Taxonomy, taxonomy_id)
  if taxonomy is None or taxonomy.taxonomy_type != REPORTING_EXTENSION_BLOCK_TYPE:
    return None

  elements_rows = (
    session.execute(
      select(Element)
      .where(Element.taxonomy_id == taxonomy_id)
      .order_by(Element.code, Element.name)
    )
    .scalars()
    .all()
  )
  structures_rows = (
    session.execute(
      select(Structure)
      .where(Structure.taxonomy_id == taxonomy_id)
      .order_by(Structure.name)
    )
    .scalars()
    .all()
  )

  structure_ids = [s.id for s in structures_rows]
  associations_rows: list[Association] = []
  if structure_ids:
    associations_rows = list(
      session.execute(
        select(Association)
        .where(Association.structure_id.in_(structure_ids))
        .order_by(Association.order_value)
      )
      .scalars()
      .all()
    )

  element_qname_by_id = {e.id: e.qname for e in elements_rows}

  missing_parent_ids = {
    e.parent_id
    for e in elements_rows
    if e.parent_id and e.parent_id not in element_qname_by_id
  }
  missing_arc_ids: set = set()
  for a in associations_rows:
    if a.from_element_id not in element_qname_by_id:
      missing_arc_ids.add(a.from_element_id)
    if a.to_element_id not in element_qname_by_id:
      missing_arc_ids.add(a.to_element_id)
  cross_ids = missing_parent_ids | missing_arc_ids
  if cross_ids:
    cross_rows = session.execute(
      select(Element.id, Element.qname).where(Element.id.in_(cross_ids))
    ).all()
    for element_id, qname in cross_rows:
      element_qname_by_id[element_id] = qname

  parent_qname_by_id = {
    e.id: element_qname_by_id.get(e.parent_id) if e.parent_id else None
    for e in elements_rows
  }

  trait_by_element: dict[str, str] = {}
  element_ids = [e.id for e in elements_rows]
  if element_ids:
    rows = session.execute(
      select(ElementTrait, Trait)
      .join(Trait, ElementTrait.trait_id == Trait.id)
      .where(
        ElementTrait.element_id.in_(element_ids),
        ElementTrait.is_primary.is_(True),
        Trait.category == "elementsOfFinancialStatements",
      )
    ).all()
    for et, tr in rows:
      trait_by_element[et.element_id] = tr.identifier

  elements = [
    TaxonomyBlockElement(
      id=e.id,
      qname=e.qname,
      name=e.name,
      trait=trait_by_element.get(e.id),
      balance_type=e.balance_type,
      period_type=e.period_type,
      element_type=e.element_type,
      is_monetary=e.is_monetary,
      parent_qname=parent_qname_by_id.get(e.id),
      depth=e.depth,
      origin="tenant",
    )
    for e in elements_rows
  ]
  structures = [
    TaxonomyBlockStructure(
      id=s.id,
      name=s.name,
      block_type=s.block_type,
      description=s.description,
      role_uri=None,
    )
    for s in structures_rows
  ]
  associations = [
    TaxonomyBlockAssociation(
      id=a.id,
      structure_id=a.structure_id,
      from_element_qname=element_qname_by_id.get(a.from_element_id) or "",
      to_element_qname=element_qname_by_id.get(a.to_element_id) or "",
      association_type=a.association_type,
      order_value=a.order_value,
      arcrole=a.arcrole,
      weight=a.weight,
    )
    for a in associations_rows
  ]

  parent_taxonomy_name: str | None = None
  if taxonomy.parent_taxonomy_id:
    parent = session.get(Taxonomy, taxonomy.parent_taxonomy_id)
    if parent is not None:
      parent_taxonomy_name = str(parent.name)

  rules = project_rules(
    session,
    taxonomy.id,
    element_ids=[e.id for e in elements_rows],
    structure_ids=[s.id for s in structures_rows],
    qname_by_element_id=element_qname_by_id,
  )

  return TaxonomyBlockEnvelope(
    id=taxonomy.id,
    name=taxonomy.name,
    taxonomy_type=taxonomy.taxonomy_type,
    display_name=DISPLAY_NAME,
    category=CATEGORY,
    parent_taxonomy_id=taxonomy.parent_taxonomy_id,
    parent_taxonomy_name=parent_taxonomy_name,
    version=taxonomy.version,
    standard=taxonomy.standard,
    namespace_uri=taxonomy.namespace_uri,
    is_locked=taxonomy.is_locked,
    elements=elements,
    structures=structures,
    associations=associations,
    rules=rules,
    verification_results=[],
    element_count=len(elements),
    structure_count=len(structures),
    association_count=len(associations),
  )
