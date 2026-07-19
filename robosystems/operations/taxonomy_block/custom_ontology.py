"""Handlers for ``taxonomy_type='custom_ontology'`` — tenant free-form ontology.

Custom ontologies are declarative tenant taxonomies with **no**
classification or balance-type discipline. Tenants use them for
climate-disclosure concepts, internal KPI definitions, or any domain
vocabulary that doesn't need accounting semantics. The handler is CoA
stripped down: same two-pass element insert for parent resolution,
but no EFS classification junction writes and no instant-period
forcing.

Validation below the Pydantic layer is minimal — ``ValueError`` for
unresolved ``parent_ref`` / ``structure_ref`` / ``from_ref`` /
``to_ref`` only. Structural rules (cycles, orphans, unique qnames) are
enforced by the shared create-envelope validator.
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
  Structure,
  Taxonomy,
)
from robosystems.operations.taxonomy_block._helpers import qname_for
from robosystems.operations.taxonomy_block.auto_rules import emit_auto_rules
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

CUSTOM_ONTOLOGY_BLOCK_TYPE = "custom_ontology"
DISPLAY_NAME = "Custom Ontology"
DISPLAY_PLURAL = "Custom Ontologies"
CATEGORY = "Custom"


def create(
  session: Session,
  payload: CreateTaxonomyBlockRequest,
  created_by: str,
) -> str:
  """Create a custom_ontology taxonomy + its elements/structures/associations.

  Returns the new taxonomy_id. Two-pass element insert resolves
  ``parent_ref`` against envelope-local qnames only — custom ontologies
  don't extend any library so there is no fallback.
  """
  if payload.taxonomy_type != CUSTOM_ONTOLOGY_BLOCK_TYPE:
    raise ValueError(
      f"custom_ontology handler received payload with taxonomy_type="
      f"{payload.taxonomy_type!r}"
    )

  issues = validate_create_envelope(payload, session)
  if issues:
    raise TaxonomyBlockValidationError(issues)

  taxonomy = Taxonomy(
    name=payload.name,
    description=payload.description,
    taxonomy_type=CUSTOM_ONTOLOGY_BLOCK_TYPE,
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
    qname = req.qname or qname_for(payload.standard, "custom", req.code, req.name)
    element = Element(
      code=req.code,
      name=req.name,
      description=req.description,
      qname=qname,
      balance_type=req.balance_type,
      period_type=req.period_type,
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

  for qname, parent_ref in parent_refs.items():
    if not parent_ref:
      continue
    parent = elements_by_qname.get(parent_ref)
    if parent is None:
      raise ValueError(
        f"element {qname!r} references parent_ref {parent_ref!r} but no "
        f"such element was declared in this envelope."
      )
    elements_by_qname[qname].parent_id = parent.id

  structures_by_name: dict[str, Structure] = {}
  for req in payload.structures:
    # role_uri rides in metadata_ — the column-less convention the
    # envelope readers already use (metadata_['role_uri']).
    structure_metadata = dict(req.metadata)
    if req.role_uri:
      structure_metadata["role_uri"] = req.role_uri
    structure = Structure(
      name=req.name,
      description=req.description,
      block_type=req.block_type,
      concept_arrangement=req.concept_arrangement,
      taxonomy_id=taxonomy.id,
      is_active=True,
      metadata_=structure_metadata,
      created_by=created_by,
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
    from_element = elements_by_qname.get(req.from_ref)
    to_element = elements_by_qname.get(req.to_ref)
    if from_element is None:
      raise ValueError(
        f"association from_ref {req.from_ref!r} is not an envelope-local element qname."
      )
    if to_element is None:
      raise ValueError(
        f"association to_ref {req.to_ref!r} is not an envelope-local element qname."
      )
    association = Association(
      structure_id=structure.id,
      from_element_id=from_element.id,
      to_element_id=to_element.id,
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
  qname = req.qname or qname_for(taxonomy.standard, "custom", req.code, req.name)
  element = Element(
    code=req.code,
    name=req.name,
    description=req.description,
    qname=qname,
    balance_type=req.balance_type,
    period_type=req.period_type,
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
  if taxonomy is None or taxonomy.taxonomy_type != CUSTOM_ONTOLOGY_BLOCK_TYPE:
    raise ValueError(
      f"taxonomy {payload.taxonomy_id!r} is not a custom_ontology taxonomy."
    )

  issues = validate_update_envelope(session, taxonomy, payload)
  if issues:
    raise TaxonomyBlockValidationError(issues)

  element_id_by_qname: dict[str, str] = {
    str(qname): str(eid)
    for eid, qname in session.execute(
      select(Element.id, Element.qname).where(Element.taxonomy_id == taxonomy.id)
    ).all()
    if qname
  }

  apply_top_level_fields(taxonomy, payload)
  apply_elements_to_update(session, taxonomy, payload, updated_by)
  new_elements = apply_elements_to_add(
    session, taxonomy, payload, updated_by, element_factory=_element_factory
  )
  apply_structures_to_update(session, taxonomy, payload, updated_by)
  new_structures = apply_structures_to_add(session, taxonomy, payload, updated_by)
  session.flush()
  apply_associations_to_add(
    session, taxonomy, new_elements, new_structures, payload, updated_by
  )
  apply_associations_to_remove(session, taxonomy, payload)
  apply_structures_to_remove(session, taxonomy, payload)
  apply_elements_to_remove(session, taxonomy, payload, element_id_by_qname)
  apply_rules_to_remove(session, taxonomy, payload)
  apply_rules_to_add(
    session, taxonomy, payload, new_elements, new_structures, updated_by
  )
  session.flush()
  return taxonomy.id


def delete(
  session: Session,
  payload: DeleteTaxonomyBlockRequest,
  deleted_by: str,
) -> int:
  taxonomy = session.get(Taxonomy, payload.taxonomy_id)
  if taxonomy is None or taxonomy.taxonomy_type != CUSTOM_ONTOLOGY_BLOCK_TYPE:
    raise ValueError(
      f"taxonomy {payload.taxonomy_id!r} is not a custom_ontology taxonomy."
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
  """Project a custom_ontology taxonomy as an envelope.

  Skips the EFS trait sidecar — custom ontologies don't carry
  FASB classifications by design. Every element projects with
  ``trait=None`` and ``origin='tenant'``.
  """
  taxonomy = session.get(Taxonomy, taxonomy_id)
  if taxonomy is None or taxonomy.taxonomy_type != CUSTOM_ONTOLOGY_BLOCK_TYPE:
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
  parent_qname_by_id = {
    e.id: element_qname_by_id.get(e.parent_id) if e.parent_id else None
    for e in elements_rows
  }

  elements = [
    TaxonomyBlockElement(
      id=e.id,
      qname=e.qname,
      name=e.name,
      trait=None,
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
