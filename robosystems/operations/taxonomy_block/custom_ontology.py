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
``to_ref`` only. The structural rule layer (cycles, orphans, unique
qnames) lands in Phase 2.3.
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

CUSTOM_ONTOLOGY_BLOCK_TYPE = "custom_ontology"
DISPLAY_NAME = "Custom Ontology"
DISPLAY_PLURAL = "Custom Ontologies"
CATEGORY = "Custom"


def _qname_for_custom(standard: str | None, code: str | None, name: str) -> str:
  """Derive the envelope-local qname when the tenant didn't supply one."""
  ns = standard or "custom"
  token = code or name.replace(" ", "")
  return f"{ns}:{token}"


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
    qname = req.qname or _qname_for_custom(payload.standard, req.code, req.name)
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
    structure = Structure(
      name=req.name,
      description=req.description,
      structure_type=req.structure_type,
      taxonomy_id=taxonomy.id,
      is_active=True,
      metadata_=dict(req.metadata),
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

  return taxonomy.id


def update(
  session: Session,
  payload: UpdateTaxonomyBlockRequest,
  updated_by: str,
) -> str:
  raise NotImplementedError(
    "custom_ontology update-taxonomy-block is not implemented yet (Phase 2.4)."
  )


def delete(
  session: Session,
  payload: DeleteTaxonomyBlockRequest,
  deleted_by: str,
) -> str:
  raise NotImplementedError(
    "custom_ontology delete-taxonomy-block is not implemented yet (Phase 2.4)."
  )


def build_envelope(session: Session, taxonomy_id: str) -> TaxonomyBlockEnvelope | None:
  """Project a custom_ontology taxonomy as an envelope.

  Skips the EFS classification sidecar — custom ontologies don't carry
  FASB classifications by design. Every element projects with
  ``classification=None`` and ``origin='tenant'``.
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
      classification=None,
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
      structure_type=s.structure_type,
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
    rules=[],
    verification_results=[],
    element_count=len(elements),
    structure_count=len(structures),
    association_count=len(associations),
  )
