"""Handlers for ``taxonomy_type='reporting_standard'`` — library taxonomies.

Library reporting taxonomies (FAC, us-gaap, rs-gaap) are seeded through
the admin-only ``library_creator.py`` operations path
(JSON-LD → ``create_library_taxonomy_elements`` / ``create_library_arcs`` /
``create_library_rules`` → ORM session inserts into the ``public`` schema).
The public Taxonomy Block surface does NOT author library rows —
``create``/``update``/``delete`` raise :class:`NotImplementedError`.

``build_envelope`` mirrors the ``chart_of_accounts`` projection; the
only observable difference is ``origin='library'`` (derived from
``taxonomy.is_locked``) on every element.
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
from robosystems.operations.taxonomy_block.rule_reads import project_rules

REPORTING_STANDARD_BLOCK_TYPE = "reporting_standard"
DISPLAY_NAME = "Reporting Standard"
DISPLAY_PLURAL = "Reporting Standards"
CATEGORY = "Library"


_ADMIN_ONLY_MESSAGE = (
  "reporting_standard taxonomies are library-origin and cannot be authored "
  "through the public envelope. Library seeding uses the admin-only "
  "robosystems/operations/taxonomy_block/library_creator.py path "
  "(JSON-LD → create_library_taxonomy_elements / create_library_arcs / "
  "create_library_rules)."
)


def create(
  session: Session,
  payload: CreateTaxonomyBlockRequest,
  created_by: str,
) -> str:
  """Intentionally not exposed; admin-only via library_creator."""
  raise NotImplementedError(_ADMIN_ONLY_MESSAGE)


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
  """Project a library ``reporting_standard`` taxonomy as an envelope.

  Returns None when the taxonomy row doesn't exist or isn't of type
  ``reporting_standard``. The projection shape mirrors CoA with
  ``origin='library'`` derived from ``taxonomy.is_locked``.
  """
  taxonomy = session.get(Taxonomy, taxonomy_id)
  if taxonomy is None or taxonomy.taxonomy_type != REPORTING_STANDARD_BLOCK_TYPE:
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
  origin = "library" if taxonomy.is_locked else "tenant"

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
      origin=origin,
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
