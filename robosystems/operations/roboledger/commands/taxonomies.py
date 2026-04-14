"""Write operations for taxonomies, structures, and mappings.

These commands mirror the POST/DELETE bodies in the old
`routers/ledger/taxonomies.py` but with business logic extracted so
the REST operation surface, MCP tools, and agents all call into one
source of truth.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse,
  CreateAssociationRequest,
  CreateStructureRequest,
  CreateTaxonomyRequest,
  StructureResponse,
  TaxonomyResponse,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Structure,
  Taxonomy,
)
from robosystems.utils.ulid import generate_prefixed_ulid


class MappingStructureNotFoundError(LookupError):
  """Raised when a referenced mapping structure does not exist."""


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
    structure_type=row.structure_type,
    taxonomy_id=row.taxonomy_id,
    is_active=row.is_active,
  )


def create_taxonomy(
  session: Session, body: CreateTaxonomyRequest, created_by: str
) -> TaxonomyResponse:
  """Insert a new taxonomy row and return its response representation."""
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
  """Insert a new structure row and return its response representation."""
  structure = Structure(
    id=generate_prefixed_ulid("struct"),
    name=body.name,
    description=body.description,
    structure_type=body.structure_type,
    taxonomy_id=body.taxonomy_id,
    created_by=created_by,
  )
  session.add(structure)
  session.flush()
  return _structure_to_response(structure)


def create_mapping_association(
  session: Session,
  mapping_id: str,
  body: CreateAssociationRequest,
  created_by: str,
) -> AssociationResponse:
  """Add a mapping association (CoA element → reporting concept).

  Raises `MappingStructureNotFoundError` if the mapping structure is
  missing, or `ElementNotFoundError` with `side="source"` / `"target"`
  if either element is missing. The caller translates these to HTTP
  status codes.
  """
  structure = session.execute(
    select(Structure).where(Structure.id == mapping_id)
  ).scalar_one_or_none()
  if structure is None:
    raise MappingStructureNotFoundError(mapping_id)

  from_elem = session.execute(
    select(Element).where(Element.id == body.from_element_id)
  ).scalar_one_or_none()
  if from_elem is None:
    raise ElementNotFoundError("source", body.from_element_id)

  to_elem = session.execute(
    select(Element).where(Element.id == body.to_element_id)
  ).scalar_one_or_none()
  if to_elem is None:
    raise ElementNotFoundError("target", body.to_element_id)

  assoc = Association(
    id=generate_prefixed_ulid("assoc"),
    structure_id=mapping_id,
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
  session: Session, mapping_id: str, association_id: str
) -> bool:
  """Delete a mapping association. Returns True if a row was deleted."""
  deleted = (
    session.query(Association)
    .filter(
      Association.id == association_id,
      Association.structure_id == mapping_id,
    )
    .delete(synchronize_session=False)
  )
  return bool(deleted)
