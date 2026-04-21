"""Read operations for library taxonomies."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from robosystems.models.api.library import (
  LibraryAssociationResponse,
  LibraryTaxonomyResponse,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Structure,
  Taxonomy,
)


def _taxonomy_to_response(
  taxonomy: Taxonomy, element_count: int | None = None
) -> LibraryTaxonomyResponse:
  return LibraryTaxonomyResponse(
    id=taxonomy.id,
    name=taxonomy.name,
    description=taxonomy.description,
    standard=taxonomy.standard,
    version=taxonomy.version,
    namespace_uri=taxonomy.namespace_uri,
    taxonomy_type=taxonomy.taxonomy_type,
    is_shared=taxonomy.is_shared,
    is_active=taxonomy.is_active,
    is_locked=taxonomy.is_locked,
    element_count=element_count,
  )


def list_taxonomies(
  session: Session,
  standard: str | None = None,
  include_element_count: bool = False,
  shared_only: bool = True,
) -> list[LibraryTaxonomyResponse]:
  """List library taxonomies visible to the current session.

  Args:
      session: SQLAlchemy session (public schema for sentinel; tenant schema for graph queries).
      standard: Optional filter by standard (sfac6, fac, us-gaap, …).
      include_element_count: If True, add a COUNT(*) per taxonomy.
      shared_only: If True (default), return only shared/canonical taxonomies.
          Pass False when the session is tenant-scoped to include tenant CoA taxonomies.
  """
  query = select(Taxonomy)
  if shared_only:
    query = query.where(Taxonomy.is_shared.is_(True))
  if standard is not None:
    query = query.where(Taxonomy.standard == standard)
  query = query.order_by(Taxonomy.standard.asc().nulls_last(), Taxonomy.version.asc())

  taxonomies = session.execute(query).scalars().all()

  counts: dict[str, int] = {}
  if include_element_count and taxonomies:
    count_query = (
      select(Element.taxonomy_id, func.count(Element.id))
      .where(Element.taxonomy_id.in_([t.id for t in taxonomies]))
      .group_by(Element.taxonomy_id)
    )
    counts = {str(tid): int(cnt) for tid, cnt in session.execute(count_query).all()}

  return [
    _taxonomy_to_response(
      t, element_count=counts.get(t.id) if include_element_count else None
    )
    for t in taxonomies
  ]


def get_taxonomy(
  session: Session,
  taxonomy_id: str | None = None,
  standard: str | None = None,
  version: str | None = None,
  include_element_count: bool = False,
) -> LibraryTaxonomyResponse | None:
  """Get a single taxonomy by id OR by (standard, version) tuple."""
  if taxonomy_id:
    query = select(Taxonomy).where(Taxonomy.id == taxonomy_id)
  elif standard and version:
    query = select(Taxonomy).where(
      Taxonomy.standard == standard,
      Taxonomy.version == version,
      Taxonomy.is_shared.is_(True),
    )
  else:
    return None

  taxonomy = session.execute(query).scalar_one_or_none()
  if taxonomy is None:
    return None

  element_count: int | None = None
  if include_element_count:
    element_count = (
      session.execute(
        select(func.count(Element.id)).where(Element.taxonomy_id == taxonomy.id)
      ).scalar()
      or 0
    )

  return _taxonomy_to_response(taxonomy, element_count=element_count)


def list_taxonomy_arcs(
  session: Session,
  taxonomy_id: str,
  association_type: str | None = None,
  limit: int = 200,
  offset: int = 0,
) -> list[LibraryAssociationResponse]:
  """Return every arc contributed by a taxonomy (via its structures).

  For mapping taxonomies (fac-to-rs-gaap, type-subtype) this
  is the whole point — the taxonomy's value is in its arcs, not in any
  concepts it owns. Joins from → element and to → element so the response
  carries qnames + names for UI display without a second round-trip.
  """
  from_elem = aliased(Element, name="from_elem")
  to_elem = aliased(Element, name="to_elem")

  query = (
    select(
      Association,
      Structure.name.label("structure_name"),
      from_elem.qname.label("from_qname"),
      from_elem.name.label("from_name"),
      to_elem.qname.label("to_qname"),
      to_elem.name.label("to_name"),
    )
    .join(Structure, Association.structure_id == Structure.id)
    .join(from_elem, Association.from_element_id == from_elem.id)
    .join(to_elem, Association.to_element_id == to_elem.id)
    .where(Structure.taxonomy_id == taxonomy_id)
  )
  if association_type is not None:
    query = query.where(Association.association_type == association_type)
  query = (
    query.order_by(
      from_elem.qname.asc().nulls_last(),
      to_elem.qname.asc().nulls_last(),
    )
    .limit(limit)
    .offset(offset)
  )

  rows = session.execute(query).all()
  return [
    LibraryAssociationResponse(
      id=row.Association.id,
      structure_id=row.Association.structure_id,
      structure_name=row.structure_name,
      from_element_id=row.Association.from_element_id,
      from_element_qname=row.from_qname,
      from_element_name=row.from_name,
      to_element_id=row.Association.to_element_id,
      to_element_qname=row.to_qname,
      to_element_name=row.to_name,
      association_type=row.Association.association_type,
      arcrole=row.Association.arcrole,
      order_value=row.Association.order_value,
      weight=row.Association.weight,
    )
    for row in rows
  ]


def count_taxonomy_arcs(session: Session, taxonomy_id: str) -> int:
  """Count every arc contributed by a taxonomy."""
  return int(
    session.execute(
      select(func.count(Association.id))
      .join(Structure, Association.structure_id == Structure.id)
      .where(Structure.taxonomy_id == taxonomy_id)
    ).scalar()
    or 0
  )
