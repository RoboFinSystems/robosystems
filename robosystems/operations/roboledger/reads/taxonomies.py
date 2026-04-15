"""Taxonomy / structure / mapping / element read operations."""

from __future__ import annotations

from datetime import date

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse,
  ElementListResponse,
  ElementResponse,
  MappedTrialBalanceResponse,
  MappedTrialBalanceRow,
  MappingCoverageResponse,
  MappingDetailResponse,
  StructureListResponse,
  StructureResponse,
  TaxonomyListResponse,
  TaxonomyResponse,
  UnmappedElementResponse,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Structure,
  Taxonomy,
)

_COA_SOURCES = ("quickbooks", "xero", "plaid", "native", "import")


class MappingNotFoundError(LookupError):
  """Raised when a mapping structure is not found."""


# ── Taxonomies ────────────────────────────────────────────────────────────


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


def list_taxonomies(
  session: Session, taxonomy_type: str | None = None
) -> TaxonomyListResponse:
  """List all active taxonomies, optionally filtered by type."""
  query = select(Taxonomy).where(Taxonomy.is_active.is_(True))
  if taxonomy_type:
    query = query.where(Taxonomy.taxonomy_type == taxonomy_type)
  rows = session.execute(query.order_by(Taxonomy.name)).scalars().all()
  return TaxonomyListResponse(taxonomies=[_taxonomy_to_response(r) for r in rows])


def get_reporting_taxonomy(session: Session) -> TaxonomyResponse | None:
  """Return the locked US GAAP reporting taxonomy, or None if absent."""
  row = session.execute(
    select(Taxonomy).where(
      Taxonomy.standard == "us-gaap",
      Taxonomy.taxonomy_type == "reporting",
      Taxonomy.is_locked.is_(True),
    )
  ).scalar_one_or_none()
  if row is None:
    return None
  return _taxonomy_to_response(row)


# ── Elements ──────────────────────────────────────────────────────────────


def element_to_response(row: Element) -> ElementResponse:
  """Map an Element row to the wire-facing ElementResponse."""
  return ElementResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
    qname=row.qname,
    namespace=row.namespace,
    classification=row.classification,
    sub_classification=row.sub_classification,
    balance_type=row.balance_type,
    period_type=row.period_type,
    is_abstract=row.is_abstract,
    element_type=row.element_type,
    source=row.source,
    taxonomy_id=row.taxonomy_id,
    parent_id=row.parent_id,
    depth=row.depth,
    is_active=row.is_active,
    external_id=row.external_id,
    external_source=row.external_source,
  )


def list_elements(
  session: Session,
  *,
  taxonomy_id: str | None = None,
  source: str | None = None,
  classification: str | None = None,
  is_abstract: bool | None = None,
  limit: int = 100,
  offset: int = 0,
) -> ElementListResponse:
  """List elements filtered by taxonomy / source / classification / abstract."""
  query = select(Element).where(Element.is_active.is_(True))
  count_query = (
    select(func.count()).select_from(Element).where(Element.is_active.is_(True))
  )

  if taxonomy_id:
    query = query.where(Element.taxonomy_id == taxonomy_id)
    count_query = count_query.where(Element.taxonomy_id == taxonomy_id)
  if source:
    query = query.where(Element.source == source)
    count_query = count_query.where(Element.source == source)
  if classification:
    query = query.where(Element.classification == classification)
    count_query = count_query.where(Element.classification == classification)
  if is_abstract is not None:
    query = query.where(Element.is_abstract == is_abstract)
    count_query = count_query.where(Element.is_abstract == is_abstract)

  total = session.execute(count_query).scalar() or 0
  rows = (
    session.execute(
      query.order_by(Element.depth, Element.code, Element.qname)
      .offset(offset)
      .limit(limit)
    )
    .scalars()
    .all()
  )

  return ElementListResponse(
    elements=[element_to_response(r) for r in rows],
    pagination=create_pagination_info(total, limit, offset),
  )


def count_coa_elements(session: Session) -> int:
  """Count active, non-abstract Chart-of-Accounts elements."""
  return (
    session.execute(
      select(func.count())
      .select_from(Element)
      .where(
        Element.source.in_(_COA_SOURCES),
        Element.is_active.is_(True),
        Element.is_abstract.is_(False),
      )
    ).scalar()
    or 0
  )


def get_element(session: Session, element_id: str) -> ElementResponse | None:
  """Return a single element by id, or None if missing."""
  row = session.execute(
    select(Element).where(Element.id == element_id)
  ).scalar_one_or_none()
  if row is None:
    return None
  return element_to_response(row)


def suggest_mapping_candidates(
  session: Session, classification: str
) -> list[ElementResponse]:
  """Return reporting-taxonomy elements (us-gaap / sfac6) matching a classification.

  Used by the MCP `suggest-mapping` tool to narrow CoA → reporting concept
  candidates by the source element's classification.
  """
  rows = (
    session.execute(
      select(Element)
      .where(
        Element.source.in_(("us-gaap", "sfac6")),
        Element.classification == classification,
        Element.is_active.is_(True),
      )
      .order_by(Element.depth, Element.name)
    )
    .scalars()
    .all()
  )
  return [element_to_response(r) for r in rows]


def list_unmapped_elements(
  session: Session, mapping_id: str | None = None
) -> list[UnmappedElementResponse]:
  """List CoA elements not yet mapped to the reporting taxonomy."""
  coa_query = select(Element).where(
    Element.source.in_(_COA_SOURCES),
    Element.is_active.is_(True),
    Element.is_abstract.is_(False),
  )
  coa_elements = session.execute(coa_query).scalars().all()

  # Get mapped element IDs (from_element_id in mapping associations)
  if mapping_id:
    mapped_query = select(Association.from_element_id).where(
      Association.structure_id == mapping_id,
      Association.association_type == "mapping",
    )
  else:
    # Check all mapping structures
    mapped_query = select(Association.from_element_id).where(
      Association.association_type == "mapping",
    )
  mapped_ids = set(session.execute(mapped_query).scalars().all())

  unmapped = [e for e in coa_elements if e.id not in mapped_ids]

  return [
    UnmappedElementResponse(
      id=e.id,
      code=e.code,
      name=e.name,
      classification=e.classification,
      balance_type=e.balance_type,
      external_source=e.external_source,
    )
    for e in unmapped
  ]


# ── Structures ────────────────────────────────────────────────────────────


def _structure_to_response(row: Structure) -> StructureResponse:
  return StructureResponse(
    id=row.id,
    name=row.name,
    description=row.description,
    structure_type=row.structure_type,
    taxonomy_id=row.taxonomy_id,
    is_active=row.is_active,
  )


def list_structures(
  session: Session,
  *,
  taxonomy_id: str | None = None,
  structure_type: str | None = None,
) -> StructureListResponse:
  """List active structures, optionally filtered by taxonomy + type."""
  query = select(Structure).where(Structure.is_active.is_(True))
  if taxonomy_id:
    query = query.where(Structure.taxonomy_id == taxonomy_id)
  if structure_type:
    query = query.where(Structure.structure_type == structure_type)
  rows = session.execute(query.order_by(Structure.name)).scalars().all()
  return StructureListResponse(structures=[_structure_to_response(r) for r in rows])


# ── Mappings ──────────────────────────────────────────────────────────────


def list_mappings(session: Session) -> StructureListResponse:
  """List all active mapping structures (structure_type = 'coa_mapping')."""
  rows = (
    session.execute(
      select(Structure)
      .where(
        Structure.structure_type == "coa_mapping",
        Structure.is_active.is_(True),
      )
      .order_by(Structure.name)
    )
    .scalars()
    .all()
  )
  return StructureListResponse(structures=[_structure_to_response(r) for r in rows])


def get_mapping_detail(
  session: Session, mapping_id: str
) -> MappingDetailResponse | None:
  """Return a mapping structure with all its associations, or None."""
  structure = session.execute(
    select(Structure).where(Structure.id == mapping_id)
  ).scalar_one_or_none()
  if structure is None:
    return None

  # Get associations with element names
  from_elem = Element.__table__.alias("from_elem")
  to_elem = Element.__table__.alias("to_elem")

  assoc_rows = session.execute(
    select(
      Association,
      from_elem.c.name.label("from_name"),
      from_elem.c.qname.label("from_qname"),
      to_elem.c.name.label("to_name"),
      to_elem.c.qname.label("to_qname"),
    )
    .join(from_elem, Association.from_element_id == from_elem.c.id)
    .join(to_elem, Association.to_element_id == to_elem.c.id)
    .where(Association.structure_id == mapping_id)
    .order_by(Association.order_value)
  ).all()

  associations = [
    AssociationResponse(
      id=a.id,
      structure_id=a.structure_id,
      from_element_id=a.from_element_id,
      from_element_name=from_name,
      from_element_qname=from_qname,
      to_element_id=a.to_element_id,
      to_element_name=to_name,
      to_element_qname=to_qname,
      association_type=a.association_type,
      order_value=a.order_value,
      weight=a.weight,
      confidence=a.confidence,
      suggested_by=a.suggested_by,
      approved_by=a.approved_by,
    )
    for a, from_name, from_qname, to_name, to_qname in assoc_rows
  ]

  return MappingDetailResponse(
    id=structure.id,
    name=structure.name,
    structure_type=structure.structure_type,
    taxonomy_id=structure.taxonomy_id,
    associations=associations,
    total_associations=len(associations),
  )


def get_mapping_coverage(session: Session, mapping_id: str) -> MappingCoverageResponse:
  """Return mapping coverage stats (total, mapped, unmapped, confidence)."""
  total_coa = (
    session.execute(
      select(func.count())
      .select_from(Element)
      .where(
        Element.source.in_(_COA_SOURCES),
        Element.is_active.is_(True),
        Element.is_abstract.is_(False),
      )
    ).scalar()
    or 0
  )

  mapping_assocs = (
    session.execute(
      select(Association).where(
        Association.structure_id == mapping_id,
        Association.association_type == "mapping",
      )
    )
    .scalars()
    .all()
  )

  mapped_count = len({a.from_element_id for a in mapping_assocs})
  unmapped_count = total_coa - mapped_count

  high = sum(
    1 for a in mapping_assocs if a.confidence is not None and a.confidence > 0.90
  )
  medium = sum(
    1
    for a in mapping_assocs
    if a.confidence is not None and 0.70 <= a.confidence <= 0.90
  )
  low = sum(
    1 for a in mapping_assocs if a.confidence is not None and a.confidence < 0.70
  )

  return MappingCoverageResponse(
    mapping_id=mapping_id,
    total_coa_elements=total_coa,
    mapped_count=mapped_count,
    unmapped_count=unmapped_count,
    coverage_percent=((mapped_count / total_coa * 100) if total_coa > 0 else 0.0),
    high_confidence=high,
    medium_confidence=medium,
    low_confidence=low,
  )


# ── Mapped Trial Balance ──────────────────────────────────────────────────


_MAPPED_TRIAL_BALANCE_SQL = text("""
  SELECT
      target.id AS reporting_element_id,
      target.qname,
      target.name AS reporting_name,
      target.classification,
      target.balance_type,
      COALESCE(SUM(li.debit_amount), 0) AS total_debits,
      COALESCE(SUM(li.credit_amount), 0) AS total_credits
  FROM elements source_elem
  JOIN line_items li ON li.element_id = source_elem.id
  JOIN entries e ON e.id = li.entry_id
  JOIN associations mapping
      ON mapping.from_element_id = source_elem.id
      AND mapping.association_type = 'mapping'
      AND mapping.structure_id = :mapping_id
  JOIN elements target ON target.id = mapping.to_element_id
  WHERE e.status = 'posted'
      AND (e.posting_date >= :start_date OR :start_date IS NULL)
      AND (e.posting_date <= :end_date OR :end_date IS NULL)
  GROUP BY target.id, target.qname, target.name,
           target.classification, target.balance_type
  ORDER BY target.qname
""")


def get_mapped_trial_balance(
  session: Session,
  mapping_id: str,
  start_date: date | str | None = None,
  end_date: date | str | None = None,
) -> MappedTrialBalanceResponse:
  """Trial balance rolled up to reporting concepts via mapping associations.

  Accepts both `date` objects (preferred, used by the GraphQL resolver
  so the wire schema exposes a `Date` scalar) and ISO-8601 strings (used
  by older REST callers). SQLAlchemy parameter binding handles either.
  """
  result = session.execute(
    _MAPPED_TRIAL_BALANCE_SQL,
    {
      "mapping_id": mapping_id,
      "start_date": start_date,
      "end_date": end_date,
    },
  )

  rows: list[MappedTrialBalanceRow] = []
  for row in result:
    debits = float(row.total_debits) / 100.0
    credits = float(row.total_credits) / 100.0
    rows.append(
      MappedTrialBalanceRow(
        reporting_element_id=row.reporting_element_id,
        qname=row.qname,
        reporting_name=row.reporting_name,
        classification=row.classification,
        balance_type=row.balance_type,
        total_debits=debits,
        total_credits=credits,
        net_balance=debits - credits,
      )
    )

  return MappedTrialBalanceResponse(mapping_id=mapping_id, rows=rows)
