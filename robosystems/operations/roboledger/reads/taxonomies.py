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
  ElementTrait,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.library.reads import efs_trait_by_element

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
      Taxonomy.taxonomy_type == "reporting_standard",
      Taxonomy.is_locked.is_(True),
    )
  ).scalar_one_or_none()
  if row is None:
    return None
  return _taxonomy_to_response(row)


# ── Elements ──────────────────────────────────────────────────────────────


# Local alias for the shared library helper — keeps call sites terse and
# makes the import site searchable (grep for ``_efs_by_element``).
_efs_by_element = efs_trait_by_element


def element_to_response(row: Element, trait: str | None = None) -> ElementResponse:
  """Map an Element row to the wire-facing ElementResponse.

  Callers that batch-load should use :func:`_efs_by_element` once and
  pass the lookup in to avoid N+1 on the EFS trait.
  """
  return ElementResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
    qname=row.qname,
    namespace=row.namespace,
    trait=trait,
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
  trait: str | None = None,
  is_abstract: bool | None = None,
  limit: int = 100,
  offset: int = 0,
) -> ElementListResponse:
  """List elements filtered by taxonomy / source / trait / abstract.

  ``trait`` filters on the FASB elementsOfFinancialStatements
  trait via the element_traits junction table.
  """
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
  if trait:
    subquery = (
      select(ElementTrait.element_id)
      .join(Trait, Trait.id == ElementTrait.trait_id)
      .where(
        Trait.category == "elementsOfFinancialStatements",
        Trait.identifier == trait,
      )
    )
    query = query.where(Element.id.in_(subquery))
    count_query = count_query.where(Element.id.in_(subquery))
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

  efs_map = _efs_by_element(session, [r.id for r in rows])
  return ElementListResponse(
    elements=[element_to_response(r, efs_map.get(r.id)) for r in rows],
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
  efs = _efs_by_element(session, [row.id]).get(row.id)
  return element_to_response(row, efs)


def suggest_mapping_candidates(
  session: Session,
  trait: str | None = None,
  element_id: str | None = None,
) -> list[ElementResponse]:
  """Return FAC candidates for a CoA element, narrowed by EFS trait.

  Filters active ``fac`` elements by the FASB
  elementsOfFinancialStatements trait (via element_traits).
  ``element_id`` is reserved for future per-element overrides but is
  currently unused — trait alone drives the filter.
  """
  del element_id  # reserved for future per-element narrowing
  if trait is None:
    return []

  rows = (
    session.execute(
      select(Element)
      .where(
        Element.source == "fac",
        Element.is_active.is_(True),
        Element.id.in_(
          select(ElementTrait.element_id)
          .join(
            Trait,
            Trait.id == ElementTrait.trait_id,
          )
          .where(
            Trait.category == "elementsOfFinancialStatements",
            Trait.identifier == trait,
          )
        ),
      )
      .order_by(Element.depth, Element.name)
    )
    .scalars()
    .all()
  )
  efs_map = _efs_by_element(session, [r.id for r in rows])
  return [element_to_response(r, efs_map.get(r.id)) for r in rows]


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
  efs_map = _efs_by_element(session, [e.id for e in unmapped])

  return [
    UnmappedElementResponse(
      id=e.id,
      code=e.code,
      name=e.name,
      trait=efs_map.get(e.id),
      balance_type=e.balance_type,
      external_source=e.external_source,
    )
    for e in unmapped
  ]


def expand_to_rs_gaap_candidates(
  session: Session,
  fac_element_id: str,
) -> dict | None:
  """Follow FAC → rs-gaap equivalence arcs, then surface specific filing candidates.

  Two shapes exist in the fac-to-rs-gaap taxonomy:

  **Narrow** (1-3 equivalents): the rs-gaap peer is a broad grouping concept
  (e.g. ``fac:CurrentAssets`` → ``rs-gaap:AssetsCurrent``). In this case the
  type-subtype children of that peer are the filing-specific candidates.

  **Wide** (4+ equivalents): the rs-gaap peers are already the filing-specific
  variants (e.g. ``fac:Revenues`` → 80 industry-specific revenue tags). Here the
  equivalents themselves are the candidates; recursing into their type-subtype
  children would produce an unmanageably large set.

  Returns::

      {
        "rs_gaap_parent": {"id": ..., "qname": ..., "name": ...} | None,
        "candidates": [{"id": ..., "qname": ..., "name": ...}, ...]
      }

  ``rs_gaap_parent`` is ``None`` in the wide-equivalence case (no single parent).
  Returns ``None`` if no fac-to-rs-gaap equivalence exists for this FAC element.
  """
  # Also fetch the FAC element's own qname for prompt context.
  fac_row = session.execute(
    text("SELECT qname FROM elements WHERE id = :fac_id"),
    {"fac_id": fac_element_id},
  ).fetchone()
  fac_qname = fac_row.qname if fac_row else fac_element_id

  # Lazy import to avoid pulling agent constants into every read path.
  from robosystems.operations.agents.implementations.mapping.constants import (
    RS_GAAP_SUBTOTAL_DENYLIST,
  )

  equiv_rows = session.execute(
    text("""
      SELECT e.id, e.qname, e.name
      FROM associations a
      JOIN structures s ON a.structure_id = s.id
      JOIN taxonomies t ON s.taxonomy_id = t.id
      JOIN elements e ON a.to_element_id = e.id
      WHERE a.from_element_id = :fac_id
        AND t.standard = 'fac-to-rs-gaap'
        AND a.association_type = 'equivalence'
      ORDER BY e.qname
    """),
    {"fac_id": fac_element_id},
  ).fetchall()

  if not equiv_rows:
    return None

  # Drop denylisted rollup concepts from the equivalent set BEFORE the
  # narrow/wide branch decision. A CoA arc to a statement-level rollup
  # like rs-gaap:Assets would land a leaf fact on a calculated total
  # and double-count when the renderer sums children. The narrow-case
  # parent fallback (max-children selection) and the wide-case
  # candidate list both flow from this filtered set.
  equiv_rows = [r for r in equiv_rows if r.qname not in RS_GAAP_SUBTOTAL_DENYLIST]

  if not equiv_rows:
    return None

  _NARROW_THRESHOLD = 3

  if len(equiv_rows) > _NARROW_THRESHOLD:
    # Wide case: equivalents are already the filing-specific candidates.
    return {
      "fac_qname": fac_qname,
      "rs_gaap_parent": None,
      "candidates": [
        {"id": r.id, "qname": r.qname, "name": r.name} for r in equiv_rows
      ],
    }

  # Narrow case (≤ 3 equivalents): walk type-subtype children under EACH
  # equivalent and union them — previously we blindly picked equiv_rows[0]
  # after alpha-sort, which discarded siblings. `fac:Assets` is the
  # canonical example: it maps to both `rs-gaap:Assets` and
  # `rs-gaap:AssetsCurrent`, and the useful type-subtype children live
  # under the Current branch. The equivalents themselves are included in
  # the candidate set so the AI can pick a parent-level target when no
  # sub-type fits.
  child_rows = session.execute(
    text("""
      SELECT e.id, e.qname, e.name, a.from_element_id AS parent_id
      FROM associations a
      JOIN structures s ON a.structure_id = s.id
      JOIN taxonomies t ON s.taxonomy_id = t.id
      JOIN elements e ON a.to_element_id = e.id
      WHERE a.from_element_id = ANY(:rs_ids)
        AND t.standard = 'type-subtype'
        AND a.association_type = 'general-special'
      ORDER BY e.qname
    """),
    {"rs_ids": [r.id for r in equiv_rows]},
  ).fetchall()

  # Pick the equivalent with the most type-subtype children as the
  # "primary parent" so the fallback path in the refinement agent lands
  # on the most-expanded branch. Ties break by the pre-existing alpha
  # sort from the equivalence query.
  children_by_parent: dict[str, int] = {}
  for r in child_rows:
    children_by_parent[r.parent_id] = children_by_parent.get(r.parent_id, 0) + 1
  parent = max(
    equiv_rows,
    key=lambda e: (children_by_parent.get(e.id, 0), e.qname),
  )

  candidate_ids: set[str] = set()
  candidates: list[dict] = []
  for r in equiv_rows:
    if r.id not in candidate_ids:
      candidate_ids.add(r.id)
      candidates.append({"id": r.id, "qname": r.qname, "name": r.name})
  # Type-subtype children may themselves be rollups (e.g. some
  # AssetsCurrent subtypes are themselves grouping concepts). Filter
  # the same denylist so the AI never sees a rollup as a candidate.
  for r in child_rows:
    if r.id in candidate_ids or r.qname in RS_GAAP_SUBTOTAL_DENYLIST:
      continue
    candidate_ids.add(r.id)
    candidates.append({"id": r.id, "qname": r.qname, "name": r.name})

  return {
    "fac_qname": fac_qname,
    "rs_gaap_parent": {"id": parent.id, "qname": parent.qname, "name": parent.name},
    "candidates": candidates,
  }


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
      tt.identifier AS trait,
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
  LEFT JOIN element_traits tet
      ON tet.element_id = target.id AND tet.is_primary = TRUE
  LEFT JOIN traits tt
      ON tt.id = tet.trait_id
      AND tt.category = 'elementsOfFinancialStatements'
  WHERE e.status = 'posted'
      AND (e.posting_date >= :start_date OR :start_date IS NULL)
      AND (e.posting_date <= :end_date OR :end_date IS NULL)
  GROUP BY target.id, target.qname, target.name, tt.identifier, target.balance_type
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
        trait=row.trait,
        balance_type=row.balance_type,
        total_debits=debits,
        total_credits=credits,
        net_balance=debits - credits,
      )
    )

  return MappedTrialBalanceResponse(mapping_id=mapping_id, rows=rows)
