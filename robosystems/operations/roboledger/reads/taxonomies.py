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
  UnreachableMapping,
)
from robosystems.models.extensions import (
  Association,
  Element,
  ElementTrait,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.library.reads import (
  efs_trait_by_element,
  liquidity_by_element,
)
from robosystems.operations.roboledger.entry_status import (
  landed_entry_bindparam,
)
from robosystems.operations.roboledger.reads.accounts import coa_element_clause


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


_efs_by_element = efs_trait_by_element
_liquidity_by_element = liquidity_by_element


def element_to_response(row: Element, trait: str | None = None) -> ElementResponse:
  """Batch callers should load traits once with ``_efs_by_element`` and pass
  ``trait`` in, avoiding an N+1."""
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
  """List active elements; ``trait`` is the FASB elementsOfFinancialStatements
  trait."""
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
        coa_element_clause(),
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
  reporting_style_id: str | None = None,
  liquidity: str | None = None,
) -> list[ElementResponse]:
  """rs-gaap mapping candidates for a CoA element with EFS ``trait``, narrowed
  by ``liquidity`` when supplied.

  Restricted to concepts that render under the Reporting Style
  (``_load_renderable_concepts``), falling back to the wider
  rs-gaap-presentation set when no style is given, and excluding rollups whose
  value comes from rendering rather than a leaf fact. ``element_id`` is unused.
  """
  del element_id  # reserved for future per-element narrowing
  if trait is None:
    return []

  from robosystems.operations.operators.implementations.mapping.constants import (
    RS_GAAP_SYNTHESIZED_DETAIL_ALLOW,
  )

  if reporting_style_id:
    presentation_set = _load_renderable_concepts(session, reporting_style_id)
  else:
    presentation_set = _load_rs_gaap_presentation_set(session)

  rows = (
    session.execute(
      select(Element)
      .where(
        Element.source == "rs-gaap",
        Element.is_active.is_(True),
        Element.id.in_(
          select(ElementTrait.element_id)
          .join(Trait, Trait.id == ElementTrait.trait_id)
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

  # Drop candidates whose liquidity contradicts the element's; a candidate
  # with no liquidity trait is not a contradiction.
  if liquidity:
    candidate_liquidity = _liquidity_by_element(session, [r.id for r in rows])
    rows = [r for r in rows if candidate_liquidity.get(r.id) in (None, liquidity)]

  # Same rule as the mapping write path (``assert_leaf_target``).
  from robosystems.operations.roboledger.reports.network_picker import (
    DEFAULT_STYLE_ID,
  )

  subtotals = load_subtotal_concepts(session, reporting_style_id or DEFAULT_STYLE_ID)

  def _denied(r) -> bool:
    return is_subtotal_target(r, subtotals)

  # PP&E gross and accumulated depreciation are absorbed into a synthesized
  # PP&E Net at render, but are the right grain for fixed-asset and contra
  # accounts (CF Investing reads ΔGross as capex), so admit them.
  filtered = [
    r
    for r in rows
    if not _denied(r)
    and (
      not presentation_set
      or r.id in presentation_set
      or r.qname in RS_GAAP_SYNTHESIZED_DETAIL_ALLOW
    )
  ]

  efs_map = _efs_by_element(session, [r.id for r in filtered])
  return [element_to_response(r, efs_map.get(r.id)) for r in filtered]


def list_unmapped_elements(
  session: Session, mapping_id: str | None = None
) -> list[UnmappedElementResponse]:
  """List CoA elements not yet mapped to the reporting taxonomy."""
  coa_query = select(Element).where(
    coa_element_clause(),
    Element.is_active.is_(True),
    Element.is_abstract.is_(False),
  )
  coa_elements = session.execute(coa_query).scalars().all()

  if mapping_id:
    mapped_query = select(Association.from_element_id).where(
      Association.structure_id == mapping_id,
      Association.association_type == "mapping",
    )
  else:
    mapped_query = select(Association.from_element_id).where(
      Association.association_type == "mapping",
    )
  mapped_ids = set(session.execute(mapped_query).scalars().all())

  unmapped = [e for e in coa_elements if e.id not in mapped_ids]
  unmapped_ids = [e.id for e in unmapped]
  efs_map = _efs_by_element(session, unmapped_ids)
  liquidity_map = _liquidity_by_element(session, unmapped_ids)

  return [
    UnmappedElementResponse(
      id=e.id,
      code=e.code,
      name=e.name,
      trait=efs_map.get(e.id),
      liquidity=liquidity_map.get(e.id),
      balance_type=e.balance_type,
      external_source=e.external_source,
    )
    for e in unmapped
  ]


_RS_GAAP_PRESENTATION_SET_ATTR = "_rs_gaap_presentation_set_cache"
_RENDERABLE_CONCEPTS_ATTR_PREFIX = "_renderable_concepts_cache_"


def _load_renderable_concepts(
  session: Session,
  reporting_style_id: str,
) -> set[str]:
  """Element ids on either end of a ``presentation`` arc in the Style's
  rendering structures: exactly what ``generate_report_facts`` walks.

  Empty means "no filter" (Style not provisioned yet). Cached on the session
  per style id.
  """
  cache_attr = f"{_RENDERABLE_CONCEPTS_ATTR_PREFIX}{reporting_style_id}"
  cached = getattr(session, cache_attr, None)
  if isinstance(cached, set):
    return cached

  rows = session.execute(
    text("""
      SELECT DISTINCT a.to_element_id AS element_id
      FROM reporting_style_networks rsn
      JOIN associations a ON a.structure_id = rsn.network_id
      WHERE rsn.reporting_style_id = :rsid
        AND a.association_type = 'presentation'
        AND a.to_element_id IS NOT NULL
      UNION
      SELECT DISTINCT a.from_element_id AS element_id
      FROM reporting_style_networks rsn
      JOIN associations a ON a.structure_id = rsn.network_id
      WHERE rsn.reporting_style_id = :rsid
        AND a.association_type = 'presentation'
        AND a.from_element_id IS NOT NULL
    """),
    {"rsid": reporting_style_id},
  ).fetchall()
  result = {r.element_id for r in rows}
  try:
    setattr(session, cache_attr, result)
  except (AttributeError, TypeError):
    pass
  return result


_ROLLUP_CONCEPTS_ATTR_PREFIX = "_rollup_concepts_cache_"


def _load_rollup_concepts(
  session: Session,
  reporting_style_id: str,
) -> set[str]:
  """Element ids that are the parent of a ``presentation`` arc on the Style's
  rendering structures, i.e. summed at render; mapping a CoA account to one
  would double-count. A roll-forward's parent is the balance its flows move,
  not their sum (``PartnersCapital``, ``MembersEquity``), so it doesn't count.

  Empty means no Style seeded (callers use the static denylist). Cached on
  the session per style id.
  """
  cache_attr = f"{_ROLLUP_CONCEPTS_ATTR_PREFIX}{reporting_style_id}"
  cached = getattr(session, cache_attr, None)
  if isinstance(cached, set):
    return cached

  rows = session.execute(
    text("""
      SELECT DISTINCT a.from_element_id AS element_id
      FROM reporting_style_networks rsn
      JOIN associations a ON a.structure_id = rsn.network_id
      JOIN structures st ON st.id = rsn.network_id
      WHERE rsn.reporting_style_id = :rsid
        AND a.association_type = 'presentation'
        AND a.from_element_id IS NOT NULL
        AND a.to_element_id IS NOT NULL
        AND st.concept_arrangement IS DISTINCT FROM 'roll_forward'
    """),
    {"rsid": reporting_style_id},
  ).fetchall()
  result = {r.element_id for r in rows}
  try:
    setattr(session, cache_attr, result)
  except (AttributeError, TypeError):
    pass
  return result


_SUBTOTAL_CONCEPTS_ATTR_PREFIX = "_subtotal_concepts_cache_"

# The renderer emits net income itself (``fact_grid._emit_net_income_facts``),
# whether or not a calculation arc names it.
_NET_INCOME_QNAME = "rs-gaap:NetIncomeLoss"


def load_subtotal_concepts(session: Session, reporting_style_id: str) -> set[str]:
  """Element ids a statement computes rather than reads: presentation parents
  on the Style's networks, and parents of any rs-gaap calculation arc. A direct
  fact on either overrides the computed value, so neither is a mapping target.

  Empty means nothing is seeded (callers use the static denylist). Cached on
  the session per style id.
  """
  from robosystems.operations.roboledger.reports.calc_dag import (
    load_rs_gaap_calculations,
  )

  cache_attr = f"{_SUBTOTAL_CONCEPTS_ATTR_PREFIX}{reporting_style_id}"
  cached = getattr(session, cache_attr, None)
  if isinstance(cached, set):
    return cached
  result = _load_rollup_concepts(session, reporting_style_id) | set(
    load_rs_gaap_calculations(session)
  )
  try:
    setattr(session, cache_attr, result)
  except (AttributeError, TypeError):
    pass
  return result


def is_subtotal_target(element: Element, subtotals: set[str]) -> bool:
  """Whether mapping an account to ``element`` would override a computed total."""
  from robosystems.operations.operators.implementations.mapping.constants import (
    RS_GAAP_SUBTOTAL_DENYLIST,
  )

  if subtotals:
    return element.id in subtotals or element.qname == _NET_INCOME_QNAME
  return element.qname in RS_GAAP_SUBTOTAL_DENYLIST


def _load_rs_gaap_presentation_set(session: Session) -> set[str]:
  """Element ids in any ``rs-gaap-presentation`` structure: the wider fallback
  when no Reporting Style is available. Empty means "no filter". Cached on the
  session (the library set is immutable per tenant).
  """
  # isinstance, not a None check: getattr on a MagicMock session returns a mock.
  cached = getattr(session, _RS_GAAP_PRESENTATION_SET_ATTR, None)
  if isinstance(cached, set):
    return cached

  rows = session.execute(
    text("""
      SELECT DISTINCT a.from_element_id AS element_id
      FROM associations a
      JOIN structures s ON s.id = a.structure_id
      JOIN taxonomies t ON t.id = s.taxonomy_id
      WHERE t.standard = 'rs-gaap-presentation'
      UNION
      SELECT DISTINCT a.to_element_id AS element_id
      FROM associations a
      JOIN structures s ON s.id = a.structure_id
      JOIN taxonomies t ON t.id = s.taxonomy_id
      WHERE t.standard = 'rs-gaap-presentation'
    """),
  ).fetchall()
  result = {r.element_id for r in rows}
  try:
    setattr(session, _RS_GAAP_PRESENTATION_SET_ATTR, result)
  except (AttributeError, TypeError):
    # Some test sessions reject attribute writes.
    pass
  return result


# ── Structures ────────────────────────────────────────────────────────────


def _structure_to_response(row: Structure) -> StructureResponse:
  return StructureResponse(
    id=row.id,
    name=row.name,
    description=row.description,
    block_type=row.block_type,
    taxonomy_id=row.taxonomy_id,
    is_active=row.is_active,
  )


def list_structures(
  session: Session,
  *,
  taxonomy_id: str | None = None,
  block_type: str | None = None,
) -> StructureListResponse:
  """List active structures, optionally filtered by taxonomy + type."""
  query = select(Structure).where(Structure.is_active.is_(True))
  if taxonomy_id:
    query = query.where(Structure.taxonomy_id == taxonomy_id)
  if block_type:
    query = query.where(Structure.block_type == block_type)
  rows = session.execute(query.order_by(Structure.name)).scalars().all()
  return StructureListResponse(structures=[_structure_to_response(r) for r in rows])


# ── Mappings ──────────────────────────────────────────────────────────────


def list_mappings(session: Session) -> StructureListResponse:
  """List active ``coa_mapping`` structures."""
  rows = (
    session.execute(
      select(Structure)
      .where(
        Structure.block_type == "coa_mapping",
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
    block_type=structure.block_type,
    taxonomy_id=structure.taxonomy_id,
    associations=associations,
    total_associations=len(associations),
  )


def get_mapping_coverage(session: Session, mapping_id: str) -> MappingCoverageResponse:
  """Mapping coverage stats (total, mapped, unmapped, confidence).

  Raises ``MappingStructureNotFoundError`` for an unknown id rather than
  reporting 0% coverage.
  """
  from robosystems.operations.roboledger.commands.taxonomies import (
    MappingStructureNotFoundError,
  )

  structure = session.execute(
    select(Structure).where(Structure.id == mapping_id)
  ).scalar_one_or_none()
  if structure is None:
    raise MappingStructureNotFoundError(mapping_id)

  total_coa = (
    session.execute(
      select(func.count())
      .select_from(Element)
      .where(
        coa_element_clause(),
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

  unreachable = check_mapping_reachability(session, mapping_assocs)

  return MappingCoverageResponse(
    mapping_id=mapping_id,
    total_coa_elements=total_coa,
    mapped_count=mapped_count,
    unmapped_count=unmapped_count,
    coverage_percent=((mapped_count / total_coa * 100) if total_coa > 0 else 0.0),
    high_confidence=high,
    medium_confidence=medium,
    low_confidence=low,
    unreachable_count=len(unreachable),
    unreachable=unreachable,
  )


# Canonical roots of the rs-gaap reporting layer. A mapping target is
# "reachable" if it traces up through the rs-gaap calc DAG to one of
# these — otherwise it lives on a dead branch and never renders.
_CANONICAL_ROOTS: tuple[str, ...] = (
  "rs-gaap:Assets",
  "rs-gaap:LiabilitiesAndStockholdersEquity",
  "rs-gaap:NetIncomeLoss",
  "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease",
)


def is_target_reachable(
  session: Session,
  target_element_id: str,
  *,
  _calc_parents: dict[str, set[str]] | None = None,
  _root_ids: set[str] | None = None,
) -> bool:
  """True if ``target_element_id`` walks up the calc DAG to a canonical
  rs-gaap root, i.e. renders. The underscore parameters let a batch check
  share one calc-DAG snapshot."""
  if _calc_parents is None:
    _calc_parents = _load_calc_parents(session)
  if _root_ids is None:
    _root_ids = _resolve_root_ids(session)

  if target_element_id in _root_ids:
    return True

  visited: set[str] = set()
  frontier: set[str] = {target_element_id}
  while frontier:
    next_frontier: set[str] = set()
    for node in frontier:
      if node in visited:
        continue
      visited.add(node)
      if node in _root_ids:
        return True
      next_frontier.update(_calc_parents.get(node, set()))
    frontier = next_frontier - visited
  return False


def check_mapping_reachability(
  session: Session,
  mapping_assocs: list[Association],
) -> list[UnreachableMapping]:
  """The mapping associations whose targets don't reach a root."""
  if not mapping_assocs:
    return []

  calc_parents = _load_calc_parents(session)
  root_ids = _resolve_root_ids(session)

  target_ids = {a.to_element_id for a in mapping_assocs}
  source_ids = {a.from_element_id for a in mapping_assocs}
  element_lookup: dict[str, Element] = {
    str(e.id): e
    for e in session.execute(
      select(Element).where(Element.id.in_(target_ids | source_ids))
    )
    .scalars()
    .all()
  }

  unreachable: list[UnreachableMapping] = []
  cache: dict[str, bool] = {}
  for assoc in mapping_assocs:
    target_id = assoc.to_element_id
    if target_id not in cache:
      cache[target_id] = is_target_reachable(
        session, target_id, _calc_parents=calc_parents, _root_ids=root_ids
      )
    if cache[target_id]:
      continue
    target = element_lookup.get(target_id)
    source = element_lookup.get(assoc.from_element_id)
    unreachable.append(
      UnreachableMapping(
        coa_element_id=assoc.from_element_id,
        coa_qname=source.qname if source else None,
        coa_code=source.code if source else None,
        coa_name=source.name if source else None,
        target_element_id=target_id,
        target_qname=target.qname if target else None,
        target_name=target.name if target else None,
      )
    )
  return unreachable


def _load_calc_parents(session: Session) -> dict[str, set[str]]:
  """``child_element_id → {parent_element_id, …}`` over all calc arcs (the
  inverse of their declared direction). Cached on the session."""
  cached = getattr(session, "_calc_parents_cache", None)
  if isinstance(cached, dict):
    return cached
  rows = session.execute(
    text("""
      SELECT from_element_id, to_element_id
      FROM associations
      WHERE association_type = 'calculation'
    """)
  ).fetchall()
  parents: dict[str, set[str]] = {}
  for parent_id, child_id in rows:
    parents.setdefault(child_id, set()).add(parent_id)
  try:
    session._calc_parents_cache = parents
  except (AttributeError, TypeError):
    pass  # MagicMock or other immutable session in tests
  return parents


def _resolve_root_ids(session: Session) -> set[str]:
  """Element ids of the canonical rs-gaap roots. Cached on the session."""
  cached = getattr(session, "_root_ids_cache", None)
  if isinstance(cached, set):
    return cached
  rows = session.execute(
    text("SELECT id FROM elements WHERE qname = ANY(:qnames)"),
    {"qnames": list(_CANONICAL_ROOTS)},
  ).fetchall()
  root_ids = {row[0] for row in rows}
  try:
    session._root_ids_cache = root_ids
  except (AttributeError, TypeError):
    pass
  return root_ids


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
  LEFT JOIN (
      SELECT et.element_id, t.identifier
      FROM element_traits et
      JOIN traits t ON t.id = et.trait_id
      WHERE et.is_primary = TRUE
        AND t.category = 'elementsOfFinancialStatements'
  ) tt ON tt.element_id = target.id
  WHERE e.status IN :landed_entry_statuses
      AND (e.posting_date >= :start_date OR :start_date IS NULL)
      AND (e.posting_date <= :end_date OR :end_date IS NULL)
  GROUP BY target.id, target.qname, target.name, tt.identifier, target.balance_type
  ORDER BY target.qname
""").bindparams(landed_entry_bindparam())


def get_mapped_trial_balance(
  session: Session,
  mapping_id: str,
  start_date: date | str | None = None,
  end_date: date | str | None = None,
) -> MappedTrialBalanceResponse:
  """Trial balance rolled up to reporting concepts via mapping associations.
  Dates may be `date` objects or ISO strings."""
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
