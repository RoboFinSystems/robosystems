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


# Local aliases for the shared library helpers, to keep call sites terse.
_efs_by_element = efs_trait_by_element
_liquidity_by_element = liquidity_by_element


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
  """Return rs-gaap candidates for a CoA element, narrowed by EFS trait
  (and ``liquidity`` when supplied).

  Filters active ``rs-gaap`` elements by the FASB
  elementsOfFinancialStatements trait (via element_traits), restricted
  to concepts that **actually render** under the active Reporting Style
  (via ``_load_renderable_concepts``). When ``reporting_style_id`` isn't
  supplied, falls back to the wider rs-gaap-presentation set so the
  function stays usable in test contexts and partial deployments.
  Subtotal rollups whose value comes from rendering, not from a leaf
  fact, are excluded via ``RS_GAAP_SUBTOTAL_DENYLIST``.

  Candidates are rs-gaap only, keeping this suggester consistent with the
  renderer. The filter narrows to ``reporting_style_networks`` rather than
  the full rs-gaap-presentation taxonomy, which would admit concepts the
  renderer never walks — e.g. ``AccountsPayableCurrent`` is in
  rs-gaap-presentation but the BS Classified rendering structure uses the
  more aggregated ``AccountsPayableAndAccruedLiabilitiesCurrent`` — so the
  "guaranteed to render" promise holds.

  ``element_id`` is reserved for future per-element overrides but is
  currently unused — ``trait`` (+ optional ``liquidity``) drive the filter.
  """
  del element_id  # reserved for future per-element narrowing
  if trait is None:
    return []

  # Lazy import to avoid pulling agent constants into every read path.
  from robosystems.operations.operators.implementations.mapping.constants import (
    RS_GAAP_SUBTOTAL_DENYLIST,
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

  # Liquidity narrowing (current / noncurrent): when the CoA element
  # carries a liquidity trait, drop candidates whose liquidity *contradicts*
  # it (a "Bank" / current account never surfaces noncurrent-asset
  # candidates, and vice versa). Candidates with no liquidity trait are
  # kept — absence isn't a contradiction, and the EFS filter still applies.
  # No-op when liquidity is None (manual elements that didn't set it, or
  # equity/revenue/expense which have no liquidity axis).
  if liquidity:
    candidate_liquidity = _liquidity_by_element(session, [r.id for r in rows])
    rows = [r for r in rows if candidate_liquidity.get(r.id) in (None, liquidity)]

  # Structure-aware rollup guard: when a Reporting Style is active and
  # seeded, deny a target only if it actually rolls up on that Style (its
  # children render). On a thin Style where the concept IS the leaf,
  # mapping to it is correct. Without a seeded Style (tests / partial
  # deployments) fall back to the static denylist.
  rollup_set = (
    _load_rollup_concepts(session, reporting_style_id) if reporting_style_id else set()
  )

  def _denied(r) -> bool:
    if rollup_set:
      return r.id in rollup_set
    return r.qname in RS_GAAP_SUBTOTAL_DENYLIST

  # Synthesized-detail concepts (PP&E Gross + accumulated depreciation)
  # aren't in the presentation set — the renderer absorbs them into a
  # synthesized PropertyPlantAndEquipmentNet — but they're the correct
  # mapping grain for fixed-asset / contra accounts (lets CF Investing
  # read ΔGross as capex). Admit them past the presentation filter.
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

  # Get mapped element IDs (from_element_id in mapping associations)
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
  """Return element_ids that render under a given Reporting Style.

  Walks ``reporting_style_networks`` to find each Statement-type rendering
  structure attached to the Style (BS Classified, IS Multi-step, CF
  Indirect, SE Roll Forward, etc.), then collects every concept that
  appears as either parent or child of a ``presentation`` association on
  those structures.

  This is the "guaranteed-to-render" filter — it matches what
  ``generate_report_facts`` traverses. Prefer it over the
  ``_load_rs_gaap_presentation_set`` fallback, which filters on the whole
  rs-gaap-presentation taxonomy: a superset of what the renderer walks,
  since rendering structures use a more aggregated vocabulary (e.g.
  ``AccountsPayableAndAccruedLiabilitiesCurrent`` rather than
  ``AccountsPayableCurrent``). Suggestions made against the wider set can
  land on concepts that never render.

  Empty result = caller treats as "no filter" so a partially-provisioned
  tenant (no reporting_style_networks rows yet) still gets candidates.

  Cached on the session keyed by reporting_style_id: switching Style
  mid-session is rare and the same id is queried repeatedly inside a single
  auto-map run.
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
  """Return element_ids that are *rolled up at render* under a Reporting Style.

  A concept is rolled up at render iff it appears as the **parent**
  (``from_element_id``) of a ``presentation`` arc on one of the Style's
  rendering structures — its value comes from summing the children the
  renderer walks, so a CoA account must not map to it (the leaf fact would
  double-count). A concept that is **not** in this set is a leaf on the
  active Style, and mapping a CoA account to it is correct.

  Structure-aware, so preferred over the static
  ``RS_GAAP_SUBTOTAL_DENYLIST``: the static list over-denies on thin Style
  structures where, e.g., ``rs-gaap:Revenues`` has no rendering children
  and is itself the leaf. Empty result = no Style structures seeded →
  callers fall back to the static denylist.

  Cached on the session keyed by reporting_style_id (same pattern as
  ``_load_renderable_concepts``).
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
      WHERE rsn.reporting_style_id = :rsid
        AND a.association_type = 'presentation'
        AND a.from_element_id IS NOT NULL
        AND a.to_element_id IS NOT NULL
    """),
    {"rsid": reporting_style_id},
  ).fetchall()
  result = {r.element_id for r in rows}
  try:
    setattr(session, cache_attr, result)
  except (AttributeError, TypeError):
    pass
  return result


def _load_rs_gaap_presentation_set(session: Session) -> set[str]:
  """Return the set of element_ids that appear in any
  ``rs-gaap-presentation`` structure (as either parent or child).

  Used as a **wider-net fallback** when a Reporting Style isn't
  available — prefer ``_load_renderable_concepts(reporting_style_id)``
  when the caller has graph context. Returns an empty set if the
  presentation taxonomy isn't seeded — caller treats empty as "no
  filter" so partial deployments still function.

  **Cached on the session** under a private attribute. The UNION query
  walks `associations → structures → taxonomies` twice and is invariant
  for the lifetime of a tenant session (rs-gaap-presentation is
  library-seeded and immutable per-tenant).
  """
  # ``isinstance(..., set)`` guards against MagicMock sessions: a vanilla
  # ``getattr(mock, attr, None)`` returns a fresh MagicMock (not None),
  # so the sentinel fallback wouldn't fire. The type check returns False
  # for MagicMock and treats the test session as uncached.
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
    # MagicMock sessions in unit tests sometimes reject arbitrary
    # attribute writes; fall through without caching in that case.
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
  """List all active mapping structures (block_type = 'coa_mapping')."""
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
  """Return mapping coverage stats (total, mapped, unmapped, confidence).

  Raises ``MappingStructureNotFoundError`` when ``mapping_id`` does not
  resolve to an existing Structure row. Without this guard the count
  query returns 0 mapped associations regardless of whether the
  structure exists, silently misleading callers into thinking coverage
  is 0 / 100 when in fact they're querying a nonexistent id.
  """
  # Lazy import: the commands module pulls in the full taxonomy write
  # surface (assert_not_library_origin, generate_prefixed_ulid, etc.)
  # which we don't need on the read path. Importing at the call site
  # keeps the reads module cheap to import for callers that never hit
  # this function.
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
  """True if ``target_element_id`` traces to a canonical rs-gaap root.

  Walks upward through ``association_type='calculation'`` arcs (each
  step takes ``to_element_id`` → ``from_element_id`` — calc arcs point
  parent→child, so we invert to walk child→parent). Reaches one of the
  rs-gaap root concepts (Assets / LiabilitiesAndStockholdersEquity /
  NetIncomeLoss / CashAndCashEquivalentsPeriodIncreaseDecrease) iff
  the target is renderable.

  The internal cache parameters let callers batch a full mapping check
  against a single calc-DAG snapshot.
  """
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
  """Return the subset of mapping associations whose targets don't reach a root.

  Single calc-DAG snapshot is loaded once and reused across all
  targets — keeps the check ~linear in the number of associations.
  """
  if not mapping_assocs:
    return []

  calc_parents = _load_calc_parents(session)
  root_ids = _resolve_root_ids(session)

  # Element metadata lookup for response detail
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
  """Return ``child_element_id → {parent_element_id, …}`` for all calc arcs.

  Calc arcs are declared parent→child (``from_element_id = parent``,
  ``to_element_id = child``). To walk *up* from a target we want the
  inverse mapping. Cached on the session — the calc DAG is constant
  for the lifetime of a tenant request and the reachability check fires
  on every `get_mapping_coverage` call.
  """
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
  """Return the element_id set for canonical rs-gaap roots.

  Cached on the session — root concept ids don't change within a
  request and the reachability check refers to this on every call.
  """
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

  Accepts both `date` objects (preferred — the GraphQL resolver passes
  these so the wire schema exposes a `Date` scalar) and ISO-8601 strings
  (REST callers). SQLAlchemy parameter binding handles either.
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
