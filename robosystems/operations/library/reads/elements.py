"""Read operations for library elements + labels + references."""

from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.library import (
  LibraryElementArcResponse,
  LibraryElementResponse,
  LibraryElementTraitResponse,
  LibraryElementTreeNode,
  LibraryEquivalenceResponse,
  LibraryLabelResponse,
  LibraryReferenceResponse,
)
from robosystems.models.extensions import (
  Association,
  Element,
  ElementLabel,
  ElementReference,
  ElementTrait,
  Structure,
  Taxonomy,
  Trait,
)

DEFAULT_ELEMENT_LIMIT = 50
MAX_ELEMENT_LIMIT = 500


def _labels_for(session: Session, element_id: str) -> list[LibraryLabelResponse]:
  rows = (
    session.execute(
      select(ElementLabel)
      .where(ElementLabel.element_id == element_id)
      .order_by(ElementLabel.role, ElementLabel.language)
    )
    .scalars()
    .all()
  )
  return [
    LibraryLabelResponse(role=r.role, language=r.language, text=r.text) for r in rows
  ]


def _references_for(
  session: Session, element_id: str
) -> list[LibraryReferenceResponse]:
  rows = (
    session.execute(
      select(ElementReference)
      .where(ElementReference.element_id == element_id)
      .order_by(ElementReference.ref_type, ElementReference.citation)
    )
    .scalars()
    .all()
  )
  return [
    LibraryReferenceResponse(ref_type=r.ref_type, citation=r.citation, uri=r.uri)
    for r in rows
  ]


def _labels_by_element(
  session: Session, element_ids: list[str]
) -> dict[str, list[LibraryLabelResponse]]:
  """Labels per element in one query; unlabelled elements map to ``[]``."""
  grouped: dict[str, list[LibraryLabelResponse]] = {eid: [] for eid in element_ids}
  if not element_ids:
    return grouped
  rows = (
    session.execute(
      select(ElementLabel)
      .where(ElementLabel.element_id.in_(element_ids))
      .order_by(ElementLabel.element_id, ElementLabel.role, ElementLabel.language)
    )
    .scalars()
    .all()
  )
  for r in rows:
    grouped[r.element_id].append(
      LibraryLabelResponse(role=r.role, language=r.language, text=r.text)
    )
  return grouped


def primary_trait_by_element(
  session: Session, element_ids: list[str], category: str
) -> dict[str, str]:
  """``{element_id: identifier}`` of each element's primary trait in ``category``.

  Elements with no assignment in the category are absent. The primary row
  wins when an element has alternates.
  """
  result: dict[str, str] = {}
  if not element_ids:
    return result
  rows = session.execute(
    select(ElementTrait.element_id, Trait.identifier)
    .join(Trait, Trait.id == ElementTrait.trait_id)
    .where(
      ElementTrait.element_id.in_(element_ids),
      Trait.category == category,
    )
    .order_by(ElementTrait.is_primary.desc())
  ).all()
  for element_id, identifier in rows:
    result.setdefault(element_id, identifier)
  return result


def efs_trait_by_element(session: Session, element_ids: list[str]) -> dict[str, str]:
  return primary_trait_by_element(session, element_ids, "elementsOfFinancialStatements")


def liquidity_by_element(session: Session, element_ids: list[str]) -> dict[str, str]:
  return primary_trait_by_element(session, element_ids, "liquidity")


_efs_trait_by_element = efs_trait_by_element


def _references_by_element(
  session: Session, element_ids: list[str]
) -> dict[str, list[LibraryReferenceResponse]]:
  grouped: dict[str, list[LibraryReferenceResponse]] = {eid: [] for eid in element_ids}
  if not element_ids:
    return grouped
  rows = (
    session.execute(
      select(ElementReference)
      .where(ElementReference.element_id.in_(element_ids))
      .order_by(
        ElementReference.element_id,
        ElementReference.ref_type,
        ElementReference.citation,
      )
    )
    .scalars()
    .all()
  )
  for r in rows:
    grouped[r.element_id].append(
      LibraryReferenceResponse(ref_type=r.ref_type, citation=r.citation, uri=r.uri)
    )
  return grouped


def _element_to_response(
  session: Session,
  element: Element,
  *,
  include_labels: bool = True,
  include_references: bool = True,
  efs_map: dict[str, str] | None = None,
) -> LibraryElementResponse:
  """Without ``efs_map`` this queries the trait per element, which is N+1 for
  tree and list walkers; they must batch it via :func:`_efs_trait_by_element`."""
  labels = _labels_for(session, element.id) if include_labels else []
  refs = _references_for(session, element.id) if include_references else []
  if efs_map is None:
    efs = _efs_trait_by_element(session, [element.id]).get(element.id)
  else:
    efs = efs_map.get(element.id)
  return LibraryElementResponse(
    id=element.id,
    qname=element.qname or element.name,
    namespace=element.namespace,
    name=element.name,
    trait=efs,
    balance_type=element.balance_type,
    period_type=element.period_type,
    is_abstract=element.is_abstract,
    is_monetary=element.is_monetary,
    element_type=element.element_type,
    source=element.source,
    taxonomy_id=element.taxonomy_id,
    parent_id=element.parent_id,
    labels=labels,
    references=refs,
  )


def _trait_filter_subquery(category: str, identifier: str):
  return (
    select(ElementTrait.element_id)
    .join(Trait, Trait.id == ElementTrait.trait_id)
    .where(
      Trait.category == category,
      Trait.identifier == identifier,
    )
  )


def list_elements(
  session: Session,
  taxonomy_id: str | None = None,
  source: str | None = None,
  trait: str | None = None,
  activity_type: str | None = None,
  element_type: str | None = None,
  is_abstract: bool | None = None,
  limit: int = DEFAULT_ELEMENT_LIMIT,
  offset: int = 0,
  include_labels: bool = False,
  include_references: bool = False,
  include_inactive: bool = False,
) -> list[LibraryElementResponse]:
  """List library elements with filters + pagination.

  `trait` filters on the elementsOfFinancialStatements trait and
  `activity_type` on the cash-flow activity trait; combined, both must match.
  Deactivated elements (e.g. QuickBooks "(deleted)" accounts) are hidden
  unless `include_inactive`.
  """
  limit = max(1, min(limit, MAX_ELEMENT_LIMIT))

  query = select(Element)
  if taxonomy_id is not None:
    query = query.where(Element.taxonomy_id == taxonomy_id)
  if source is not None:
    query = query.where(Element.source == source)
  if trait is not None:
    query = query.where(
      Element.id.in_(_trait_filter_subquery("elementsOfFinancialStatements", trait))
    )
  if activity_type is not None:
    query = query.where(
      Element.id.in_(_trait_filter_subquery("activityType", activity_type))
    )
  if element_type is not None:
    query = query.where(Element.element_type == element_type)
  if is_abstract is not None:
    query = query.where(Element.is_abstract.is_(is_abstract))
  if not include_inactive:
    query = query.where(Element.is_active.is_(True))
  query = query.order_by(Element.qname.asc()).limit(limit).offset(offset)

  elements = session.execute(query).scalars().all()
  if not elements:
    return []

  element_ids = [e.id for e in elements]
  labels_by_id = _labels_by_element(session, element_ids) if include_labels else {}
  refs_by_id = (
    _references_by_element(session, element_ids) if include_references else {}
  )
  efs_by_id = _efs_trait_by_element(session, element_ids)

  return [
    LibraryElementResponse(
      id=e.id,
      qname=e.qname or e.name,
      namespace=e.namespace,
      name=e.name,
      trait=efs_by_id.get(e.id),
      balance_type=e.balance_type,
      period_type=e.period_type,
      is_abstract=e.is_abstract,
      is_monetary=e.is_monetary,
      element_type=e.element_type,
      source=e.source,
      taxonomy_id=e.taxonomy_id,
      parent_id=e.parent_id,
      labels=labels_by_id.get(e.id, []),
      references=refs_by_id.get(e.id, []),
    )
    for e in elements
  ]


def get_element(
  session: Session,
  element_id: str,
) -> LibraryElementResponse | None:
  element = session.execute(
    select(Element).where(Element.id == element_id)
  ).scalar_one_or_none()
  if element is None:
    return None
  return _element_to_response(session, element)


def get_element_by_qname(session: Session, qname: str) -> LibraryElementResponse | None:
  element = session.execute(
    select(Element).where(Element.qname == qname)
  ).scalar_one_or_none()
  if element is None:
    return None
  return _element_to_response(session, element)


def search_elements(
  session: Session,
  query_text: str,
  limit: int = DEFAULT_ELEMENT_LIMIT,
  source: str | None = None,
) -> list[LibraryElementResponse]:
  """Substring search across qname, name and label text.

  ILIKE with no index, so cost scales with library size.
  """
  limit = max(1, min(limit, MAX_ELEMENT_LIMIT))
  pattern = f"%{query_text.lower()}%"

  q = (
    select(Element)
    .outerjoin(ElementLabel, ElementLabel.element_id == Element.id)
    .where(
      or_(
        Element.qname.ilike(pattern),
        Element.name.ilike(pattern),
        ElementLabel.text.ilike(pattern),
      )
    )
    .distinct()
    .order_by(Element.qname.asc())
    .limit(limit)
  )
  if source is not None:
    q = q.where(Element.source == source)

  elements = session.execute(q).scalars().all()
  return [_element_to_response(session, e) for e in elements]


def get_element_tree(
  session: Session,
  element_id: str,
  max_depth: int = 5,
  structure_id: str | None = None,
) -> LibraryElementTreeNode | None:
  """The element and its presentation-arc descendants up to `max_depth`.

  Pass ``structure_id`` when a taxonomy has several layout variants
  (classified vs unclassified BS): without it, a concept reused across
  variants gets a blended child set matching no real statement.
  """
  root = session.execute(
    select(Element).where(Element.id == element_id)
  ).scalar_one_or_none()
  if root is None:
    return None

  # Enumerate ids breadth-first first, so elements and traits batch-load.
  root_id = str(root.id)
  children_by_parent: dict[str, list[str]] = {}
  visited: set[str] = {root_id}
  frontier: list[tuple[str, int]] = [(root_id, 0)]
  while frontier:
    parent_id, depth = frontier.pop(0)
    if depth >= max_depth:
      continue
    q = select(Association.to_element_id).where(
      Association.from_element_id == parent_id,
      Association.association_type == "presentation",
    )
    if structure_id is not None:
      q = q.where(Association.structure_id == structure_id)
    q = q.order_by(Association.order_value.asc().nulls_last())
    assoc_rows = session.execute(q).scalars().all()
    ordered_children = list(assoc_rows)
    children_by_parent[parent_id] = ordered_children
    for child_id in ordered_children:
      if child_id not in visited:
        visited.add(child_id)
        frontier.append((child_id, depth + 1))

  all_ids = list(visited)
  elements_by_id: dict[str, Element] = {
    str(e.id): e
    for e in session.execute(select(Element).where(Element.id.in_(all_ids)))
    .scalars()
    .all()
  }
  efs_map = _efs_trait_by_element(session, all_ids)

  def _build(elem_id: str) -> LibraryElementTreeNode | None:
    elem = elements_by_id.get(elem_id)
    if elem is None:
      return None
    child_nodes: list[LibraryElementTreeNode] = []
    for cid in children_by_parent.get(elem_id, []):
      child_node = _build(cid)
      if child_node is not None:
        child_nodes.append(child_node)
    # Labels and references would cost 2 queries per node.
    return LibraryElementTreeNode(
      element=_element_to_response(
        session,
        elem,
        include_labels=False,
        include_references=False,
        efs_map=efs_map,
      ),
      children=child_nodes,
    )

  return _build(root_id)


def get_element_equivalents(
  session: Session,
  element_id: str,
) -> LibraryEquivalenceResponse | None:
  """The element's peers over equivalence arcs, in both directions."""
  element = session.execute(
    select(Element).where(Element.id == element_id)
  ).scalar_one_or_none()
  if element is None:
    return None

  from_ids = (
    session.execute(
      select(Association.to_element_id).where(
        Association.from_element_id == element.id,
        Association.association_type == "equivalence",
      )
    )
    .scalars()
    .all()
  )
  to_ids = (
    session.execute(
      select(Association.from_element_id).where(
        Association.to_element_id == element.id,
        Association.association_type == "equivalence",
      )
    )
    .scalars()
    .all()
  )
  peer_ids = list({str(pid) for pid in (*from_ids, *to_ids)} - {str(element.id)})

  equivalents: list[LibraryElementResponse] = []
  if peer_ids:
    peers = (
      session.execute(
        select(Element).where(Element.id.in_(peer_ids)).order_by(Element.qname.asc())
      )
      .scalars()
      .all()
    )
    equivalents = [
      _element_to_response(session, p, include_labels=False, include_references=False)
      for p in peers
    ]

  return LibraryEquivalenceResponse(
    element=_element_to_response(session, element), equivalents=equivalents
  )


def get_element_arcs(
  session: Session,
  element_id: str,
) -> list[LibraryElementArcResponse]:
  """Cross-taxonomy arcs where this element is source or target.

  Covers mapping and classification-assignment taxonomies and CoA-mapping
  structures; presentation/calculation arcs within one reporting taxonomy are
  excluded. ``direction`` is from this element's side.
  """
  base_select = (
    select(
      Association,
      Element,
      Structure.id.label("structure_id"),
      Structure.name.label("structure_name"),
      Taxonomy.id.label("taxonomy_id"),
      Taxonomy.standard.label("taxonomy_standard"),
      Taxonomy.name.label("taxonomy_name"),
    )
    .join(Structure, Association.structure_id == Structure.id)
    .join(Taxonomy, Structure.taxonomy_id == Taxonomy.id)
    .where(
      (Taxonomy.taxonomy_type == "mapping")
      | (Taxonomy.taxonomy_type == "classification-assignment")
      | (Structure.block_type == "coa_mapping")
    )
  )

  outgoing = base_select.join(Element, Association.to_element_id == Element.id).where(
    Association.from_element_id == element_id
  )
  incoming = base_select.join(Element, Association.from_element_id == Element.id).where(
    Association.to_element_id == element_id
  )

  rows: list[LibraryElementArcResponse] = []
  for assoc, peer, struct_id, struct_name, tax_id, tax_std, tax_name in session.execute(
    outgoing
  ).all():
    rows.append(
      LibraryElementArcResponse(
        id=assoc.id,
        direction="outgoing",
        association_type=assoc.association_type,
        arcrole=assoc.arcrole,
        taxonomy_id=tax_id,
        taxonomy_standard=tax_std,
        taxonomy_name=tax_name,
        structure_id=struct_id,
        structure_name=struct_name,
        peer=_element_to_response(
          session, peer, include_labels=False, include_references=False
        ),
      )
    )
  for assoc, peer, struct_id, struct_name, tax_id, tax_std, tax_name in session.execute(
    incoming
  ).all():
    rows.append(
      LibraryElementArcResponse(
        id=assoc.id,
        direction="incoming",
        association_type=assoc.association_type,
        arcrole=assoc.arcrole,
        taxonomy_id=tax_id,
        taxonomy_standard=tax_std,
        taxonomy_name=tax_name,
        structure_id=struct_id,
        structure_name=struct_name,
        peer=_element_to_response(
          session, peer, include_labels=False, include_references=False
        ),
      )
    )

  rows.sort(
    key=lambda r: (
      r.taxonomy_standard or "",
      r.association_type,
      r.direction,
      r.peer.qname,
    )
  )
  return rows


def get_element_traits(
  session: Session,
  element_id: str,
) -> list[LibraryElementTraitResponse]:
  """All traits assigned to the element, ordered by category then identifier."""
  rows = session.execute(
    select(Trait, ElementTrait.is_primary)
    .join(ElementTrait, Trait.id == ElementTrait.trait_id)
    .where(ElementTrait.element_id == element_id)
    .order_by(Trait.category, Trait.identifier)
  ).all()
  return [
    LibraryElementTraitResponse(
      category=trait.category,
      identifier=trait.identifier,
      name=trait.name,
      is_primary=is_primary or False,
    )
    for trait, is_primary in rows
  ]
