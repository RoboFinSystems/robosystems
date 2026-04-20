"""Read operations for library elements + labels + references."""

from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.library import (
  LibraryElementArcResponse,
  LibraryElementResponse,
  LibraryElementTreeNode,
  LibraryEquivalenceResponse,
  LibraryLabelResponse,
  LibraryReferenceResponse,
)
from robosystems.models.extensions import (
  Association,
  Classification,
  Element,
  ElementClassification,
  ElementLabel,
  ElementReference,
  Structure,
  Taxonomy,
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
  """Batch-load labels for every element in `element_ids`.

  Single ``WHERE element_id IN (...)`` query avoids the N+1 pattern when
  a caller (e.g., `list_elements(include_labels=True)`) would otherwise
  issue one query per element. Returns a dict keyed by ``element_id``
  with an empty list for elements that have no labels.
  """
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


def _efs_classification_by_element(
  session: Session, element_ids: list[str]
) -> dict[str, str]:
  """Batch-load FASB elementsOfFinancialStatements classification identifier.

  Returns ``{element_id: identifier}`` — e.g., ``{"elem_xyz": "asset"}``.
  Elements without an EFS classification assignment are absent from the
  map (callers should default to ``None``).
  """
  result: dict[str, str] = {}
  if not element_ids:
    return result
  # ORDER BY is_primary DESC ensures the primary EFS assignment wins
  # when multiple rows exist for one element (the migration allows the
  # junction to carry alternates alongside a single primary).
  rows = session.execute(
    select(ElementClassification.element_id, Classification.identifier)
    .join(Classification, Classification.id == ElementClassification.classification_id)
    .where(
      ElementClassification.element_id.in_(element_ids),
      Classification.category == "elementsOfFinancialStatements",
    )
    .order_by(ElementClassification.is_primary.desc())
  ).all()
  for element_id, identifier in rows:
    result.setdefault(element_id, identifier)
  return result


def _references_by_element(
  session: Session, element_ids: list[str]
) -> dict[str, list[LibraryReferenceResponse]]:
  """Batch-load references for every element in `element_ids`."""
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
  """Convert an Element row to its library response.

  ``efs_map`` is a pre-loaded ``{element_id: classification}`` batch. When
  omitted, a single-element query fires — convenient for standalone
  callers but **N+1 when walking a tree or iterating a list**. Tree /
  list walkers must build the map once via
  :func:`_efs_classification_by_element` and pass it through.
  """
  labels = _labels_for(session, element.id) if include_labels else []
  refs = _references_for(session, element.id) if include_references else []
  if efs_map is None:
    efs = _efs_classification_by_element(session, [element.id]).get(element.id)
  else:
    efs = efs_map.get(element.id)
  return LibraryElementResponse(
    id=element.id,
    qname=element.qname or element.name,
    namespace=element.namespace,
    name=element.name,
    classification=efs,
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


def list_elements(
  session: Session,
  taxonomy_id: str | None = None,
  source: str | None = None,
  classification: str | None = None,
  statement_context: str | None = None,
  derivation_role: str | None = None,
  element_type: str | None = None,
  is_abstract: bool | None = None,
  limit: int = DEFAULT_ELEMENT_LIMIT,
  offset: int = 0,
  include_labels: bool = False,
  include_references: bool = False,
) -> list[LibraryElementResponse]:
  """List library elements with filters + pagination.

  `include_labels` / `include_references` default False to keep list
  queries cheap; callers enable them for detail views.

  The three classification axes (`classification` = economic_nature,
  `statement_context`, `derivation_role`) AND together. Pass each
  independently; omit to skip that filter.

  `is_abstract=True` returns only abstract grouping concepts (hypercubes,
  LineItems, RollUps). `is_abstract=False` returns only concrete concepts.
  `None` returns both.
  """
  limit = max(1, min(limit, MAX_ELEMENT_LIMIT))

  query = select(Element)
  if taxonomy_id is not None:
    query = query.where(Element.taxonomy_id == taxonomy_id)
  if source is not None:
    query = query.where(Element.source == source)
  if classification is not None:
    # FASB elementsOfFinancialStatements trait — EXISTS via junction.
    query = query.where(
      Element.id.in_(
        select(ElementClassification.element_id)
        .join(
          Classification,
          Classification.id == ElementClassification.classification_id,
        )
        .where(
          Classification.category == "elementsOfFinancialStatements",
          Classification.identifier == classification,
        )
      )
    )
  # statement_context and derivation_role were hand-rolled axes that have
  # no FASB metamodel equivalent; accept and ignore for API compat.
  if element_type is not None:
    query = query.where(Element.element_type == element_type)
  if is_abstract is not None:
    query = query.where(Element.is_abstract.is_(is_abstract))
  query = query.order_by(Element.qname.asc()).limit(limit).offset(offset)

  elements = session.execute(query).scalars().all()
  if not elements:
    return []

  # Batch-load labels + references + EFS classifications in one query each
  # (avoids N+1 when include_labels=True; label/ref loaders are no-ops
  # when the flag is False).
  element_ids = [e.id for e in elements]
  labels_by_id = _labels_by_element(session, element_ids) if include_labels else {}
  refs_by_id = (
    _references_by_element(session, element_ids) if include_references else {}
  )
  efs_by_id = _efs_classification_by_element(session, element_ids)

  return [
    LibraryElementResponse(
      id=e.id,
      qname=e.qname or e.name,
      namespace=e.namespace,
      name=e.name,
      classification=efs_by_id.get(e.id),
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
  """Get a single element by id with labels + references."""
  element = session.execute(
    select(Element).where(Element.id == element_id)
  ).scalar_one_or_none()
  if element is None:
    return None
  return _element_to_response(session, element)


def get_element_by_qname(session: Session, qname: str) -> LibraryElementResponse | None:
  """Get an element by qname ('sfac6:Assets', 'fac:CostOfRevenue', …)."""
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
  """Substring search across qname + name + label text.

  POC uses ILIKE; upgrade to full-text search as the library grows.
  """
  limit = max(1, min(limit, MAX_ELEMENT_LIMIT))
  pattern = f"%{query_text.lower()}%"

  # Matches on element qname, name, or any standard-label text.
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
) -> LibraryElementTreeNode | None:
  """Walk parent→children descendants using `parent-child` associations.

  Returns the element plus a nested tree of descendants up to `max_depth`.
  """
  root = session.execute(
    select(Element).where(Element.id == element_id)
  ).scalar_one_or_none()
  if root is None:
    return None

  # First pass: breadth-first enumerate every element id reachable from
  # the root within `max_depth`, plus the association ordering for each
  # parent. This lets us batch-load elements + EFS classifications in
  # one shot instead of hitting the DB once per node.
  root_id = str(root.id)
  children_by_parent: dict[str, list[str]] = {}
  visited: set[str] = {root_id}
  frontier: list[tuple[str, int]] = [(root_id, 0)]
  while frontier:
    parent_id, depth = frontier.pop(0)
    if depth >= max_depth:
      continue
    assoc_rows = (
      session.execute(
        select(Association.to_element_id)
        .where(
          Association.from_element_id == parent_id,
          Association.association_type == "presentation",
        )
        .order_by(Association.order_value.asc().nulls_last())
      )
      .scalars()
      .all()
    )
    ordered_children = list(assoc_rows)
    children_by_parent[parent_id] = ordered_children
    for child_id in ordered_children:
      if child_id not in visited:
        visited.add(child_id)
        frontier.append((child_id, depth + 1))

  # Batch-load every element + its EFS classification.
  all_ids = list(visited)
  elements_by_id: dict[str, Element] = {
    str(e.id): e
    for e in session.execute(select(Element).where(Element.id.in_(all_ids)))
    .scalars()
    .all()
  }
  efs_map = _efs_classification_by_element(session, all_ids)

  def _build(elem_id: str) -> LibraryElementTreeNode | None:
    elem = elements_by_id.get(elem_id)
    if elem is None:
      return None
    child_nodes: list[LibraryElementTreeNode] = []
    for cid in children_by_parent.get(elem_id, []):
      child_node = _build(cid)
      if child_node is not None:
        child_nodes.append(child_node)
    # Tree navigation doesn't need the per-node label + reference payload;
    # skipping them saves 2 queries per node for deep/wide trees. Callers
    # that need full element detail use `get_element` on a specific id.
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
  """Return the equivalence fan-out for a library element.

  Traverses `association_type='equivalence'` arcs in both directions so
  `fac:CostOfRevenue` returns all us-gaap variants AND vice versa.
  """
  element = session.execute(
    select(Element).where(Element.id == element_id)
  ).scalar_one_or_none()
  if element is None:
    return None

  # Find equivalence arcs where this element is on either side.
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
  """Return all mapping arcs where this element is source or target.

  Filters to arcs whose structure belongs to a `taxonomy_type='mapping'`
  taxonomy — the cross-taxonomy bridges (fac↔rs-gaap equivalence,
  sfac6→fac general-special, type-subtype). Hierarchical arcs inside
  a single reporting taxonomy (presentation/calculation) are excluded.

  Each arc is returned once, oriented from this element's perspective:
  `direction='outgoing'` means this element is `from_element_id`,
  `direction='incoming'` means this element is `to_element_id`.
  """
  # One join chain serves both directions; we read Association, the peer
  # element id+row, Structure (for name), and Taxonomy (for standard/name
  # and the taxonomy_type filter).
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
      | (Structure.structure_type == "coa_mapping")
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
