"""Library (taxonomy library) GraphQL resolvers.

The library is the shared reference material in the extensions DB
public schema. Two access modes are supported:

1. Canonical browse via the `graph_id="library"` sentinel:

       POST /extensions/library/graphql

   `search_path = public` → returns the canonical library only.

2. Tenant-scope via an entity graph_id:

       POST /extensions/kg…/graphql

   `search_path = {schema}, public` → returns the tenant's library
   copy plus any tenant extensions of library tables (CoA elements,
   anchor associations, etc). This is how the roboledger-app `/library`
   surface reads library + tenant data in a single unified view.

Access contract — handled by `get_context`:

- Any authenticated user with graph access may read. `check_graph_access`
  already enforces per-graph ACLs before the resolver runs.
- Library fields are NOT gated by a per-graph extension flag. Data
  visibility is driven entirely by the session's search_path, which
  is implicit in the graph_id.

Each resolver opens `open_library_session(info)` and delegates to
`operations/library/reads/*`. No business logic lives here.
"""

from __future__ import annotations

import strawberry
from strawberry.types import Info

from robosystems.db.extensions import LIBRARY_GRAPH_ID
from robosystems.graphql.context import GraphQLContext, require_graph_id
from robosystems.graphql.resolvers._common import (
  open_library_session as _open_session,
)
from robosystems.graphql.resolvers._common import (
  validate_pagination as _validate_pagination,
)
from robosystems.graphql.types.library import (
  LibraryAssociation,
  LibraryElement,
  LibraryElementArc,
  LibraryElementClassification,
  LibraryElementTreeNode,
  LibraryEquivalence,
  LibraryStructure,
  LibraryTaxonomy,
)
from robosystems.operations.library.reads import (
  count_taxonomy_arcs,
  get_element,
  get_element_arcs,
  get_element_by_qname,
  get_element_classifications,
  get_element_equivalents,
  get_element_tree,
  get_structure,
  get_taxonomy,
  list_elements,
  list_structures,
  list_taxonomies,
  list_taxonomy_arcs,
  search_elements,
)


@strawberry.type
class LibraryQuery:
  """Query root for the taxonomy library.

  All fields read from `public` schema via `extensions_session("library")`.
  Available only when the request was routed through
  `/extensions/library/graphql`.
  """

  # ── Taxonomies ──────────────────────────────────────────────────────────

  @strawberry.field
  def library_taxonomies(
    self,
    info: Info[GraphQLContext, None],
    standard: str | None = None,
    include_element_count: bool = False,
  ) -> list[LibraryTaxonomy]:
    """List taxonomies visible to this graph.

    On the library sentinel returns only shared/canonical taxonomies.
    On a tenant graph_id also returns tenant-owned taxonomies (e.g. CoA).
    """
    is_sentinel = require_graph_id(info) == LIBRARY_GRAPH_ID
    with _open_session(info) as session:
      rows = list_taxonomies(
        session,
        standard=standard,
        include_element_count=include_element_count,
        shared_only=is_sentinel,
      )
      return [LibraryTaxonomy.from_pydantic(r) for r in rows]

  @strawberry.field
  def library_taxonomy(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID | None = None,
    standard: str | None = None,
    version: str | None = None,
    include_element_count: bool = False,
  ) -> LibraryTaxonomy | None:
    """Get a taxonomy by id or (standard, version)."""
    with _open_session(info) as session:
      row = get_taxonomy(
        session,
        taxonomy_id=str(id) if id else None,
        standard=standard,
        version=version,
        include_element_count=include_element_count,
      )
      return LibraryTaxonomy.from_pydantic(row) if row else None

  @strawberry.field
  def library_taxonomy_arcs(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_id: strawberry.ID,
    association_type: str | None = None,
    limit: int = 200,
    offset: int = 0,
  ) -> list[LibraryAssociation]:
    """List every arc contributed by a taxonomy (via its structures).

    For mapping taxonomies (fac-to-rs-gaap, type-subtype)
    this is the primary browse view — the arcs ARE what the taxonomy
    contributes, not concepts. Each entry includes the from/to element
    qname + name so the UI can render the arc directly.
    """
    _validate_pagination(limit, offset)
    with _open_session(info) as session:
      rows = list_taxonomy_arcs(
        session,
        taxonomy_id=str(taxonomy_id),
        association_type=association_type,
        limit=limit,
        offset=offset,
      )
      return [LibraryAssociation.from_pydantic(r) for r in rows]

  @strawberry.field
  def library_taxonomy_arc_count(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_id: strawberry.ID,
  ) -> int:
    """Count of arcs contributed by a taxonomy."""
    with _open_session(info) as session:
      return count_taxonomy_arcs(session, str(taxonomy_id))

  # ── Elements ────────────────────────────────────────────────────────────

  @strawberry.field
  def library_elements(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_id: strawberry.ID | None = None,
    source: str | None = None,
    classification: str | None = None,
    element_type: str | None = None,
    is_abstract: bool | None = None,
    limit: int = 50,
    offset: int = 0,
    include_labels: bool = False,
    include_references: bool = False,
  ) -> list[LibraryElement]:
    """List library elements with filters + pagination.

    `classification` filters on the FASB elementsOfFinancialStatements axis.
    `isAbstract=true` → abstract only; `false` → concrete only; omit for both.
    """
    _validate_pagination(limit, offset)
    with _open_session(info) as session:
      rows = list_elements(
        session,
        taxonomy_id=str(taxonomy_id) if taxonomy_id else None,
        source=source,
        classification=classification,
        element_type=element_type,
        is_abstract=is_abstract,
        limit=limit,
        offset=offset,
        include_labels=include_labels,
        include_references=include_references,
      )
      return [LibraryElement.from_pydantic(r) for r in rows]

  @strawberry.field
  def library_element(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID | None = None,
    qname: str | None = None,
  ) -> LibraryElement | None:
    """Get a single element by id or by qname ('sfac6:Assets', etc)."""
    with _open_session(info) as session:
      if id is not None:
        row = get_element(session, element_id=str(id))
      elif qname is not None:
        row = get_element_by_qname(session, qname)
      else:
        row = None
      return LibraryElement.from_pydantic(row) if row else None

  @strawberry.field
  def search_library_elements(
    self,
    info: Info[GraphQLContext, None],
    query: str,
    source: str | None = None,
    limit: int = 50,
  ) -> list[LibraryElement]:
    """Substring search across qname, name, and standard label text."""
    _validate_pagination(limit, 0)
    with _open_session(info) as session:
      rows = search_elements(session, query_text=query, limit=limit, source=source)
      return [LibraryElement.from_pydantic(r) for r in rows]

  @strawberry.field
  def library_element_tree(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
    max_depth: int = 5,
  ) -> LibraryElementTreeNode | None:
    """Walk presentation arcs down from an element."""
    if max_depth < 1 or max_depth > 10:
      raise strawberry.exceptions.StrawberryGraphQLError(
        message="max_depth must be between 1 and 10",
        extensions={"code": "INVALID_ARGUMENT"},
      )
    with _open_session(info) as session:
      node = get_element_tree(session, element_id=str(id), max_depth=max_depth)
      return LibraryElementTreeNode.from_pydantic(node) if node else None

  @strawberry.field
  def library_element_equivalents(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
  ) -> LibraryEquivalence | None:
    """Return the equivalence fan-out (FAC ↔ us-gaap collapse)."""
    with _open_session(info) as session:
      row = get_element_equivalents(session, element_id=str(id))
      return LibraryEquivalence.from_pydantic(row) if row else None

  @strawberry.field
  def library_element_arcs(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
  ) -> list[LibraryElementArc]:
    """Return all mapping arcs where this element is source or target.

    Covers every `taxonomy_type='mapping'` bridge — equivalence,
    general-special, type-subtype. Each row is oriented from the
    element's perspective (`direction` = outgoing | incoming).
    """
    with _open_session(info) as session:
      rows = get_element_arcs(session, element_id=str(id))
      return [LibraryElementArc.from_pydantic(r) for r in rows]

  @strawberry.field
  def library_element_classifications(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
  ) -> list[LibraryElementClassification]:
    """All classification traits assigned to this element, grouped by category."""
    with _open_session(info) as session:
      rows = get_element_classifications(session, element_id=str(id))
      return [LibraryElementClassification.from_pydantic(r) for r in rows]

  # ── Structures ─────────────────────────────────────────────────────────

  @strawberry.field
  def library_structures(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_id: strawberry.ID | None = None,
    structure_type: str | None = None,
  ) -> list[LibraryStructure]:
    """List structures (extended link roles) — BS, IS, custom, etc."""
    with _open_session(info) as session:
      rows = list_structures(
        session,
        taxonomy_id=str(taxonomy_id) if taxonomy_id else None,
        structure_type=structure_type,
      )
      return [LibraryStructure.from_pydantic(r) for r in rows]

  @strawberry.field
  def library_structure(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
  ) -> LibraryStructure | None:
    with _open_session(info) as session:
      row = get_structure(session, structure_id=str(id))
      return LibraryStructure.from_pydantic(row) if row else None


__all__ = [
  "LibraryAssociation",
  "LibraryElementArc",
  "LibraryElementClassification",
  "LibraryQuery",
]
