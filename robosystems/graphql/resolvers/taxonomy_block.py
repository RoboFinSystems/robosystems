"""Taxonomy Block GraphQL resolver — cross-domain, always-on.

Mirrors :class:`InformationBlockQuery`. Always composed into the Query
root regardless of per-domain extension flags. Uses
:func:`open_library_session` so the endpoint works on both the library
sentinel (``graph_id='library'``) and any tenant graph_id — reads are
driven by the session's ``search_path``, no per-graph extension gate
needed.

Writes stay on the ``POST /extensions/roboledger/{g}/operations/
create-taxonomy-block`` surface mounted by the registrar; this module
is read-only.
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
from robosystems.graphql.types.taxonomy_block import TaxonomyBlock
from robosystems.operations.taxonomy_block.reads import (
  get_taxonomy_block,
  list_taxonomy_blocks,
)


@strawberry.type
class TaxonomyBlockQuery:
  """Query root for the Taxonomy Block read surface.

  Both fields open a library-session so they work on the library
  sentinel AND on per-graph tenant endpoints. On a tenant graph_id the
  session's ``search_path`` includes the tenant schema, so
  tenant-created blocks surface alongside chained library rows. On the
  library sentinel, only block types with ``surfaces_in_library=True``
  appear — today that's just ``reporting_standard``.
  """

  @strawberry.field
  def taxonomy_block(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
  ) -> TaxonomyBlock | None:
    """Fetch a single Taxonomy Block envelope by id."""
    with _open_session(info) as session:
      envelope = get_taxonomy_block(session, str(id))
      return TaxonomyBlock.from_pydantic(envelope) if envelope else None

  @strawberry.field
  def taxonomy_blocks(
    self,
    info: Info[GraphQLContext, None],
    taxonomy_type: str | None = None,
    parent_taxonomy_id: strawberry.ID | None = None,
    category: str | None = None,
    limit: int = 50,
    offset: int = 0,
  ) -> list[TaxonomyBlock]:
    """List Taxonomy Blocks with optional filters.

    ``taxonomyType`` narrows to one registered type (e.g.
    ``'chart_of_accounts'``). ``parentTaxonomyId`` narrows to extensions
    of a specific library taxonomy. ``category`` filters on the registry
    entry's category label (``'Chart'``, ``'Reporting'``, ``'Library'``,
    ``'Custom'``, ``'Close'``). Filters combine as AND.
    """
    _validate_pagination(limit, offset)
    graph_id = require_graph_id(info)
    with _open_session(info) as session:
      rows = list_taxonomy_blocks(
        session,
        taxonomy_type=taxonomy_type,
        parent_taxonomy_id=str(parent_taxonomy_id)
        if parent_taxonomy_id is not None
        else None,
        category=category,
        limit=limit,
        offset=offset,
        library_sentinel=(graph_id == LIBRARY_GRAPH_ID),
      )
      return [TaxonomyBlock.from_pydantic(r) for r in rows]


__all__ = [
  "TaxonomyBlockQuery",
]
