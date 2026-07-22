"""Information Block GraphQL resolver — cross-domain, always-on.

Mirrors the :class:`LibraryQuery` pattern: always composed into the
Query root regardless of which per-domain extension flags are on.
Uses :func:`open_library_session` so the endpoint works on both the
library sentinel (``graph_id='library'``) and any tenant graph_id —
reads are driven by the session's ``search_path``, no per-graph
extension gate needed.

Writes stay on ``POST /extensions/roboledger/{g}/operations/create-information-block``
where the existing registrar pattern mounts them; this module is
read-only.
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
  resolve_pagination as _resolve_pagination,
)
from robosystems.graphql.types.information_block import InformationBlock
from robosystems.operations.information_block import (
  get_information_block,
  list_information_blocks,
)


@strawberry.type
class InformationBlockQuery:
  """Query root for the Information Block read surface.

  Both fields open a library-session (``open_library_session``) so they
  work on the library sentinel AND on per-graph tenant endpoints. On a
  tenant graph_id the session's ``search_path`` includes the tenant
  schema, so tenant-created blocks surface. On the library sentinel,
  only block types with ``surfaces_in_library=True`` appear — Schedule
  is tenant-only, so it returns [] on the sentinel.
  """

  @strawberry.field
  def information_block(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
  ) -> InformationBlock | None:
    """Fetch a single Information Block envelope by id."""
    with _open_session(info) as session:
      envelope = get_information_block(session, str(id))
      return InformationBlock.from_pydantic(envelope) if envelope else None

  @strawberry.field
  def information_blocks(
    self,
    info: Info[GraphQLContext, None],
    block_type: str | None = None,
    category: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
  ) -> list[InformationBlock]:
    """List Information Blocks with optional block_type + category filters.

    ``blockType`` filters to one registered block type (e.g.
    ``'schedule'``). ``category`` filters on the registry entry's
    category label ('Close', 'Reporting', …). Both combine as AND.
    """
    limit, offset = _resolve_pagination(limit, offset, default_limit=50)
    graph_id = require_graph_id(info)
    with _open_session(info) as session:
      rows = list_information_blocks(
        session,
        block_type=block_type,
        category=category,
        limit=limit,
        offset=offset,
        library_sentinel=(graph_id == LIBRARY_GRAPH_ID),
      )
      return [InformationBlock.from_pydantic(r) for r in rows]


__all__ = [
  "InformationBlockQuery",
]
