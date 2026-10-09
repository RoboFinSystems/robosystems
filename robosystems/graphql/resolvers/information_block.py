"""Information Block GraphQL resolver: cross-domain, always composed, read-only.

Uses `open_library_session`, so it serves both the `library` sentinel and
tenant graphs via the session `search_path`.
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
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError


@strawberry.type
class InformationBlockQuery:
  """Query root for the Information Block read surface.

  Both fields open a library-session (`open_library_session`) so they
  work on the library sentinel AND on per-graph tenant endpoints. On a
  tenant graph_id the session's `search_path` includes the tenant
  schema, so tenant-created blocks surface. On the library sentinel,
  only block types with `surfaces_in_library=True` appear — Schedule
  is tenant-only, so it returns [] on the sentinel.
  """

  @strawberry.field
  def information_block(
    self,
    info: Info[GraphQLContext, None],
    id: strawberry.ID,
    scenario_id: str | None = None,
    # Nullable for codegen clients that send explicit null (see resolve_pagination).
    series: bool | None = None,
    series_history: int | None = None,
    series_forecast: int | None = None,
    entity_id: str | None = None,
  ) -> InformationBlock | None:
    """Fetch a single Information Block envelope by id.

    `scenarioId` selects the FactSet slice: omitted = actuals; a
    forecast block's structure id = that scenario's parallel universe
    (statement envelopes bind its latest computed month, metric
    envelopes extend the series with its forward columns, labeled
    "(forecast)").

    `series` renders a statement block as its whole report-set time
    series — one column per period, actuals-preferred at the seam when
    combined with `scenarioId`; forecast columns carry
    `periods[].forecast = true`. Non-statement block types ignore it.

    `seriesHistory` / `seriesForecast` window the series to its
    seam-adjacent columns — the last N actual columns and the first N
    forecast columns; omitted = unbounded. Pass the visible window so
    the envelope scales with the screen, not the ledger's age.

    `entityId` picks whose books a block shared by the group reads
    (statements, metrics, disclosures); omitted = the scenario's entity,
    else the group parent. A schedule, reconciliation or forecast is one
    entity's own and always reads its owner's. `entityId` on the envelope
    names the entity it read.

    Args:
      id: The block's structure id.
      scenario_id: A forecast block's structure id. Omit for actuals.
      series: Render a statement block as its whole report-set time series, one column
        per period. Ignored by non-statement block types.
      series_history: Cap the series at the last N actual columns. Omit for unbounded.
      series_forecast: Cap the series at the first N forecast columns. Omit for
        unbounded.
      entity_id: The entity whose books a shared block reads. Omit for the group
        parent.
    """
    graph_id = require_graph_id(info)
    try:
      with _open_session(info) as session:
        envelope = get_information_block(
          session,
          str(id),
          scenario_id=scenario_id,
          series=bool(series),
          series_history=series_history,
          series_forecast=series_forecast,
          entity_id=entity_id,
          library_sentinel=(graph_id == LIBRARY_GRAPH_ID),
        )
    except EntityNotInGraphError as exc:
      raise strawberry.exceptions.StrawberryGraphQLError(str(exc)) from exc
    return InformationBlock.from_pydantic(envelope) if envelope else None

  @strawberry.field
  def information_blocks(
    self,
    info: Info[GraphQLContext, None],
    block_type: str | None = None,
    category: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
    scenario_id: str | None = None,
    entity_id: str | None = None,
  ) -> list[InformationBlock]:
    """List Information Blocks with optional block_type + category filters.

    `blockType` filters to one registered block type (e.g.
    `'schedule'`). `category` filters on the registry entry's
    category label ('Close', 'Reporting', …). Both combine as AND.
    `scenarioId` threads into each envelope's FactSet binding (the
    Structure list itself is scenario-independent).

    `entityId` (omitted = the scenario's entity, else the group parent)
    lists the blocks shared by the group plus that entity's own schedules,
    reconciliations and forecasts, never another entity's; shared blocks
    read its books.

    Args:
      block_type: Filter to one registered block type, e.g. `schedule`.
      category: Filter on the registry entry's category label, e.g. `Close` or
        `Reporting`.
      scenario_id: A forecast block's structure id, threaded into each envelope's
        FactSet binding.
      entity_id: The entity whose blocks and books to list. Omit for the group
        parent.
    """
    limit, offset = _resolve_pagination(limit, offset, default_limit=50)
    graph_id = require_graph_id(info)
    try:
      with _open_session(info) as session:
        rows = list_information_blocks(
          session,
          block_type=block_type,
          category=category,
          limit=limit,
          offset=offset,
          library_sentinel=(graph_id == LIBRARY_GRAPH_ID),
          scenario_id=scenario_id,
          entity_id=entity_id,
        )
    except EntityNotInGraphError as exc:
      raise strawberry.exceptions.StrawberryGraphQLError(str(exc)) from exc
    return [InformationBlock.from_pydantic(r) for r in rows]


__all__ = [
  "InformationBlockQuery",
]
