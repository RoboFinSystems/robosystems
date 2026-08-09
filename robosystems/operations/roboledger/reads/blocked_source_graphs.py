"""Read operations for the cross-graph share block list."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.blocked_source_graphs import (
  BlockedSourceGraphListResponse,
  BlockedSourceGraphResponse,
)
from robosystems.models.extensions import BlockedSourceGraph


def _to_response(
  row: BlockedSourceGraph, graph_name: str | None = None
) -> BlockedSourceGraphResponse:
  return BlockedSourceGraphResponse(
    id=row.id,
    source_graph_id=row.source_graph_id,
    source_graph_name=graph_name,
    blocked_by=row.blocked_by,
    blocked_at=row.blocked_at,
    reason=row.reason,
  )


def enrich_blocks(
  rows: list[BlockedSourceGraph],
) -> list[BlockedSourceGraphResponse]:
  """Attach graph display names from the platform DB.

  Name only, deliberately — unlike publish-list members this does not resolve
  the owning org. A block is a record the recipient keeps about a sender they
  already know; there is no reason for it to widen what they can learn.
  """
  if not rows:
    return []

  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph

  graph_ids = {r.source_graph_id for r in rows}
  names: dict[str, str | None] = {}
  with SessionFactory() as platform_session:
    for graph_id, graph_name in platform_session.execute(
      select(Graph.graph_id, Graph.graph_name).where(Graph.graph_id.in_(graph_ids))
    ).all():
      names[graph_id] = graph_name

  return [_to_response(r, graph_name=names.get(r.source_graph_id)) for r in rows]


def list_blocked_source_graphs(
  session: Session, *, limit: int = 100, offset: int = 0
) -> BlockedSourceGraphListResponse:
  """List every source graph blocked from sharing into this graph."""
  total = (
    session.execute(select(func.count()).select_from(BlockedSourceGraph)).scalar() or 0
  )
  rows = (
    session.execute(
      select(BlockedSourceGraph)
      .order_by(BlockedSourceGraph.blocked_at.desc())
      .offset(offset)
      .limit(limit)
    )
    .scalars()
    .all()
  )

  return BlockedSourceGraphListResponse(
    blocked_source_graphs=enrich_blocks(list(rows)),
    pagination=create_pagination_info(total, limit, offset),
  )


def is_source_blocked(session: Session, source_graph_id: str) -> bool:
  """Whether this graph has blocked ``source_graph_id`` from sharing in.

  The enforcement predicate — called from ``share_report``'s per-target copy
  against the *target* tenant's session, before anything is written.
  """
  return (
    session.execute(
      select(BlockedSourceGraph.id).where(
        BlockedSourceGraph.source_graph_id == source_graph_id
      )
    ).scalar_one_or_none()
    is not None
  )
