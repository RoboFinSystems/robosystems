"""Read operations for publish lists (report distribution lists)."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.publish_lists import (
  PublishListDetailResponse,
  PublishListListResponse,
  PublishListMemberResponse,
  PublishListResponse,
)
from robosystems.models.extensions import PublishList, PublishListMember


def _list_to_response(row: PublishList, member_count: int = 0) -> PublishListResponse:
  return PublishListResponse(
    id=row.id,
    name=row.name,
    description=row.description,
    member_count=member_count,
    created_by=row.created_by,
    created_at=row.created_at,
    updated_at=row.updated_at,
  )


def _member_to_response(
  row: PublishListMember,
  graph_name: str | None = None,
  org_name: str | None = None,
) -> PublishListMemberResponse:
  return PublishListMemberResponse(
    id=row.id,
    target_graph_id=row.target_graph_id,
    target_graph_name=graph_name,
    target_org_name=org_name,
    added_by=row.added_by,
    added_at=row.added_at,
  )


def enrich_members(
  members: list[PublishListMember],
) -> list[PublishListMemberResponse]:
  """Enrich member rows with graph and org names from the platform DB."""
  if not members:
    return []

  graph_ids = {m.target_graph_id for m in members}

  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph
  from robosystems.models.core.org import Org

  graph_map: dict[str, tuple[str | None, str | None]] = {}
  with SessionFactory() as platform_session:
    rows = platform_session.execute(
      select(Graph.graph_id, Graph.graph_name, Org.name)
      .outerjoin(Org, Graph.org_id == Org.id)
      .where(Graph.graph_id.in_(graph_ids))
    ).all()
    for graph_id, graph_name, org_name in rows:
      graph_map[graph_id] = (graph_name, org_name)

  return [
    _member_to_response(
      m,
      graph_name=graph_map.get(m.target_graph_id, (None, None))[0],
      org_name=graph_map.get(m.target_graph_id, (None, None))[1],
    )
    for m in members
  ]


def list_publish_lists(
  session: Session, *, limit: int = 100, offset: int = 0
) -> PublishListListResponse:
  """List all publish lists, paginated."""
  total = session.execute(select(func.count()).select_from(PublishList)).scalar() or 0
  rows = (
    session.execute(
      select(PublishList).order_by(PublishList.name).offset(offset).limit(limit)
    )
    .scalars()
    .all()
  )

  counts: dict[str, int] = {}
  if rows:
    list_ids = [r.id for r in rows]
    count_rows = session.execute(
      select(
        PublishListMember.publish_list_id,
        func.count(),
      )
      .where(PublishListMember.publish_list_id.in_(list_ids))
      .group_by(PublishListMember.publish_list_id)
    ).all()
    counts = {str(list_id): int(count) for list_id, count in count_rows}

  return PublishListListResponse(
    publish_lists=[
      _list_to_response(r, member_count=counts.get(r.id, 0)) for r in rows
    ],
    pagination=create_pagination_info(total, limit, offset),
  )


def get_publish_list(
  session: Session, list_id: str
) -> PublishListDetailResponse | None:
  """Return a publish list with enriched members, or None if not found."""
  row = session.execute(
    select(PublishList).where(PublishList.id == list_id)
  ).scalar_one_or_none()
  if row is None:
    return None

  members = (
    session.execute(
      select(PublishListMember).where(PublishListMember.publish_list_id == list_id)
    )
    .scalars()
    .all()
  )
  enriched = enrich_members(members)

  return PublishListDetailResponse(
    id=row.id,
    name=row.name,
    description=row.description,
    member_count=len(enriched),
    created_by=row.created_by,
    created_at=row.created_at,
    updated_at=row.updated_at,
    members=enriched,
  )
