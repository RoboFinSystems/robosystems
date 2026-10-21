"""Read operations for the sender's record of cross-graph report shares."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.report_shares import (
  ReportShareListResponse,
  ReportShareResponse,
)
from robosystems.models.extensions import ReportShare


def _to_response(
  row: ReportShare, graph_name: str | None = None
) -> ReportShareResponse:
  return ReportShareResponse(
    id=row.id,
    report_id=row.report_id,
    target_graph_id=row.target_graph_id,
    target_graph_name=graph_name,
    shared_by=row.shared_by,
    shared_at=row.shared_at,
    revoked_at=row.revoked_at,
    fact_count=row.fact_count,
  )


def enrich_shares(rows: list[ReportShare]) -> list[ReportShareResponse]:
  """Attach recipient graph display names from the platform DB.

  Name only: the recipient's owning org is the publish list's business, not
  the share record's.
  """
  if not rows:
    return []

  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph

  graph_ids = {r.target_graph_id for r in rows}
  names: dict[str, str | None] = {}
  with SessionFactory() as platform_session:
    for graph_id, graph_name in platform_session.execute(
      select(Graph.graph_id, Graph.graph_name).where(Graph.graph_id.in_(graph_ids))
    ).all():
      names[graph_id] = graph_name

  return [_to_response(r, graph_name=names.get(r.target_graph_id)) for r in rows]


def list_report_shares(
  session: Session,
  *,
  report_id: str | None = None,
  include_revoked: bool = False,
  limit: int = 100,
  offset: int = 0,
) -> ReportShareListResponse:
  """List where this graph's reports have been shared, newest first.

  ``report_id`` narrows to one report; without it the list is the graph's
  whole distribution log. Revoked rows are history and stay out unless
  ``include_revoked`` asks for them: the revoke picker wants the copies still
  out there, an audit view wants everything.
  """
  conditions = []
  if report_id is not None:
    conditions.append(ReportShare.report_id == report_id)
  if not include_revoked:
    conditions.append(ReportShare.revoked_at.is_(None))

  total = (
    session.execute(
      select(func.count()).select_from(ReportShare).where(*conditions)
    ).scalar()
    or 0
  )
  rows = (
    session.execute(
      select(ReportShare)
      .where(*conditions)
      .order_by(ReportShare.shared_at.desc(), ReportShare.id.desc())
      .offset(offset)
      .limit(limit)
    )
    .scalars()
    .all()
  )

  return ReportShareListResponse(
    report_shares=enrich_shares(list(rows)),
    pagination=create_pagination_info(total, limit, offset),
  )
