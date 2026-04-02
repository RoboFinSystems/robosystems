"""Ledger publish list endpoints — manage report distribution lists."""

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.publish_lists import (
  AddMembersRequest,
  CreatePublishListRequest,
  PublishListDetailResponse,
  PublishListListResponse,
  PublishListMemberResponse,
  PublishListResponse,
  UpdatePublishListRequest,
)
from robosystems.models.core import User
from robosystems.models.extensions import PublishList, PublishListMember

router = APIRouter()


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


def _enrich_members(
  members: list[PublishListMember],
) -> list[PublishListMemberResponse]:
  """Enrich members with graph and org names from the platform DB."""
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


# ── Publish List CRUD ──────────────────────────────────────────────────────


@router.post(
  "/publish-lists",
  response_model=PublishListResponse,
  operation_id="createPublishList",
  summary="Create Publish List",
  tags=["Ledger"],
  status_code=201,
)
async def create_publish_list(
  body: CreatePublishListRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      publish_list = PublishList(
        name=body.name,
        description=body.description,
        created_by=current_user.id,
      )
      session.add(publish_list)
      try:
        session.flush()
      except IntegrityError:
        raise HTTPException(
          status_code=409, detail=f"A publish list named '{body.name}' already exists."
        )
      return _list_to_response(publish_list, member_count=0)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Ledger module not initialized.")


@router.get(
  "/publish-lists",
  response_model=PublishListListResponse,
  operation_id="listPublishLists",
  summary="List Publish Lists",
  tags=["Ledger"],
)
async def list_publish_lists(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      total = (
        session.execute(select(func.count()).select_from(PublishList)).scalar() or 0
      )

      rows = (
        session.execute(
          select(PublishList).order_by(PublishList.name).offset(offset).limit(limit)
        )
        .scalars()
        .all()
      )

      # Batch-load member counts
      if rows:
        list_ids = [r.id for r in rows]
        counts = dict(
          session.execute(
            select(
              PublishListMember.publish_list_id,
              func.count(),
            )
            .where(PublishListMember.publish_list_id.in_(list_ids))
            .group_by(PublishListMember.publish_list_id)
          ).all()
        )
      else:
        counts = {}

      return PublishListListResponse(
        publish_lists=[
          _list_to_response(r, member_count=counts.get(r.id, 0)) for r in rows
        ],
        pagination=create_pagination_info(total, limit, offset),
      )
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Ledger module not initialized.")


@router.get(
  "/publish-lists/{list_id}",
  response_model=PublishListDetailResponse,
  operation_id="getPublishList",
  summary="Get Publish List",
  tags=["Ledger"],
)
async def get_publish_list(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  list_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(PublishList).where(PublishList.id == list_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Publish list not found.")

      members = (
        session.execute(
          select(PublishListMember).where(PublishListMember.publish_list_id == list_id)
        )
        .scalars()
        .all()
      )

      enriched = _enrich_members(members)

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
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Ledger module not initialized.")


@router.patch(
  "/publish-lists/{list_id}",
  response_model=PublishListResponse,
  operation_id="updatePublishList",
  summary="Update Publish List",
  tags=["Ledger"],
)
async def update_publish_list(
  body: UpdatePublishListRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  list_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(PublishList).where(PublishList.id == list_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Publish list not found.")
      if row.created_by != current_user.id:
        raise HTTPException(
          status_code=403, detail="Not authorized to modify this publish list."
        )

      updates = body.model_dump(exclude_unset=True)
      for field, value in updates.items():
        setattr(row, field, value)

      try:
        session.flush()
      except IntegrityError:
        raise HTTPException(
          status_code=409,
          detail=f"A publish list named '{body.name or row.name}' already exists.",
        )

      member_count = (
        session.execute(
          select(func.count())
          .select_from(PublishListMember)
          .where(PublishListMember.publish_list_id == list_id)
        ).scalar()
        or 0
      )

      return _list_to_response(row, member_count=member_count)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Ledger module not initialized.")


@router.delete(
  "/publish-lists/{list_id}",
  status_code=204,
  operation_id="deletePublishList",
  summary="Delete Publish List",
  tags=["Ledger"],
)
async def delete_publish_list(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  list_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(PublishList).where(PublishList.id == list_id)
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Publish list not found.")
      if row.created_by != current_user.id:
        raise HTTPException(
          status_code=403, detail="Not authorized to delete this publish list."
        )
      session.delete(row)  # CASCADE deletes members
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Ledger module not initialized.")


# ── Member Management ──────────────────────────────────────────────────────


@router.post(
  "/publish-lists/{list_id}/members",
  response_model=list[PublishListMemberResponse],
  operation_id="addPublishListMembers",
  summary="Add Members to Publish List",
  tags=["Ledger"],
  status_code=201,
)
async def add_members(
  body: AddMembersRequest,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  list_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      # Verify list exists
      publish_list = session.execute(
        select(PublishList).where(PublishList.id == list_id)
      ).scalar_one_or_none()
      if not publish_list:
        raise HTTPException(status_code=404, detail="Publish list not found.")

      # Validate target graphs exist and have roboledger extension
      from robosystems.db.platform import SessionFactory
      from robosystems.models.core import Graph

      with SessionFactory() as platform_session:
        graphs = (
          platform_session.execute(
            select(Graph).where(Graph.graph_id.in_(body.target_graph_ids))
          )
          .scalars()
          .all()
        )
        found_ids = {str(g.graph_id) for g in graphs}

        missing = set(body.target_graph_ids) - found_ids
        if missing:
          raise HTTPException(
            status_code=404,
            detail=f"Graph(s) not found: {', '.join(sorted(missing))}",
          )

        for g in graphs:
          extensions = g.schema_extensions or []
          if "roboledger" not in extensions:
            raise HTTPException(
              status_code=422,
              detail=f"Graph '{g.graph_id}' does not have the roboledger extension.",
            )

      # Prevent adding self
      if graph_id in body.target_graph_ids:
        raise HTTPException(
          status_code=422, detail="Cannot add your own graph to a publish list."
        )

      # Pre-check for duplicates
      existing = (
        session.execute(
          select(PublishListMember.target_graph_id).where(
            PublishListMember.publish_list_id == list_id,
            PublishListMember.target_graph_id.in_(body.target_graph_ids),
          )
        )
        .scalars()
        .all()
      )
      if existing:
        raise HTTPException(
          status_code=409,
          detail=f"Already in this list: {', '.join(existing)}",
        )

      added = []
      for target_id in body.target_graph_ids:
        member = PublishListMember(
          publish_list_id=list_id,
          target_graph_id=target_id,
          added_by=current_user.id,
        )
        session.add(member)
        added.append(member)
      session.flush()

      return _enrich_members(added)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Ledger module not initialized.")


@router.delete(
  "/publish-lists/{list_id}/members/{member_id}",
  status_code=204,
  operation_id="removePublishListMember",
  summary="Remove Member from Publish List",
  tags=["Ledger"],
)
async def remove_member(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  list_id: str = Path(...),
  member_id: str = Path(...),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      row = session.execute(
        select(PublishListMember).where(
          PublishListMember.id == member_id,
          PublishListMember.publish_list_id == list_id,
        )
      ).scalar_one_or_none()
      if not row:
        raise HTTPException(status_code=404, detail="Member not found in this list.")
      session.delete(row)
  except ProgrammingError:
    raise HTTPException(status_code=404, detail="Ledger module not initialized.")
