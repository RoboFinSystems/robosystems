"""Organization management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.api.common import AUTHENTICATED_ERROR_RESPONSES, RESOURCE_ERROR_RESPONSES
from ...models.api.orgs import (
  OrgDetailResponse,
  OrgListResponse,
  OrgResponse,
  UpdateOrgRequest,
)
from ...models.core import Graph, GraphUser, OrgRole, OrgUser, User

logger = get_logger(__name__)

router = APIRouter(tags=["Org"])


def _visible_org_graphs(
  org_id: str, membership: OrgUser, user_id: str, db: Session
) -> list[Graph]:
  """Org graphs this user may see.

  Org membership alone grants no graph access — a plain member needs an
  explicit `GraphUser` grant — so listing every org graph to any member
  discloses the names, tiers and IDs of graphs they cannot reach. Owners and
  admins are implicit admins on every org-owned graph, so for them the visible
  set is the whole org.

  Mirrors the resolution `GraphUser.get_effective_role` performs per graph,
  applied as a single query rather than per-row.
  """
  query = db.query(Graph).filter(Graph.org_id == org_id)

  if membership.role in (OrgRole.OWNER, OrgRole.ADMIN):
    return query.all()

  granted = [gu.graph_id for gu in GraphUser.get_by_user_id(user_id, db)]
  if not granted:
    return []

  return query.filter(Graph.graph_id.in_(granted)).all()


@router.get(
  "/orgs",
  response_model=OrgListResponse,
  summary="List User's Organizations",
  operation_id="listUserOrgs",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
async def list_user_orgs(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgListResponse:
  try:
    # Get all org memberships for the user
    org_memberships = OrgUser.get_user_orgs(current_user.id, db)

    orgs = []
    for membership in org_memberships:
      org = membership.org
      # Count members and graphs for each org. The graph count matches what the
      # org detail view will actually list — counting every org graph here left
      # a member reading "2 graphs" on the org card and then finding an empty
      # list, since membership alone grants no graph access. Member count stays
      # org-wide: the roster is visible to everyone in the org.
      member_count = len(OrgUser.get_org_users(org.id, db))
      graph_count = len(_visible_org_graphs(org.id, membership, current_user.id, db))

      orgs.append(
        OrgResponse(
          id=org.id,
          name=org.name,
          org_type=org.org_type,
          role=membership.role,
          member_count=member_count,
          graph_count=graph_count,
          created_at=org.created_at,
          joined_at=membership.joined_at,
        )
      )

    return OrgListResponse(
      orgs=orgs,
      total=len(orgs),
    )

  except Exception as e:
    logger.error(f"Error listing user organizations: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list organizations",
    )


@router.get(
  "/orgs/{org_id}",
  response_model=OrgDetailResponse,
  summary="Get Organization",
  operation_id="getOrg",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def get_org(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgDetailResponse:
  try:
    # Check if user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    org = membership.org

    # Get all members
    memberships = OrgUser.get_org_users(org_id, db)
    members = []
    for m in memberships:
      user = m.user
      members.append(
        {
          "user_id": user.id,
          "name": user.name,
          "email": user.email,
          "role": m.role,
          "joined_at": m.joined_at,
        }
      )

    # Get org limits
    from ...models.core import OrgLimits

    limits = OrgLimits.get_by_org_id(org_id, db)

    # Get graphs
    graphs = _visible_org_graphs(org_id, membership, current_user.id, db)
    graph_list = [
      {
        "graph_id": g.graph_id,
        "graph_name": g.graph_name,
        "graph_type": g.graph_type,
        "graph_tier": g.graph_tier,
        "created_at": g.created_at,
      }
      for g in graphs
    ]

    return OrgDetailResponse(
      id=org.id,
      name=org.name,
      org_type=org.org_type,
      user_role=membership.role,
      members=members,
      graphs=graph_list,
      limits={
        "max_graphs": limits.max_graphs if limits else None,
      }
      if limits
      else None,
      created_at=org.created_at,
      updated_at=org.updated_at,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error getting organization {org_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get organization",
    )


@router.put(
  "/orgs/{org_id}",
  response_model=OrgDetailResponse,
  summary="Update Organization",
  description="Requires admin or owner role. Only owners can change the org type.",
  operation_id="updateOrg",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def update_org(
  org_id: str,
  request: UpdateOrgRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgDetailResponse:
  try:
    # Check if user is an admin or owner of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    if membership.role not in [OrgRole.ADMIN, OrgRole.OWNER]:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Only admins and owners can update organization details",
      )

    org = membership.org

    # Update fields if provided
    if request.name is not None:
      org.name = request.name

    if request.org_type is not None and membership.role == OrgRole.OWNER:
      # Only owners can change org type
      org.org_type = request.org_type

    db.commit()
    db.refresh(org)

    return await get_org(org_id, current_user, db)

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error updating organization {org_id}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to update organization",
    )


@router.get(
  "/orgs/{org_id}/graphs",
  response_model=list[dict],
  summary="List Organization Graphs",
  operation_id="listOrgGraphs",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_org_graphs(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> list[dict]:
  try:
    # Check if user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    # Graphs this user may see — not every graph the org owns (see helper).
    graphs = _visible_org_graphs(org_id, membership, current_user.id, db)

    result = []
    for graph in graphs:
      # Get graph credits info
      from ...models.core import GraphCredits

      credits = GraphCredits.get_by_graph_id(graph.graph_id, db)
      usage = credits.get_usage_summary(db) if credits else None

      result.append(
        {
          "graph_id": graph.graph_id,
          "graph_name": graph.graph_name,
          "graph_type": graph.graph_type,
          "graph_tier": graph.graph_tier,
          "credits_available": usage["current_balance"] if usage else 0,
          "credits_used": usage["consumed_this_month"] if usage else 0,
          "created_at": graph.created_at,
          "updated_at": graph.updated_at,
        }
      )

    return result

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error listing organization graphs: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list organization graphs",
    )
