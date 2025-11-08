"""Organization management endpoints."""

from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.iam import User, Org, OrgUser, OrgRole, Graph
from ...models.api.orgs import (
  OrgResponse,
  CreateOrgRequest,
  UpdateOrgRequest,
  OrgListResponse,
  OrgDetailResponse,
)
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Org"])


@router.get(
  "/orgs",
  response_model=OrgListResponse,
  summary="List User's Organizations",
  description="Get all organizations the current user belongs to, with their role in each.",
  operation_id="listUserOrgs",
)
async def list_user_orgs(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgListResponse:
  """List all organizations the user belongs to."""
  try:
    # Get all org memberships for the user
    org_memberships = OrgUser.get_user_orgs(current_user.id, db)

    orgs = []
    for membership in org_memberships:
      org = membership.org
      # Count members and graphs for each org
      member_count = len(OrgUser.get_org_users(org.id, db))
      graph_count = db.query(Graph).filter(Graph.org_id == org.id).count()

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
    logger.error(f"Error listing user organizations: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list organizations",
    )


@router.post(
  "/orgs",
  response_model=OrgDetailResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Create Organization",
  description="Create a new organization. The creating user becomes the owner.",
  operation_id="createOrg",
)
async def create_org(
  request: CreateOrgRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgDetailResponse:
  """Create a new organization."""
  try:
    # Create the organization
    org = Org.create(
      name=request.name,
      org_type=request.org_type,
      session=db,
      auto_commit=False,
    )

    # Add the creator as owner
    OrgUser.create(
      org_id=org.id,
      user_id=current_user.id,
      role=OrgRole.OWNER,
      session=db,
      auto_commit=False,
    )

    # Create org limits with defaults
    from ...models.iam import OrgLimits

    OrgLimits.create_default_limits(
      org_id=org.id,
      session=db,
    )

    db.commit()
    db.refresh(org)

    # Get the created org with details
    return await get_org(org.id, current_user, db)

  except Exception as e:
    db.rollback()
    logger.error(f"Error creating organization: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create organization",
    )


@router.get(
  "/orgs/{org_id}",
  response_model=OrgDetailResponse,
  summary="Get Organization",
  description="Get detailed information about an organization.",
  operation_id="getOrg",
)
async def get_org(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgDetailResponse:
  """Get organization details."""
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
    from ...models.iam import OrgLimits

    limits = OrgLimits.get_by_org_id(org_id, db)

    # Get graphs
    graphs = db.query(Graph).filter(Graph.org_id == org_id).all()
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
    logger.error(f"Error getting organization {org_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get organization",
    )


@router.put(
  "/orgs/{org_id}",
  response_model=OrgDetailResponse,
  summary="Update Organization",
  description="Update organization information. Requires admin or owner role.",
  operation_id="updateOrg",
)
async def update_org(
  org_id: str,
  request: UpdateOrgRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgDetailResponse:
  """Update organization details."""
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
    logger.error(f"Error updating organization {org_id}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to update organization",
    )


@router.get(
  "/orgs/{org_id}/graphs",
  response_model=List[dict],
  summary="List Organization Graphs",
  description="Get all graphs belonging to an organization.",
  operation_id="listOrgGraphs",
)
async def list_org_graphs(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> List[dict]:
  """List all graphs in an organization."""
  try:
    # Check if user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    # Get all graphs for the org
    graphs = db.query(Graph).filter(Graph.org_id == org_id).all()

    result = []
    for graph in graphs:
      # Get graph credits info
      from ...models.iam import GraphCredits

      credits = GraphCredits.get_by_graph_id(graph.graph_id, db)

      result.append(
        {
          "graph_id": graph.graph_id,
          "graph_name": graph.graph_name,
          "graph_type": graph.graph_type,
          "graph_tier": graph.graph_tier,
          "credits_available": float(credits.available_credits) if credits else 0,
          "credits_used": float(credits.total_consumed) if credits else 0,
          "created_at": graph.created_at,
          "updated_at": graph.updated_at,
        }
      )

    return result

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error listing organization graphs: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list organization graphs",
    )
