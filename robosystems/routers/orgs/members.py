"""Organization member management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.cache import api_key_cache
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.api.common import (
  RESOURCE_ERROR_RESPONSES,
)
from ...models.api.orgs import (
  OrgMemberListResponse,
  OrgMemberResponse,
  UpdateMemberRoleRequest,
)
from ...models.core import Graph, GraphUser, OrgRole, OrgUser, User

logger = get_logger(__name__)

router = APIRouter(tags=["Org Members"])


@router.get(
  "/orgs/{org_id}/members",
  response_model=OrgMemberListResponse,
  summary="List Organization Members",
  operation_id="listOrgMembers",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_org_members(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgMemberListResponse:
  try:
    # Check if user is a member of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    # Get all members
    memberships = OrgUser.get_org_users(org_id, db)
    members = []

    for m in memberships:
      user = m.user
      members.append(
        OrgMemberResponse(
          user_id=user.id,
          name=user.name,
          email=user.email,
          role=m.role,
          joined_at=m.joined_at,
          is_active=user.is_active,
        )
      )

    return OrgMemberListResponse(
      members=members,
      total=len(members),
      org_id=org_id,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error listing organization members: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list organization members",
    )


@router.put(
  "/orgs/{org_id}/members/{user_id}",
  response_model=OrgMemberResponse,
  summary="Update Member Role",
  description="Requires admin or owner role. Owner promotion/demotion requires a dedicated ownership transfer workflow.",
  operation_id="updateOrgMemberRole",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def update_member_role(
  org_id: str,
  user_id: str,
  request: UpdateMemberRoleRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgMemberResponse:
  try:
    # Check if current user is an admin or owner of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    if membership.role not in [OrgRole.ADMIN, OrgRole.OWNER]:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Only admins and owners can update member roles",
      )

    # Can't change your own role unless you're the owner
    if user_id == current_user.id and membership.role != OrgRole.OWNER:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="You cannot change your own role",
      )

    # Get the target member
    target_membership = OrgUser.get_by_org_and_user(org_id, user_id, db)
    if not target_membership:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User is not a member of this organization",
      )

    # Prevent any owner role changes - requires dedicated ownership transfer workflow
    if target_membership.role == OrgRole.OWNER:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          "Cannot change owner role through this endpoint. "
          "Transferring ownership requires a dedicated workflow to ensure "
          "proper handoff and prevent accidental loss of org access."
        ),
      )

    # Prevent promoting anyone to owner - requires dedicated ownership transfer workflow
    if request.role == OrgRole.OWNER:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          "Cannot promote to owner through this endpoint. "
          "Granting ownership requires a dedicated workflow."
        ),
      )

    # Ensure at least one owner remains (redundant now, but keep for safety)
    if target_membership.role == OrgRole.OWNER and request.role != OrgRole.OWNER:
      owner_count = (
        db.query(OrgUser)
        .filter(
          OrgUser.org_id == org_id,
          OrgUser.role == OrgRole.OWNER,
        )
        .count()
      )
      if owner_count <= 1:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Organization must have at least one owner",
        )

    # Update the role
    target_membership.role = request.role
    db.commit()
    db.refresh(target_membership)

    target_user = target_membership.user

    logger.info(
      f"User {current_user.id} updated role for user {user_id} in org {org_id} to {request.role}"
    )

    return OrgMemberResponse(
      user_id=target_user.id,
      name=target_user.name,
      email=target_user.email,
      role=target_membership.role,
      joined_at=target_membership.joined_at,
      is_active=target_user.is_active,
    )

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error updating member role: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to update member role",
    )


@router.delete(
  "/orgs/{org_id}/members/{user_id}",
  status_code=status.HTTP_204_NO_CONTENT,
  summary="Remove Member",
  description="Requires admin or owner role. Members may remove themselves.",
  operation_id="removeOrgMember",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def remove_member(
  org_id: str,
  user_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  try:
    # Check if current user is an admin or owner of the org
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

    is_self_member_removal = (
      user_id == current_user.id and membership.role == OrgRole.MEMBER
    )

    if not is_self_member_removal and membership.role not in [
      OrgRole.ADMIN,
      OrgRole.OWNER,
    ]:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Only admins and owners can remove members",
      )

    # Get the target member
    target_membership = OrgUser.get_by_org_and_user(org_id, user_id, db)
    if not target_membership:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User is not a member of this organization",
      )

    # Can't remove an owner unless you're an owner
    if target_membership.role == OrgRole.OWNER and membership.role != OrgRole.OWNER:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Only owners can remove other owners",
      )

    # Ensure at least one owner remains
    if target_membership.role == OrgRole.OWNER:
      owner_count = (
        db.query(OrgUser)
        .filter(
          OrgUser.org_id == org_id,
          OrgUser.role == OrgRole.OWNER,
        )
        .count()
      )
      if owner_count <= 1:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail="Organization must have at least one owner",
        )

    # Members can remove themselves
    if user_id == current_user.id and membership.role == OrgRole.MEMBER:
      # Allow self-removal for members
      pass
    elif user_id == current_user.id:
      # Admins and owners need another admin/owner to remove them
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Admins and owners cannot remove themselves",
      )

    # Remove the member, revoking their access to org-owned graphs.
    # Org membership is what justified the graph grants; without the cascade
    # a removed member would keep full access to every org graph.
    org_graph_ids = [
      row.graph_id
      for row in db.query(Graph.graph_id).filter(Graph.org_id == org_id).all()
    ]
    if org_graph_ids:
      db.query(GraphUser).filter(
        GraphUser.user_id == user_id,
        GraphUser.graph_id.in_(org_graph_ids),
      ).delete(synchronize_session=False)

    db.delete(target_membership)
    db.commit()

    api_key_cache.invalidate_user_jwt_graph_access(user_id)
    api_key_cache.invalidate_user_data(user_id)

    logger.info(f"User {user_id} removed from org {org_id} by user {current_user.id}")

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error removing member from organization: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to remove member",
    )
