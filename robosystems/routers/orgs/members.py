"""Organization member management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...config import env
from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...models.api.orgs import (
  InviteMemberRequest,
  OrgMemberListResponse,
  OrgMemberResponse,
  UpdateMemberRoleRequest,
)
from ...models.iam import OrgRole, OrgUser, User
from ..auth.utils import hash_password

logger = get_logger(__name__)

router = APIRouter(tags=["Org Members"])


@router.get(
  "/orgs/{org_id}/members",
  response_model=OrgMemberListResponse,
  summary="List Organization Members",
  description="Get all members of an organization with their roles.",
  operation_id="listOrgMembers",
)
async def list_org_members(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgMemberListResponse:
  """List all members of an organization."""
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


@router.post(
  "/orgs/{org_id}/members",
  response_model=OrgMemberResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Invite Member",
  description="""Invite a user to join the organization. Requires admin or owner role.

  **⚠️ FEATURE NOT READY**: This endpoint is disabled by default (ORG_MEMBER_INVITATIONS_ENABLED=false).
  Returns 501 NOT IMPLEMENTED when disabled. See endpoint implementation for TODO list before enabling.""",
  operation_id="inviteOrgMember",
)
async def invite_member(
  org_id: str,
  request: InviteMemberRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgMemberResponse:
  """Invite a member to the organization.

  WARNING: This feature is incomplete and disabled by default (ORG_MEMBER_INVITATIONS_ENABLED=false).

  TODO before enabling:
  - Implement email invitation system with secure token-based activation
  - Add invitation expiration (e.g., 7 days)
  - Add cleanup job for expired/abandoned invitations
  - Consider whether to support inviting existing users vs current "new users only" model
  - Add invitation tracking/audit logging
  """
  if not env.ORG_MEMBER_INVITATIONS_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_501_NOT_IMPLEMENTED,
      detail="Organization member invitations are not enabled",
    )

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
        detail="Only admins and owners can invite members",
      )

    # Check if a user with this email already exists
    existing_user = User.get_by_email(request.email, db)

    if existing_user:
      # User already exists - they have their own org from signup
      # Cannot invite existing users to organizations
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          f"A user with email {request.email} already exists in the system. "
          "Existing users cannot be invited to organizations since they already have their own. "
          "To join this organization, they must either: "
          "1) Change their email address on their existing account to free up this email, or "
          "2) Use a different email address for this organization."
        ),
      )

    # Create a pending user account that will be part of this org
    import secrets

    # Generate a secure temporary password and hash it properly
    temp_password = f"pending_{secrets.token_urlsafe(32)}"

    invited_user = User(
      email=request.email,
      name=request.email.split("@")[0],  # Default name from email
      password_hash=hash_password(temp_password),  # Properly hashed temporary password
      is_active=False,  # Inactive until they activate via email link and set password
    )
    db.add(invited_user)
    db.flush()  # Get the user ID without committing

    # Add the user to the organization
    new_membership = OrgUser.create(
      org_id=org_id,
      user_id=invited_user.id,
      role=request.role or OrgRole.MEMBER,
      session=db,
      auto_commit=False,
    )

    db.commit()
    db.refresh(new_membership)
    db.refresh(invited_user)

    # TODO: Implement invitation email system before enabling this feature
    # Required:
    # 1. Generate secure activation token (e.g., JWT or random token stored in DB)
    # 2. Create invitation email template with activation link
    # 3. Send email via configured email provider (SES, SendGrid, etc.)
    # 4. Add activation endpoint to handle token validation and password setup
    # 5. Token should expire after 7 days
    #
    # Example flow:
    # token = generate_invitation_token(invited_user.id, org_id)
    # activation_link = f"{env.FRONTEND_URL}/activate?token={token}"
    # send_email(
    #   to=invited_user.email,
    #   subject=f"You've been invited to {org.name}",
    #   template="org_invitation",
    #   data={"activation_link": activation_link, "inviter": current_user.name}
    # )

    return OrgMemberResponse(
      user_id=invited_user.id,
      name=invited_user.name,
      email=invited_user.email,
      role=new_membership.role,
      joined_at=new_membership.joined_at,
      is_active=invited_user.is_active,
    )

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error inviting member to organization: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to invite member",
    )


@router.put(
  "/orgs/{org_id}/members/{user_id}",
  response_model=OrgMemberResponse,
  summary="Update Member Role",
  description="Update a member's role in the organization. Requires admin or owner role.",
  operation_id="updateOrgMemberRole",
)
async def update_member_role(
  org_id: str,
  user_id: str,
  request: UpdateMemberRoleRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgMemberResponse:
  """Update a member's role in the organization."""
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
  description="Remove a member from the organization. Requires admin or owner role.",
  operation_id="removeOrgMember",
)
async def remove_member(
  org_id: str,
  user_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  """Remove a member from the organization."""
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

    # Remove the member
    db.delete(target_membership)
    db.commit()

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
