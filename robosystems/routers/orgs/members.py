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
from ...models.core import Graph, GraphRole, GraphUser, OrgRole, OrgUser, User
from ...operations.billing import (
  ProviderCancellationError,
  cancel_user_repository_subscriptions,
)
from ...security import SecurityAuditLogger, SecurityEventType

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
    membership = OrgUser.get_by_org_and_user(org_id, current_user.id, db)
    if not membership:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="You are not a member of this organization",
      )

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

    target_membership = OrgUser.get_by_org_and_user(org_id, user_id, db)
    if not target_membership:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="User is not a member of this organization",
      )

    # Owner role changes need a dedicated ownership-transfer workflow.
    if target_membership.role == OrgRole.OWNER:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          "Cannot change owner role through this endpoint. "
          "Transferring ownership requires a dedicated workflow to ensure "
          "proper handoff and prevent accidental loss of org access."
        ),
      )

    if request.role == OrgRole.OWNER:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          "Cannot promote to owner through this endpoint. "
          "Granting ownership requires a dedicated workflow."
        ),
      )

    # Defense in depth: the owner-role guards above already cover this.
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

    previous_role = target_membership.role
    target_membership.role = request.role

    # Explicit graph-admin rows (the creator's, or ones granted while an org
    # admin) would otherwise keep admin access past the demotion.
    admin_grants_downgraded = 0
    if previous_role in (OrgRole.OWNER, OrgRole.ADMIN) and request.role == (
      OrgRole.MEMBER
    ):
      org_graph_ids = [
        row.graph_id
        for row in db.query(Graph.graph_id).filter(Graph.org_id == org_id).all()
      ]
      if org_graph_ids:
        admin_grants_downgraded = (
          db.query(GraphUser)
          .filter(
            GraphUser.user_id == user_id,
            GraphUser.graph_id.in_(org_graph_ids),
            GraphUser.role == GraphRole.ADMIN.value,
          )
          .update({"role": GraphRole.MEMBER.value}, synchronize_session=False)
        )
    db.commit()
    db.refresh(target_membership)

    # Org owner/admin hold implicit graph admin on org graphs, so a role
    # change alters graph access — drop the target's cached grants.
    api_key_cache.invalidate_user_jwt_graph_access(user_id)
    api_key_cache.invalidate_user_data(user_id)

    target_user = target_membership.user

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.ORG_MEMBER_ROLE_CHANGED,
      user_id=current_user.id,
      details={
        "action": "org_member_role_changed",
        "org_id": org_id,
        "target_user_id": user_id,
        "previous_role": previous_role.value,
        "new_role": request.role.value,
        "graph_admin_grants_downgraded": admin_grants_downgraded,
      },
      risk_level="low",
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
      pass
    elif user_id == current_user.id:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Admins and owners cannot remove themselves",
      )

    # Org membership justified the org-billed repository subscriptions, so
    # cancel them first. Provider first: if it refuses, the removal aborts
    # rather than off-boarding a member the org keeps paying for.
    try:
      canceled = cancel_user_repository_subscriptions(
        user_id=user_id,
        session=db,
        actor_user_id=current_user.id,
      )
    except ProviderCancellationError:
      raise HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail=(
          "The payment provider could not cancel this member's repository "
          "subscriptions. No changes were made — please try again."
        ),
      )
    if canceled:
      logger.info(
        f"Canceled {len(canceled)} repository subscription(s) while removing "
        f"user {user_id} from org {org_id}"
      )

    # Membership justified the org graph grants; without the cascade a removed
    # member would keep access to every org graph.
    org_graph_ids = [
      row.graph_id
      for row in db.query(Graph.graph_id).filter(Graph.org_id == org_id).all()
    ]
    graph_grants_revoked = 0
    if org_graph_ids:
      graph_grants_revoked = (
        db.query(GraphUser)
        .filter(
          GraphUser.user_id == user_id,
          GraphUser.graph_id.in_(org_graph_ids),
        )
        .delete(synchronize_session=False)
      )

    removed_role = target_membership.role
    db.delete(target_membership)
    db.commit()

    api_key_cache.invalidate_user_jwt_graph_access(user_id)
    api_key_cache.invalidate_user_data(user_id)

    # One org per user: removing the last membership leaves an account that
    # can authenticate but reach nothing. Deactivating makes that honest (the
    # kill switch also bumps `session_version` and revokes API keys);
    # reversible by an admin via `activate`.
    #
    # Best-effort: the removal is already committed, so a failure here must
    # not 500 the request or skip the audit record below. Log loudly instead;
    # repair with `users deactivate`.
    deactivated_user = False
    if not OrgUser.get_user_orgs(user_id, db):
      removed_user = User.get_by_id(user_id, db)
      if removed_user is not None and removed_user.is_active:
        try:
          removed_user.deactivate(db)
          deactivated_user = True
          logger.info(
            f"Deactivated user {user_id}: removal from org {org_id} left them "
            f"with no organization"
          )
        except Exception as exc:
          logger.critical(
            f"Removed user {user_id} from org {org_id} but could not deactivate "
            f"the account, which now has no organization and is still active: "
            f"{exc!s}. Repair with: admin users deactivate {user_id}",
            exc_info=True,
          )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.ORG_MEMBER_REMOVED,
      user_id=current_user.id,
      details={
        "action": "org_member_removed",
        "org_id": org_id,
        "target_user_id": user_id,
        "previous_role": removed_role.value,
        "self_removal": user_id == current_user.id,
        "org_graph_grants_revoked": graph_grants_revoked,
        "repository_subscriptions_canceled": len(canceled),
        "user_deactivated": deactivated_user,
      },
      risk_level="low",
    )

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error removing member from organization: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to remove member",
    )
