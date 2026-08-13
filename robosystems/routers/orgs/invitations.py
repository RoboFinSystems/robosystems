"""Organization invitation management endpoints.

Invitations are issued to email addresses that do not yet have a user account.
The invited registrant accepts via the tokened registration link and joins the
issuing org at the invited role instead of receiving a personal org.
"""

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  HTTPException,
  Request,
  status,
)
from sqlalchemy.orm import Session

from ...config import env
from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.rate_limits import general_api_rate_limit_dependency
from ...middleware.sse import build_email_job_config, run_and_monitor_dagster_job
from ...models.api.common import RESOURCE_ERROR_RESPONSES, ErrorResponse
from ...models.api.orgs import (
  CreateInvitationRequest,
  OrgInvitationListResponse,
  OrgInvitationResponse,
)
from ...models.core import Org, OrgInvitation, OrgRole, OrgUser, User
from ...security import SecurityAuditLogger, SecurityEventType
from ..auth.utils import detect_app_source

logger = get_logger(__name__)

router = APIRouter(tags=["Org Members"])


def _require_invitations_enabled() -> None:
  if not env.ORG_MEMBER_INVITATIONS_ENABLED:
    raise HTTPException(
      status_code=status.HTTP_501_NOT_IMPLEMENTED,
      detail="Organization member invitations are not enabled",
    )


def _require_org_admin(org_id: str, user: User, db: Session) -> OrgUser:
  membership = OrgUser.get_by_org_and_user(org_id, user.id, db)
  if not membership:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="You are not a member of this organization",
    )
  if membership.role not in [OrgRole.ADMIN, OrgRole.OWNER]:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Only admins and owners can manage invitations",
    )
  return membership


def _invitation_response(invitation: OrgInvitation) -> OrgInvitationResponse:
  invited_by_user = invitation.invited_by_user
  return OrgInvitationResponse(
    id=invitation.id,
    org_id=invitation.org_id,
    email=invitation.email,
    role=invitation.role,
    status=invitation.status,
    invited_by_user_id=invitation.invited_by,
    invited_by_name=invited_by_user.name if invited_by_user else None,
    created_at=invitation.created_at,
    expires_at=invitation.expires_at,
    is_expired=invitation.is_expired,
  )


def _queue_invitation_email(
  background_tasks: BackgroundTasks,
  fastapi_request: Request,
  invitation: OrgInvitation,
  org_name: str,
  inviter: User,
  raw_token: str,
) -> None:
  app = detect_app_source(fastapi_request)
  run_config = build_email_job_config(
    email_type="org_invitation",
    to_email=invitation.email,
    user_name="there",
    token=raw_token,
    app=app,
    org_name=org_name,
    inviter_name=inviter.name,
  )
  background_tasks.add_task(
    run_and_monitor_dagster_job,
    job_name="send_email_job",
    operation_id=None,
    run_config=run_config,
  )

  client_ip = fastapi_request.client.host if fastapi_request.client else None
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.EMAIL_SENT,
    user_id=inviter.id,
    ip_address=client_ip,
    user_agent=fastapi_request.headers.get("user-agent"),
    endpoint=str(fastapi_request.url.path),
    details={
      "email_type": "org_invitation",
      "org_id": invitation.org_id,
      "invitation_id": invitation.id,
      "app_source": app,
    },
    risk_level="low",
  )


@router.post(
  "/orgs/{org_id}/invitations",
  response_model=OrgInvitationResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Invite Member",
  description="Invite a new user to the organization by email. Disabled by default (ORG_MEMBER_INVITATIONS_ENABLED=false); returns 501 when disabled. Requires admin or owner role. Only emails without an existing account can be invited.",
  operation_id="createOrgInvitation",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    409: {"model": ErrorResponse, "description": "Email already registered or invited"},
    501: {"model": ErrorResponse, "description": "Feature not enabled"},
  },
)
async def create_invitation(
  org_id: str,
  request: CreateInvitationRequest,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgInvitationResponse:
  _require_invitations_enabled()

  try:
    _require_org_admin(org_id, current_user, db)

    org = Org.get_by_id(org_id, db)
    if org is None:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Organization not found",
      )

    email = request.email.strip().lower()

    if User.get_by_email(email, db):
      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
          f"A user with email {email} already exists. Each user belongs to a "
          "single organization, so existing accounts cannot be invited. "
          "Contact support if this email should be freed up."
        ),
      )

    if OrgInvitation.get_pending_by_org_and_email(org_id, email, db):
      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
          f"An invitation for {email} is already pending. "
          "Resend or revoke the existing invitation instead."
        ),
      )

    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=org_id,
      email=email,
      role=request.role,
      invited_by=current_user.id,
      session=db,
    )

    _queue_invitation_email(
      background_tasks, fastapi_request, invitation, org.name, current_user, raw_token
    )

    logger.info(
      f"User {current_user.id} invited {email} to org {org_id} as {request.role.value}"
    )
    response = _invitation_response(invitation)
    # Test-support only, and never in production (the guard forces it off there):
    # surface the raw token so automated authorization tests can complete the
    # invite -> register flow. See env.expose_invite_token_in_response().
    if env.expose_invite_token_in_response():
      response.token = raw_token
    return response

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error creating org invitation: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create invitation",
    )


@router.get(
  "/orgs/{org_id}/invitations",
  response_model=OrgInvitationListResponse,
  summary="List Pending Invitations",
  description="Requires admin or owner role.",
  operation_id="listOrgInvitations",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def list_invitations(
  org_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgInvitationListResponse:
  try:
    _require_org_admin(org_id, current_user, db)

    invitations = [
      _invitation_response(i) for i in OrgInvitation.get_pending_for_org(org_id, db)
    ]
    return OrgInvitationListResponse(
      invitations=invitations,
      total=len(invitations),
      org_id=org_id,
    )

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error listing org invitations: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to list invitations",
    )


@router.delete(
  "/orgs/{org_id}/invitations/{invitation_id}",
  status_code=status.HTTP_204_NO_CONTENT,
  summary="Revoke Invitation",
  description="Requires admin or owner role. Only pending invitations can be revoked.",
  operation_id="revokeOrgInvitation",
  responses={**RESOURCE_ERROR_RESPONSES},
)
async def revoke_invitation(
  org_id: str,
  invitation_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> None:
  try:
    _require_org_admin(org_id, current_user, db)

    invitation = OrgInvitation.get_by_id(invitation_id, db)
    if invitation is None or invitation.org_id != org_id:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Invitation not found",
      )
    if invitation.status != "pending":
      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"Invitation is {invitation.status} and cannot be revoked",
      )

    invitation.revoke(db)
    logger.info(f"User {current_user.id} revoked invitation {invitation_id}")

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error revoking org invitation: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to revoke invitation",
    )


@router.post(
  "/orgs/{org_id}/invitations/{invitation_id}/resend",
  response_model=OrgInvitationResponse,
  summary="Resend Invitation",
  description="Requires admin or owner role. Rotates the invitation token, extends its expiry, and re-sends the email.",
  operation_id="resendOrgInvitation",
  responses={
    **RESOURCE_ERROR_RESPONSES,
    501: {"model": ErrorResponse, "description": "Feature not enabled"},
  },
)
async def resend_invitation(
  org_id: str,
  invitation_id: str,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> OrgInvitationResponse:
  _require_invitations_enabled()

  try:
    _require_org_admin(org_id, current_user, db)

    invitation = OrgInvitation.get_by_id(invitation_id, db)
    if invitation is None or invitation.org_id != org_id:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Invitation not found",
      )
    if invitation.status != "pending":
      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"Invitation is {invitation.status} and cannot be resent",
      )

    org = Org.get_by_id(org_id, db)
    if org is None:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Organization not found",
      )

    raw_token = invitation.rotate_token(db)
    _queue_invitation_email(
      background_tasks, fastapi_request, invitation, org.name, current_user, raw_token
    )

    logger.info(f"User {current_user.id} resent invitation {invitation_id}")
    return _invitation_response(invitation)

  except HTTPException:
    raise
  except Exception as e:
    db.rollback()
    logger.error(f"Error resending org invitation: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to resend invitation",
    )
