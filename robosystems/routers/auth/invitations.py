"""Public org-invitation preview endpoint.

Lets the registration page show which organization an invitation token joins
before the invitee has an account. Unauthenticated by design — the
high-entropy token is the credential.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import logger
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...models.api.common import COMMON_ERROR_RESPONSES
from ...models.api.orgs import InvitationPreviewResponse
from ...models.core import Org, OrgInvitation

router = APIRouter()


@router.get(
  "/invitations/{token}",
  response_model=InvitationPreviewResponse,
  summary="Preview Invitation",
  description="Look up a pending org invitation by its token so the registration page can show what is being joined. Returns 404 for unknown, expired, revoked, or accepted tokens.",
  operation_id="getInvitationPreview",
  responses={**COMMON_ERROR_RESPONSES},
)
async def get_invitation_preview(
  token: str,
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(auth_rate_limit_dependency),
) -> InvitationPreviewResponse:
  invitation = OrgInvitation.get_valid_by_token(token, session)
  if invitation is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="Invitation not found or no longer valid",
    )

  org = Org.get_by_id(invitation.org_id, session)
  if org is None:
    logger.warning(f"Invitation {invitation.id} references missing org")
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="Invitation not found or no longer valid",
    )

  return InvitationPreviewResponse(
    org_name=org.name,
    email=invitation.email,
    role=invitation.role,
    expires_at=invitation.expires_at,
  )
