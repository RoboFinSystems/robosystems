"""Admin API for SCIM provisioning bootstrap (support-plane, ALB-isolated)."""

from fastapi import APIRouter, HTTPException, Request, status

from ...db.platform import get_db_session
from ...logger import get_logger
from ...middleware.auth.admin import require_admin
from ...models.api.admin import (
  ScimBootstrapRequest,
  ScimBootstrapResponse,
  ScimTokenRevokeResponse,
)
from ...operations.admin import (
  OrgBoundaryError,
  OrgNotFoundError,
  bootstrap_scim,
  revoke_scim_token,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/scim", tags=["admin-scim"])


@router.post("/bootstrap", response_model=ScimBootstrapResponse)
@require_admin(permissions=["orgs:write"])
async def scim_bootstrap(
  request: Request, data: ScimBootstrapRequest
) -> ScimBootstrapResponse:
  """Create-or-reuse the enterprise org and mint a SCIM token.

  The raw token is returned once and never recoverable — paste it into the
  customer's IdP immediately.
  """
  session = next(get_db_session())
  try:
    try:
      result = bootstrap_scim(
        session,
        org_name=data.org_name,
        org_id=data.org_id,
        token_name=data.token_name,
        expires_in_days=data.expires_in_days,
      )
    except OrgNotFoundError as exc:
      raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except OrgBoundaryError as exc:
      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    logger.info(
      "Admin bootstrapped SCIM",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "org_id": result.org_id,
        "scim_token_id": result.scim_token_id,
      },
    )
    return ScimBootstrapResponse(
      org_id=result.org_id,
      org_name=result.org_name,
      scim_token_id=result.scim_token_id,
      token=result.raw_token,
      expires_at=result.expires_at.isoformat(),
    )
  finally:
    session.close()


@router.post("/tokens/{token_id}/revoke", response_model=ScimTokenRevokeResponse)
@require_admin(permissions=["orgs:write"])
async def scim_revoke_token(request: Request, token_id: str) -> ScimTokenRevokeResponse:
  """Revoke a SCIM token, immediately refusing further provisioning with it."""
  session = next(get_db_session())
  try:
    revoked = revoke_scim_token(session, token_id)
    if not revoked:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail=f"SCIM token {token_id} not found"
      )
    logger.info(
      "Admin revoked SCIM token",
      extra={"admin_key_id": request.state.admin_key_id, "scim_token_id": token_id},
    )
    return ScimTokenRevokeResponse(scim_token_id=token_id, revoked=True)
  finally:
    session.close()
