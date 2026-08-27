"""Connected apps: the user's MCP OAuth grants."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...models.api.common import (
  AUTHENTICATED_ERROR_RESPONSES,
  RESOURCE_ERROR_RESPONSES,
  ErrorCode,
  SuccessResponse,
  create_error_response,
)
from ...models.api.user import OAuthGrantsResponse
from ...models.core import User
from ...operations.oauth_server.grants import (
  GrantNotFound,
  list_user_grants,
  revoke_user_grant,
)

router = APIRouter(tags=["User"])


@router.get(
  "/user/oauth/grants",
  response_model=OAuthGrantsResponse,
  summary="List Connected Apps",
  description=(
    "Every MCP client the user has authorized through OAuth, with the one graph "
    "each connection reaches. Active grants only: a revoked grant cannot be "
    "reinstated, so it leaves the list."
  ),
  operation_id="listUserOAuthGrants",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/oauth/grants", business_event_type="oauth_grants_listed"
)
async def list_oauth_grants(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> OAuthGrantsResponse:
  user_id = str(current_user.id)
  try:
    grants = list_user_grants(user_id, db)
    get_endpoint_metrics().record_business_event(
      endpoint="/v1/user/oauth/grants",
      method="GET",
      event_type="oauth_grants_listed",
      event_data={"user_id": user_id, "active_grants": len(grants)},
      user_id=user_id,
    )
    return OAuthGrantsResponse(grants=grants)
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error listing OAuth grants: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error listing connected apps",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.delete(
  "/user/oauth/grants/{grant_id}",
  response_model=SuccessResponse,
  summary="Revoke Connected App",
  description=(
    "Revokes the grant and every access and refresh token minted from it. The "
    "client's next request fails with 401 and it must ask the user to authorize "
    "again. Revoking an already revoked grant succeeds and changes nothing."
  ),
  operation_id="revokeUserOAuthGrant",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/oauth/grants/{grant_id}",
  business_event_type="oauth_grant_revoked",
)
async def revoke_oauth_grant(
  grant_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> SuccessResponse:
  user_id = str(current_user.id)
  try:
    tokens_revoked = revoke_user_grant(user_id, grant_id, db)
  except GrantNotFound:
    get_endpoint_metrics().record_business_event(
      endpoint="/v1/user/oauth/grants/{grant_id}",
      method="DELETE",
      event_type="oauth_grant_revoke_not_found",
      event_data={"user_id": user_id, "requested_grant_id": grant_id},
      user_id=user_id,
    )
    raise create_error_response(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="Grant not found or access denied",
      code=ErrorCode.NOT_FOUND,
    )
  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Error revoking OAuth grant: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error revoking connected app",
      code=ErrorCode.INTERNAL_ERROR,
    )

  get_endpoint_metrics().record_business_event(
    endpoint="/v1/user/oauth/grants/{grant_id}",
    method="DELETE",
    event_type="oauth_grant_revoked",
    event_data={
      "user_id": user_id,
      "grant_id": grant_id,
      "tokens_revoked": tokens_revoked,
    },
    user_id=user_id,
  )
  return SuccessResponse(
    success=True,
    message="Connected app revoked",
    data={"grant_id": grant_id, "tokens_revoked": tokens_revoked},
  )
