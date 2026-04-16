"""User logout endpoint."""

from fastapi import (
  APIRouter,
  Depends,
  Request,
  Response,
)

from ...logger import logger
from ...middleware.auth.cache import api_key_cache
from ...middleware.auth.jwt import revoke_jwt_token
from ...middleware.rate_limits import logout_rate_limit_dependency
from ...models.api.common import COMMON_ERROR_RESPONSES

# Create router for logout endpoint
router = APIRouter()


@router.post(
  "/logout",
  summary="User Logout",
  operation_id="logoutUser",
  responses={**COMMON_ERROR_RESPONSES},
)
async def logout(
  request: Request,
  response: Response,
  _rate_limit: None = Depends(logout_rate_limit_dependency),
) -> dict:
  try:
    # Extract JWT token from Authorization header (doesn't show in OpenAPI params)
    authorization = request.headers.get("authorization")
    jwt_token = None
    if authorization and authorization.startswith("Bearer "):
      jwt_token = authorization[7:]  # Remove "Bearer " prefix

    # If there's a valid JWT token, revoke it
    if jwt_token:
      try:
        # Revoke token using the revocation system
        revoke_success = revoke_jwt_token(jwt_token, reason="user_logout")

        if revoke_success:
          logger.info("JWT token successfully revoked on logout")
        else:
          logger.warning("Failed to revoke JWT token on logout")

        # Also invalidate any cached JWT validation data
        api_key_cache.invalidate_jwt_token(jwt_token)

      except Exception as e:
        logger.warning(f"Failed to revoke JWT token during logout: {e}")
        # Continue with logout even if revocation fails

    return {"message": "Logout successful"}

  except Exception as e:
    logger.error(f"Logout error: {e!s}")
    return {"message": "Logout completed"}
