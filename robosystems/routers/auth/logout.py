"""User logout endpoint."""

from typing import Optional

from fastapi import (
  APIRouter,
  Depends,
  Header,
  Response,
  status,
)

from ...logger import logger
from ...middleware.auth.cache import api_key_cache
from ...middleware.rate_limits import logout_rate_limit_dependency

from ...middleware.auth.jwt import revoke_jwt_token

# Create router for logout endpoint
router = APIRouter()


@router.post(
  "/logout",
  status_code=status.HTTP_200_OK,
  summary="User Logout",
  description="Logout user and invalidate session.",
  operation_id="logoutUser",
)
async def logout(
  response: Response,
  authorization: Optional[str] = Header(None),
  _rate_limit: None = Depends(logout_rate_limit_dependency),
) -> dict:
  """
  Logout user and invalidate JWT token.

  Args:
      authorization: Authorization header with Bearer token

  Returns:
      Success message
  """
  try:
    # Extract JWT token from Bearer header
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
    logger.error(f"Logout error: {str(e)}")
    return {"message": "Logout completed"}
