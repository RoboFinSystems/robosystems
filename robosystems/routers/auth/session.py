"""Session management endpoints (me, refresh)."""

from sqlalchemy.orm import Session
from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Request,
  status,
)

from ...models.api.auth import AuthResponse
from ...models.api.common import ErrorResponse
from ...models.iam import User
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.rate_limits import (
  auth_status_rate_limit_dependency,
)
from ...middleware.rate_limits.rate_limiting import jwt_refresh_rate_limit_dependency
from ...middleware.auth.cache import api_key_cache

from ...middleware.auth.jwt import (
  verify_jwt_token,
  create_jwt_token,
  revoke_jwt_token,
)
from ...security.device_fingerprinting import extract_device_fingerprint
from ...config import env

# Create router for session endpoints
router = APIRouter()


@router.get(
  "/me",
  summary="Get Current User",
  description="Get the currently authenticated user.",
  operation_id="getCurrentAuthUser",
  responses={
    401: {"model": ErrorResponse, "description": "Not authenticated"},
  },
)
async def get_me(
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(auth_status_rate_limit_dependency),
) -> dict:
  """
  Get current authenticated user from JWT token.

  Returns:
      User information

  Raises:
      HTTPException: If not authenticated
  """
  try:
    # Extract JWT token from Authorization header (doesn't show in OpenAPI params)
    authorization = fastapi_request.headers.get("authorization")
    jwt_token = None
    if authorization and authorization.startswith("Bearer "):
      jwt_token = authorization[7:]  # Remove "Bearer " prefix

    if not jwt_token:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
      )

    # Extract device fingerprint for verification
    device_fingerprint = extract_device_fingerprint(fastapi_request)

    # Verify JWT token with device binding
    user_id = verify_jwt_token(jwt_token, device_fingerprint)
    if not user_id:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
      )

    # Get user from database
    user = User.get_by_id(user_id, session)
    if not user:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="User not found",
        headers={"WWW-Authenticate": "Bearer"},
      )

    if not user.is_active:
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="User account is deactivated",
      )

    return {
      "id": user.id,
      "email": user.email,
      "name": user.name,
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Get current user error: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get user information",
    )


@router.post(
  "/refresh",
  response_model=AuthResponse,
  summary="Refresh Session",
  description="Refresh authentication session with a new JWT token.",
  operation_id="refreshAuthSession",
  responses={
    401: {"model": ErrorResponse, "description": "Not authenticated"},
  },
)
async def refresh_session(
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(jwt_refresh_rate_limit_dependency),
) -> AuthResponse:
  """
  Refresh user session and extend authentication token.

  Returns:
      AuthResponse: Success response with updated user data

  Raises:
      HTTPException: If not authenticated or token is invalid
  """
  # Import jwt at function level to avoid circular imports
  import jwt
  from datetime import datetime, timedelta, timezone
  from ...middleware.auth.jwt import JWTConfig

  # Initialize payload variable
  payload = None

  try:
    # Extract JWT token from Authorization header (doesn't show in OpenAPI params)
    authorization = fastapi_request.headers.get("authorization")
    jwt_token = None
    if authorization and authorization.startswith("Bearer "):
      jwt_token = authorization[7:]  # Remove "Bearer " prefix

    if not jwt_token:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated"
      )

    # Extract device fingerprint for verification
    device_fingerprint = extract_device_fingerprint(fastapi_request)

    # Verify current JWT token - allow recently expired tokens for refresh
    user_id = verify_jwt_token(jwt_token, device_fingerprint)
    if not user_id:
      # For refresh endpoint, try to verify recently expired tokens
      try:
        payload = jwt.decode(
          jwt_token,
          JWTConfig.get_jwt_secret(),
          algorithms=["HS256"],
          options={"verify_exp": False},  # Allow expired tokens for grace period
        )

        # Check if token expired recently (within reduced grace period)
        exp = payload.get("exp")
        if exp:
          exp_time = datetime.fromtimestamp(exp, tz=timezone.utc)
          grace_period = timedelta(
            minutes=env.TOKEN_GRACE_PERIOD_MINUTES
          )  # Reduced grace period for security
          time_since_expiry = datetime.now(timezone.utc) - exp_time

          if time_since_expiry > grace_period:
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED,
              detail="Token expired beyond grace period",
            )

          # CRITICAL SECURITY: Only allow grace period refresh if token actually expired
          # Reject tokens that haven't expired (negative time_since_expiry indicates other failure reasons)
          if time_since_expiry < timedelta(0):
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED,
              detail="Token verification failed - not expired",
            )

          # CRITICAL SECURITY: Always validate device fingerprint in grace period
          from ...security.device_fingerprinting import create_device_hash

          stored_device_hash = payload.get("device_hash")
          if stored_device_hash:
            current_device_hash = create_device_hash(device_fingerprint)
            if current_device_hash != stored_device_hash:
              logger.warning(
                f"Device hash mismatch during refresh for user {payload.get('user_id')}: stored={stored_device_hash[:8]}... current={current_device_hash[:8]}..."
              )
              raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Device changes detected. Please re-authenticate.",
              )

          # CRITICAL SECURITY: Always check revocation status in grace period
          user_id = payload.get("user_id")
          jti = payload.get("jti")
          if jti and user_id:
            from ...middleware.auth.jwt import is_jwt_token_revoked

            if is_jwt_token_revoked(jwt_token):
              logger.warning(f"Attempted refresh of revoked token for user {user_id}")
              raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has been revoked",
              )

        if not user_id:
          raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
          )

        logger.info(f"Accepted expired token within grace period for user {user_id}")

      except jwt.InvalidTokenError:
        raise HTTPException(
          status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    # Get user
    user = User.get_by_id(user_id, session)
    if not user or not user.is_active:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive"
      )

    # Revoke the old token before issuing a new one
    revoke_success = revoke_jwt_token(jwt_token, reason="session_refresh")
    if revoke_success:
      logger.info(f"Old JWT token revoked during session refresh for user {user_id}")
    else:
      logger.warning(
        f"Failed to revoke old JWT token during session refresh for user {user_id}"
      )

    # Also invalidate old token cache
    api_key_cache.invalidate_jwt_token(jwt_token)

    # Create new JWT token with fresh expiry, device binding, and additional entropy
    # Add timestamp nonce for additional security against replay attacks
    import time

    device_fingerprint_with_entropy = {
      **device_fingerprint,
      "refresh_timestamp": int(time.time()),
      "refresh_nonce": payload.get("jti", "")[:8]
      if payload
      else "",  # Use part of old JTI as nonce
    }
    new_jwt_token = create_jwt_token(user.id, device_fingerprint_with_entropy)

    # Log successful refresh for security monitoring
    from ...security import SecurityAuditLogger, SecurityEventType

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.TOKEN_REFRESH,
      user_id=user.id,
      ip_address=fastapi_request.client.host if fastapi_request.client else None,
      user_agent=fastapi_request.headers.get("user-agent"),
      details={
        "old_token_jti": payload.get("jti") if payload else None,
        "new_token_created": True,
        "refresh_method": "jwt_refresh",
      },
    )

    # Return new token for Bearer authentication
    return AuthResponse(
      user={
        "id": user.id,
        "name": user.name,
        "email": user.email,
      },
      message="Session refreshed successfully",
      token=new_jwt_token,  # Return new JWT for Bearer authentication
    )

  except HTTPException:
    raise
  except Exception as e:
    client_ip = fastapi_request.client.host if fastapi_request.client else "unknown"
    logger.error(f"Session refresh error from IP {client_ip}: {str(e)}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Session refresh failed"
    )
