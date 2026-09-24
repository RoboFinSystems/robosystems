"""Session management endpoints (me, refresh)."""

from datetime import UTC

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Request,
  status,
)
from sqlalchemy.orm import Session

from robosystems.security.request_context import publish_principal

from ...config import env
from ...config.constants import JWT_EXPIRY_HOURS, TOKEN_GRACE_PERIOD_MINUTES
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.auth.jwt import (
  create_jwt_token,
  is_session_access_token,
  revoke_jwt_token,
  verify_jwt_claims,
)
from ...middleware.rate_limits import (
  auth_status_rate_limit_dependency,
)
from ...middleware.rate_limits.rate_limiting import jwt_refresh_rate_limit_dependency
from ...models.api.auth import AuthResponse
from ...models.api.common import COMMON_ERROR_RESPONSES
from ...models.core import User
from ...security.device_fingerprinting import extract_device_fingerprint

router = APIRouter()


@router.get(
  "/me",
  summary="Get Current User",
  operation_id="getCurrentAuthUser",
  responses={**COMMON_ERROR_RESPONSES},
)
async def get_me(
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(auth_status_rate_limit_dependency),
) -> dict:
  try:
    # Read directly so it doesn't show in the OpenAPI params.
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

    device_fingerprint = extract_device_fingerprint(fastapi_request)

    verify_result = verify_jwt_claims(jwt_token, device_fingerprint)
    if not verify_result:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
      )
    user_id, token_session_version = verify_result

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

    if int(user.session_version or 0) != int(token_session_version):
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token session has been invalidated",
        headers={"WWW-Authenticate": "Bearer"},
      )

    publish_principal(fastapi_request, str(user.id), "jwt_token")

    return {
      "id": user.id,
      "email": user.email,
      "name": user.name,
      "email_verified": user.email_verified,
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Get current user error: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to get user information",
    )


@router.post(
  "/refresh",
  response_model=AuthResponse,
  summary="Refresh Session",
  description="Revokes the current token and issues a new one. Accepts recently-expired tokens within the grace period.",
  operation_id="refreshAuthSession",
  responses={**COMMON_ERROR_RESPONSES},
)
async def refresh_session(
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(jwt_refresh_rate_limit_dependency),
) -> AuthResponse:
  from datetime import datetime, timedelta

  import jwt

  from ...middleware.auth.jwt import JWTConfig

  payload = None

  try:
    # Read directly so it doesn't show in the OpenAPI params.
    authorization = fastapi_request.headers.get("authorization")
    jwt_token = None
    if authorization and authorization.startswith("Bearer "):
      jwt_token = authorization[7:]  # Remove "Bearer " prefix

    if not jwt_token:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated"
      )

    device_fingerprint = extract_device_fingerprint(fastapi_request)

    user_id: str | None = None
    token_session_version: int = 0
    verify_result = verify_jwt_claims(jwt_token, device_fingerprint)
    if verify_result:
      user_id, token_session_version = verify_result
    else:
      # Refresh accepts a recently expired token within the grace period.
      try:
        payload = jwt.decode(
          jwt_token,
          JWTConfig.get_jwt_secret(),
          algorithms=["HS256"],
          issuer=env.JWT_ISSUER,
          audience=env.JWT_AUDIENCE,
          options={"verify_exp": False},  # Allow expired tokens for grace period
        )

        # Purpose-scoped tokens (SSO handoff, MFA challenge) fail
        # `verify_jwt_claims` on type; the grace path must re-apply that gate
        # or an expired MFA/SSO token mints a session JWT.
        if not is_session_access_token(payload) or not payload.get("jti"):
          raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
          )

        exp = payload.get("exp")
        if exp:
          exp_time = datetime.fromtimestamp(exp, tz=UTC)
          grace_period = timedelta(
            minutes=TOKEN_GRACE_PERIOD_MINUTES
          )  # Reduced grace period for security
          time_since_expiry = datetime.now(UTC) - exp_time

          if time_since_expiry > grace_period:
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED,
              detail="Token expired beyond grace period",
            )

          # A negative value means verification failed for some other reason
          # than expiry; the grace path must not paper over it.
          if time_since_expiry < timedelta(0):
            raise HTTPException(
              status_code=status.HTTP_401_UNAUTHORIZED,
              detail="Token verification failed - not expired",
            )

          # The grace path still enforces device binding and revocation.
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

        try:
          token_session_version = int(payload.get("session_version", 0))
        except (TypeError, ValueError):
          raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token session",
          )

        logger.info(f"Accepted expired token within grace period for user {user_id}")

      except jwt.InvalidTokenError:
        raise HTTPException(
          status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token"
        )

    user = User.get_by_id(user_id, session)
    if not user or not user.is_active:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive"
      )

    if int(user.session_version or 0) != int(token_session_version):
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token session has been invalidated",
      )

    publish_principal(fastapi_request, str(user.id), "jwt_token")

    revoke_success = revoke_jwt_token(jwt_token, reason="session_refresh")
    if revoke_success:
      logger.info(f"Old JWT token revoked during session refresh for user {user_id}")
    else:
      logger.warning(
        f"Failed to revoke old JWT token during session refresh for user {user_id}"
      )

    new_jwt_token = create_jwt_token(user.id, device_fingerprint, session=session)

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

    expires_in = int(JWT_EXPIRY_HOURS * 3600)  # Convert hours to seconds
    refresh_threshold = TOKEN_GRACE_PERIOD_MINUTES * 60  # Convert minutes to seconds

    return AuthResponse(
      user={
        "id": user.id,
        "name": user.name,
        "email": user.email,
        "email_verified": user.email_verified,
      },
      message="Session refreshed successfully",
      token=new_jwt_token,  # Return new JWT for Bearer authentication
      expires_in=expires_in,  # Token expires in 30 minutes (1800 seconds)
      refresh_threshold=refresh_threshold,  # Refresh 5 minutes before expiry (300 seconds)
    )

  except HTTPException:
    raise
  except Exception as e:
    client_ip = fastapi_request.client.host if fastapi_request.client else "unknown"
    logger.error(f"Session refresh error from IP {client_ip}: {e!s}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Session refresh failed"
    )
