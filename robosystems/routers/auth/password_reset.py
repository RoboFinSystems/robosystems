"""Password reset endpoints."""

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  HTTPException,
  Query,
  Request,
  status,
)
from sqlalchemy.orm import Session

from robosystems.middleware.sse import (
  build_email_job_config,
  run_and_monitor_dagster_job,
)

from ...config.constants import (
  JWT_EXPIRY_HOURS,
  PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
  TOKEN_GRACE_PERIOD_MINUTES,
)
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.auth.jwt import create_jwt_token
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...models.api.auth import (
  AuthResponse,
  ForgotPasswordRequest,
  ResetPasswordRequest,
  ResetPasswordValidateResponse,
)
from ...models.api.common import COMMON_ERROR_RESPONSES
from ...models.core import User, UserToken
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.device_fingerprinting import extract_device_fingerprint
from ...security.input_validation import (
  sanitize_string,
  validate_email,
)
from ...security.password import PasswordSecurity
from .utils import detect_app_source, hash_password

# Create router for password reset endpoints
router = APIRouter()


@router.post(
  "/password/forgot",
  summary="Forgot Password",
  description="Request password reset email. Always returns success to prevent email enumeration.",
  operation_id="forgotPassword",
  responses={**COMMON_ERROR_RESPONSES},
)
async def forgot_password(
  request: ForgotPasswordRequest,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(auth_rate_limit_dependency),
) -> dict:
  # Validate and sanitize email
  if not validate_email(request.email):
    # For security, still return success to prevent enumeration
    return {
      "message": "If an account exists with this email, a password reset link has been sent."
    }

  sanitized_email = sanitize_string(request.email, max_length=254)

  # Get client details
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  # Try to find user
  user = User.get_by_email(sanitized_email, session)

  if user and user.is_active:
    # Generate reset token
    token = UserToken.create_token(
      user_id=user.id,
      token_type="password_reset",
      hours=PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
      session=session,
      ip_address=client_ip,
      user_agent=user_agent,
    )

    app = detect_app_source(fastapi_request)

    # Queue reset email via Dagster (async with retry logic)
    run_config = build_email_job_config(
      email_type="password_reset",
      to_email=user.email,
      user_name=user.name,
      token=token,
      app=app,
    )
    background_tasks.add_task(
      run_and_monitor_dagster_job,
      job_name="send_email_job",
      operation_id=None,  # No SSE tracking needed for auth emails
      run_config=run_config,
    )

    logger.info(f"Queued password reset email to {sanitized_email}")

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.PASSWORD_RESET_REQUESTED,
      user_id=user.id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/password/forgot",
      details={
        "app_source": app,
      },
      risk_level="medium",
    )
  else:
    # Log attempt for non-existent user (security monitoring)
    logger.warning(
      f"Password reset requested for non-existent email: {sanitized_email}"
    )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.PASSWORD_RESET_REQUESTED,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/password/forgot",
      details={
        "email": sanitized_email,
        "user_exists": False,
      },
      risk_level="low",
    )

  # Always return success (enumeration protection)
  return {
    "message": "If an account exists with this email, a password reset link has been sent."
  }


@router.get(
  "/password/reset/validate",
  response_model=ResetPasswordValidateResponse,
  summary="Validate Reset Token",
  description="Check if a password reset token is valid without consuming it. Returns masked email on success.",
  operation_id="validateResetToken",
  responses={**COMMON_ERROR_RESPONSES},
)
async def validate_reset_token(
  token: str = Query(..., description="Password reset token"),
  session: Session = Depends(get_async_db_session),
) -> ResetPasswordValidateResponse:
  # Validate token without consuming it
  user_id = UserToken.validate_token(
    raw_token=token,
    token_type="password_reset",
    session=session,
  )

  if not user_id:
    return ResetPasswordValidateResponse(valid=False, email=None)

  # Get user to return masked email
  user = User.get_by_id(user_id, session)
  if not user:
    return ResetPasswordValidateResponse(valid=False, email=None)

  # Mask email for privacy (e.g., "jo***@example.com")
  email_parts = user.email.split("@")
  if len(email_parts) == 2:
    username = email_parts[0]
    domain = email_parts[1]
    if len(username) > 2:
      masked_email = f"{username[:2]}***@{domain}"
    else:
      masked_email = f"***@{domain}"
  else:
    masked_email = "***"

  return ResetPasswordValidateResponse(valid=True, email=masked_email)


@router.post(
  "/password/reset",
  response_model=AuthResponse,
  summary="Reset Password",
  description="Reset password with token from email. Invalidates all existing sessions. Returns JWT for auto-login.",
  operation_id="resetPassword",
  responses={**COMMON_ERROR_RESPONSES},
)
async def reset_password(
  request: ResetPasswordRequest,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
) -> AuthResponse:
  # Verify token and get user
  user_id = UserToken.verify_token(
    raw_token=request.token,
    token_type="password_reset",
    session=session,
  )

  if not user_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid or expired reset token",
    )

  user = User.get_by_id(user_id, session)
  if not user:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="User not found",
    )

  # Validate password strength (uses same rules as /password/check endpoint)
  password_result = PasswordSecurity.validate_password(request.new_password)
  if not password_result.is_valid:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Password requirements not met: {', '.join(password_result.errors)}",
    )

  password_hash = hash_password(request.new_password)

  # Update user's password
  user.update(session, password_hash=password_hash)

  # Invalidate every existing JWT (including refresh chain) by bumping the
  # user's session_version. The claim is checked on every auth, so prior
  # tokens stop working on their next request. The previous implementation
  # iterated UserToken records (email/reset tokens) and called revoke on
  # them, which never matched any actual JWT.
  try:
    user.invalidate_sessions(session)
  except Exception as invalidate_err:
    logger.error(f"Error invalidating sessions during password reset: {invalidate_err}")
    # Log the error but continue with the password reset

  # Get client details for security logging
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.PASSWORD_RESET_COMPLETED,
    user_id=user.id,
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/password/reset",
    details={},
    risk_level="high",
  )

  # Generate new JWT token for auto-login with device binding.
  # This picks up the just-bumped session_version.
  device_fingerprint = extract_device_fingerprint(fastapi_request)
  jwt_token = create_jwt_token(user.id, device_fingerprint, session=session)

  logger.info(f"Password reset completed for user {user.email}")

  expires_in = int(JWT_EXPIRY_HOURS * 3600)
  refresh_threshold = int(TOKEN_GRACE_PERIOD_MINUTES * 60)

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
      "email_verified": user.email_verified,
    },
    message="Password reset successfully. You are now logged in.",
    token=jwt_token,
    expires_in=expires_in,
    refresh_threshold=refresh_threshold,
  )
