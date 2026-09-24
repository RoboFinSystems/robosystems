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
from .utils import (
  detect_app_source,
  hash_password_async,
  may_issue_session_without_login,
  require_password_auth,
)

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
  _password_auth: None = Depends(require_password_auth),
) -> dict:
  if not validate_email(request.email):
    # Still report success, to prevent enumeration.
    return {
      "message": "If an account exists with this email, a password reset link has been sent."
    }

  sanitized_email = sanitize_string(request.email, max_length=254)

  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  # A user with no password_hash is IdP-governed: never issue a reset token,
  # so a reset email can't bootstrap a password onto an IdP-owned account.
  # Same generic response either way.
  user = User.get_by_email(sanitized_email, session)

  if user and user.is_active and user.password_hash:
    token = UserToken.create_token(
      user_id=user.id,
      token_type="password_reset",
      hours=PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
      session=session,
      ip_address=client_ip,
      user_agent=user_agent,
    )

    app = detect_app_source(fastapi_request)

    # Queued via Dagster, with retries.
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
    logger.warning(
      f"Password reset refused for email: {sanitized_email} (exists={user is not None})"
    )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.PASSWORD_RESET_REQUESTED,
      user_id=str(user.id) if user else None,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/password/forgot",
      details={
        "email": sanitized_email,
        "user_exists": user is not None,
        "passwordless_account": bool(user and not user.password_hash),
      },
      risk_level="low",
    )

  # Always success (enumeration protection).
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
  _password_auth: None = Depends(require_password_auth),
) -> ResetPasswordValidateResponse:
  user_id = UserToken.validate_token(
    raw_token=token,
    token_type="password_reset",
    session=session,
  )

  if not user_id:
    return ResetPasswordValidateResponse(valid=False, email=None)

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
  _password_auth: None = Depends(require_password_auth),
) -> AuthResponse:
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

  # Backstop to the forgot-request guard for IdP-governed accounts; same
  # response as a bad token.
  if not user.password_hash:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_FAILURE,
      user_id=user.id,
      endpoint="/v1/auth/password/reset",
      details={"failure_reason": "password_reset_on_passwordless_account"},
      risk_level="medium",
    )
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid or expired reset token",
    )

  # Same rules as the /password/check endpoint.
  password_result = PasswordSecurity.validate_password(request.new_password)
  if not password_result.is_valid:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Password requirements not met: {', '.join(password_result.errors)}",
    )

  password_hash = await hash_password_async(request.new_password)

  user.update(session, password_hash=password_hash)

  # Bumping session_version invalidates every existing JWT, including the
  # refresh chain, on its next request.
  try:
    user.invalidate_sessions(session)
  except Exception as invalidate_err:
    logger.error(f"Error invalidating sessions during password reset: {invalidate_err}")

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

  user_info = {
    "id": user.id,
    "name": user.name,
    "email": user.email,
    "email_verified": user.email_verified,
  }

  if not may_issue_session_without_login(session, user):
    logger.info(f"Password reset completed for user {user.email}; sign-in required")
    return AuthResponse(
      user=user_info,
      message="Password reset successfully. Sign in to continue.",
    )

  # Auto-login; picks up the just-bumped session_version.
  device_fingerprint = extract_device_fingerprint(fastapi_request)
  jwt_token = create_jwt_token(user.id, device_fingerprint, session=session)

  logger.info(f"Password reset completed for user {user.email}")

  expires_in = int(JWT_EXPIRY_HOURS * 3600)
  refresh_threshold = int(TOKEN_GRACE_PERIOD_MINUTES * 60)

  return AuthResponse(
    user=user_info,
    message="Password reset successfully. You are now logged in.",
    token=jwt_token,
    expires_in=expires_in,
    refresh_threshold=refresh_threshold,
  )
