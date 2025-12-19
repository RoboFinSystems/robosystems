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
  run_and_monitor_dagster_job,
  build_email_job_config,
)
from ...config import env
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...models.api.auth import (
  AuthResponse,
  ForgotPasswordRequest,
  ResetPasswordRequest,
  ResetPasswordValidateResponse,
)
from ...models.api.common import ErrorResponse
from ...models.iam import User, UserToken
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.input_validation import (
  validate_email,
  sanitize_string,
  validate_password_strength,
)

from .utils import detect_app_source, hash_password
from ...middleware.auth.jwt import create_jwt_token, revoke_jwt_token

# Create router for password reset endpoints
router = APIRouter()


@router.post(
  "/password/forgot",
  status_code=status.HTTP_200_OK,
  summary="Forgot Password",
  description="Request password reset email. Always returns success to prevent email enumeration.",
  operation_id="forgotPassword",
  responses={
    429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
  },
)
async def forgot_password(
  request: ForgotPasswordRequest,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(auth_rate_limit_dependency),
) -> dict:
  """
  Request password reset email.

  Args:
      request: Forgot password request with email
      fastapi_request: FastAPI request object
      background_tasks: FastAPI background tasks for async email
      session: Database session
      _rate_limit: Rate limiting dependency

  Returns:
      Success message (always, for security)
  """
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
      hours=env.PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
      session=session,
      ip_address=client_ip,
      user_agent=user_agent,
    )

    # Detect app source
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

    # Log security event
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
  status_code=status.HTTP_200_OK,
  summary="Validate Reset Token",
  description="Check if a password reset token is valid without consuming it.",
  operation_id="validateResetToken",
)
async def validate_reset_token(
  token: str = Query(..., description="Password reset token"),
  session: Session = Depends(get_async_db_session),
) -> ResetPasswordValidateResponse:
  """
  Validate a password reset token without consuming it.

  Args:
      token: Reset token from query parameter
      session: Database session

  Returns:
      Validation response with masked email if valid
  """
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
  status_code=status.HTTP_200_OK,
  summary="Reset Password",
  description="Reset password with token from email. Returns JWT for auto-login.",
  operation_id="resetPassword",
  responses={
    400: {"model": ErrorResponse, "description": "Invalid token or password"},
  },
)
async def reset_password(
  request: ResetPasswordRequest,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
) -> AuthResponse:
  """
  Reset password with token.

  Args:
      request: Reset password request with token and new password
      fastapi_request: FastAPI request object
      session: Database session

  Returns:
      Auth response with JWT token for auto-login

  Raises:
      HTTPException: If token is invalid or password doesn't meet requirements
  """
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

  # Get user
  user = User.get_by_id(user_id, session)
  if not user:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="User not found",
    )

  # Validate password strength
  password_valid, password_issues = validate_password_strength(request.new_password)
  if not password_valid:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Password requirements not met: {', '.join(password_issues)}",
    )

  # Hash new password
  password_hash = hash_password(request.new_password)

  # Update user's password
  user.update(session, password_hash=password_hash)

  # Revoke all existing JWT tokens for this user
  try:
    # Get all active tokens for this user
    # This implementation assumes you have a method to fetch active user tokens
    active_tokens = UserToken.get_active_tokens_for_user(user.id, session)

    for token in active_tokens:
      revoke_jwt_token(token.token, reason="password_reset")
  except Exception as revoke_err:
    logger.error(f"Error revoking tokens during password reset: {revoke_err}")
    # Log the error but continue with the password reset

  # Get client details for security logging
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  # Log security event
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.PASSWORD_RESET_COMPLETED,
    user_id=user.id,
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/password/reset",
    details={},
    risk_level="high",
  )

  # Generate new JWT token for auto-login
  jwt_token = create_jwt_token(user.id)

  logger.info(f"Password reset completed for user {user.email}")

  expires_in = int(env.JWT_EXPIRY_HOURS * 3600)
  refresh_threshold = int(env.TOKEN_GRACE_PERIOD_MINUTES * 60)

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
    },
    message="Password reset successfully. You are now logged in.",
    token=jwt_token,
    expires_in=expires_in,
    refresh_threshold=refresh_threshold,
  )
