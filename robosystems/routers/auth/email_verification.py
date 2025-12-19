"""Email verification endpoints."""

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from robosystems.middleware.sse import (
  run_and_monitor_dagster_job,
  build_email_job_config,
)
from ...config import env
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...models.api.auth import AuthResponse, EmailVerificationRequest
from ...models.api.common import ErrorResponse
from ...models.iam import User, UserToken
from ...security import SecurityAuditLogger, SecurityEventType

from .utils import detect_app_source
from ...middleware.auth.jwt import verify_jwt_token, create_jwt_token

# Create router for email verification endpoints
router = APIRouter()


async def get_current_user_for_email_verification(
  request: Request,
  session: Session = Depends(get_async_db_session),
) -> User:
  """
  Get the authenticated user for email verification endpoints.

  This is a local version to avoid circular imports with middleware.auth.dependencies.

  Args:
      request: FastAPI request object
      session: Database session

  Returns:
      User: The authenticated user

  Raises:
      HTTPException: If authentication fails
  """
  # Extract JWT token from Authorization header (doesn't show in OpenAPI params)
  authorization = request.headers.get("authorization")

  if not authorization or not authorization.startswith("Bearer "):
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Authentication required",
      headers={"WWW-Authenticate": "Bearer"},
    )

  jwt_token = authorization[7:]  # Remove "Bearer " prefix
  user_id = verify_jwt_token(jwt_token)

  if not user_id:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers={"WWW-Authenticate": "Bearer"},
    )

  user = User.get_by_id(user_id, session)
  if not user or not user.is_active:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="User not found or inactive",
    )

  # Log successful authentication
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  SecurityAuditLogger.log_auth_success(
    user_id=str(user_id),
    ip_address=client_ip,
    user_agent=user_agent,
    auth_method="jwt_token",
  )

  return user


@router.post(
  "/email/resend",
  status_code=status.HTTP_200_OK,
  summary="Resend Email Verification",
  description="Resend verification email to the authenticated user. Rate limited to 3 per hour.",
  operation_id="resendVerificationEmail",
  responses={
    400: {"model": ErrorResponse, "description": "Email already verified"},
    429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
    503: {"model": ErrorResponse, "description": "Email service unavailable"},
  },
)
async def resend_verification_email(
  request: Request,
  background_tasks: BackgroundTasks,
  current_user: User = Depends(get_current_user_for_email_verification),
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(auth_rate_limit_dependency),
) -> dict:
  """
  Resend email verification link to the authenticated user.

  Args:
      request: FastAPI request object
      background_tasks: FastAPI background tasks for async email
      current_user: Currently authenticated user
      session: Database session
      _rate_limit: Rate limiting dependency

  Returns:
      Success message

  Raises:
      HTTPException: If email is already verified
  """
  # Check if already verified
  if current_user.email_verified:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Email is already verified",
    )

  # Get client details for token creation
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")

  # Generate new verification token
  token = UserToken.create_token(
    user_id=current_user.id,
    token_type="email_verification",
    hours=env.EMAIL_TOKEN_EXPIRY_HOURS,
    session=session,
    ip_address=client_ip,
    user_agent=user_agent,
  )

  # Detect app source from request
  app = detect_app_source(request)

  # Queue verification email via Dagster (async with retry logic)
  run_config = build_email_job_config(
    email_type="email_verification",
    to_email=current_user.email,
    user_name=current_user.name,
    token=token,
    app=app,
  )
  background_tasks.add_task(
    run_and_monitor_dagster_job,
    job_name="send_email_job",
    operation_id=None,  # No SSE tracking needed for auth emails
    run_config=run_config,
  )

  # Log security event
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.EMAIL_SENT,
    user_id=current_user.id,
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/email/resend",
    details={
      "email_type": "verification",
      "app_source": app,
    },
    risk_level="low",
  )

  logger.info(f"Queued verification email to {current_user.email}")
  return {"message": "Verification email sent. Please check your inbox."}


@router.post(
  "/email/verify",
  response_model=AuthResponse,
  status_code=status.HTTP_200_OK,
  summary="Verify Email",
  description="Verify email address with token from email link. Returns JWT for auto-login.",
  operation_id="verifyEmail",
  responses={
    400: {"model": ErrorResponse, "description": "Invalid or expired token"},
  },
)
async def verify_email(
  request: EmailVerificationRequest,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  session: Session = Depends(get_async_db_session),
) -> AuthResponse:
  """
  Verify email address with token.

  Args:
      request: Email verification request with token
      fastapi_request: FastAPI request object
      background_tasks: FastAPI background tasks for async email
      session: Database session

  Returns:
      Auth response with JWT token for auto-login

  Raises:
      HTTPException: If token is invalid or expired
  """
  # Verify token and get user
  user_id = UserToken.verify_token(
    raw_token=request.token,
    token_type="email_verification",
    session=session,
  )

  if not user_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Invalid or expired verification token",
    )

  # Get user
  user = User.get_by_id(user_id, session)
  if not user:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="User not found",
    )

  # Mark email as verified
  user.verify_email(session)

  # Get client details for security logging
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  # Detect app source
  app = detect_app_source(fastapi_request)

  # Queue welcome email via Dagster (async with retry logic)
  run_config = build_email_job_config(
    email_type="welcome",
    to_email=user.email,
    user_name=user.name,
    app=app,
  )
  background_tasks.add_task(
    run_and_monitor_dagster_job,
    job_name="send_email_job",
    operation_id=None,  # No SSE tracking needed for auth emails
    run_config=run_config,
  )

  # Log security event
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.EMAIL_VERIFIED,
    user_id=user.id,
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/email/verify",
    details={
      "app_source": app,
    },
    risk_level="low",
  )

  # Generate JWT token for auto-login
  jwt_token = create_jwt_token(user.id)

  logger.info(f"Email verified for user {user.email}")

  expires_in = int(env.JWT_EXPIRY_HOURS * 3600)
  refresh_threshold = int(env.TOKEN_GRACE_PERIOD_MINUTES * 60)

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
    },
    message="Email verified successfully. Welcome to RoboSystems!",
    token=jwt_token,
    expires_in=expires_in,
    refresh_threshold=refresh_threshold,
  )
