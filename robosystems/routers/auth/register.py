"""User registration endpoint."""

from sqlalchemy.orm import Session
from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Request,
  Response,
  status,
)

from ...config import env
from ...models.iam import User, OrgLimits, Org
from ...models.api.auth import RegisterRequest, AuthResponse
from ...models.api.common import ErrorResponse
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.captcha import captcha_service
from ...security.auth_protection import AdvancedAuthProtection
from ...security.input_validation import (
  validate_email,
  sanitize_string,
  validate_password_strength,
)
from ...security.device_fingerprinting import extract_device_fingerprint
from ...middleware.otel.metrics import record_auth_metrics, endpoint_metrics_decorator

from .utils import hash_password
from ...middleware.auth.jwt import create_jwt_token

# Create router for register endpoint
router = APIRouter()


@router.post(
  "/register",
  response_model=AuthResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Register New User",
  description="Register a new user account with email and password. Security controls vary by environment: CAPTCHA and email verification are disabled in development for API testing, but required in production.",
  operation_id="registerUser",
  responses={
    409: {"model": ErrorResponse, "description": "Email already registered"},
    400: {
      "model": ErrorResponse,
      "description": "Invalid request data or missing CAPTCHA token (production only)",
    },
    503: {"model": ErrorResponse, "description": "Registration temporarily disabled"},
  },
)
@endpoint_metrics_decorator("/v1/auth/register", business_event_type="user_registered")
async def register(
  request: RegisterRequest,
  response: Response,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  rate_limit: None = Depends(auth_rate_limit_dependency),
) -> AuthResponse:
  """
  Register a new user account with environment-based security controls.

  Security controls vary by environment:
  - Development: CAPTCHA and email verification disabled for API testing convenience
  - Production: CAPTCHA and email verification required for security

  Args:
      request: Registration request with name, email, password, and optional CAPTCHA token
      response: FastAPI response object for setting cookies
      fastapi_request: FastAPI request object for security logging
      session: Database session
      rate_limit: Rate limiting dependency

  Returns:
      AuthResponse: Success response with user data and environment-appropriate message

  Raises:
      HTTPException: If email is already registered, missing CAPTCHA token (production), or other validation errors
  """
  # Check if registration is enabled
  if not env.USER_REGISTRATION_ENABLED:
    # Get client details for security logging
    client_ip = fastapi_request.client.host if fastapi_request.client else None
    user_agent = fastapi_request.headers.get("user-agent")

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/register",
      details={
        "reason": "registration_disabled",
        "attempted_email": request.email,
      },
      risk_level="low",
    )

    raise HTTPException(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      detail="Registration is temporarily disabled. Please check back later or contact support for early access.",
    )

  # Validate and sanitize input
  if not validate_email(request.email):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid email format"
    )

  # Validate password strength
  password_valid, password_issues = validate_password_strength(request.password)
  if not password_valid:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Password requirements not met: {', '.join(password_issues)}",
    )

  # Sanitize inputs
  sanitized_email = sanitize_string(request.email, max_length=254)
  sanitized_name = sanitize_string(request.name, max_length=100)

  # Record auth attempt
  record_auth_metrics(
    endpoint="/v1/auth/register",
    method="POST",
    auth_type="email_password_registration",
    success=False,  # Will update on success
  )

  # Get client details for security logging
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  # Advanced authentication protection checks
  if client_ip:
    # Check if IP is currently blocked
    is_blocked, block_time = AdvancedAuthProtection.check_ip_blocked(client_ip)
    if is_blocked:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint="/v1/auth/register",
        details={
          "reason": "ip_temporarily_blocked",
          "block_expires_in": block_time,
          "attempted_email": sanitized_email,
        },
        risk_level="high",
      )

      # Add security headers to response
      security_headers = AdvancedAuthProtection.get_security_headers(client_ip)
      for header, value in security_headers.items():
        response.headers[header] = value

      raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many failed attempts. Please try again later.",
        headers=security_headers,
      )

    # Check progressive delay
    delay = AdvancedAuthProtection.get_progressive_delay(client_ip)
    if delay > 0:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.RATE_LIMIT_EXCEEDED,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint="/v1/auth/register",
        details={
          "reason": "progressive_delay_active",
          "delay_seconds": delay,
          "attempted_email": sanitized_email,
        },
        risk_level="medium",
      )

      raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail=f"Please wait {delay} seconds before trying again.",
        headers={"Retry-After": str(delay)},
      )

  # Environment-based security checks - CAPTCHA verification
  captcha_result = await captcha_service.verify_captcha_or_skip(
    token=request.captcha_token, remote_ip=client_ip
  )

  if not captcha_result.success:
    error_details = {
      "error": "captcha_verification_failed",
      "captcha_error_codes": captcha_result.error_codes,
      "environment": env.ENVIRONMENT,
      "attempted_email": request.email,
    }

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.INPUT_VALIDATION_FAILURE,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/register",
      details=error_details,
      risk_level="high",  # Failed CAPTCHA is high risk (potential bot)
    )

    # Return user-friendly error message
    if "missing-input-response" in captcha_result.error_codes:
      detail = "CAPTCHA verification is required for registration"
    else:
      detail = "CAPTCHA verification failed. Please try again."

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)

  # Log successful CAPTCHA verification
  if captcha_service.is_captcha_required():
    logger.info(
      f"CAPTCHA verification successful for registration: {sanitized_email} (Environment: {env.ENVIRONMENT})"
    )
  else:
    logger.info(
      f"CAPTCHA verification skipped for registration: {sanitized_email} (Environment: {env.ENVIRONMENT})"
    )

  # Check if user already exists
  existing_user = User.get_by_email(sanitized_email, session)
  if existing_user:
    record_auth_metrics(
      endpoint="/v1/auth/register",
      method="POST",
      auth_type="email_password_registration",
      success=False,
      failure_reason="email_already_exists",
    )
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_FAILURE,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/register",
      details={
        "failure_reason": "email_already_exists",
        "attempted_email": sanitized_email,
      },
      risk_level="low",
    )
    raise HTTPException(
      status_code=status.HTTP_409_CONFLICT, detail="Email already registered"
    )

  # Hash the password
  password_hash = hash_password(request.password)

  # Use transaction for atomic user creation
  try:
    # Create new user with email verification status based on environment
    user = User.create(
      email=sanitized_email,
      name=sanitized_name,
      password_hash=password_hash,
      session=session,
    )

    # Set email verification status based on environment
    if not env.EMAIL_VERIFICATION_ENABLED:
      # In development, automatically verify emails for convenience
      user.verify_email(session)
      logger.info(
        f"Email automatically verified for development user: {sanitized_email}"
      )
    else:
      # In production, email verification is required
      # TODO: Send email verification email here
      logger.info(f"Email verification required for production user: {sanitized_email}")

    # Create personal organization for the user
    org = Org.create_phantom_org_for_user(
      user_id=user.id,
      user_name=sanitized_name,
      session=session,
    )
    logger.info(f"Created personal org {org.id} for user {sanitized_email}")

    # Create default org limits (safety limits for resource provisioning)
    # Orgs can purchase subscriptions to increase limits
    OrgLimits.create_default_limits(org.id, session)

    # Commit the transaction
    session.commit()

  except Exception as e:
    session.rollback()
    logger.error(f"Failed to create user account: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create user account",
    )

  # Extract device fingerprint for token binding
  device_fingerprint = extract_device_fingerprint(fastapi_request)

  # Create JWT token with device binding
  jwt_token = create_jwt_token(user.id, device_fingerprint)

  # No longer setting auth cookies - using Bearer token authentication
  # Token is returned in the response body for the frontend to store

  # Record successful auth
  record_auth_metrics(
    endpoint="/v1/auth/register",
    method="POST",
    auth_type="email_password_registration",
    success=True,
    user_id=user.id,
  )

  # Log successful registration for security audit
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.AUTH_SUCCESS,
    user_id=user.id,
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/register",
    details={
      "action": "user_registration",
      "email_verified": user.email_verified,
      "captcha_required": env.CAPTCHA_ENABLED,
      "captcha_provided": bool(request.captcha_token),
      "environment": env.ENVIRONMENT,
    },
    risk_level="low",
  )

  # Record successful registration for protection system
  if client_ip:
    AdvancedAuthProtection.record_auth_attempt(
      ip_address=client_ip, success=True, email=sanitized_email, user_agent=user_agent
    )

  # Prepare success message based on email verification requirement
  message = "User registered successfully"
  if env.EMAIL_VERIFICATION_ENABLED and not user.email_verified:
    message += ". Please check your email to verify your account."

  expires_in = int(env.JWT_EXPIRY_HOURS * 3600)
  refresh_threshold = int(env.TOKEN_GRACE_PERIOD_MINUTES * 60)

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
    },
    message=message,
    token=jwt_token,
    expires_in=expires_in,
    refresh_threshold=refresh_threshold,
  )
