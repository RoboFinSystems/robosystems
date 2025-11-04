"""User login endpoint."""

# Third-party
from sqlalchemy.orm import Session
from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Request,
  Response,
  status,
)

# Local imports
from ...models.iam import User
from ...models.api.auth import LoginRequest, AuthResponse
from ...models.api.common import ErrorResponse
from ...database import get_async_db_session
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.auth_protection import AdvancedAuthProtection
from ...security.input_validation import validate_email, sanitize_string
from ...security.device_fingerprinting import extract_device_fingerprint
from ...middleware.otel.metrics import record_auth_metrics, endpoint_metrics_decorator

from .utils import verify_password
from ...middleware.auth.jwt import create_jwt_token

# Create router for login endpoint
router = APIRouter()


@router.post(
  "/login",
  response_model=AuthResponse,
  status_code=status.HTTP_200_OK,
  summary="User Login",
  description="Authenticate user with email and password.",
  operation_id="loginUser",
  responses={
    401: {"model": ErrorResponse, "description": "Invalid credentials"},
    400: {"model": ErrorResponse, "description": "Invalid request data"},
  },
)
@endpoint_metrics_decorator("/v1/auth/login", business_event_type="user_login")
async def login(
  request: LoginRequest,
  response: Response,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  rate_limit: None = Depends(auth_rate_limit_dependency),
) -> AuthResponse:
  """
  Authenticate user with email and password.

  Args:
      request: Login request with email and password
      response: FastAPI response object for setting cookies

  Returns:
      AuthResponse: Success response with user data

  Raises:
      HTTPException: If credentials are invalid
  """
  # Validate and sanitize input
  if not validate_email(request.email):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid email format"
    )

  # Sanitize email to prevent any injection attempts
  sanitized_email = sanitize_string(request.email, max_length=254)

  # Record auth attempt
  record_auth_metrics(
    endpoint="/v1/auth/login",
    method="POST",
    auth_type="email_password_login",
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
        endpoint="/v1/auth/login",
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
        endpoint="/v1/auth/login",
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

  # Find user by email
  user = User.get_by_email(sanitized_email, session)
  if not user or not user.password_hash or not user.is_active:
    # Record failed attempt for protection system
    if client_ip:
      AdvancedAuthProtection.record_auth_attempt(
        ip_address=client_ip,
        success=False,
        email=sanitized_email,
        user_agent=user_agent,
      )
      AdvancedAuthProtection.apply_progressive_delay(client_ip)

    record_auth_metrics(
      endpoint="/v1/auth/login",
      method="POST",
      auth_type="email_password_login",
      success=False,
      failure_reason="user_not_found_or_inactive",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password"
    )

  # Verify password
  if not verify_password(request.password, user.password_hash):
    # Record failed attempt for protection system
    if client_ip:
      AdvancedAuthProtection.record_auth_attempt(
        ip_address=client_ip,
        success=False,
        email=sanitized_email,
        user_agent=user_agent,
      )
      AdvancedAuthProtection.apply_progressive_delay(client_ip)

    record_auth_metrics(
      endpoint="/v1/auth/login",
      method="POST",
      auth_type="email_password_login",
      success=False,
      failure_reason="invalid_password",
      user_id=user.id,
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password"
    )

  # Extract device fingerprint for token binding
  device_fingerprint = extract_device_fingerprint(fastapi_request)

  # Create JWT token with device binding
  jwt_token = create_jwt_token(str(user.id), device_fingerprint)

  # No longer setting auth cookies - using Bearer token authentication instead
  # This enables proper cross-domain authentication for all three domains

  # Record successful auth
  record_auth_metrics(
    endpoint="/v1/auth/login",
    method="POST",
    auth_type="email_password_login",
    success=True,
    user_id=user.id,
  )

  # Record successful login for protection system
  if client_ip:
    AdvancedAuthProtection.record_auth_attempt(
      ip_address=client_ip, success=True, email=sanitized_email, user_agent=user_agent
    )

  # Calculate token expiry and refresh threshold
  from ...config import env

  expires_in = int(env.JWT_EXPIRY_HOURS * 3600)  # Convert hours to seconds
  refresh_threshold = env.TOKEN_GRACE_PERIOD_MINUTES * 60  # Convert minutes to seconds

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
    },
    message="Login successful",
    token=jwt_token,  # Return JWT for Bearer authentication
    expires_in=expires_in,  # Token expires in 30 minutes (1800 seconds)
    refresh_threshold=refresh_threshold,  # Refresh 5 minutes before expiry (300 seconds)
  )
