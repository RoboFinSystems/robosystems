"""User login endpoint."""

# Third-party
from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Request,
  Response,
  status,
)
from sqlalchemy.orm import Session

from ...config import env
from ...database import get_async_db_session
from ...middleware.auth.jwt import create_jwt_token, create_mfa_token
from ...middleware.otel.metrics import endpoint_metrics_decorator, record_auth_metrics
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...models.api.auth import AuthResponse, LoginRequest
from ...models.api.common import COMMON_ERROR_RESPONSES, ErrorResponse

# Local imports
from ...models.core import User
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.auth_protection import AdvancedAuthProtection
from ...security.device_fingerprinting import extract_device_fingerprint
from ...security.input_validation import sanitize_string, validate_email
from ...security.password import PasswordSecurity
from .utils import require_password_auth, verify_password_async

# Create router for login endpoint
router = APIRouter()


@router.post(
  "/login",
  response_model=AuthResponse,
  summary="User Login",
  description="Returns a JWT token on success. IP-based progressive delays apply after repeated failures.",
  operation_id="loginUser",
  responses={
    **COMMON_ERROR_RESPONSES,
    401: {"model": ErrorResponse, "description": "Invalid credentials"},
  },
)
@endpoint_metrics_decorator("/v1/auth/login", business_event_type="user_login")
async def login(
  request: LoginRequest,
  response: Response,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  rate_limit: None = Depends(auth_rate_limit_dependency),
  _password_auth: None = Depends(require_password_auth),
) -> AuthResponse:
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
    # Burn one bcrypt verification so this branch costs the same wall-clock
    # as a wrong password against a real account. The generic message below
    # is only generic if the timing is too.
    await PasswordSecurity.equalize_verify_timing()

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

  if not await verify_password_async(request.password, user.password_hash):
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

  # Passkey MFA interposes exactly here: password verified, session not yet
  # minted. Enrolled users always get challenged (enrolling opts you in);
  # unenrolled privileged users are forced through enrollment only when the
  # enforcement flag is on. Flag off → this block is inert and login is
  # byte-identical to the pre-passkey behavior. The OIDC lane never passes
  # through this endpoint, so IdP-governed sessions are never challenged.
  if env.PASSKEYS_ENABLED:
    from ...models.core import UserPasskey
    from ...operations.passkeys import user_requires_mfa_enrollment

    mfa_status_value: str | None = None
    mfa_purpose = "login"
    if UserPasskey.count_for_user(str(user.id), session) > 0:
      mfa_status_value = "mfa_required"
    elif user_requires_mfa_enrollment(session, user):
      mfa_status_value = "mfa_enrollment_required"
      mfa_purpose = "enroll"

    if mfa_status_value is not None:
      mfa_token, _jti = create_mfa_token(str(user.id), mfa_purpose, session=session)
      record_auth_metrics(
        endpoint="/v1/auth/login",
        method="POST",
        auth_type="email_password_login",
        success=True,
        user_id=user.id,
      )
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.MFA_CHALLENGE_ISSUED,
        user_id=str(user.id),
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint="/v1/auth/login",
        details={"status": mfa_status_value},
        risk_level="low",
      )
      return AuthResponse(
        user={
          "id": user.id,
          "name": user.name,
          "email": user.email,
          "email_verified": user.email_verified,
        },
        message="Additional verification required",
        status=mfa_status_value,  # type: ignore[arg-type]
        mfa_token=mfa_token,
      )

  # Extract device fingerprint for token binding
  device_fingerprint = extract_device_fingerprint(fastapi_request)

  # Create JWT token with device binding
  jwt_token = create_jwt_token(str(user.id), device_fingerprint, session=session)

  # Bearer-token auth rather than a cookie, so the same token works across all
  # three app domains.

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

  # Audit symmetry with the OIDC and passkey lanes, which already log this.
  SecurityAuditLogger.log_auth_success(
    user_id=str(user.id),
    ip_address=client_ip,
    user_agent=user_agent,
    auth_method="password",
  )

  # Calculate token expiry and refresh threshold
  from ...config.constants import JWT_EXPIRY_HOURS, TOKEN_GRACE_PERIOD_MINUTES

  expires_in = int(JWT_EXPIRY_HOURS * 3600)
  refresh_threshold = int(TOKEN_GRACE_PERIOD_MINUTES * 60)

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
      "email_verified": user.email_verified,
    },
    message="Login successful",
    token=jwt_token,  # Return JWT for Bearer authentication
    expires_in=expires_in,  # Token expires in 30 minutes (1800 seconds)
    refresh_threshold=refresh_threshold,  # Refresh 5 minutes before expiry (300 seconds)
  )
