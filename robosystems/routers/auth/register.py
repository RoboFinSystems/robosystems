"""User registration endpoint."""

from fastapi import (
  APIRouter,
  BackgroundTasks,
  Depends,
  HTTPException,
  Request,
  Response,
  status,
)
from sqlalchemy.orm import Session

from ...config import env
from ...config.constants import (
  JWT_EXPIRY_HOURS,
  TOKEN_GRACE_PERIOD_MINUTES,
)
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.auth.jwt import create_jwt_token
from ...middleware.otel.metrics import endpoint_metrics_decorator, record_auth_metrics
from ...middleware.rate_limits import auth_rate_limit_dependency
from ...middleware.sse import build_email_job_config, run_and_monitor_dagster_job
from ...models.api.auth import AuthResponse, RegisterRequest
from ...models.api.common import COMMON_ERROR_RESPONSES, ErrorResponse
from ...models.core import Org, OrgInvitation, User
from ...operations.user_provisioning import (
  EmailAlreadyRegisteredError,
  provision_user,
)
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.auth_protection import AdvancedAuthProtection
from ...security.captcha import captcha_service
from ...security.device_fingerprinting import extract_device_fingerprint
from ...security.input_validation import (
  sanitize_string,
  validate_email,
)
from ...security.password import PasswordSecurity
from .utils import detect_app_source, hash_password_async, require_password_auth

router = APIRouter()


@router.post(
  "/register",
  response_model=AuthResponse,
  status_code=status.HTTP_201_CREATED,
  summary="Register New User",
  description="Creates the user and a personal organization. CAPTCHA required in production. Sends verification email when email verification is enabled.",
  operation_id="registerUser",
  responses={
    **COMMON_ERROR_RESPONSES,
    409: {"model": ErrorResponse, "description": "Email already registered"},
    503: {"model": ErrorResponse, "description": "Registration temporarily disabled"},
  },
)
@endpoint_metrics_decorator("/v1/auth/register", business_event_type="user_registered")
async def register(
  request: RegisterRequest,
  response: Response,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  session: Session = Depends(get_async_db_session),
  rate_limit: None = Depends(auth_rate_limit_dependency),
  _password_auth: None = Depends(require_password_auth),
) -> AuthResponse:
  # Closed registration still admits invitations (invite-only mode). The
  # password-auth guard above outranks this, so an invitation can't become a
  # side door to a password account on an IdP deployment. The token is
  # resolved here, not just checked for presence, and fully re-validated below.
  if not env.USER_REGISTRATION_ENABLED:
    has_valid_invitation = (
      request.invite_token is not None
      and OrgInvitation.get_valid_by_token(request.invite_token, session) is not None
    )

    if not has_valid_invitation:
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
          "invitation_presented": request.invite_token is not None,
        },
        risk_level="low",
      )

      raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Registration is temporarily disabled. Please check back later or contact support for early access.",
      )

  if not validate_email(request.email):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid email format"
    )

  # Same rules as the /password/check endpoint.
  password_result = PasswordSecurity.validate_password(request.password, request.email)
  if not password_result.is_valid:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Password requirements not met: {', '.join(password_result.errors)}",
    )

  sanitized_email = sanitize_string(request.email, max_length=254)
  sanitized_name = sanitize_string(request.name, max_length=100)

  record_auth_metrics(
    endpoint="/v1/auth/register",
    method="POST",
    auth_type="email_password_registration",
    success=False,  # Will update on success
  )

  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  if client_ip:
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

      security_headers = AdvancedAuthProtection.get_security_headers(client_ip)
      for header, value in security_headers.items():
        response.headers[header] = value

      raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many failed attempts. Please try again later.",
        headers=security_headers,
      )

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

    if "missing-input-response" in captcha_result.error_codes:
      detail = "CAPTCHA verification is required for registration"
    else:
      detail = "CAPTCHA verification failed. Please try again."

    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)

  if captcha_service.is_captcha_required():
    logger.info(
      f"CAPTCHA verification successful for registration: {sanitized_email} (Environment: {env.ENVIRONMENT})"
    )
  else:
    logger.info(
      f"CAPTCHA verification skipped for registration: {sanitized_email} (Environment: {env.ENVIRONMENT})"
    )

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

  # Resolve the invitation before creating anything: a personal-org fallback
  # would strand the user outside the inviting org for good (one org per user).
  invitation = None
  invited_org = None
  if request.invite_token:
    invitation = OrgInvitation.get_valid_by_token(request.invite_token, session)
    if invitation is not None:
      invited_org = Org.get_by_id(invitation.org_id, session)

    if invitation is None or invited_org is None:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_FAILURE,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint="/v1/auth/register",
        details={
          "failure_reason": "invalid_invitation_token",
          "attempted_email": sanitized_email,
        },
        risk_level="medium",
      )
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
          "This invitation link is invalid or has expired. "
          "Ask your organization admin to send a new invitation."
        ),
      )

    if invitation.email != sanitized_email.lower():
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="This invitation was issued for a different email address.",
      )

  password_hash = await hash_password_async(request.password)

  # The invitation token proves control of the mailbox, and dev skips
  # verification; only the remaining case sends a verification email.
  needs_verification_email = invitation is None and env.EMAIL_VERIFICATION_ENABLED

  try:
    provisioned = provision_user(
      session,
      email=sanitized_email,
      name=sanitized_name,
      password_hash=password_hash,
      email_verified=not needs_verification_email,
      invitation=invitation,
      invited_org=invited_org,
      create_verification_token=needs_verification_email,
      ip_address=client_ip,
      user_agent=user_agent,
    )
  except EmailAlreadyRegisteredError:
    # Race backstop; the pre-check above already audited the common case.
    raise HTTPException(
      status_code=status.HTTP_409_CONFLICT, detail="Email already registered"
    )
  except Exception as e:
    logger.error(f"Failed to create user account: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create user account",
    )

  user = provisioned.user
  org = provisioned.org

  if provisioned.verification_token is not None:
    app = detect_app_source(fastapi_request)

    run_config = build_email_job_config(
      email_type="email_verification",
      to_email=sanitized_email,
      user_name=sanitized_name,
      token=provisioned.verification_token,
      app=app,
    )
    background_tasks.add_task(
      run_and_monitor_dagster_job,
      job_name="send_email_job",
      operation_id=None,
      run_config=run_config,
    )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.EMAIL_SENT,
      user_id=user.id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/register",
      details={"email_type": "verification", "app_source": app},
      risk_level="low",
    )

    logger.info(f"Queued verification email for new user: {sanitized_email}")
  elif invitation is not None:
    logger.info(f"Email verified via invitation token for: {sanitized_email}")
  else:
    logger.info(f"Email automatically verified for development user: {sanitized_email}")

  if invitation is not None:
    # The joiner is the actor; the inviter is carried so the grant chain is
    # reconstructible from this one record.
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.ORG_MEMBER_ADDED,
      user_id=user.id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/register",
      details={
        "action": "org_member_added",
        "org_id": org.id,
        "target_user_id": user.id,
        "new_role": invitation.role.value,
        "via": "invitation",
        "invitation_id": invitation.id,
        "invited_by": invitation.invited_by,
      },
      risk_level="low",
    )
  else:
    logger.info(
      f"Created personal org '{org.name}' ({org.id}) for user {sanitized_email}",
      extra={"user_id": user.id, "org_id": org.id, "org_type": org.org_type.value},
    )

  device_fingerprint = extract_device_fingerprint(fastapi_request)

  jwt_token = create_jwt_token(user.id, device_fingerprint, session=session)

  # Bearer-token auth: the token goes in the response body; no auth cookie.

  record_auth_metrics(
    endpoint="/v1/auth/register",
    method="POST",
    auth_type="email_password_registration",
    success=True,
    user_id=user.id,
  )

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
      "invited": invitation is not None,
      "environment": env.ENVIRONMENT,
    },
    risk_level="low",
  )

  if client_ip:
    AdvancedAuthProtection.record_auth_attempt(
      ip_address=client_ip, success=True, email=sanitized_email, user_agent=user_agent
    )

  if invitation is not None:
    message = f"User registered successfully and joined {org.name}"
  else:
    message = "User registered successfully"
    if env.EMAIL_VERIFICATION_ENABLED and not user.email_verified:
      message += ". Please check your email to verify your account."

  expires_in = int(JWT_EXPIRY_HOURS * 3600)
  refresh_threshold = int(TOKEN_GRACE_PERIOD_MINUTES * 60)

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
      "email_verified": user.email_verified,
    },
    org={
      "id": org.id,
      "name": org.name,
      "type": org.org_type.value,
    },
    message=message,
    token=jwt_token,
    expires_in=expires_in,
    refresh_threshold=refresh_threshold,
  )
