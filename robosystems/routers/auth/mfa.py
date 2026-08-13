"""MFA second-factor handshake and status endpoints.

Login (password verified, passkey enrolled) returns ``mfa_required`` with a
short-lived purpose-scoped token; this router redeems it. The token is
decoded directly (never accepted as a bearer), its ``jti`` carries a retry
budget and single-use state in Valkey, and every verification failure feeds
the same progressive-delay machinery as password failures — recovery codes
are brute-forceable, assertions are not, both get the same accounting.
"""

import json

from fastapi import (
  APIRouter,
  Depends,
  HTTPException,
  Request,
  Response,
  status,
)
from sqlalchemy.orm import Session

from ...config.valkey_registry import ValkeyDatabase, create_redis_client
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.auth.jwt import (
  MFA_TOKEN_EXPIRY_SECONDS,
  create_jwt_token,
  decode_mfa_token,
)
from ...middleware.otel.metrics import endpoint_metrics_decorator, record_auth_metrics
from ...middleware.rate_limits import (
  mfa_rate_limit_dependency,
  passkey_management_rate_limit_dependency,
)
from ...models.api.auth import AuthResponse
from ...models.api.common import COMMON_ERROR_RESPONSES, ErrorResponse
from ...models.api.passkeys import (
  CeremonyOptionsResponse,
  MfaOptionsRequest,
  MfaStatusResponse,
  MfaVerifyRequest,
  RecoveryCodesRequest,
  RecoveryCodesResponse,
)
from ...models.core import User
from ...operations import passkeys as passkey_ops
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.auth_protection import AdvancedAuthProtection
from ...security.device_fingerprinting import extract_device_fingerprint
from .utils import require_passkeys_enabled

router = APIRouter()

MFA_MAX_FAILURES = 5

_USED_KEY_PREFIX = "mfa:used:"
_FAIL_KEY_PREFIX = "mfa:fail:"


def _mfa_token_burned(jti: str) -> bool:
  """Whether this token already minted a session or exhausted its retries.

  Fails closed: an unreachable store reads as burned.
  """
  try:
    client = create_redis_client(ValkeyDatabase.AUTH)
    if client.get(f"{_USED_KEY_PREFIX}{jti}"):
      return True
    failures = client.get(f"{_FAIL_KEY_PREFIX}{jti}")
    return failures is not None and int(failures) >= MFA_MAX_FAILURES
  except Exception as exc:
    logger.error(f"Failed to read MFA token state: {exc}")
    return True


def _mark_mfa_token_used(jti: str) -> None:
  try:
    client = create_redis_client(ValkeyDatabase.AUTH)
    client.setex(f"{_USED_KEY_PREFIX}{jti}", MFA_TOKEN_EXPIRY_SECONDS, "1")
  except Exception as exc:
    # The token still dies at its 5-minute exp; losing single-use narrowing
    # is logged, not fatal.
    logger.error(f"Failed to mark MFA token used: {exc}")


def _record_mfa_failure(jti: str) -> None:
  try:
    client = create_redis_client(ValkeyDatabase.AUTH)
    key = f"{_FAIL_KEY_PREFIX}{jti}"
    count = client.incr(key)
    if count == 1:
      client.expire(key, MFA_TOKEN_EXPIRY_SECONDS)
  except Exception as exc:
    logger.error(f"Failed to record MFA failure: {exc}")


def _invalid_mfa_token() -> HTTPException:
  return HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid or expired MFA token",
  )


def _resolve_mfa_principal(
  mfa_token: str, expected_purpose: str, session: Session
) -> tuple[User, str]:
  """Decode an MFA token and resolve its live user, or raise a generic 401.

  Mirrors the SSO completion path's re-checks: the user must still exist,
  still be active, and still be on the session_version the token was minted
  against (a password change mid-handshake kills the flow).
  """
  payload = decode_mfa_token(mfa_token, expected_purpose)
  if payload is None:
    raise _invalid_mfa_token()
  jti = str(payload["jti"])
  if _mfa_token_burned(jti):
    raise _invalid_mfa_token()
  user = User.get_by_id(str(payload["user_id"]), session)
  if user is None or not bool(user.is_active):
    raise _invalid_mfa_token()
  if int(payload.get("session_version", -1)) != int(user.session_version):
    raise _invalid_mfa_token()
  return user, jti


def _apply_login_preamble(
  fastapi_request: Request, response: Response, endpoint: str
) -> tuple[str | None, str | None]:
  """The login endpoint's IP block / progressive delay gate, shared here."""
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")
  if client_ip:
    is_blocked, block_time = AdvancedAuthProtection.check_ip_blocked(client_ip)
    if is_blocked:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={"reason": "ip_temporarily_blocked", "block_expires_in": block_time},
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
      raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail=f"Please wait {delay} seconds before trying again.",
        headers={"Retry-After": str(delay)},
      )
  return client_ip, user_agent


def _record_verify_failure(
  jti: str,
  user: User,
  client_ip: str | None,
  user_agent: str | None,
  reason: str,
) -> None:
  _record_mfa_failure(jti)
  if client_ip:
    AdvancedAuthProtection.record_auth_attempt(
      ip_address=client_ip,
      success=False,
      email=str(user.email),
      user_agent=user_agent,
    )
    AdvancedAuthProtection.apply_progressive_delay(client_ip)
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.MFA_FAILED,
    user_id=str(user.id),
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/mfa/verify",
    details={"reason": reason},
    risk_level="medium",
  )


def _authenticated_response(user: User, jwt_token: str) -> AuthResponse:
  from ...config.constants import JWT_EXPIRY_HOURS, TOKEN_GRACE_PERIOD_MINUTES

  return AuthResponse(
    user={
      "id": user.id,
      "name": user.name,
      "email": user.email,
      "email_verified": user.email_verified,
    },
    message="Login successful",
    token=jwt_token,
    expires_in=int(JWT_EXPIRY_HOURS * 3600),
    refresh_threshold=int(TOKEN_GRACE_PERIOD_MINUTES * 60),
  )


@router.post(
  "/mfa/options",
  response_model=CeremonyOptionsResponse,
  summary="MFA Assertion Options",
  description="Exchange an mfa_required login token for passkey assertion options.",
  operation_id="getMfaOptions",
  responses={
    **COMMON_ERROR_RESPONSES,
    401: {"model": ErrorResponse, "description": "Invalid or expired MFA token"},
  },
)
async def get_mfa_options(
  request: MfaOptionsRequest,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  rate_limit: None = Depends(mfa_rate_limit_dependency),
  _passkeys: None = Depends(require_passkeys_enabled),
) -> CeremonyOptionsResponse:
  user, jti = _resolve_mfa_principal(request.mfa_token, "login", session)
  try:
    options = passkey_ops.begin_authentication(session, user, jti)
  except passkey_ops.PasskeyError:
    raise _invalid_mfa_token()

  client_ip = fastapi_request.client.host if fastapi_request.client else None
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.MFA_CHALLENGE_ISSUED,
    user_id=str(user.id),
    ip_address=client_ip,
    endpoint="/v1/auth/mfa/options",
    details={},
    risk_level="low",
  )
  return CeremonyOptionsResponse(options=json.loads(options.options_json))


@router.post(
  "/mfa/verify",
  response_model=AuthResponse,
  summary="MFA Verify",
  description="Complete the second factor with a passkey assertion or a recovery code.",
  operation_id="verifyMfa",
  responses={
    **COMMON_ERROR_RESPONSES,
    401: {"model": ErrorResponse, "description": "Verification failed"},
  },
)
@endpoint_metrics_decorator("/v1/auth/mfa/verify", business_event_type="user_login")
async def verify_mfa(
  request: MfaVerifyRequest,
  response: Response,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  rate_limit: None = Depends(mfa_rate_limit_dependency),
  _passkeys: None = Depends(require_passkeys_enabled),
) -> AuthResponse:
  client_ip, user_agent = _apply_login_preamble(
    fastapi_request, response, "/v1/auth/mfa/verify"
  )
  user, jti = _resolve_mfa_principal(request.mfa_token, "login", session)

  if request.assertion is not None:
    auth_method = "password+passkey"
    try:
      passkey_ops.complete_authentication(
        session,
        request.assertion,
        expected_flow="mfa",
        expected_user_id=str(user.id),
        expected_jti=jti,
      )
    except passkey_ops.PasskeyError as exc:
      _record_verify_failure(jti, user, client_ip, user_agent, type(exc).__name__)
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="MFA verification failed",
      )
  else:
    auth_method = "password+recovery_code"
    try:
      passkey_ops.consume_recovery_code(session, user, request.recovery_code or "")
    except passkey_ops.RecoveryCodeInvalidError:
      _record_verify_failure(jti, user, client_ip, user_agent, "recovery_code_invalid")
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="MFA verification failed",
      )
    remaining = passkey_ops.mfa_status(session, user).recovery_codes_remaining
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.MFA_RECOVERY_USED,
      user_id=str(user.id),
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/mfa/verify",
      details={"codes_remaining": remaining},
      risk_level="medium",
    )

  _mark_mfa_token_used(jti)
  device_fingerprint = extract_device_fingerprint(fastapi_request)
  jwt_token = create_jwt_token(str(user.id), device_fingerprint, session=session)

  record_auth_metrics(
    endpoint="/v1/auth/mfa/verify",
    method="POST",
    auth_type="mfa_verify",
    success=True,
    user_id=str(user.id),
  )
  if client_ip:
    AdvancedAuthProtection.record_auth_attempt(
      ip_address=client_ip,
      success=True,
      email=str(user.email),
      user_agent=user_agent,
    )
  SecurityAuditLogger.log_auth_success(
    user_id=str(user.id),
    ip_address=client_ip,
    user_agent=user_agent,
    auth_method=auth_method,
  )
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.MFA_VERIFIED,
    user_id=str(user.id),
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/mfa/verify",
    details={"auth_method": auth_method},
    risk_level="low",
  )
  return _authenticated_response(user, jwt_token)


@router.get(
  "/mfa/status",
  response_model=MfaStatusResponse,
  summary="MFA Status",
  description="The authenticated user's MFA posture for account settings.",
  operation_id="getMfaStatus",
  responses=COMMON_ERROR_RESPONSES,
)
async def get_mfa_status(
  session: Session = Depends(get_async_db_session),
  _passkeys: None = Depends(require_passkeys_enabled),
  rate_limit: None = Depends(passkey_management_rate_limit_dependency),
  user: User = Depends(get_current_user),
) -> MfaStatusResponse:
  status_result = passkey_ops.mfa_status(session, user)
  return MfaStatusResponse(
    passkey_count=status_result.passkey_count,
    recovery_codes_remaining=status_result.recovery_codes_remaining,
    enforcement_applies=status_result.enforcement_applies,
  )


@router.post(
  "/mfa/recovery-codes/regenerate",
  response_model=RecoveryCodesResponse,
  summary="Regenerate Recovery Codes",
  description="Replace the recovery-code set after re-authentication; codes are shown once.",
  operation_id="regenerateMfaRecoveryCodes",
  responses={
    **COMMON_ERROR_RESPONSES,
    401: {"model": ErrorResponse, "description": "Re-authentication failed"},
  },
)
async def regenerate_recovery_codes(
  request: RecoveryCodesRequest,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  _passkeys: None = Depends(require_passkeys_enabled),
  rate_limit: None = Depends(passkey_management_rate_limit_dependency),
  user: User = Depends(get_current_user),
) -> RecoveryCodesResponse:
  try:
    passkey_ops.verify_reauth(
      session, user, password=request.password, assertion=request.assertion
    )
  except passkey_ops.ReauthInvalidError:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Re-authentication failed",
    )
  codes = passkey_ops.generate_recovery_codes(session, user)
  logger.info(f"Recovery codes regenerated for user {user.id}")
  return RecoveryCodesResponse(codes=codes)
