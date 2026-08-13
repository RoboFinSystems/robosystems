"""Passkey lifecycle and passwordless login endpoints.

Enrollment accepts two principals: an authenticated session (the settings
flow) or an ``enroll``-purpose MFA token (the forced-enrollment lane, where
the login refused to mint a session until a passkey exists). Purpose scoping
keeps the lanes disjoint — an enroll token can never satisfy ``/mfa/verify``
and a login token can never authorize enrollment.
"""

import json

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

from ...database import get_async_db_session
from ...logger import logger
from ...middleware.auth.dependencies import get_current_user, get_optional_user
from ...middleware.auth.jwt import create_jwt_token
from ...middleware.otel.metrics import endpoint_metrics_decorator, record_auth_metrics
from ...middleware.rate_limits import (
  mfa_rate_limit_dependency,
  passkey_management_rate_limit_dependency,
)
from ...middleware.sse import build_email_job_config, run_and_monitor_dagster_job
from ...models.api.auth import AuthResponse
from ...models.api.common import (
  COMMON_ERROR_RESPONSES,
  ErrorResponse,
  SuccessResponse,
)
from ...models.api.passkeys import (
  CeremonyOptionsResponse,
  PasskeyDeleteRequest,
  PasskeyInfo,
  PasskeyListResponse,
  PasskeyLoginVerifyRequest,
  PasskeyRegisterOptionsRequest,
  PasskeyRegisterVerifyRequest,
  PasskeyRegisterVerifyResponse,
)
from ...models.core import User, UserPasskey
from ...operations import passkeys as passkey_ops
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.auth_protection import AdvancedAuthProtection
from ...security.device_fingerprinting import extract_device_fingerprint
from .mfa import (
  _apply_login_preamble,
  _authenticated_response,
  _mark_mfa_token_used,
  _resolve_mfa_principal,
)
from .utils import detect_app_source, require_passkeys_enabled

router = APIRouter()


def _passkey_info(passkey: UserPasskey) -> PasskeyInfo:
  return PasskeyInfo(
    id=str(passkey.id),
    name=str(passkey.name),
    created_at=passkey.created_at.isoformat(),
    last_used_at=(
      passkey.last_used_at.isoformat() if passkey.last_used_at is not None else None
    ),
    backup_eligible=bool(passkey.backup_eligible),
    backup_state=bool(passkey.backup_state),
  )


def _resolve_enrollment_principal(
  session_user: User | None, mfa_token: str | None, session: Session
) -> tuple[User, str | None]:
  """Session user (settings flow) or enroll-token principal (forced lane).

  Returns ``(user, jti)`` — jti is None in the session flow.
  """
  if session_user is not None:
    return session_user, None
  if mfa_token:
    return _resolve_mfa_principal(mfa_token, "enroll", session)
  raise HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
  )


@router.post(
  "/passkeys/register/options",
  response_model=CeremonyOptionsResponse,
  summary="Passkey Registration Options",
  description="Begin a passkey enrollment ceremony.",
  operation_id="getPasskeyRegistrationOptions",
  responses={
    **COMMON_ERROR_RESPONSES,
    401: {"model": ErrorResponse, "description": "Authentication required"},
  },
)
async def get_registration_options(
  request: PasskeyRegisterOptionsRequest,
  session: Session = Depends(get_async_db_session),
  session_user: User | None = Depends(get_optional_user),
  rate_limit: None = Depends(mfa_rate_limit_dependency),
  _passkeys: None = Depends(require_passkeys_enabled),
) -> CeremonyOptionsResponse:
  user, _jti = _resolve_enrollment_principal(session_user, request.mfa_token, session)
  options = passkey_ops.begin_registration(session, user)
  return CeremonyOptionsResponse(options=json.loads(options.options_json))


@router.post(
  "/passkeys/register/verify",
  response_model=PasskeyRegisterVerifyResponse,
  summary="Passkey Registration Verify",
  description=(
    "Finish enrollment. The first passkey returns recovery codes (once); "
    "the forced-enrollment lane also completes the login."
  ),
  operation_id="verifyPasskeyRegistration",
  responses={
    **COMMON_ERROR_RESPONSES,
    400: {"model": ErrorResponse, "description": "Registration failed"},
    401: {"model": ErrorResponse, "description": "Authentication required"},
  },
)
@endpoint_metrics_decorator(
  "/v1/auth/passkeys/register/verify", business_event_type="passkey_enrollment"
)
async def verify_registration(
  request: PasskeyRegisterVerifyRequest,
  fastapi_request: Request,
  background_tasks: BackgroundTasks,
  session: Session = Depends(get_async_db_session),
  session_user: User | None = Depends(get_optional_user),
  rate_limit: None = Depends(mfa_rate_limit_dependency),
  _passkeys: None = Depends(require_passkeys_enabled),
) -> PasskeyRegisterVerifyResponse:
  user, jti = _resolve_enrollment_principal(session_user, request.mfa_token, session)
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  user_agent = fastapi_request.headers.get("user-agent")

  try:
    registered = passkey_ops.complete_registration(
      session, user, request.credential, name=request.name
    )
  except passkey_ops.PasskeyError as exc:
    if client_ip:
      AdvancedAuthProtection.record_auth_attempt(
        ip_address=client_ip,
        success=False,
        email=str(user.email),
        user_agent=user_agent,
      )
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Passkey registration failed: {type(exc).__name__}",
    )

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.PASSKEY_ENROLLED,
    user_id=str(user.id),
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint="/v1/auth/passkeys/register/verify",
    details={
      "passkey_id": str(registered.passkey.id),
      "backup_eligible": bool(registered.passkey.backup_eligible),
      "enrollment_lane": "forced" if jti else "settings",
    },
    risk_level="medium",
  )

  # Security notification: a new sign-in credential exists. The standard
  # mitigation for a stolen first factor enrolling its own second factor.
  background_tasks.add_task(
    run_and_monitor_dagster_job,
    job_name="send_email_job",
    operation_id=None,
    run_config=build_email_job_config(
      email_type="passkey_enrolled",
      to_email=str(user.email),
      user_name=str(user.name or user.email),
      app=detect_app_source(fastapi_request),
      passkey_name=str(registered.passkey.name),
    ),
  )

  auth: AuthResponse | None = None
  if jti is not None:
    _mark_mfa_token_used(jti)
    device_fingerprint = extract_device_fingerprint(fastapi_request)
    jwt_token = create_jwt_token(str(user.id), device_fingerprint, session=session)
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
      auth_method="password+passkey",
    )
    auth = _authenticated_response(user, jwt_token)

  return PasskeyRegisterVerifyResponse(
    passkey=_passkey_info(registered.passkey),
    recovery_codes=registered.recovery_codes,
    auth=auth,
  )


@router.get(
  "/passkeys",
  response_model=PasskeyListResponse,
  summary="List Passkeys",
  description="The authenticated user's enrolled passkeys.",
  operation_id="listUserPasskeys",
  responses=COMMON_ERROR_RESPONSES,
)
async def list_passkeys(
  session: Session = Depends(get_async_db_session),
  _passkeys: None = Depends(require_passkeys_enabled),
  rate_limit: None = Depends(passkey_management_rate_limit_dependency),
  user: User = Depends(get_current_user),
) -> PasskeyListResponse:
  passkeys = UserPasskey.get_all_for_user(str(user.id), session)
  return PasskeyListResponse(passkeys=[_passkey_info(pk) for pk in passkeys])


@router.post(
  "/passkeys/reauth/options",
  response_model=CeremonyOptionsResponse,
  summary="Re-authentication Options",
  description="Fresh-assertion options for destructive passkey lifecycle actions.",
  operation_id="getPasskeyReauthOptions",
  responses=COMMON_ERROR_RESPONSES,
)
async def get_reauth_options(
  session: Session = Depends(get_async_db_session),
  _passkeys: None = Depends(require_passkeys_enabled),
  rate_limit: None = Depends(passkey_management_rate_limit_dependency),
  user: User = Depends(get_current_user),
) -> CeremonyOptionsResponse:
  try:
    options = passkey_ops.begin_reauth(session, user)
  except passkey_ops.PasskeyNotFoundError:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="No passkeys enrolled",
    )
  return CeremonyOptionsResponse(options=json.loads(options.options_json))


@router.delete(
  "/passkeys/{passkey_id}",
  response_model=SuccessResponse,
  summary="Remove Passkey",
  description=(
    "Remove one passkey after re-authentication (password or fresh assertion). "
    "The last passkey of an MFA-required role cannot be removed while "
    "enforcement is active."
  ),
  operation_id="deleteUserPasskey",
  responses={
    **COMMON_ERROR_RESPONSES,
    401: {"model": ErrorResponse, "description": "Re-authentication failed"},
    409: {
      "model": ErrorResponse,
      "description": "Last passkey of an MFA-required role",
    },
  },
)
async def delete_passkey(
  passkey_id: str,
  request: PasskeyDeleteRequest,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  _passkeys: None = Depends(require_passkeys_enabled),
  rate_limit: None = Depends(passkey_management_rate_limit_dependency),
  user: User = Depends(get_current_user),
) -> SuccessResponse:
  client_ip = fastapi_request.client.host if fastapi_request.client else None
  try:
    passkey_ops.remove_passkey(
      session,
      user,
      passkey_id,
      password=request.password,
      assertion=request.assertion,
    )
  except passkey_ops.ReauthInvalidError:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Re-authentication failed",
    )
  except passkey_ops.LastPasskeyError:
    raise HTTPException(
      status_code=status.HTTP_409_CONFLICT,
      detail="This role requires MFA; enroll another passkey before removing this one",
    )
  except passkey_ops.PasskeyNotFoundError:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="No such passkey",
    )

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.PASSKEY_REMOVED,
    user_id=str(user.id),
    ip_address=client_ip,
    user_agent=fastapi_request.headers.get("user-agent"),
    endpoint="/v1/auth/passkeys",
    details={"passkey_id": passkey_id},
    risk_level="medium",
  )
  return SuccessResponse(message="Passkey removed")


@router.post(
  "/passkeys/login/options",
  response_model=CeremonyOptionsResponse,
  summary="Passwordless Login Options",
  description="Usernameless assertion options for passwordless (passkey) login.",
  operation_id="getPasskeyLoginOptions",
  responses=COMMON_ERROR_RESPONSES,
)
async def get_passwordless_options(
  rate_limit: None = Depends(mfa_rate_limit_dependency),
  _passkeys: None = Depends(require_passkeys_enabled),
) -> CeremonyOptionsResponse:
  options = passkey_ops.begin_passwordless_authentication()
  return CeremonyOptionsResponse(options=json.loads(options.options_json))


@router.post(
  "/passkeys/login/verify",
  response_model=AuthResponse,
  summary="Passwordless Login Verify",
  description=(
    "Complete a passwordless login. A user-verified passkey assertion is "
    "two factors in one gesture."
  ),
  operation_id="verifyPasskeyLogin",
  responses={
    **COMMON_ERROR_RESPONSES,
    401: {"model": ErrorResponse, "description": "Verification failed"},
  },
)
@endpoint_metrics_decorator(
  "/v1/auth/passkeys/login/verify", business_event_type="user_login"
)
async def verify_passwordless_login(
  request: PasskeyLoginVerifyRequest,
  response: Response,
  fastapi_request: Request,
  session: Session = Depends(get_async_db_session),
  rate_limit: None = Depends(mfa_rate_limit_dependency),
  _passkeys: None = Depends(require_passkeys_enabled),
) -> AuthResponse:
  client_ip, user_agent = _apply_login_preamble(
    fastapi_request, response, "/v1/auth/passkeys/login/verify"
  )

  record_auth_metrics(
    endpoint="/v1/auth/passkeys/login/verify",
    method="POST",
    auth_type="passkey_login",
    success=False,
  )

  try:
    result = passkey_ops.complete_authentication(
      session, request.assertion, expected_flow="pwl"
    )
  except passkey_ops.PasskeyError as exc:
    if client_ip:
      AdvancedAuthProtection.record_auth_attempt(
        ip_address=client_ip, success=False, email=None, user_agent=user_agent
      )
      AdvancedAuthProtection.apply_progressive_delay(client_ip)
    record_auth_metrics(
      endpoint="/v1/auth/passkeys/login/verify",
      method="POST",
      auth_type="passkey_login",
      success=False,
      failure_reason=type(exc).__name__,
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Passkey login failed",
    )

  user = result.user
  if result.sign_count_regressed:
    logger.warning(
      f"Sign-count regression on passwordless login for user {user.id} "
      f"(credential {result.passkey.id})"
    )

  device_fingerprint = extract_device_fingerprint(fastapi_request)
  jwt_token = create_jwt_token(str(user.id), device_fingerprint, session=session)

  record_auth_metrics(
    endpoint="/v1/auth/passkeys/login/verify",
    method="POST",
    auth_type="passkey_login",
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
    auth_method="passkey",
  )
  return _authenticated_response(user, jwt_token)
