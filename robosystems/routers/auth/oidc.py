"""OIDC login endpoints — the browser-redirect surface for enterprise SSO.

Mounted only when ``SSO_OIDC_ENABLED`` (see ``routers/auth/__init__.py``);
both endpoints are ``include_in_schema=False`` because they speak in
redirects, not JSON — this is deliberately not SDK surface.

Flow: ``GET /oidc/login`` 302s to the IdP (also serving as the IdP-initiated
login URI for the Okta app tile, which sends ``iss``); the IdP redirects back
to ``GET /oidc/callback``, which validates the ID token, resolves the user
(link-only), and hands the browser to the login home's existing
``?session_id=`` bridge entry — the same ``sso-complete`` consumption path the
cross-app bridge uses, so no JWT is minted here.

Failures redirect to the login home with a ``reason`` code rather than
returning JSON: a top-level browser navigation that dead-ends on an error
body is unrecoverable UX. The distinction between failure kinds lives in the
audit log, not the browser.
"""

import json
import secrets
import uuid
from datetime import UTC, datetime
from urllib.parse import quote

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from ...config import env
from ...database import get_async_db_session
from ...logger import logger
from ...middleware.auth.jwt import get_async_redis_client
from ...middleware.otel.metrics import record_auth_metrics
from ...middleware.rate_limits import oidc_rate_limit_dependency
from ...operations.oidc import (
  OIDC_STATE_TTL_SECONDS,
  OIDCError,
  OIDCState,
  OIDCTokenInvalidError,
  OIDCUserInactiveError,
  OIDCUserNotProvisionedError,
  build_authorize_redirect,
  exchange_code,
  fetch_server_metadata,
  get_oidc_connection,
  parse_return_to,
  resolve_oidc_user,
  validate_id_token,
)
from ...security import SecurityAuditLogger, SecurityEventType
from .utils import AVAILABLE_APPS, SSO_SESSION_EXPIRY_SECONDS, Config

router = APIRouter()

# Browser-binding cookie for the OIDC flow: mirrors the state's browser_state
# secret so the callback can prove the completing browser is the one that
# started the flow. Path-scoped to the callback so it is sent nowhere else.
_BIND_COOKIE = "oidc_flow"
_BIND_COOKIE_PATH = "/v1/auth/oidc/callback"


def _login_home_url() -> str:
  app_urls = Config.get_app_urls()
  return app_urls.get(env.LOGIN_HOME_APP, app_urls["robosystems"])


def _clear_bind_cookie(response: RedirectResponse) -> RedirectResponse:
  response.delete_cookie(_BIND_COOKIE, path=_BIND_COOKIE_PATH)
  return response


def _fail_redirect(reason: str) -> RedirectResponse:
  # Always clears the binding cookie: reachable both before it is set (login
  # errors) and after (callback errors); deleting an absent cookie is a no-op.
  return _clear_bind_cookie(
    RedirectResponse(f"{_login_home_url()}/login?reason={reason}", status_code=302)
  )


def _client_details(request: Request) -> tuple[str | None, str | None]:
  client_ip = request.client.host if request.client else None
  return client_ip, request.headers.get("user-agent")


@router.get("/oidc/login", include_in_schema=False)
async def oidc_login(
  request: Request,
  iss: str | None = Query(default=None),
  return_to: str | None = Query(default=None),
  _rate_limit: None = Depends(oidc_rate_limit_dependency),
) -> RedirectResponse:
  """Start an OIDC login: mint flow state and 302 to the IdP."""
  client_ip, user_agent = _client_details(request)

  try:
    connection = get_oidc_connection()
  except OIDCError:
    # Mounted but unconfigured — boot validation should prevent this.
    logger.error("OIDC login requested but no connection is configured")
    return _fail_redirect("oidc_failed")

  # Third-party-initiated login (the Okta app tile) sends `iss`. Validate it
  # against the configured issuer and refuse a mismatch rather than following
  # it — an honored attacker-supplied `iss` would start an auth-code flow
  # against a hostile authorization server.
  if iss is not None and iss.rstrip("/") != connection.issuer:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_FAILURE,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/oidc/login",
      details={"failure_reason": "oidc_iss_mismatch"},
      risk_level="high",
    )
    return _fail_redirect("oidc_failed")

  browser_state = secrets.token_urlsafe(32)
  try:
    metadata = await fetch_server_metadata(connection.issuer)
    authorize_url = build_authorize_redirect(
      connection,
      metadata,
      return_to=parse_return_to(return_to, AVAILABLE_APPS),
      browser_state=browser_state,
    )
  except OIDCError as exc:
    logger.error(f"OIDC login could not start: {exc}")
    return _fail_redirect("oidc_failed")

  response = RedirectResponse(authorize_url, status_code=302)
  # SameSite=Lax so the cookie survives the IdP's top-level GET redirect back
  # to the callback while still blocking cross-site POST use. Secure outside
  # local dev (localhost is http).
  response.set_cookie(
    _BIND_COOKIE,
    browser_state,
    max_age=OIDC_STATE_TTL_SECONDS,
    path=_BIND_COOKIE_PATH,
    httponly=True,
    samesite="lax",
    secure=not env.is_development(),
  )
  return response


@router.get("/oidc/callback", include_in_schema=False)
async def oidc_callback(
  request: Request,
  code: str | None = Query(default=None),
  state: str | None = Query(default=None),
  error: str | None = Query(default=None),
  session: Session = Depends(get_async_db_session),
  _rate_limit: None = Depends(oidc_rate_limit_dependency),
) -> RedirectResponse:
  """Complete an OIDC login and hand the browser to the login home bridge."""
  client_ip, user_agent = _client_details(request)

  def _denied(reason_code: str, failure_reason: str, risk: str) -> RedirectResponse:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.OIDC_LOGIN_DENIED
      if reason_code == "not_provisioned"
      else SecurityEventType.AUTH_FAILURE,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/oidc/callback",
      details={"failure_reason": failure_reason},
      risk_level=risk,
    )
    record_auth_metrics(
      endpoint="/v1/auth/oidc/callback",
      method="GET",
      auth_type="oidc",
      success=False,
      failure_reason=failure_reason,
    )
    return _fail_redirect(reason_code)

  try:
    connection = get_oidc_connection()
    metadata = await fetch_server_metadata(connection.issuer)
  except OIDCError as exc:
    logger.error(f"OIDC callback with no usable connection: {exc}")
    return _fail_redirect("oidc_failed")

  if error is not None:
    # The IdP declined (user cancelled, policy denied, …).
    return _denied("oidc_failed", f"idp_error:{error}", "medium")
  if not code or not state:
    return _denied("oidc_failed", "missing_code_or_state", "medium")

  # GETDEL: a replayed or expired state reads as invalid.
  state_data = OIDCState.validate(state)
  if state_data is None:
    return _denied("oidc_failed", "state_invalid_or_replayed", "high")

  # Browser binding: the completing browser must present the cookie set when
  # this flow began. Defeats login-CSRF (an attacker delivering their own
  # captured callback URL to a victim) — the victim's browser has no matching
  # cookie. compare_digest to avoid leaking the match via timing.
  bind_cookie = request.cookies.get(_BIND_COOKIE) or ""
  if not secrets.compare_digest(bind_cookie, str(state_data.get("browser_state", ""))):
    return _denied("oidc_failed", "browser_binding_mismatch", "high")

  try:
    id_token = await exchange_code(
      connection,
      metadata,
      code=code,
      code_verifier=state_data["code_verifier"],
    )
    claims = await validate_id_token(
      id_token,
      metadata=metadata,
      connection=connection,
      nonce=state_data["nonce"],
    )
  except OIDCTokenInvalidError as exc:
    return _denied("oidc_failed", f"token_invalid:{exc}", "high")
  except OIDCError as exc:
    logger.error(f"OIDC callback dependency failure: {exc}")
    return _fail_redirect("oidc_failed")

  try:
    resolution = resolve_oidc_user(
      session,
      issuer=connection.issuer,
      subject=str(claims["sub"]),
      email=claims.get("email"),
      email_verified=claims.get("email_verified"),
    )
  except OIDCUserNotProvisionedError:
    return _denied("not_provisioned", "user_not_provisioned", "medium")
  except OIDCUserInactiveError:
    return _denied("not_provisioned", "user_inactive", "high")

  user = resolution.user

  # Hand off through the existing cross-app bridge consumption path: the
  # login home reads ?session_id= and POSTs /sso-complete, which mints the
  # platform JWT. sso-complete hard-requires a token_id in the payload (it
  # deletes the originating single-use SSO token) — an OIDC login has no such
  # token, so a synthetic one is written and the deletes no-op harmlessly.
  session_id = str(uuid.uuid4())
  payload = {
    "user_id": user.id,
    "token_id": str(uuid.uuid4()),
    "target_app": env.LOGIN_HOME_APP,
    "return_url": "/",
    "session_version": user.session_version,
    "created_at": datetime.now(UTC).isoformat(),
  }
  try:
    redis_client = await get_async_redis_client()
    await redis_client.setex(
      f"sso_session:{session_id}", SSO_SESSION_EXPIRY_SECONDS, json.dumps(payload)
    )
  except Exception as exc:
    logger.error(f"Failed to write OIDC bridge session: {exc}")
    return _fail_redirect("oidc_failed")

  SecurityAuditLogger.log_auth_success(
    user_id=str(user.id),
    ip_address=client_ip,
    user_agent=user_agent,
    auth_method="oidc",
  )
  if resolution.linked:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=str(user.id),
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint="/v1/auth/oidc/callback",
      details={"action": "oidc_identity_linked"},
      risk_level="low",
    )
  record_auth_metrics(
    endpoint="/v1/auth/oidc/callback",
    method="GET",
    auth_type="oidc",
    success=True,
    user_id=str(user.id),
  )

  location = f"{_login_home_url()}/login?session_id={session_id}"
  return_to = state_data.get("return_to")
  if return_to:
    location += f"&return_to={quote(return_to)}"
  return _clear_bind_cookie(RedirectResponse(location, status_code=302))
