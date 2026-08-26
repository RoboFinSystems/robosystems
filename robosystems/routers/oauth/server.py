"""MCP OAuth 2.1 authorization server — the wire surface.

Exactly what MCP clients need and nothing more (spec decision #2): RFC 8414
metadata, RFC 9728 protected-resource metadata for both MCP routes, the
authorization-code flow with PKCE (the consent screen lives on the login
home), the token endpoint with refresh rotation, RFC 7009 revocation, and
RFC 7591 dynamic registration. Everything is gated on ``MCP_OAUTH_ENABLED``
and answers 404 when the flag is off, so the surface is invisible rather
than merely refusing.

Every endpoint is excluded from the OpenAPI schema: this is a protocol
surface for OAuth clients, not part of the SDK-facing API.
"""

import base64
import binascii
import re
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, status
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from robosystems.config import env
from robosystems.database import get_db_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_optional_jwt_user
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import (
  oauth_authorize_rate_limit_dependency,
  oauth_consent_rate_limit_dependency,
  oauth_register_rate_limit_dependency,
  oauth_token_rate_limit_dependency,
)
from robosystems.models.api.oauth_server import (
  ConsentDecisionRequest,
  ConsentDecisionResponse,
  PendingAuthorizationResponse,
)
from robosystems.models.core import OAuthClient, User
from robosystems.operations.oauth_server.authorization import (
  AuthorizeError,
  AuthorizeParams,
  ConsentError,
  PendingAuthorizationStore,
  begin_authorization,
  error_callback,
  record_decision,
)
from robosystems.operations.oauth_server.clients import (
  ClientError,
  authenticate_client,
  register_dynamic_client,
  resolve_client,
)
from robosystems.operations.oauth_server.resources import (
  agnostic_target,
  authorization_server_metadata,
  graph_target,
  protected_resource_metadata,
)
from robosystems.operations.oauth_server.tokens import (
  TokenError,
  exchange_authorization_code,
  refresh_access_token,
  revoke_presented_token,
)
from robosystems.security import SecurityAuditLogger, SecurityEventType

router = APIRouter()

_NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}
_METADATA_CACHE = {"Cache-Control": "public, max-age=3600"}
_MAX_FORM_FIELD = 4096


def _require_enabled() -> None:
  if not env.MCP_OAUTH_ENABLED:
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")


def _oauth_error(
  error: str, description: str, *, status_code: int = 400, headers: dict | None = None
) -> JSONResponse:
  return JSONResponse(
    status_code=status_code,
    content={"error": error, "error_description": description},
    headers={**_NO_STORE, **(headers or {})},
  )


def _client_ip(request: Request) -> str | None:
  return request.client.host if request.client else None


# --- discovery -----------------------------------------------------------


@router.get("/.well-known/oauth-authorization-server", include_in_schema=False)
async def authorization_server_metadata_document() -> JSONResponse:
  _require_enabled()
  return JSONResponse(authorization_server_metadata(), headers=_METADATA_CACHE)


@router.get("/.well-known/oauth-protected-resource", include_in_schema=False)
async def protected_resource_root() -> JSONResponse:
  """Root fallback (RFC 9728 §3.1) — describes the graph-agnostic route."""
  _require_enabled()
  return JSONResponse(
    protected_resource_metadata(agnostic_target()), headers=_METADATA_CACHE
  )


@router.get("/.well-known/oauth-protected-resource/v1/mcp", include_in_schema=False)
async def protected_resource_agnostic() -> JSONResponse:
  _require_enabled()
  return JSONResponse(
    protected_resource_metadata(agnostic_target()), headers=_METADATA_CACHE
  )


@router.get(
  "/.well-known/oauth-protected-resource/v1/graphs/{graph_id}/mcp",
  include_in_schema=False,
)
async def protected_resource_graph(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
) -> JSONResponse:
  """Per-graph resource metadata. Exists for every well-formed id — the
  document says nothing about whether the graph exists, so it is not an
  enumeration oracle."""
  _require_enabled()
  return JSONResponse(
    protected_resource_metadata(graph_target(graph_id)), headers=_METADATA_CACHE
  )


# --- authorization -------------------------------------------------------


@router.get("/v1/oauth/authorize", include_in_schema=False)
async def authorize(
  request: Request,
  response_type: str | None = Query(None),
  client_id: str | None = Query(None),
  redirect_uri: str | None = Query(None),
  state: str | None = Query(None),
  code_challenge: str | None = Query(None),
  code_challenge_method: str | None = Query(None),
  scope: str | None = Query(None),
  resource: str | None = Query(None),
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(oauth_authorize_rate_limit_dependency),
):
  """Start the authorization-code flow: validate, park the request, and
  send the browser to the login home's consent page."""
  _require_enabled()
  params = AuthorizeParams(
    response_type=response_type,
    client_id=client_id,
    redirect_uri=redirect_uri,
    state=state,
    code_challenge=code_challenge,
    code_challenge_method=code_challenge_method,
    scope=scope,
    resource=resource,
  )
  try:
    # Client resolution may fetch a metadata document; keep it off the loop.
    location = await run_in_threadpool(begin_authorization, params, session)
  except AuthorizeError as exc:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
      ip_address=_client_ip(request),
      endpoint="/v1/oauth/authorize",
      details={"action": "oauth_authorize_rejected", "error": exc.error},
      risk_level="low",
    )
    if exc.redirect_uri:
      return RedirectResponse(error_callback(exc), status_code=302, headers=_NO_STORE)
    return _oauth_error(exc.error, exc.description)
  return RedirectResponse(location, status_code=302, headers=_NO_STORE)


async def _require_session_user(user: User | None) -> User:
  """Consent is a browser-session act: the app's JWT only. An API key must
  never be able to grant a third party a durable credential."""
  if user is None:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Sign in to continue",
      headers={"WWW-Authenticate": "Bearer"},
    )
  return user


@router.get(
  "/v1/oauth/authorize/{request_id}",
  include_in_schema=False,
  response_model=PendingAuthorizationResponse,
)
async def pending_authorization(
  request_id: str = Path(..., min_length=16, max_length=128),
  user: User | None = Depends(get_optional_jwt_user),
  _rate_limit: None = Depends(oauth_consent_rate_limit_dependency),
) -> PendingAuthorizationResponse:
  """What the consent page renders."""
  _require_enabled()
  await _require_session_user(user)
  pending = PendingAuthorizationStore.peek(request_id)
  if pending is None:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="Authorization request expired or already answered",
    )
  return PendingAuthorizationResponse(
    request_id=pending.request_id,
    client_name=pending.client_name,
    client_uri=pending.client_uri,
    logo_uri=pending.logo_uri,
    is_trusted=pending.is_trusted,
    redirect_host=pending.redirect_host,
    is_loopback_redirect=pending.is_loopback,
    resource=pending.resource,
    graph_id=pending.graph_id,
    scope=pending.scope,
  )


@router.post(
  "/v1/oauth/authorize/{request_id}/decision",
  include_in_schema=False,
  response_model=ConsentDecisionResponse,
)
async def consent_decision(
  body: ConsentDecisionRequest,
  request_id: str = Path(..., min_length=16, max_length=128),
  user: User | None = Depends(get_optional_jwt_user),
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(oauth_consent_rate_limit_dependency),
) -> ConsentDecisionResponse:
  """Approve or deny. Returns the client's callback URL for the browser."""
  _require_enabled()
  session_user = await _require_session_user(user)
  try:
    redirect_to = await record_decision(
      request_id=request_id,
      user=session_user,
      approved=body.approved,
      graph_id=body.graph_id,
      session=session,
    )
  except ConsentError as exc:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail)
  return ConsentDecisionResponse(redirect_to=redirect_to)


# --- token -----------------------------------------------------------------


def _form_field(form: Any, name: str) -> str | None:
  value = form.get(name)
  if value is None:
    return None
  if not isinstance(value, str) or len(value) > _MAX_FORM_FIELD:
    raise TokenError("invalid_request", f"{name} is malformed")
  return value


async def _read_form(request: Request) -> Any:
  content_type = request.headers.get("content-type", "").split(";", 1)[0].strip()
  if content_type.casefold() != "application/x-www-form-urlencoded":
    raise TokenError(
      "invalid_request", "Content-Type must be application/x-www-form-urlencoded"
    )
  try:
    return await request.form()
  except Exception as exc:
    raise TokenError("invalid_request", "Malformed form body") from exc


def _client_credentials(request: Request, form: Any) -> tuple[str | None, str | None]:
  """``client_id``/``client_secret`` from HTTP Basic (RFC 6749 §2.3.1)
  when present, else from the body. Basic wins when both are sent."""
  authorization = request.headers.get("authorization", "")
  if authorization.startswith("Basic "):
    try:
      decoded = base64.b64decode(authorization[6:], validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
      raise TokenError(
        "invalid_client", "Malformed client credentials", status_code=401
      ) from exc
    client_id, _, client_secret = decoded.partition(":")
    from urllib.parse import unquote

    return unquote(client_id), unquote(client_secret) or None
  return _form_field(form, "client_id"), _form_field(form, "client_secret")


def _authenticated_client(request: Request, form: Any, session: Session) -> OAuthClient:
  client_id, client_secret = _client_credentials(request, form)
  try:
    client = resolve_client(client_id, session)
    authenticate_client(client, client_secret)
  except ClientError as exc:
    raise TokenError(exc.error, exc.description, status_code=401) from exc
  return client


@router.post("/v1/oauth/token", include_in_schema=False)
async def token(
  request: Request,
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(oauth_token_rate_limit_dependency),
):
  """Authorization-code exchange and refresh-token rotation."""
  _require_enabled()
  try:
    form = await _read_form(request)
    client = await run_in_threadpool(_authenticated_client, request, form, session)
    grant_type = _form_field(form, "grant_type")
    if grant_type == "authorization_code":
      issued = exchange_authorization_code(
        code=_form_field(form, "code"),
        client=client,
        code_verifier=_form_field(form, "code_verifier"),
        redirect_uri=_form_field(form, "redirect_uri"),
        resource=_form_field(form, "resource"),
        session=session,
      )
    elif grant_type == "refresh_token":
      issued = refresh_access_token(
        refresh_token=_form_field(form, "refresh_token"),
        client=client,
        scope=_form_field(form, "scope"),
        resource=_form_field(form, "resource"),
        session=session,
      )
    else:
      raise TokenError(
        "unsupported_grant_type",
        "grant_type must be authorization_code or refresh_token",
      )
  except TokenError as exc:
    headers = {"WWW-Authenticate": "Basic"} if exc.status_code == 401 else None
    return _oauth_error(
      exc.error, exc.description, status_code=exc.status_code, headers=headers
    )
  return JSONResponse(issued.as_dict(), headers=_NO_STORE)


@router.post("/v1/oauth/revoke", include_in_schema=False)
async def revoke(
  request: Request,
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(oauth_token_rate_limit_dependency),
):
  """RFC 7009. Always 200 for an authenticated client, whatever the token."""
  _require_enabled()
  try:
    form = await _read_form(request)
    client = await run_in_threadpool(_authenticated_client, request, form, session)
    revoke_presented_token(
      token=_form_field(form, "token"), client=client, session=session
    )
  except TokenError as exc:
    return _oauth_error(exc.error, exc.description, status_code=exc.status_code)
  return JSONResponse({}, headers=_NO_STORE)


# --- registration ----------------------------------------------------------


@router.post("/v1/oauth/register", include_in_schema=False, status_code=201)
async def register(
  request: Request,
  session: Session = Depends(get_db_session),
  _rate_limit: None = Depends(oauth_register_rate_limit_dependency),
):
  """RFC 7591 dynamic client registration (public or confidential)."""
  _require_enabled()
  content_type = request.headers.get("content-type", "").split(";", 1)[0].strip()
  if content_type.casefold() != "application/json":
    return _oauth_error(
      "invalid_client_metadata", "Content-Type must be application/json"
    )
  try:
    payload = await request.json()
  except Exception:
    return _oauth_error("invalid_client_metadata", "Request body is not valid JSON")

  try:
    client, secret = register_dynamic_client(
      payload, registration_ip=_client_ip(request), session=session
    )
  except ClientError as exc:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.INPUT_VALIDATION_FAILURE,
      ip_address=_client_ip(request),
      endpoint="/v1/oauth/register",
      details={"action": "oauth_registration_rejected", "error": exc.error},
      risk_level="low",
    )
    return _oauth_error(exc.error, exc.description)

  logger.info(
    "OAuth client registered dynamically",
    extra={"oauth_client_id": client.id, "client_name": client.client_name},
  )
  body: dict[str, Any] = {
    "client_id": client.client_id,
    "client_id_issued_at": int(client.created_at.timestamp()),
    "client_name": client.client_name,
    "redirect_uris": list(client.redirect_uris),
    "token_endpoint_auth_method": client.token_endpoint_auth_method,
    "grant_types": ["authorization_code", "refresh_token"],
    "response_types": ["code"],
  }
  if client.client_uri:
    body["client_uri"] = client.client_uri
  if client.logo_uri:
    body["logo_uri"] = client.logo_uri
  if client.scope:
    body["scope"] = client.scope
  if secret is not None:
    body["client_secret"] = secret
    body["client_secret_expires_at"] = 0
  return JSONResponse(body, status_code=201, headers=_NO_STORE)


_ = re  # GRAPH_OR_SUBGRAPH_ID_PATTERN is applied by FastAPI's Path validator
