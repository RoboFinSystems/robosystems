"""The authorization leg: authorize → login home → consent → code.

State that must survive the browser round-trip lives in Valkey under
SHA-256 keys with short TTLs, and is redeemed with ``GETDEL`` so every
redemption is atomically single-use (the ``OIDCState`` pattern). Nothing
here trusts a value the browser carried back except by that lookup.

Two kinds of error leave the authorization endpoint (RFC 6749 §4.1.2.1):
when the client or its redirect URI cannot be verified, the error is shown
to the user and never redirected; otherwise it is delivered to the client's
registered redirect with the original ``state``.
"""

import hashlib
import json
import secrets
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.config.constants import (
  OAUTH_AUTHORIZATION_CODE_TTL_SECONDS,
  OAUTH_PENDING_AUTHORIZATION_TTL_SECONDS,
)
from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
from robosystems.logger import logger
from robosystems.models.core import OAuthClient, OAuthGrant, User
from robosystems.security import SecurityAuditLogger, SecurityEventType

from .clients import ClientError, pick_redirect_uri, resolve_client
from .resources import (
  issuer,
  normalize_scope,
  resolve_resource,
)

_PENDING_KEY_PREFIX = "oauth:authz:"
_CODE_KEY_PREFIX = "oauth:code:"

# PKCE (RFC 7636 §4.2): a base64url SHA-256 digest is exactly 43 characters.
_CODE_CHALLENGE_LENGTH = 43
_MAX_STATE_LENGTH = 1024


class AuthorizeError(Exception):
  """An authorization-request error.

  ``redirect_uri`` set → deliver to the client (its identity and redirect
  were verified); unset → render to the user, never redirect.
  """

  def __init__(
    self,
    error: str,
    description: str,
    *,
    redirect_uri: str | None = None,
    state: str | None = None,
  ):
    super().__init__(description)
    self.error = error
    self.description = description
    self.redirect_uri = redirect_uri
    self.state = state


class AuthorizationUnavailableError(Exception):
  """The pending-request / code store is unreachable — fail closed."""


@dataclass(frozen=True)
class AuthorizeParams:
  response_type: str | None
  client_id: str | None
  redirect_uri: str | None
  state: str | None
  code_challenge: str | None
  code_challenge_method: str | None
  scope: str | None
  resource: str | None


@dataclass(frozen=True)
class PendingAuthorization:
  """An authorization request parked while the user signs in and consents."""

  request_id: str
  client_row_id: str
  client_id: str
  client_name: str
  client_uri: str | None
  logo_uri: str | None
  is_trusted: bool
  redirect_uri: str
  state: str | None
  code_challenge: str
  resource: str
  graph_id: str | None
  scope: str
  created_at: str

  @property
  def redirect_host(self) -> str:
    parts = urlsplit(self.redirect_uri)
    return parts.hostname or parts.scheme or ""

  @property
  def is_loopback(self) -> bool:
    from .clients import is_loopback_redirect

    return is_loopback_redirect(self.redirect_uri)


def _key(prefix: str, value: str) -> str:
  return f"{prefix}{hashlib.sha256(value.encode()).hexdigest()}"


class PendingAuthorizationStore:
  """Valkey-backed park for in-flight authorization requests."""

  @staticmethod
  def create(pending: PendingAuthorization) -> None:
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      client.setex(
        _key(_PENDING_KEY_PREFIX, pending.request_id),
        OAUTH_PENDING_AUTHORIZATION_TTL_SECONDS,
        json.dumps(asdict(pending)),
      )
    except Exception as exc:
      logger.error(f"Failed to persist pending OAuth authorization: {exc}")
      raise AuthorizationUnavailableError("Unable to start authorization") from exc

  @staticmethod
  def _parse(raw: Any) -> PendingAuthorization | None:
    if not raw:
      return None
    try:
      data = json.loads(raw)
      return PendingAuthorization(**data)
    except (TypeError, ValueError) as exc:
      logger.error(f"Discarding malformed pending OAuth authorization: {exc}")
      return None

  @classmethod
  def peek(cls, request_id: str) -> PendingAuthorization | None:
    """Read without consuming — the consent page renders from this."""
    if not request_id or len(request_id) > 128:
      return None
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      raw = client.get(_key(_PENDING_KEY_PREFIX, request_id))
    except Exception as exc:
      logger.error(f"Failed to read pending OAuth authorization: {exc}")
      return None
    return cls._parse(raw)

  @classmethod
  def consume(cls, request_id: str) -> PendingAuthorization | None:
    """Atomically take the request out of the store (``GETDEL``)."""
    if not request_id or len(request_id) > 128:
      return None
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      raw = client.getdel(_key(_PENDING_KEY_PREFIX, request_id))
    except Exception as exc:
      logger.error(f"Failed to consume pending OAuth authorization: {exc}")
      return None
    return cls._parse(raw)


class AuthorizationCodeStore:
  """Single-use authorization codes. Payload binds everything the token
  endpoint must verify: user, client, grant, redirect, PKCE, resource."""

  @staticmethod
  def issue(payload: dict[str, Any]) -> str:
    code = secrets.token_urlsafe(32)
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      client.setex(
        _key(_CODE_KEY_PREFIX, code),
        OAUTH_AUTHORIZATION_CODE_TTL_SECONDS,
        json.dumps(payload),
      )
    except Exception as exc:
      logger.error(f"Failed to persist OAuth authorization code: {exc}")
      raise AuthorizationUnavailableError("Unable to issue authorization code") from exc
    return code

  @staticmethod
  def consume(code: str) -> dict[str, Any] | None:
    if not code or not isinstance(code, str) or len(code) > 128:
      return None
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      raw = client.getdel(_key(_CODE_KEY_PREFIX, code))
    except Exception as exc:
      logger.error(f"Failed to consume OAuth authorization code: {exc}")
      return None
    if not raw:
      return None
    try:
      payload = json.loads(raw)
    except (TypeError, ValueError):
      return None
    return payload if isinstance(payload, dict) else None


def append_query(url: str, params: dict[str, str]) -> str:
  """Append parameters to a redirect URI, preserving any registered query."""
  parts = urlsplit(url)
  query = parts.query
  extra = urlencode(params)
  merged = f"{query}&{extra}" if query else extra
  return urlunsplit((parts.scheme, parts.netloc, parts.path, merged, ""))


def client_callback(
  redirect_uri: str, params: dict[str, str], state: str | None
) -> str:
  """The client's callback URL with ``iss`` (RFC 9207) and ``state``."""
  ordered: dict[str, str] = dict(params)
  if state is not None:
    ordered["state"] = state
  ordered["iss"] = issuer()
  return append_query(redirect_uri, ordered)


def error_callback(error: AuthorizeError) -> str:
  assert error.redirect_uri is not None
  return client_callback(
    error.redirect_uri,
    {"error": error.error, "error_description": error.description},
    error.state,
  )


def consent_url(request_id: str) -> str:
  """Where the browser goes next: the login home's consent page, which
  bounces through sign-in when there is no session."""
  from robosystems.routers.auth.utils import Config

  app_urls = Config.get_app_urls()
  login_home = app_urls.get(env.LOGIN_HOME_APP, app_urls["robosystems"]).rstrip("/")
  return f"{login_home}/oauth/consent?{urlencode({'request_id': request_id})}"


def begin_authorization(params: AuthorizeParams, session: Session) -> str:
  """Validate an authorization request and park it. Returns the consent
  URL to redirect the browser to. Raises ``AuthorizeError``."""
  # Client + redirect first: until both verify, nothing may be redirected.
  try:
    client = resolve_client(params.client_id, session)
  except ClientError as exc:
    raise AuthorizeError(exc.error, exc.description) from exc
  try:
    redirect_uri = pick_redirect_uri(client, params.redirect_uri)
  except ClientError as exc:
    raise AuthorizeError(exc.error, exc.description) from exc

  state = params.state
  if state is not None and (
    not isinstance(state, str) or len(state) > _MAX_STATE_LENGTH
  ):
    raise AuthorizeError("invalid_request", "state is too long")

  def fail(error: str, description: str) -> AuthorizeError:
    return AuthorizeError(error, description, redirect_uri=redirect_uri, state=state)

  if params.response_type != "code":
    raise fail("unsupported_response_type", "response_type must be code")

  challenge = params.code_challenge
  if (
    not challenge
    or not isinstance(challenge, str)
    or len(challenge) != _CODE_CHALLENGE_LENGTH
    or not all(c.isalnum() or c in "-_" for c in challenge)
  ):
    raise fail("invalid_request", "code_challenge (S256) is required")
  if params.code_challenge_method != "S256":
    raise fail("invalid_request", "code_challenge_method must be S256")

  scope = normalize_scope(params.scope)
  if scope is None:
    raise fail("invalid_scope", "Unsupported scope")

  target = resolve_resource(params.resource)
  if target is None:
    raise fail("invalid_target", "resource must be one of this server's MCP URLs")

  request_id = secrets.token_urlsafe(32)
  pending = PendingAuthorization(
    request_id=request_id,
    client_row_id=str(client.id),
    client_id=str(client.client_id),
    client_name=str(client.client_name),
    client_uri=client.client_uri,
    logo_uri=client.logo_uri,
    is_trusted=bool(client.is_trusted),
    redirect_uri=redirect_uri,
    state=state,
    code_challenge=challenge,
    resource=target.resource,
    graph_id=target.graph_id,
    scope=scope,
    created_at=datetime.now(UTC).isoformat(),
  )
  try:
    PendingAuthorizationStore.create(pending)
  except AuthorizationUnavailableError as exc:
    raise fail("temporarily_unavailable", "Authorization is unavailable") from exc

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.AUTH_SUCCESS,
    details={
      "action": "oauth_authorization_started",
      "oauth_client_id": client.id,
      "resource": target.resource,
      "redirect_host": pending.redirect_host,
    },
    risk_level="low",
  )
  return consent_url(request_id)


class ConsentError(Exception):
  """A consent decision that cannot be honored (HTTP status + detail)."""

  def __init__(self, status_code: int, detail: str):
    super().__init__(detail)
    self.status_code = status_code
    self.detail = detail


async def record_decision(
  *,
  request_id: str,
  user: User,
  approved: bool,
  graph_id: str | None,
  session: Session,
) -> str:
  """Consume the pending request and answer it.

  Approve → the chosen graph is checked against the user's live access,
  a grant is written, a single-use code is issued, and the client's
  callback URL is returned. Deny → the client's callback with
  ``access_denied``. Either way the pending request is gone.
  """
  pending = PendingAuthorizationStore.consume(request_id)
  if pending is None:
    raise ConsentError(404, "Authorization request expired or already answered")

  if not approved:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
      user_id=str(user.id),
      details={
        "action": "oauth_consent_denied",
        "oauth_client_id": pending.client_row_id,
        "resource": pending.resource,
      },
      risk_level="low",
    )
    return client_callback(
      pending.redirect_uri,
      {"error": "access_denied", "error_description": "The user denied the request"},
      pending.state,
    )

  # The graph: fixed by the URL on a per-graph resource, chosen by the user
  # on the agnostic one. The body may not disagree with a URL-fixed graph.
  if pending.graph_id is not None:
    if graph_id not in (None, pending.graph_id):
      raise ConsentError(400, "graph_id does not match the requested resource")
    chosen = pending.graph_id
  else:
    if not graph_id or not isinstance(graph_id, str):
      raise ConsentError(400, "graph_id is required")
    chosen = graph_id

  import re

  from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
  from robosystems.routers.graphs.mcp.handlers import validate_mcp_access

  if not re.fullmatch(GRAPH_OR_SUBGRAPH_ID_PATTERN, chosen):
    raise ConsentError(400, "Invalid graph_id format")
  # Live access check — the same gate every MCP call runs. A user cannot
  # consent to a graph they cannot read.
  from fastapi import HTTPException

  try:
    await validate_mcp_access(chosen, user, session, "read")
  except HTTPException as exc:
    raise ConsentError(403, "You do not have access to that graph") from exc

  client = OAuthClient.get_by_id(pending.client_row_id, session)
  if client is None or not client.is_usable:
    raise ConsentError(400, "The client is no longer registered")

  grant = OAuthGrant.create(
    user_id=str(user.id),
    oauth_client_id=str(client.id),
    graph_id=chosen,
    resource=pending.resource,
    scope=pending.scope,
    session=session,
  )
  client.mark_used(session)

  try:
    code = AuthorizationCodeStore.issue(
      {
        "user_id": str(user.id),
        "client_row_id": str(client.id),
        "client_id": str(client.client_id),
        "grant_id": str(grant.id),
        "redirect_uri": pending.redirect_uri,
        "code_challenge": pending.code_challenge,
        "resource": pending.resource,
        "scope": pending.scope,
      }
    )
  except AuthorizationUnavailableError as exc:
    raise ConsentError(503, "Authorization is temporarily unavailable") from exc

  return client_callback(pending.redirect_uri, {"code": code}, pending.state)
