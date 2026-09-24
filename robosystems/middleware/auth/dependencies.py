"""FastAPI authentication dependencies.

Dependencies accept a JWT bearer or an ``X-API-Key`` header, JWT first; the MCP
routes also take an OAuth bearer. Only ``get_current_user_sse`` reads a
credential from the query string (EventSource cannot send headers); log
``request.url.path``, never the full URL.
"""

from typing import Any

from fastapi import Depends, Header, HTTPException, Query, Request, Security, status
from fastapi.security import APIKeyHeader

from ...logger import logger
from ...models.core import User
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.device_fingerprinting import extract_device_fingerprint
from ...security.request_context import API_KEY_PREFIX_LENGTH, publish_principal
from .cache import api_key_cache
from .jwt import (
  verify_jwt_claims as verify_jwt_claims_from_auth,
)
from .oauth import (
  OAuthPrincipal,
  is_oauth_access_token,
  validate_oauth_access_token,
)
from .utils import (
  validate_api_key,
  validate_api_key_with_graph,
  validate_repository_access,
)


def _validate_cached_user_data(user_data: dict) -> bool:
  """Reject cached user payloads whose required fields are missing or the
  wrong type, so a corrupted entry can't materialize a User.
  """
  if not isinstance(user_data, dict):
    return False

  user_id = user_data.get("id")
  email = user_data.get("email")

  if not user_id or not isinstance(user_id, (int, str)):
    return False

  if not email or not isinstance(email, str) or "@" not in email:
    return False

  name = user_data.get("name")
  if name is not None and not isinstance(name, str):
    return False

  # Entries written before a field joined the payload are stale, not merely
  # incomplete: defaulting `email_verified` here would report False for a
  # verified user, silently downgrading an authorization input. Rejecting sends
  # the caller to the database, which re-caches the entry in full.
  if "email_verified" not in user_data:
    return False

  is_active = user_data.get("is_active")
  return not (is_active is not None and not isinstance(is_active, bool))


def _create_user_from_cache(user_data: dict) -> User | None:
  """Build a `User` from cached data, or None when the data fails validation."""
  if not _validate_cached_user_data(user_data):
    logger.warning("Invalid cached user data detected, falling back to database")
    return None

  try:
    user = User(
      id=user_data.get("id"),
      email=user_data.get("email"),
      name=user_data.get("name"),
      email_verified=bool(user_data["email_verified"]),
      is_active=user_data.get("is_active", True),
      session_version=user_data.get("session_version", 0),
    )
    return user
  except (TypeError, ValueError) as e:
    logger.error(f"Invalid data type in cached user data: {e}")
    return None
  except Exception as e:
    logger.error(f"Unexpected error creating User from cached data: {e}")
    return None


def _db_get_user_by_id(user_id: str) -> User | None:
  """Look up a user in a short-lived session and return it detached.

  The scoped ``session`` proxy would hold a pool connection until end of
  request, which long-running endpoints (MCP, SSE) can't afford.
  """
  from ...database import SessionFactory

  sess = SessionFactory()
  try:
    user = User.get_by_id(user_id, sess)
    if not user:
      return None
    sess.expunge(user)
    return user
  finally:
    sess.close()


def _db_check_graph_access(
  user_id: str, graph_id: str, *, allow_deprovisioned: bool = False
) -> bool:
  """Check graph access in a short-lived session (see ``_db_get_user_by_id``).

  ``allow_deprovisioned`` is threaded only from the backup export path.
  """
  from ...database import SessionFactory
  from ...models.core import GraphUser

  sess = SessionFactory()
  try:
    return GraphUser.user_has_access(
      user_id, graph_id, sess, allow_deprovisioned=allow_deprovisioned
    )
  finally:
    sess.close()


def _serialize_user_for_jwt_cache(user: User) -> dict[str, Any]:
  """Serialize the stable user fields needed by JWT auth dependencies.

  Every field a consumer reads off the reconstructed `User` must be here — a
  column omitted comes back as ``None`` rather than its declared default, since
  column defaults apply on insert and a cache-built `User` is never flushed.
  """
  return {
    "id": str(user.id),
    "email": user.email,
    "name": user.name,
    "email_verified": bool(user.email_verified),
    "is_active": bool(user.is_active),
    "session_version": _safe_user_session_version(user),
  }


def _safe_user_session_version(user: User) -> int:
  """Read session_version defensively for real users and lightweight test mocks."""
  try:
    return int(getattr(user, "session_version", 0) or 0)
  except (TypeError, ValueError):
    return 0


def _get_user_for_verified_jwt(user_id: str, token_session_version: int) -> User | None:
  """Get user data after strict JWT verification has succeeded.

  Uses the user-id keyed cache only when its session_version matches the token.
  On cache miss, confirms the DB session_version still matches before caching.
  """
  cached_data = api_key_cache.get_cached_jwt_user_data(user_id, token_session_version)
  if isinstance(cached_data, dict):
    user_data = cached_data.get("user_data", {})
    user = _create_user_from_cache(user_data)
    if user:
      return user

  user = _db_get_user_by_id(user_id)
  if not user or not bool(user.is_active):
    return None

  current_session_version = _safe_user_session_version(user)
  if current_session_version != int(token_session_version):
    logger.info(
      f"JWT user DB lookup rejected session_version mismatch for {user_id} "
      f"(token={token_session_version}, current={current_session_version})"
    )
    return None

  api_key_cache.cache_jwt_user_data(
    user_id, _serialize_user_for_jwt_cache(user), current_session_version
  )
  return user


API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _stash_api_key_identity(
  request: Request,
  api_key: str,
  user: "User | None" = None,
  auth_method: str = "api_key",
) -> None:
  """Record which API key authenticated this request.

  Without a ``user`` only the key's stored prefix is set on ``request.state``;
  with one, the full principal is published for the access log, rate limiter
  and audit trail.
  """
  if user is None:
    request.state.api_key_prefix = api_key[:API_KEY_PREFIX_LENGTH]
    return
  publish_principal(request, str(user.id), auth_method, api_key=api_key)


def _publish_jwt_identity(request: Request, user_id: str) -> None:
  """The JWT counterpart of `_stash_api_key_identity`: publish the session
  principal so JWT-authenticated requests are attributable downstream too."""
  publish_principal(request, str(user_id), "jwt_token")


def _publish_graph_authorization(request: Request, graph_id: str) -> None:
  """Record that the caller was authorized on ``graph_id``.

  The rate limiter charges a graph's budget only on this evidence, so set it
  after the access check passes, never before.
  """
  request.state.auth_graph_id = graph_id


def verify_jwt_claims(
  token: str, device_fingerprint: dict[str, Any] | None = None
) -> tuple[str, int] | None:
  """Return (user_id, session_version) for a valid JWT, else None.

  Does NOT compare session_version against the User row; callers must (see
  ``_get_user_for_verified_jwt``).
  """
  return verify_jwt_claims_from_auth(token, device_fingerprint)


async def get_optional_user(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
) -> User | None:
  """Return the authenticated user, or None when no valid credential is sent."""
  authorization = request.headers.get("authorization")
  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]

  if jwt_token:
    device_fingerprint = extract_device_fingerprint(request)
    verify_result = verify_jwt_claims(jwt_token, device_fingerprint)
    if verify_result:
      user_id, token_session_version = verify_result
      user = _get_user_for_verified_jwt(user_id, token_session_version)
      if user:
        _publish_jwt_identity(request, user_id)
        return user

  if api_key:
    user = validate_api_key(api_key)
    if user:
      _stash_api_key_identity(request, api_key, user)
    return user

  return None


async def get_optional_jwt_user(request: Request) -> User | None:
  """Return the JWT-session user, or None — API keys are not accepted.

  For surfaces that manage sign-in credentials themselves (passkey
  enrollment): a programmatic key must never be able to mint an interactive
  credential that outlives the key's own revocation.
  """
  authorization = request.headers.get("authorization")
  if not authorization or not authorization.startswith("Bearer "):
    return None

  device_fingerprint = extract_device_fingerprint(request)
  verify_result = verify_jwt_claims(authorization[7:], device_fingerprint)
  if not verify_result:
    return None
  user_id, token_session_version = verify_result
  return _get_user_for_verified_jwt(user_id, token_session_version)


async def require_jwt_user(
  user: User | None = Depends(get_optional_jwt_user),
) -> User:
  """The JWT-session user, or 401. For routes that change sign-in credentials."""
  if user is None:
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="An interactive session is required for this action",
    )
  return user


async def get_current_user(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
) -> User:
  """Return the authenticated user, raising 401 when authentication fails."""
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  authorization = request.headers.get("authorization")
  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]

  # JWT takes precedence over the API key header.
  if jwt_token:
    device_fingerprint = extract_device_fingerprint(request)
    verify_result = verify_jwt_claims(jwt_token, device_fingerprint)
    if verify_result:
      user_id, token_session_version = verify_result
      user = _get_user_for_verified_jwt(user_id, token_session_version)
      if user:
        _publish_jwt_identity(request, user_id)
        SecurityAuditLogger.log_auth_success(
          user_id=str(user_id),
          ip_address=client_ip,
          user_agent=user_agent,
          auth_method="jwt_token",
        )
        return user

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_TOKEN_INVALID,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"token_type": "jwt"},
      risk_level="high",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers={"WWW-Authenticate": "Bearer"},
    )

  if api_key:
    user = validate_api_key(api_key)
    if user:
      _stash_api_key_identity(request, api_key, user)
      SecurityAuditLogger.log_auth_success(
        user_id=str(user.id),
        ip_address=client_ip,
        user_agent=user_agent,
        auth_method="api_key",
      )
      return user
    else:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.API_KEY_INVALID,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={"api_key_prefix": api_key[:8] if api_key else ""},
        risk_level="high",
      )
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
        headers={"WWW-Authenticate": "ApiKey"},
      )

  SecurityAuditLogger.log_auth_failure(
    reason="No authentication provided",
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint=endpoint,
  )
  raise HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
    headers={"WWW-Authenticate": "Bearer, ApiKey"},
  )


async def _resolve_user_with_graph(
  request: Request,
  graph_id: str,
  api_key: str,
  *,
  allow_deprovisioned: bool = False,
) -> User:
  """Authenticate the caller and confirm they have access to `graph_id`.

  Proves graph *membership* only. Write operations must additionally call
  `require_graph_write_role`, since a viewer passes this check.

  Raises 401 when authentication fails and 403 when the graph is not the
  caller's. ``allow_deprovisioned`` (backup export routes only) bypasses the
  cached access decision and is never written back, so it can't leak to
  another route.
  """
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  authorization = request.headers.get("authorization")
  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]

  # JWT takes precedence over the API key header.
  if jwt_token:
    device_fingerprint = extract_device_fingerprint(request)
    verify_result = verify_jwt_claims(jwt_token, device_fingerprint)
    user = None
    user_id = None
    if verify_result:
      user_id, token_session_version = verify_result
      user = _get_user_for_verified_jwt(user_id, token_session_version)

      if user and bool(user.is_active):
        has_access = (
          None
          if allow_deprovisioned
          else api_key_cache.get_cached_jwt_graph_access(str(user_id), graph_id)
        )

        if has_access is None:
          from robosystems.config.shared_repositories import (
            is_shared_repository_or_subgraph,
          )

          from ..graph.utils import MultiTenantUtils

          if is_shared_repository_or_subgraph(graph_id):
            # Shared repositories are never deprovisioned; the flag is moot.
            has_access = MultiTenantUtils.validate_repository_access(
              graph_id,
              user_id,
              "read",
            )
          else:
            has_access = _db_check_graph_access(
              user_id, graph_id, allow_deprovisioned=allow_deprovisioned
            )

          if not allow_deprovisioned:
            api_key_cache.cache_jwt_graph_access(str(user_id), graph_id, has_access)

        if has_access:
          _publish_jwt_identity(request, user_id)
          SecurityAuditLogger.log_auth_success(
            user_id=str(user_id),
            ip_address=client_ip,
            user_agent=user_agent,
            auth_method="jwt_token",
          )
          _publish_graph_authorization(request, graph_id)
          return user
        else:
          SecurityAuditLogger.log_authorization_denied(
            user_id=str(user_id),
            resource=f"graph_database:{graph_id}",
            action="access",
            ip_address=client_ip,
            endpoint=endpoint,
          )
          raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to graph",
          )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_TOKEN_INVALID,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"token_type": "jwt"},
      risk_level="high",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers={"WWW-Authenticate": "Bearer"},
    )

  if api_key:
    user = validate_api_key_with_graph(
      api_key, graph_id, allow_deprovisioned=allow_deprovisioned
    )
    if user:
      _stash_api_key_identity(request, api_key, user)
      SecurityAuditLogger.log_auth_success(
        user_id=str(user.id),
        ip_address=client_ip,
        user_agent=user_agent,
        auth_method="api_key",
      )
      _publish_graph_authorization(request, graph_id)
      return user
    else:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.API_KEY_INVALID,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={
          "api_key_prefix": api_key[:8] if api_key else "",
          "graph_id": graph_id,
        },
        risk_level="high",
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid API key or access denied to graph",
        headers={"WWW-Authenticate": "ApiKey"},
      )

  SecurityAuditLogger.log_auth_failure(
    reason="No authentication provided",
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint=endpoint,
  )
  raise HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
    headers={"WWW-Authenticate": "Bearer, ApiKey"},
  )


def graph_access_dependency(allow_deprovisioned: bool = False):
  """Build the graph-membership dependency.

  ``allow_deprovisioned=True`` is for the backup-list and backup-download
  routes only, so a departing org's OWNER/ADMIN can export during the grace
  period.
  """

  async def dependency(
    request: Request,
    graph_id: str,
    api_key: str = Security(API_KEY_HEADER),
  ) -> User:
    return await _resolve_user_with_graph(
      request, graph_id, api_key, allow_deprovisioned=allow_deprovisioned
    )

  return dependency


get_current_user_with_graph = graph_access_dependency()

# A named singleton so it is a stable, overridable dependency.
get_current_user_with_deprovisioned_graph = graph_access_dependency(
  allow_deprovisioned=True
)


def _oauth_bearer_token(request: Request) -> str | None:
  """The opaque OAuth access token in ``Authorization: Bearer``, or None.

  Distinguished from the app's JWT by its ``rfso`` prefix before any
  parsing — a JWT never starts with it, and an OAuth token is never a JWT.
  """
  authorization = request.headers.get("authorization")
  if not authorization or not authorization.startswith("Bearer "):
    return None
  candidate = authorization[7:].strip()
  return candidate if is_oauth_access_token(candidate) else None


def _mcp_challenge_headers(
  graph_id: str | None,
  *,
  product: str | None = None,
  error: str | None = None,
  description: str | None = None,
) -> dict[str, str]:
  """``WWW-Authenticate`` naming the route's resource metadata, which is how
  an OAuth client discovers the authorization server. Plain challenge when
  the OAuth surface is off."""
  from robosystems.config import env

  if not env.MCP_OAUTH_ENABLED:
    return {"WWW-Authenticate": "Bearer, ApiKey"}

  from robosystems.operations.oauth_server.resources import (
    bearer_challenge,
    route_target,
  )

  target = route_target(graph_id, product)
  return {
    "WWW-Authenticate": bearer_challenge(
      target, error=error, error_description=description
    )
  }


def _oauth_principal_graph_access(
  request: Request,
  principal: OAuthPrincipal,
  graph_id: str,
  *,
  route_graph_id: str | None,
  product: str | None = None,
) -> None:
  """Live membership check for an OAuth principal, so a revoked membership
  stops a still-valid token. Raises 403 ``insufficient_scope``; the challenge
  names the route's resource (``route_graph_id``), not the grant's graph.
  """
  client_ip = request.client.host if request.client else None
  endpoint = str(request.url.path)
  user_id = principal.user_id

  has_access = api_key_cache.get_cached_jwt_graph_access(user_id, graph_id)
  if has_access is None:
    from robosystems.config.shared_repositories import (
      is_shared_repository_or_subgraph,
    )

    from ..graph.utils import MultiTenantUtils

    if is_shared_repository_or_subgraph(graph_id):
      has_access = MultiTenantUtils.validate_repository_access(
        graph_id, user_id, "read"
      )
    else:
      has_access = _db_check_graph_access(user_id, graph_id)
    api_key_cache.cache_jwt_graph_access(user_id, graph_id, bool(has_access))

  if not has_access:
    SecurityAuditLogger.log_authorization_denied(
      user_id=user_id,
      resource=f"graph_database:{graph_id}",
      action="access",
      ip_address=client_ip,
      endpoint=endpoint,
    )
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Access denied to graph",
      headers=_mcp_challenge_headers(
        route_graph_id,
        product=product,
        error="insufficient_scope",
        description="Access denied to graph",
      ),
    )


def _resolve_oauth_principal(
  request: Request, token: str, graph_id: str | None, product: str | None = None
) -> OAuthPrincipal:
  """Validate an OAuth bearer and bind it to this MCP route.

  ``graph_id`` is ``None`` on the graph-agnostic routes. The token's audience
  must be exactly this route's resource, and its graph the URL's graph where
  the URL names one. Failures answer 401 ``invalid_token`` so clients refresh.
  """
  from robosystems.operations.oauth_server.resources import route_target

  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  principal = validate_oauth_access_token(token)
  expected = route_target(graph_id, product)
  audience_ok = principal is not None and principal.resource == expected.resource
  graph_ok = principal is not None and (
    graph_id is None or principal.graph_id == graph_id
  )

  if principal is None or not audience_ok or not graph_ok:
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_TOKEN_INVALID,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={
        "token_type": "oauth",
        "reason": "invalid"
        if principal is None
        else ("audience_mismatch" if not audience_ok else "graph_mismatch"),
      },
      risk_level="high" if principal is None else "medium",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers=_mcp_challenge_headers(
        graph_id,
        product=product,
        error="invalid_token",
        description="Invalid or expired token",
      ),
    )

  resolved_graph = graph_id or principal.graph_id
  _oauth_principal_graph_access(
    request, principal, resolved_graph, route_graph_id=graph_id, product=product
  )

  publish_principal(request, principal.user_id, "oauth", api_key=token)
  SecurityAuditLogger.log_auth_success(
    user_id=principal.user_id,
    ip_address=client_ip,
    user_agent=user_agent,
    auth_method="oauth",
  )
  _publish_graph_authorization(request, resolved_graph)
  return principal


async def get_current_user_with_graph_or_oauth(
  request: Request,
  graph_id: str,
  api_key: str = Security(API_KEY_HEADER),
) -> User:
  """Graph authentication for the per-graph MCP route.

  An OAuth bearer (when enabled) must be bound to this route; otherwise this is
  ``get_current_user_with_graph``. Nothing is read from the query string. A 401
  carries the OAuth discovery challenge.
  """
  from robosystems.config import env

  oauth_token = _oauth_bearer_token(request)
  if oauth_token is not None and env.MCP_OAUTH_ENABLED:
    return _resolve_oauth_principal(request, oauth_token, graph_id).user

  try:
    return await get_current_user_with_graph(request, graph_id, api_key)
  except HTTPException as exc:
    if exc.status_code == status.HTTP_401_UNAUTHORIZED:
      exc.headers = _mcp_challenge_headers(graph_id)
    raise


async def get_oauth_mcp_principal(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
  token: str | None = Query(None, include_in_schema=False),
) -> OAuthPrincipal:
  """The graph-agnostic MCP route's only credential: an OAuth bearer.

  Only an OAuth token carries the grant, so any other credential answers 401
  with the discovery challenge. ``token`` is read only to audit it. The
  principal's graph becomes the transport's ``graph_id``.
  """
  return _require_agnostic_oauth_principal(request, api_key, token, product=None)


async def get_oauth_roboledger_mcp_principal(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
  token: str | None = Query(None, include_in_schema=False),
) -> OAuthPrincipal:
  """As ``get_oauth_mcp_principal``, bound to ``/v1/mcp/roboledger``."""
  from robosystems.operations.oauth_server.resources import PRODUCT_ROBOLEDGER

  return _require_agnostic_oauth_principal(
    request, api_key, token, product=PRODUCT_ROBOLEDGER
  )


def _require_agnostic_oauth_principal(
  request: Request, api_key: str | None, token: str | None, product: str | None
) -> OAuthPrincipal:
  from robosystems.config import env

  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  if not env.MCP_OAUTH_ENABLED:
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")

  oauth_token = _oauth_bearer_token(request)
  if oauth_token is None:
    if api_key or token:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={"action": "non_oauth_credential_on_agnostic_mcp"},
        risk_level="low",
      )
    else:
      SecurityAuditLogger.log_auth_failure(
        reason="No authentication provided",
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
      )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="OAuth bearer token required",
      headers=_mcp_challenge_headers(None, product=product),
    )

  return _resolve_oauth_principal(request, oauth_token, None, product)


def require_graph_write_role(user_id: str, graph_id: str) -> None:
  """Assert the user may write to the graph right now; raises 403 otherwise.

  Checks role (membership alone is not enough; ``viewer`` is read-only) and
  lifecycle (``require_graph_access(require_write=True)``). The shared write
  gate for every command surface, so a state that blocks writes on one blocks
  them on all.
  """
  from robosystems.database import SessionFactory
  from robosystems.middleware.billing.enforcement import require_graph_access
  from robosystems.models.core import GraphUser

  session = SessionFactory()
  try:
    if not GraphUser.user_has_write_access(user_id, graph_id, session):
      # Emits the `AuthorizationDenied` detective-control metric.
      from robosystems.security import SecurityAuditLogger

      SecurityAuditLogger.log_authorization_denied(
        user_id=user_id, resource=graph_id, action="write"
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Write access denied to graph {graph_id}; your role is read-only.",
      )
    require_graph_access(graph_id, session, require_write=True)
  finally:
    session.close()


def user_is_graph_admin(user_id: str, graph_id: str) -> bool:
  """Whether the user holds the admin role on the graph.

  A predicate, not a gate: callers use it to widen an authorization rule.
  """
  from robosystems.database import SessionFactory
  from robosystems.models.core import GraphUser

  session = SessionFactory()
  try:
    return GraphUser.user_has_admin_access(user_id, graph_id, session)
  finally:
    session.close()


async def get_current_user_with_repository_access(
  request: Request,
  repository_id: str,
  operation_type: str = "read",
  api_key: str = Security(API_KEY_HEADER),
) -> User:
  """Authenticate the caller and confirm `operation_type` access to a shared
  repository such as `sec`. Raises 403 when access is denied.
  """
  current_user = await get_current_user(request, api_key)

  if not validate_repository_access(current_user, repository_id, operation_type):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"{repository_id.upper()} repository {operation_type} access denied",
    )

  return current_user


def get_repository_user_dependency(repository_id: str, operation_type: str = "read"):
  """Build a dependency that gates on `operation_type` access to a repository."""

  async def _get_repository_user(
    request: Request,
    api_key: str = Security(API_KEY_HEADER),
  ) -> User:
    return await get_current_user_with_repository_access(
      request, repository_id, operation_type, api_key
    )

  return _get_repository_user


async def get_current_user_sse(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
  authorization: str | None = Header(None),
  token: str | None = Query(None, description="JWT token for SSE authentication"),
) -> User:
  """Authenticate an SSE caller, also accepting the JWT in a `token` query
  param (EventSource cannot send headers; `middleware/logging.py` redacts it).
  """
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]
  elif token:
    jwt_token = token

  if jwt_token:
    device_fingerprint = extract_device_fingerprint(request)
    verify_result = verify_jwt_claims(jwt_token, device_fingerprint)
    if verify_result:
      user_id, token_session_version = verify_result
      user = _get_user_for_verified_jwt(user_id, token_session_version)
      if user:
        _publish_jwt_identity(request, user_id)
        SecurityAuditLogger.log_auth_success(
          user_id=str(user_id),
          ip_address=client_ip,
          user_agent=user_agent,
          auth_method="jwt_token",
        )
        return user

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_TOKEN_INVALID,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"token_type": "jwt"},
      risk_level="high",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers={"WWW-Authenticate": "Bearer"},
    )

  if api_key:
    user = validate_api_key(api_key)
    if user:
      _stash_api_key_identity(request, api_key, user)
      SecurityAuditLogger.log_auth_success(
        user_id=str(user.id),
        ip_address=client_ip,
        user_agent=user_agent,
        auth_method="api_key",
      )
      return user
    else:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.API_KEY_INVALID,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={"api_key_prefix": api_key[:8] if api_key else ""},
        risk_level="high",
      )
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
        headers={"WWW-Authenticate": "ApiKey"},
      )

  SecurityAuditLogger.log_auth_failure(
    reason="No authentication provided",
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint=endpoint,
  )
  raise HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
    headers={"WWW-Authenticate": "Bearer, ApiKey"},
  )
