"""FastAPI authentication dependencies.

Every dependency here accepts either a JWT bearer token or an ``X-API-Key``
header, with JWT taking precedence. Two variants also read a credential from
a query parameter, because EventSource (SSE) and MCP connector clients cannot
send custom headers.

Query-parameter credentials are redacted from logs by the sensitive-params
list in ``middleware/logging.py`` — log ``request.url.path``, never the full
URL, or the credential leaks.
"""

from typing import Any

from fastapi import Header, HTTPException, Query, Request, Security, status
from fastapi.security import APIKeyHeader

from ...logger import logger
from ...models.core import User
from ...security import SecurityAuditLogger, SecurityEventType
from ...security.device_fingerprinting import extract_device_fingerprint
from .cache import api_key_cache

# Import JWT helpers from local jwt module to avoid circular imports
from .jwt import (
  verify_jwt_claims as verify_jwt_claims_from_auth,
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
  """Look up a user by ID using a short-lived session.

  Returns a detached User object so no pool connection is held after
  this function returns.  The scoped ``session`` proxy would keep the
  connection checked out until the DatabaseSessionMiddleware cleanup at
  end-of-request — disastrous for long-running endpoints (MCP, SSE).
  """
  from ...database import SessionFactory

  sess = SessionFactory()
  try:
    user = User.get_by_id(user_id, sess)
    if not user:
      return None
    # Detach so the pool connection is returned immediately.
    sess.expunge(user)
    return user
  finally:
    sess.close()


def _db_check_graph_access(user_id: str, graph_id: str) -> bool:
  """Check graph access using a short-lived session.

  Same rationale as ``_db_get_user_by_id`` — avoid holding a pool
  connection for the entire request duration.
  """
  from ...database import SessionFactory
  from ...models.core import GraphUser

  sess = SessionFactory()
  try:
    return GraphUser.user_has_access(user_id, graph_id, sess)
  finally:
    sess.close()


def _serialize_user_for_jwt_cache(user: User) -> dict[str, Any]:
  """Serialize the stable user fields needed by JWT auth dependencies."""
  return {
    "id": str(user.id),
    "email": user.email,
    "name": user.name,
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
  """Record which API key authenticated this request on request.state.

  The first 8 characters are the key's stored identification prefix
  (``UserAPIKey.prefix``), so downstream telemetry can attribute activity
  to a specific key without access to the raw header. JWT-authenticated
  requests leave the attribute unset.

  When the resolved ``user`` is passed, the sanitized principal is also
  published as ``request.state.auth_user_id`` / ``auth_method``, so
  downstream consumers that cannot reparse the credential — the rate
  limiter for URL-token connectors, and eventually opaque OAuth bearer
  tokens — can identify the caller without re-validating anything.
  """
  request.state.api_key_prefix = api_key[:8]
  if user is not None:
    request.state.auth_user_id = str(user.id)
    request.state.auth_method = auth_method


def verify_jwt_claims(
  token: str, device_fingerprint: dict[str, Any] | None = None
) -> tuple[str, int] | None:
  """Verify a JWT token's claims and return (user_id, session_version) if valid.

  See ``robosystems.middleware.auth.jwt.verify_jwt_claims`` for full semantics.
  Notably, this does NOT compare session_version against the User row; the
  caller must do that (the dependency wrappers in this module use
  ``_get_user_for_verified_jwt`` which handles it via the user-data cache).
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
        return user

  if api_key:
    user = validate_api_key(api_key)
    if user:
      _stash_api_key_identity(request, api_key, user)
    return user

  return None


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


async def get_current_user_with_graph(
  request: Request,
  graph_id: str,
  api_key: str = Security(API_KEY_HEADER),
) -> User:
  """Authenticate the caller and confirm they have access to `graph_id`.

  Proves graph *membership* only. Write operations must additionally call
  `require_graph_write_role`, since a viewer passes this check.

  Raises 401 when authentication fails and 403 when the graph is not the
  caller's.
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
        has_access = api_key_cache.get_cached_jwt_graph_access(str(user_id), graph_id)

        if has_access is None:
          from robosystems.config.shared_repositories import (
            is_shared_repository_or_subgraph,
          )

          from ..graph.utils import MultiTenantUtils

          if is_shared_repository_or_subgraph(graph_id):
            # Resolves a subgraph to its parent repository before checking.
            has_access = MultiTenantUtils.validate_repository_access(
              graph_id,
              user_id,
              "read",
            )
          else:
            has_access = _db_check_graph_access(user_id, graph_id)

          api_key_cache.cache_jwt_graph_access(str(user_id), graph_id, has_access)

        if has_access:
          SecurityAuditLogger.log_auth_success(
            user_id=str(user_id),
            ip_address=client_ip,
            user_agent=user_agent,
            auth_method="jwt_token",
          )
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
    user = validate_api_key_with_graph(api_key, graph_id)
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


async def get_current_user_with_graph_or_url_token(
  request: Request,
  graph_id: str,
  api_key: str = Security(API_KEY_HEADER),
  token: str | None = Query(
    None,
    description="Graph-scoped API key carried in the URL, for MCP connector "
    "clients that cannot send custom headers",
  ),
) -> User:
  """Graph authentication that also accepts a graph-scoped API key via the
  ``token`` query parameter.

  Built for the MCP Streamable HTTP endpoint: claude.ai / Claude Desktop
  custom connectors cannot send custom headers, so the connector URL carries a
  graph-scoped key instead. Header and JWT authentication take precedence and
  behave exactly like ``get_current_user_with_graph``; the query parameter is
  consulted only when neither is present, and it honors only keys whose scope
  covers the URL's graph — an account-wide key in a URL is rejected outright,
  so the account credential can never be the one that travels in a URL.

  ``token`` is in the sensitive-query-params redaction list
  (``middleware/logging.py``), so it never appears in logged URLs.
  """
  authorization = request.headers.get("authorization")
  has_header_auth = bool(api_key) or bool(
    authorization and authorization.startswith("Bearer ")
  )

  if has_header_auth or not token:
    # Normal path — including the no-credentials 401.
    return await get_current_user_with_graph(request, graph_id, api_key)

  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  user = validate_api_key_with_graph(token, graph_id, require_scoped=True)
  if user:
    _stash_api_key_identity(request, token, user, auth_method="api_key_url")
    SecurityAuditLogger.log_auth_success(
      user_id=str(user.id),
      ip_address=client_ip,
      user_agent=user_agent,
      auth_method="api_key_url",
    )
    return user

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.API_KEY_INVALID,
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint=endpoint,
    details={
      "api_key_prefix": token[:8] if token else "",
      "graph_id": graph_id,
      "carriage": "url_token",
    },
    risk_level="high",
  )
  # Same deliberate conflation as the header path: no validity oracle.
  raise HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail="Invalid API key or access denied to graph",
    headers={"WWW-Authenticate": "ApiKey"},
  )


def require_graph_write_role(user_id: str, graph_id: str) -> None:
  """Assert the user holds a write role (member/admin) on the graph.

  ``viewer`` is read-only; bare graph *membership* (which
  ``get_current_user_with_graph`` proves) is not sufficient to mutate a graph.
  This is the single write-role authorization check the command surfaces share
  — REST command ops (the extensions registrar, content-ops) call it before
  dispatching, and the MCP surface enforces the same via
  ``validate_mcp_access(..., "write")``. Raises 403 for a read-only role.

  Opens a short-lived platform session — ``GraphUser`` lives in the platform
  DB, not the per-graph OLTP DB.
  """
  from robosystems.database import SessionFactory
  from robosystems.models.core import GraphUser

  session = SessionFactory()
  try:
    if not GraphUser.user_has_write_access(user_id, graph_id, session):
      # Audit every under-privileged write attempt from the one shared gate:
      # this covers MCP, the REST registrar, content-ops, and the hand-written
      # lifecycle ops, and emits the `AuthorizationDenied` detective-control
      # metric. (No request context here — this is a plain helper, not a dep.)
      from robosystems.security import SecurityAuditLogger

      SecurityAuditLogger.log_authorization_denied(
        user_id=user_id, resource=graph_id, action="write"
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"Write access denied to graph {graph_id}; your role is read-only.",
      )
  finally:
    session.close()


def user_is_graph_admin(user_id: str, graph_id: str) -> bool:
  """Whether the user holds the admin role on the graph.

  A predicate, not a gate — unlike ``require_graph_write_role`` this returns a
  bool rather than raising, because callers use it to *widen* an authorization
  rule rather than to deny. The cross-graph share surface is the first such
  caller: a report copied in from another graph carries the *sender's* user id
  in ``created_by``, so the receiving graph's owner rule can never match, and a
  graph admin is who gets to remove it.

  Opens a short-lived platform session — ``GraphUser`` lives in the platform DB,
  not the per-graph OLTP DB.
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
  """Authenticate an SSE caller, accepting the JWT in a `token` query param.

  The EventSource API cannot send custom headers, so the browser passes the
  token in the URL. `middleware/logging.py` redacts it from logged URLs.
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
