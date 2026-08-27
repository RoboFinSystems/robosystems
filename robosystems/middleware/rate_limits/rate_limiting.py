"""Rate limiting dependencies for FastAPI."""

import time

import jwt
from fastapi import HTTPException, Request, status

from ...config import env
from ...config.rate_limits import BURST_LIMITS
from ...security import SecurityAuditLogger, SecurityEventType
from .cache import rate_limit_cache
from .subscription_rate_limits import (
  get_endpoint_category,
  get_subscription_rate_limit,
  should_use_subscription_limits,
)


def _verify_jwt_for_rate_limiting(token: str) -> str | None:
  """Safely verify JWT token for rate limiting purposes only."""
  try:
    secret_key = env.JWT_SECRET_KEY
    if not secret_key:
      return None

    # Validate the signature, but skip audience/issuer checks. Tokens are
    # minted with `aud`/`iss` claims (see middleware/auth/jwt.py), and PyJWT
    # raises InvalidAudienceError when an `aud` claim is present but no
    # `audience` is passed to decode(). That exception would be swallowed
    # below and drop every authenticated request to the anonymous "base"
    # rate-limit tier. For rate limiting we only need to identify the user,
    # so the signature is what matters — not aud/iss.
    payload = jwt.decode(
      token,
      secret_key,
      algorithms=["HS256"],
      options={"verify_aud": False, "verify_iss": False},
    )
    return payload.get("user_id")
  except Exception:
    # If JWT is invalid, treat as unauthenticated for rate limiting
    return None


def _api_key_digest(api_key: str) -> str:
  import hashlib

  return hashlib.sha256(api_key.encode()).hexdigest()


def _api_key_is_cached_valid(api_key_hash: str) -> bool:
  from robosystems.middleware.auth.cache import api_key_cache

  try:
    return api_key_cache.get_cached_api_key_validation(api_key_hash) is not None
  except Exception:
    return False


def _caller_is_authorized_on_graph(
  request: Request, user_id: str, graph_id: str, parent_graph_id: str
) -> bool:
  """Whether the identified caller is known to be authorized on the graph.

  A graph's budget belongs to its members, so it is charged only on proven
  authorization — never on a request that merely names the graph in its
  URL. Evidence, cheapest first: the graph auth
  dependency already ran and published its decision on ``request.state``;
  otherwise the auth cache holds a positive decision for this principal on
  the graph (or its parent) from an earlier request. Absent both, the
  request is not graph-keyed — it falls back to the caller's own budget.
  """
  if getattr(request.state, "auth_graph_id", None) in (graph_id, parent_graph_id):
    return True

  from robosystems.middleware.auth.cache import api_key_cache

  try:
    if user_id.startswith("apikey_"):
      digest = user_id[len("apikey_") :]
      lookup = lambda g: api_key_cache.get_cached_graph_access(digest, g)  # noqa: E731
    else:
      lookup = lambda g: api_key_cache.get_cached_jwt_graph_access(user_id, g)  # noqa: E731
    return any(lookup(g) is True for g in dict.fromkeys((graph_id, parent_graph_id)))
  except Exception:
    return False


def get_user_identifier(request: Request) -> str:
  """Get user identifier for rate limiting based on authentication."""
  # A key is an identity only once it is known to be valid (see
  # `get_user_from_request`); an unverified header falls through to the
  # published principal or the IP bucket, so junk keys cannot each mint a
  # budget of their own.
  api_key = request.headers.get("X-API-Key")
  if api_key:
    api_key_hash = _api_key_digest(api_key)
    if _api_key_is_cached_valid(api_key_hash):
      return f"apikey:{api_key_hash}"

  auth_header = request.headers.get("Authorization")
  if auth_header and auth_header.startswith("Bearer "):
    token = auth_header[7:]
    user_id = _verify_jwt_for_rate_limiting(token)
    if user_id:
      return f"jwt:{user_id}"

  # Check for JWT authentication in HTTP-only cookie
  auth_token = request.cookies.get("auth-token")
  if auth_token:
    user_id = _verify_jwt_for_rate_limiting(auth_token)
    if user_id:
      return f"jwt:{user_id}"

  # Principal published by the auth dependencies (request.state.auth_user_id).
  # Covers credentials this function cannot reparse — the MCP connector's
  # URL-carried key today, opaque OAuth bearer tokens later. Only set after
  # successful validation, so it never grants identity to junk credentials.
  auth_user_id = getattr(request.state, "auth_user_id", None)
  if isinstance(auth_user_id, str) and auth_user_id:
    return f"apikey:user:{auth_user_id}"

  # Fallback to IP address for unauthenticated requests
  client_ip = request.client.host if request.client else "unknown"
  return f"ip:{client_ip}"


def get_rate_limit_config(identifier: str) -> tuple[int, int]:
  """Get rate limiting configuration based on identifier type.

  BURST-FOCUSED: Short windows for burst protection.
  Volume is controlled by credits, not rate limits.
  """
  # Environment-based rate limits - BURST PROTECTION ONLY
  if identifier.startswith("apikey:"):
    # API key users get very high burst limits
    limit = BURST_LIMITS["api_key"]  # 1k/minute default
    window = 60  # 1 minute (60k/hour possible)
  elif identifier.startswith("jwt:"):
    # JWT users get high burst limits
    limit = BURST_LIMITS["jwt"]  # 500/minute default
    window = 60  # 1 minute (30k/hour possible)
  else:
    # IP-based (unauthenticated) users still get restricted
    limit = BURST_LIMITS["anonymous"]  # 10/minute default
    window = 60  # 1 minute (600/hour possible)

  return limit, window


def rate_limit_dependency(request: Request):
  """FastAPI dependency for rate limiting."""
  identifier = get_user_identifier(request)
  limit, window = get_rate_limit_config(identifier)

  allowed, remaining = rate_limit_cache.check_rate_limit(identifier, limit, window)

  if not allowed:
    # Log rate limit violation for security monitoring
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    # Extract user ID if available
    user_id = None
    if ":" in identifier and identifier.startswith("user:"):
      user_id = identifier.split(":")[1]

    identifier_type = identifier.split(":")[0] if ":" in identifier else "anonymous"
    limit_type_str = f"general_api_{identifier_type}"

    SecurityAuditLogger.log_rate_limit_exceeded(
      user_id=user_id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=limit_type_str,
    )

    # Record OTel rate limit metric
    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type=limit_type_str,
        identifier_type=identifier_type,
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

    # Calculate reset time safely
    current_time = getattr(request.state, "current_time", None) or int(time.time())
    reset_time = int(current_time + window)

    raise HTTPException(
      status_code=status.HTTP_429_TOO_MANY_REQUESTS,
      detail="Rate limit exceeded",
      headers={
        "Retry-After": str(window),
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(reset_time),
      },
    )

  request.state.rate_limit_remaining = remaining
  request.state.rate_limit_limit = limit


def auth_rate_limit_dependency(request: Request):
  """Strict rate limiting for authentication endpoints."""
  client_ip = request.client.host if request.client else "unknown"
  identifier = f"auth_ip:{client_ip}"

  # Get endpoint-specific limits (security constants)
  path = request.url.path
  if "/login" in path:
    limit = BURST_LIMITS["login_attempts"]
    window = BURST_LIMITS["login_window"]  # 5 minutes
  elif "/register" in path:
    limit = BURST_LIMITS["register_attempts"]
    window = BURST_LIMITS["register_window"]  # 1 hour
  else:
    # Default auth endpoint limits
    limit = BURST_LIMITS["auth_attempts"]  # 10 attempts
    window = BURST_LIMITS["auth_window"]  # 5 minutes

  # Auth endpoints fail CLOSED: if the limiter backend is down, deny rather
  # than silently disable brute-force protection on login/register.
  allowed, remaining = rate_limit_cache.check_rate_limit(
    identifier, limit, window, fail_closed=True
  )

  if not allowed:
    # Log authentication rate limit violation - this is high risk
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    auth_limit_type = f"auth_{path.split('/')[-1] if '/' in path else 'unknown'}"

    SecurityAuditLogger.log_rate_limit_exceeded(
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=auth_limit_type,
    )

    # Record OTel rate limit metric
    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type=auth_limit_type,
        identifier_type="ip",
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

    # Also log as suspicious activity for auth endpoints
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={
        "rate_limit_exceeded": True,
        "endpoint_type": "authentication",
        "limit": limit,
        "window": window,
      },
      risk_level="high",
    )

    raise HTTPException(
      status_code=status.HTTP_429_TOO_MANY_REQUESTS,
      detail="Too many authentication attempts. Please try again later.",
      headers={
        "Retry-After": str(window),
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": "0",
      },
    )

  request.state.auth_rate_limit_remaining = remaining
  request.state.auth_rate_limit_limit = limit


def get_user_from_request(request: Request) -> str | None:
  """Extract user ID from request for user-specific rate limiting."""
  auth_header = request.headers.get("Authorization")
  if auth_header and auth_header.startswith("Bearer "):
    token = auth_header[7:]
    user_id = _verify_jwt_for_rate_limiting(token)
    if user_id:
      return user_id

  # Check for JWT authentication in HTTP-only cookie
  auth_token = request.cookies.get("auth-token")
  if auth_token:
    user_id = _verify_jwt_for_rate_limiting(auth_token)
    if user_id:
      return user_id

  # API key: an identity only once the key is known to be valid. The auth
  # cache is keyed by the same digest and is populated by every successful
  # validation, so this is one cache read and no re-validation. Identity
  # requires a cache-validated key; an unvalidated header mints none, so a
  # budget is always tied to a principal the platform has authenticated.
  api_key = request.headers.get("X-API-Key")
  if api_key:
    api_key_hash = _api_key_digest(api_key)
    if _api_key_is_cached_valid(api_key_hash):
      return f"apikey_{api_key_hash}"

  # Principal published by the auth dependencies for credentials that carry
  # no reparseable header — the MCP connector's URL-carried key today, opaque
  # OAuth bearer tokens later. Without this, connector traffic is bucketed as
  # anonymous-by-IP, and every client behind one egress IP shares that budget.
  auth_user_id = getattr(request.state, "auth_user_id", None)
  if isinstance(auth_user_id, str) and auth_user_id:
    return auth_user_id

  return None


# Custom rate limiting dependencies for different endpoint categories
def create_custom_rate_limit_dependency(
  limit_per_hour: int,
  window_seconds: int = 3600,
  limit_name: str = "custom",
  identifier_fn=None,
  fail_closed: bool = False,
):
  """Create a custom rate limiting dependency with specific limits.

  ``identifier_fn`` overrides how the bucket key is derived from the request
  (default: ``get_user_identifier``). Return an ``apikey:``/``jwt:``-prefixed
  string to grant the full limit, anything else for the anonymous 1/10th.

  ``fail_closed=True`` denies when the limiter backend is unavailable
  instead of waving traffic through — for surfaces whose protection must not
  silently disappear with Redis (the auth pattern).
  """
  resolve_identifier = identifier_fn or get_user_identifier

  def dependency(request: Request):
    identifier = resolve_identifier(request)

    # Include limit_name in the cache key to isolate each rate limit category
    # This prevents high-limit endpoints from polluting low-limit endpoint counters
    cache_key = f"{identifier}:{limit_name}"

    # Apply the specified limit regardless of user type
    # (authenticated users still get better limits than anonymous)
    if identifier.startswith("apikey:"):
      # API key users get the full limit
      limit = limit_per_hour
    elif identifier.startswith("jwt:"):
      # JWT users get the full limit
      limit = limit_per_hour
    else:
      # Anonymous users get 1/10th the limit
      limit = max(1, limit_per_hour // 10)

    window = window_seconds

    allowed, remaining = rate_limit_cache.check_rate_limit(
      cache_key, limit, window, fail_closed=fail_closed
    )

    if not allowed:
      current_time = getattr(request.state, "current_time", None) or int(time.time())
      reset_time = int(current_time + window)

      raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail=f"Rate limit exceeded for {limit_name} operations",
        headers={
          "Retry-After": str(window),
          "X-RateLimit-Limit": str(limit),
          "X-RateLimit-Remaining": "0",
          "X-RateLimit-Reset": str(reset_time),
          "X-RateLimit-Type": limit_name,
        },
      )

    setattr(request.state, f"{limit_name}_rate_limit_remaining", remaining)
    setattr(request.state, f"{limit_name}_rate_limit_limit", limit)

  return dependency


# Specific rate limiting dependencies for different endpoint categories
# BURST-FOCUSED: All use per-minute windows for burst protection


def user_management_rate_limit_dependency(request: Request):
  """Rate limiting for user profile and settings endpoints."""
  limit = BURST_LIMITS["user_management"]  # 600/minute (10/second)
  return create_custom_rate_limit_dependency(limit, 60, "user_management")(request)


def sync_operations_rate_limit_dependency(request: Request):
  """Rate limiting for external sync operations (QB, SEC)."""
  limit = BURST_LIMITS["sync_ops"]  # 50/minute
  return create_custom_rate_limit_dependency(limit, 60, "sync_operations")(request)


def connection_management_rate_limit_dependency(request: Request):
  """Rate limiting for external connection setup/management."""
  limit = BURST_LIMITS["connection_mgmt"]  # 30/minute
  return create_custom_rate_limit_dependency(limit, 60, "connection_mgmt")(request)


def analytics_rate_limit_dependency(request: Request):
  """Rate limiting for graph analytics and metrics endpoints."""
  limit = BURST_LIMITS["analytics"]  # 100/minute
  return create_custom_rate_limit_dependency(limit, 60, "analytics")(request)


def backup_operations_rate_limit_dependency(request: Request):
  """Rate limiting for backup creation and export operations."""
  limit = BURST_LIMITS["backup_ops"]  # 10/minute (expensive operations)
  return create_custom_rate_limit_dependency(limit, 60, "backup_operations")(request)


def sensitive_auth_rate_limit_dependency(request: Request):
  """Rate limiting for sensitive auth operations (refresh, SSO)."""
  limit = BURST_LIMITS["sensitive_auth"]  # 60/minute
  return create_custom_rate_limit_dependency(limit, 60, "sensitive_auth")(request)


def logout_rate_limit_dependency(request: Request):
  """Rate limiting for logout endpoint - more generous to handle expired tokens."""
  # 300/minute for authenticated, 30/minute for anonymous (after division by 10)
  limit = BURST_LIMITS["logout"]
  return create_custom_rate_limit_dependency(limit, 60, "logout")(request)


def tasks_management_rate_limit_dependency(request: Request):
  """Rate limiting for task monitoring and management."""
  limit = BURST_LIMITS["tasks"]  # 200/minute
  return create_custom_rate_limit_dependency(limit, 60, "tasks")(request)


def auth_status_rate_limit_dependency(request: Request):
  """Rate limiting for auth status check endpoints (like /auth/me)."""
  limit = BURST_LIMITS["auth_status"]  # 600/minute (10/second)
  return create_custom_rate_limit_dependency(limit, 60, "auth_status")(request)


def sso_rate_limit_dependency(request: Request):
  """Rate limiting for SSO operations (token generation/exchange)."""
  limit = BURST_LIMITS["sso"]  # 100/minute
  return create_custom_rate_limit_dependency(limit, 60, "sso")(request)


def oidc_rate_limit_dependency(request: Request):
  """Rate limiting for the OIDC login/callback browser surface.

  Both endpoints share this IP-keyed bucket, and a full flow spends two
  requests (login → callback). Anonymous callers get limit//10, so the
  default keeps ~12/min per IP — several complete flows plus retries.
  """
  limit = BURST_LIMITS["oidc"]  # 120/minute (2 per flow)
  return create_custom_rate_limit_dependency(limit, 60, "oidc")(request)


def oauth_authorize_rate_limit_dependency(request: Request):
  """The OAuth authorization endpoint: a browser GET, IP-keyed (anonymous
  callers get limit//10, ~12/min per IP — several consent flows plus
  retries). Fails closed: the endpoint cannot complete without Valkey
  anyway, and the client-metadata fetch it triggers must never run
  unmetered."""
  limit = BURST_LIMITS["oauth_authorize"]
  return create_custom_rate_limit_dependency(
    limit, 60, "oauth_authorize", fail_closed=True
  )(request)


def oauth_consent_rate_limit_dependency(request: Request):
  """The consent read + decision endpoints (JWT session)."""
  limit = BURST_LIMITS["oauth_consent"]
  return create_custom_rate_limit_dependency(limit, 60, "oauth_consent")(request)


def oauth_token_rate_limit_dependency(request: Request):
  """The token + revocation endpoints. Unauthenticated by nature (the
  client proves itself in the body), so effectively per-IP at limit//10.
  Fails closed: a token mint must not proceed unmetered."""
  limit = BURST_LIMITS["oauth_token"]
  return create_custom_rate_limit_dependency(
    limit, 60, "oauth_token", fail_closed=True
  )(request)


def oauth_register_rate_limit_dependency(request: Request):
  """RFC 7591 dynamic registration — the platform's only unauthenticated
  write. Per-IP at limit//10 here, plus the daily cap in
  ``operations.oauth_server.clients``. Fails closed."""
  limit = BURST_LIMITS["oauth_register"]
  return create_custom_rate_limit_dependency(
    limit, 60, "oauth_register", fail_closed=True
  )(request)


def mfa_rate_limit_dependency(request: Request):
  """Rate limiting for the passkey login/MFA challenge surface.

  Covers the second-factor handshake, passwordless login, and both
  registration lanes. Pre-session callers (the handshake, passwordless,
  forced enrollment) are anonymous and get limit//10 (~12/min per IP; a
  full ceremony spends two requests); the authenticated settings-flow
  enrollment shares the bucket at the full JWT-keyed limit.
  Fails closed: this is an authentication surface, and its throttle must
  not silently vanish with the limiter backend (the SCIM precedent).
  """
  limit = BURST_LIMITS["mfa"]  # 120/minute (2 per handshake)
  return create_custom_rate_limit_dependency(limit, 60, "mfa", fail_closed=True)(
    request
  )


def passkey_management_rate_limit_dependency(request: Request):
  """Rate limiting for authenticated passkey lifecycle endpoints
  (list/enroll/remove/recovery-codes)."""
  limit = BURST_LIMITS["passkey_management"]  # 60/minute
  return create_custom_rate_limit_dependency(
    limit, 60, "passkey_management", fail_closed=True
  )(request)


def _scim_rate_limit_identifier(request: Request) -> str:
  """Key the SCIM bucket off the bearer token, not the caller IP.

  The SCIM token is an opaque ``rfss...`` string that ``get_user_identifier``
  cannot reparse, so without this it would fall to the anonymous IP path —
  throttling a legitimate IdP full-sync to a tenth of the budget and making
  every tenant behind one egress IP share a bucket. The token *hash* is the
  key; the raw token never lands in a cache key.
  """
  auth_header = request.headers.get("Authorization", "")
  if auth_header.startswith("Bearer "):
    import hashlib

    token_hash = hashlib.sha256(auth_header[7:].encode()).hexdigest()
    return f"apikey:scim:{token_hash}"
  # No bearer → anonymous; the request 401s at require_scim_org anyway.
  return get_user_identifier(request)


def scim_rate_limit_dependency(request: Request):
  """Rate limiting for the SCIM provisioning surface (server-to-server).

  Fails closed: SCIM is an authentication surface, and its throttles must
  not silently vanish with the limiter backend. The IdP retries a denial.
  """
  limit = BURST_LIMITS["scim"]  # 120/minute per token
  return create_custom_rate_limit_dependency(
    limit, 60, "scim", identifier_fn=_scim_rate_limit_identifier, fail_closed=True
  )(request)


def _scim_ip_identifier(request: Request) -> str:
  # The `apikey:` prefix opts into the full limit (see
  # create_custom_rate_limit_dependency); the key itself is purely IP-derived.
  client_ip = request.client.host if request.client else "unknown"
  return f"apikey:scim-ip:{client_ip}"


def scim_ip_rate_limit_dependency(request: Request):
  """Pre-authentication IP throttle in front of the SCIM surface.

  The per-token bucket above keys off the *presented* bearer, so each newly
  invented bogus bearer would otherwise mint a fresh full-size bucket; this
  IP-keyed backstop caps that churn before authentication. Sized above the
  per-token ``scim`` limit so a legitimate IdP burst exhausts its per-token
  budget first, never this one.
  """
  limit = BURST_LIMITS["scim_ip"]  # 300/minute per IP
  return create_custom_rate_limit_dependency(
    limit, 60, "scim_ip", identifier_fn=_scim_ip_identifier, fail_closed=True
  )(request)


def general_api_rate_limit_dependency(request: Request):
  """General rate limiting for standard API endpoints."""
  limit = BURST_LIMITS["general_api"]  # 200/minute
  return create_custom_rate_limit_dependency(limit, 60, "general_api")(request)


def billing_rate_limit_dependency(request: Request):
  """Generous rate limiting for billing/checkout endpoints.

  Payment flows should never be rate-limited during normal use.
  All user types (JWT, API key, IP) get the same generous limit
  because checkout redirects from Stripe may not carry auth cookies.
  """
  limit = BURST_LIMITS["billing"]  # 60/minute (checkout polls at ~20/min)
  identifier = get_user_identifier(request)
  cache_key = f"{identifier}:billing"

  # No penalty for IP-based users — they may be mid-checkout redirect
  window = 60

  allowed, remaining = rate_limit_cache.check_rate_limit(cache_key, limit, window)

  if not allowed:
    current_time = getattr(request.state, "current_time", None) or int(time.time())
    reset_time = int(current_time + window)

    raise HTTPException(
      status_code=status.HTTP_429_TOO_MANY_REQUESTS,
      detail="Rate limit exceeded for billing operations",
      headers={
        "Retry-After": str(window),
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(reset_time),
        "X-RateLimit-Type": "billing",
      },
    )

  request.state.billing_rate_limit_remaining = remaining
  request.state.billing_rate_limit_limit = limit


def webhook_rate_limit_dependency(request: Request):
  """Per-source-IP limit for inbound provider webhooks (Stripe).

  The body is signature-verified downstream, so this only bounds pre-verify
  body reads and retries from one source. Fails open (the default): a Redis
  outage must not drop legitimate billing events — the signature check is the
  security control, this is DoS defense in depth. Generous, because Stripe
  legitimately bursts redeliveries.
  """
  limit = BURST_LIMITS["webhook"]  # anonymous → 120/min per IP
  return create_custom_rate_limit_dependency(limit, 60, "webhook")(request)


def public_api_rate_limit_dependency(request: Request):
  """Rate limiting for public API endpoints (no auth required)."""
  # More generous for anonymous users since these endpoints are meant to be public
  # 600/minute for authenticated, 60/minute for anonymous (after division by 10)
  limit = BURST_LIMITS["public_api"]
  return create_custom_rate_limit_dependency(limit, 60, "public_api")(request)


def jwt_refresh_rate_limit_dependency(request: Request):
  """Very strict rate limiting for JWT refresh operations."""
  limit = BURST_LIMITS["jwt_refresh"]
  return create_custom_rate_limit_dependency(limit, 60, "jwt_refresh")(request)


def subscription_aware_rate_limit_dependency(request: Request):
  """Rate limiting that adapts based on user's subscription tier.

  This dependency:
  1. Identifies the user and their subscription tier
  2. Determines the endpoint category
  3. Applies appropriate rate limits based on subscription
  """
  if not should_use_subscription_limits(request.url.path):
    # Fall back to general rate limiting for non-subscription endpoints
    return rate_limit_dependency(request)

  from ...config.rate_limits import RateLimitConfig
  from ...config.shared_repositories import is_shared_repository_or_subgraph
  from ..graph.types import parse_graph_id
  from .graph_tier_resolver import (
    FALLBACK_TIER,
    extract_graph_id,
    resolve_graph_tier,
  )

  user_id = get_user_from_request(request)
  if not user_id:
    # Anonymous users get base tier limits
    subscription_tier = "base"
    identifier = f"anon_sub:{request.client.host if request.client else 'unknown'}"
  else:
    subscription_tier = FALLBACK_TIER
    identifier = f"user_sub:{user_id}"

  category = get_endpoint_category(request.url.path, request.method)
  if not category:
    return rate_limit_dependency(request)

  # Graph-scoped requests are bucketed by graph and limited by that graph's
  # own tier, so per-graph pricing buys per-graph throughput instead of one
  # budget shared across a customer's graphs.
  #
  # Three conditions, and each excludes a case where per-graph bucketing is
  # actively wrong:
  #
  #   1. The request is graph-scoped at all.
  #   2. The graph is NOT a shared repository. `sec` is one graph that every
  #      tenant queries, so keying the bucket by graph id would put all of them
  #      in a single budget and let one heavy user rate-limit everyone else off
  #      it. Shared repositories stay user-keyed, which is also what isolates
  #      tenants from each other. Subgraphs count too — `sec_historical` is as
  #      shared as `sec`.
  #   3. The category hits the tenant's own instance. Shared-infrastructure
  #      categories stay user-keyed and tier-flat, or a customer could multiply
  #      their load on OpenSearch or the extensions RDS by creating graphs.
  #   4. The caller is authorized on the graph. The bucket is the tenant's
  #      budget; only a member may draw from it. When the auth dependency
  #      has not run yet (router-level mounting) and no cached decision
  #      exists, the request stays on the caller's own budget.
  graph_id = extract_graph_id(request.url.path) if user_id else None
  if graph_id is None and user_id:
    # The graph-agnostic MCP route names no graph in its path; the OAuth
    # dependency has already published the grant's graph on request.state,
    # so a directory connector draws from that graph's budget too.
    published = getattr(request.state, "auth_graph_id", None)
    if isinstance(published, str):
      graph_id = published
  if (
    graph_id
    and user_id
    and not is_shared_repository_or_subgraph(graph_id)
    and category in RateLimitConfig.DEDICATED_RESOURCE_CATEGORIES
  ):
    # A subgraph lives on its parent's instance and is not separately priced,
    # so it draws from the parent's budget: kg123_dev buckets as kg123. Without
    # this, an XLarge tenant with 25 subgraphs would hold 26 independent
    # budgets against one machine. Resolving the parent also spares the
    # resolver a per-subgraph cache entry and database row dependency.
    parent_graph_id, _subgraph_name = parse_graph_id(graph_id)
    if _caller_is_authorized_on_graph(request, user_id, graph_id, parent_graph_id):
      subscription_tier = resolve_graph_tier(parent_graph_id)
      identifier = f"graph_sub:{parent_graph_id}"

  limit_config = get_subscription_rate_limit(subscription_tier, category)
  if not limit_config:
    # Fall back to general rate limiting if no specific limits
    return rate_limit_dependency(request)

  limit, window = limit_config

  category_identifier = f"{identifier}:{category.value}"

  allowed, remaining = rate_limit_cache.check_rate_limit(
    category_identifier, limit, window
  )

  if not allowed:
    # Log rate limit violation
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    sub_limit_type = f"subscription_{subscription_tier}_{category.value}"

    SecurityAuditLogger.log_rate_limit_exceeded(
      user_id=user_id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=sub_limit_type,
    )

    # Record OTel rate limit metric
    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type=sub_limit_type,
        identifier_type="subscription",
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

    # Calculate reset time
    current_time = getattr(request.state, "current_time", None) or int(time.time())
    reset_time = int(current_time + window)

    # Provide helpful error message
    upgrade_msg = ""
    if subscription_tier == "base":
      upgrade_msg = " Upgrade your subscription for higher limits."

    raise HTTPException(
      status_code=status.HTTP_429_TOO_MANY_REQUESTS,
      detail=f"Rate limit exceeded for {category.value.replace('_', ' ')} operations.{upgrade_msg}",
      headers={
        "Retry-After": str(window),
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(reset_time),
        "X-RateLimit-Tier": subscription_tier,
        "X-RateLimit-Category": category.value,
      },
    )

  request.state.rate_limit_remaining = remaining
  request.state.rate_limit_limit = limit
  request.state.rate_limit_tier = subscription_tier
  request.state.rate_limit_category = category.value


def graph_scoped_rate_limit_dependency(request: Request):
  """Rate limiting specifically for graph-scoped endpoints.
  Always uses subscription-based limits.
  """
  return subscription_aware_rate_limit_dependency(request)


def sse_connection_rate_limit_dependency(request: Request):
  """Rate limiting specifically for SSE connection endpoints.
  Limits the rate at which authenticated users can establish new SSE connections.
  Uses subscription-tier-based rate limits from centralized configuration.
  Note: SSE endpoints require authentication, so anonymous users cannot access them.
  """
  from robosystems.config.rate_limits import EndpointCategory, RateLimitConfig

  user_id = get_user_from_request(request)

  # Determine subscription tier
  # For now, all authenticated users get ladybug-standard tier
  # In the future, this could check actual subscription status
  subscription_tier = "ladybug-standard" if user_id else "base"

  rate_limit = RateLimitConfig.get_rate_limit(subscription_tier, EndpointCategory.SSE)

  if rate_limit:
    limit, window = rate_limit
  else:
    # Fallback when the tier is not configured in the subscription limits.
    limit = BURST_LIMITS["sse_connections"]
    window = BURST_LIMITS["sse_connections_window"]

  identifier = get_user_identifier(request)

  allowed, remaining = rate_limit_cache.check_rate_limit(
    f"{identifier}:sse_connections", limit, window
  )

  if not allowed:
    # Log SSE rate limit violation
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    SecurityAuditLogger.log_rate_limit_exceeded(
      user_id=user_id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type="sse_connections",
    )

    # Record OTel rate limit metric
    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type="sse_connections",
        identifier_type="user",
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

    # Calculate reset time
    current_time = getattr(request.state, "current_time", None) or int(time.time())
    reset_time = int(current_time + window)

    raise HTTPException(
      status_code=status.HTTP_429_TOO_MANY_REQUESTS,
      detail="Too many SSE connection attempts. Please wait before opening new connections.",
      headers={
        "Retry-After": str(window),
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(reset_time),
        "X-RateLimit-Type": "sse_connections",
      },
    )

  request.state.sse_rate_limit_remaining = remaining
  request.state.sse_rate_limit_limit = limit
