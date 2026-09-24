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

    # Signature only: identifying the user is all rate limiting needs, and
    # PyJWT rejects a present `aud` claim when no audience is passed.
    payload = jwt.decode(
      token,
      secret_key,
      algorithms=["HS256"],
      options={"verify_aud": False, "verify_iss": False},
    )
    return payload.get("user_id")
  except Exception:
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
  """Whether the caller is known to be authorized on the graph.

  A graph's budget is charged only on proven authorization, never on a URL
  that merely names the graph. Evidence: the auth dependency's decision on
  ``request.state``, else a cached positive decision for the graph or parent.
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
  """Bucket key for the caller.

  A key is an identity only once known valid, so junk keys cannot each mint
  a budget.
  """
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

  auth_token = request.cookies.get("auth-token")
  if auth_token:
    user_id = _verify_jwt_for_rate_limiting(auth_token)
    if user_id:
      return f"jwt:{user_id}"

  # Published by the auth dependencies only after validation; covers
  # credentials this function cannot reparse (opaque OAuth bearers).
  auth_user_id = getattr(request.state, "auth_user_id", None)
  if isinstance(auth_user_id, str) and auth_user_id:
    return f"apikey:user:{auth_user_id}"

  client_ip = request.client.host if request.client else "unknown"
  return f"ip:{client_ip}"


def get_rate_limit_config(identifier: str) -> tuple[int, int]:
  """(limit, window) for an identifier. Burst protection only; volume is
  controlled by credits."""
  if identifier.startswith("apikey:"):
    limit = BURST_LIMITS["api_key"]
    window = 60
  elif identifier.startswith("jwt:"):
    limit = BURST_LIMITS["jwt"]
    window = 60
  else:
    limit = BURST_LIMITS["anonymous"]
    window = 60

  return limit, window


def _user_id_from_identifier(identifier: str) -> str | None:
  for prefix in ("jwt:", "apikey:user:"):
    if identifier.startswith(prefix):
      return identifier[len(prefix) :] or None
  return None


def rate_limit_dependency(request: Request):
  identifier = get_user_identifier(request)
  limit, window = get_rate_limit_config(identifier)

  allowed, remaining = rate_limit_cache.check_rate_limit(identifier, limit, window)

  if not allowed:
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    user_id = _user_id_from_identifier(identifier)

    identifier_type = identifier.split(":")[0] if ":" in identifier else "anonymous"
    limit_type_str = f"general_api_{identifier_type}"

    SecurityAuditLogger.log_rate_limit_exceeded(
      user_id=user_id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=limit_type_str,
    )

    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type=limit_type_str,
        identifier_type=identifier_type,
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

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

  path = request.url.path
  if "/login" in path:
    limit = BURST_LIMITS["login_attempts"]
    window = BURST_LIMITS["login_window"]
  elif "/register" in path:
    limit = BURST_LIMITS["register_attempts"]
    window = BURST_LIMITS["register_window"]
  else:
    limit = BURST_LIMITS["auth_attempts"]
    window = BURST_LIMITS["auth_window"]

  # Fail closed: brute-force protection must not vanish with the backend.
  allowed, remaining = rate_limit_cache.check_rate_limit(
    identifier, limit, window, fail_closed=True
  )

  if not allowed:
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    auth_limit_type = f"auth_{path.split('/')[-1] if '/' in path else 'unknown'}"

    SecurityAuditLogger.log_rate_limit_exceeded(
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=auth_limit_type,
    )

    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type=auth_limit_type,
        identifier_type="ip",
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

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

  auth_token = request.cookies.get("auth-token")
  if auth_token:
    user_id = _verify_jwt_for_rate_limiting(auth_token)
    if user_id:
      return user_id

  # A key is an identity only once the auth cache holds it as validated.
  api_key = request.headers.get("X-API-Key")
  if api_key:
    api_key_hash = _api_key_digest(api_key)
    if _api_key_is_cached_valid(api_key_hash):
      return f"apikey_{api_key_hash}"

  # Validated principal for credentials with no reparseable header (OAuth
  # bearers); otherwise they'd share an anonymous per-IP budget.
  auth_user_id = getattr(request.state, "auth_user_id", None)
  if isinstance(auth_user_id, str) and auth_user_id:
    return auth_user_id

  return None


def create_custom_rate_limit_dependency(
  limit_per_hour: int,
  window_seconds: int = 3600,
  limit_name: str = "custom",
  identifier_fn=None,
  fail_closed: bool = False,
):
  """Build a rate-limit dependency.

  ``identifier_fn`` derives the bucket key (default ``get_user_identifier``);
  an ``apikey:``/``jwt:`` prefix gets the full limit, anything else 1/10th.
  ``fail_closed=True`` denies when the limiter backend is unavailable.
  """
  resolve_identifier = identifier_fn or get_user_identifier

  def dependency(request: Request):
    identifier = resolve_identifier(request)

    cache_key = f"{identifier}:{limit_name}"

    if identifier.startswith("apikey:"):
      limit = limit_per_hour
    elif identifier.startswith("jwt:"):
      # Authenticated callers get the full limit, anonymous ones a tenth.
      limit = limit_per_hour
    else:
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


def user_management_rate_limit_dependency(request: Request):
  """Rate limiting for user profile and settings endpoints."""
  limit = BURST_LIMITS["user_management"]
  return create_custom_rate_limit_dependency(limit, 60, "user_management")(request)


def sync_operations_rate_limit_dependency(request: Request):
  """Rate limiting for external sync operations (QB, SEC)."""
  limit = BURST_LIMITS["sync_ops"]
  return create_custom_rate_limit_dependency(limit, 60, "sync_operations")(request)


def connection_management_rate_limit_dependency(request: Request):
  """Rate limiting for external connection setup/management."""
  limit = BURST_LIMITS["connection_mgmt"]
  return create_custom_rate_limit_dependency(limit, 60, "connection_mgmt")(request)


def analytics_rate_limit_dependency(request: Request):
  """Rate limiting for graph analytics and metrics endpoints."""
  limit = BURST_LIMITS["analytics"]
  return create_custom_rate_limit_dependency(limit, 60, "analytics")(request)


def backup_operations_rate_limit_dependency(request: Request):
  """Rate limiting for backup creation and export operations."""
  limit = BURST_LIMITS["backup_ops"]
  return create_custom_rate_limit_dependency(limit, 60, "backup_operations")(request)


def sensitive_auth_rate_limit_dependency(request: Request):
  """Rate limiting for sensitive auth operations (refresh, SSO)."""
  limit = BURST_LIMITS["sensitive_auth"]
  return create_custom_rate_limit_dependency(limit, 60, "sensitive_auth")(request)


def logout_rate_limit_dependency(request: Request):
  """Generous, since logout often arrives with an expired token."""
  limit = BURST_LIMITS["logout"]
  return create_custom_rate_limit_dependency(limit, 60, "logout")(request)


def tasks_management_rate_limit_dependency(request: Request):
  """Rate limiting for task monitoring and management."""
  limit = BURST_LIMITS["tasks"]
  return create_custom_rate_limit_dependency(limit, 60, "tasks")(request)


def auth_status_rate_limit_dependency(request: Request):
  """Rate limiting for auth status check endpoints (like /auth/me)."""
  limit = BURST_LIMITS["auth_status"]
  return create_custom_rate_limit_dependency(limit, 60, "auth_status")(request)


def sso_rate_limit_dependency(request: Request):
  """Rate limiting for SSO operations (token generation/exchange)."""
  limit = BURST_LIMITS["sso"]
  return create_custom_rate_limit_dependency(limit, 60, "sso")(request)


def oidc_rate_limit_dependency(request: Request):
  """OIDC login + callback share this IP-keyed bucket; a flow spends two."""
  limit = BURST_LIMITS["oidc"]
  return create_custom_rate_limit_dependency(limit, 60, "oidc")(request)


def oauth_authorize_rate_limit_dependency(request: Request):
  """The OAuth authorization endpoint (IP-keyed). Fails closed: the
  client-metadata fetch it triggers must never run unmetered."""
  limit = BURST_LIMITS["oauth_authorize"]
  return create_custom_rate_limit_dependency(
    limit, 60, "oauth_authorize", fail_closed=True
  )(request)


def oauth_consent_rate_limit_dependency(request: Request):
  """The consent read + decision endpoints (JWT session)."""
  limit = BURST_LIMITS["oauth_consent"]
  return create_custom_rate_limit_dependency(limit, 60, "oauth_consent")(request)


def oauth_token_rate_limit_dependency(request: Request):
  """The token + revocation endpoints, effectively per-IP. Fails closed."""
  limit = BURST_LIMITS["oauth_token"]
  return create_custom_rate_limit_dependency(
    limit, 60, "oauth_token", fail_closed=True
  )(request)


def oauth_register_rate_limit_dependency(request: Request):
  """RFC 7591 dynamic registration, the only unauthenticated write. Per-IP,
  plus a daily cap in ``operations.oauth_server.clients``. Fails closed."""
  limit = BURST_LIMITS["oauth_register"]
  return create_custom_rate_limit_dependency(
    limit, 60, "oauth_register", fail_closed=True
  )(request)


def mfa_rate_limit_dependency(request: Request):
  """The passkey login/MFA challenge surface, including both registration
  lanes. Pre-session callers are per-IP. Fails closed (authentication surface).
  """
  limit = BURST_LIMITS["mfa"]
  return create_custom_rate_limit_dependency(limit, 60, "mfa", fail_closed=True)(
    request
  )


def passkey_management_rate_limit_dependency(request: Request):
  """Rate limiting for authenticated passkey lifecycle endpoints
  (list/enroll/remove/recovery-codes)."""
  limit = BURST_LIMITS["passkey_management"]
  return create_custom_rate_limit_dependency(
    limit, 60, "passkey_management", fail_closed=True
  )(request)


def _scim_rate_limit_identifier(request: Request) -> str:
  """Key the SCIM bucket off the bearer token's hash, not the caller IP, so
  an IdP full-sync isn't throttled as anonymous."""
  auth_header = request.headers.get("Authorization", "")
  if auth_header.startswith("Bearer "):
    import hashlib

    token_hash = hashlib.sha256(auth_header[7:].encode()).hexdigest()
    return f"apikey:scim:{token_hash}"
  # No bearer → anonymous; the request 401s at require_scim_org anyway.
  return get_user_identifier(request)


def scim_rate_limit_dependency(request: Request):
  """The SCIM provisioning surface. Fails closed; the IdP retries a denial."""
  limit = BURST_LIMITS["scim"]
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

  The per-token bucket keys off an unverified bearer, so this caps the
  per-IP total. Sized above ``scim`` so a legitimate IdP hits that first.
  """
  limit = BURST_LIMITS["scim_ip"]
  return create_custom_rate_limit_dependency(
    limit, 60, "scim_ip", identifier_fn=_scim_ip_identifier, fail_closed=True
  )(request)


def general_api_rate_limit_dependency(request: Request):
  """General rate limiting for standard API endpoints."""
  limit = BURST_LIMITS["general_api"]
  return create_custom_rate_limit_dependency(limit, 60, "general_api")(request)


def billing_rate_limit_dependency(request: Request):
  """Billing/checkout: the same limit for every caller type, since a Stripe
  checkout redirect may not carry auth cookies."""
  limit = BURST_LIMITS["billing"]
  identifier = get_user_identifier(request)
  cache_key = f"{identifier}:billing"
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

  Fails open: the signature check is the security control, and a limiter
  outage must not drop billing events. Generous for Stripe's redelivery bursts.
  """
  limit = BURST_LIMITS["webhook"]
  return create_custom_rate_limit_dependency(limit, 60, "webhook")(request)


def public_api_rate_limit_dependency(request: Request):
  """Rate limiting for public API endpoints (no auth required)."""
  limit = BURST_LIMITS["public_api"]
  return create_custom_rate_limit_dependency(limit, 60, "public_api")(request)


def jwt_refresh_rate_limit_dependency(request: Request):
  """Very strict rate limiting for JWT refresh operations."""
  limit = BURST_LIMITS["jwt_refresh"]
  return create_custom_rate_limit_dependency(limit, 60, "jwt_refresh")(request)


def subscription_aware_rate_limit_dependency(request: Request):
  """Rate limiting by subscription tier and endpoint category."""
  if not should_use_subscription_limits(request.url.path):
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
    subscription_tier = "base"
    identifier = f"anon_sub:{request.client.host if request.client else 'unknown'}"
  else:
    subscription_tier = FALLBACK_TIER
    identifier = f"user_sub:{user_id}"

  category = get_endpoint_category(request.url.path, request.method)
  if not category:
    return rate_limit_dependency(request)

  # Graph-scoped requests bucket by graph at that graph's tier, only when:
  # the graph is not a shared repository (one heavy user would starve every
  # tenant of `sec`); the category hits the tenant's own instance (else
  # creating graphs multiplies load on shared infra); and the caller is
  # proven authorized on it. Otherwise the caller's own budget applies.
  graph_id = extract_graph_id(request.url.path) if user_id else None
  if graph_id is None and user_id:
    # The graph-agnostic MCP route: use the OAuth grant's published graph.
    published = getattr(request.state, "auth_graph_id", None)
    if isinstance(published, str):
      graph_id = published
  if (
    graph_id
    and user_id
    and not is_shared_repository_or_subgraph(graph_id)
    and category in RateLimitConfig.DEDICATED_RESOURCE_CATEGORIES
  ):
    # A subgraph shares its parent's instance and budget.
    parent_graph_id, _subgraph_name = parse_graph_id(graph_id)
    if _caller_is_authorized_on_graph(request, user_id, graph_id, parent_graph_id):
      subscription_tier = resolve_graph_tier(parent_graph_id)
      identifier = f"graph_sub:{parent_graph_id}"

  limit_config = get_subscription_rate_limit(subscription_tier, category)
  if not limit_config:
    return rate_limit_dependency(request)

  limit, window = limit_config

  category_identifier = f"{identifier}:{category.value}"

  allowed, remaining = rate_limit_cache.check_rate_limit(
    category_identifier, limit, window
  )

  if not allowed:
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

    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type=sub_limit_type,
        identifier_type="subscription",
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

    current_time = getattr(request.state, "current_time", None) or int(time.time())
    reset_time = int(current_time + window)

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
  """Alias of ``subscription_aware_rate_limit_dependency``."""
  return subscription_aware_rate_limit_dependency(request)


def sse_connection_rate_limit_dependency(request: Request):
  """Rate of new SSE connections per caller."""
  from robosystems.config.rate_limits import EndpointCategory, RateLimitConfig

  user_id = get_user_from_request(request)

  # Every authenticated user gets the ladybug-standard SSE limit.
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

    try:
      from robosystems.middleware.otel.metrics import get_endpoint_metrics

      get_endpoint_metrics().record_rate_limit_rejection(
        endpoint=endpoint,
        limit_type="sse_connections",
        identifier_type="user",
      )
    except Exception:
      pass  # Metrics are best-effort, never break rate limiting

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
