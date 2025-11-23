"""Rate limiting dependencies for FastAPI."""

import time
from typing import Optional

import jwt
from fastapi import HTTPException, Request, status

from ...config import env
from .cache import rate_limit_cache
from .subscription_rate_limits import (
  get_subscription_rate_limit,
  get_endpoint_category,
  should_use_subscription_limits,
)
from ...security import SecurityAuditLogger, SecurityEventType


def get_int_env(key: str, default: str) -> int:
  """Get integer environment variable, stripping any inline comments."""
  # Use getattr to dynamically get env attribute
  value = str(getattr(env, key, default))
  # Strip inline comments (anything after #)
  value = value.split("#")[0].strip()
  return int(value)


def _verify_jwt_for_rate_limiting(token: str) -> Optional[str]:
  """Safely verify JWT token for rate limiting purposes only."""
  try:
    secret_key = env.JWT_SECRET_KEY
    if not secret_key:
      return None

    # Properly validate JWT signature
    payload = jwt.decode(token, secret_key, algorithms=["HS256"])
    return payload.get("user_id")
  except Exception:
    # If JWT is invalid, treat as unauthenticated for rate limiting
    return None


def get_user_identifier(request: Request) -> str:
  """Get user identifier for rate limiting based on authentication."""
  # Check for API key authentication
  api_key = request.headers.get("X-API-Key")
  if api_key:
    # Hash the API key for privacy
    import hashlib

    api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    return f"apikey:{api_key_hash}"

  # Check for JWT authentication in Authorization header
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

  # Fallback to IP address for unauthenticated requests
  client_ip = request.client.host if request.client else "unknown"
  return f"ip:{client_ip}"


def get_rate_limit_config(identifier: str) -> tuple[int, int]:
  """
  Get rate limiting configuration based on identifier type.

  BURST-FOCUSED: Short windows for burst protection.
  Volume is controlled by credits, not rate limits.

  Returns:
      tuple[int, int]: (requests_per_window, window_seconds)
  """
  # Environment-based rate limits - BURST PROTECTION ONLY
  if identifier.startswith("apikey:"):
    # API key users get very high burst limits
    limit = get_int_env("RATE_LIMIT_API_KEY", "1000")  # 1k/minute default
    window = 60  # 1 minute (60k/hour possible)
  elif identifier.startswith("jwt:"):
    # JWT users get high burst limits
    limit = get_int_env("RATE_LIMIT_JWT", "500")  # 500/minute default
    window = 60  # 1 minute (30k/hour possible)
  else:
    # IP-based (unauthenticated) users still get restricted
    limit = get_int_env("RATE_LIMIT_ANONYMOUS", "10")  # 10/minute default
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

    SecurityAuditLogger.log_rate_limit_exceeded(
      user_id=user_id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=f"general_api_{identifier.split(':')[0] if ':' in identifier else 'anonymous'}",
    )

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

  # Add rate limit info to request state for response headers
  request.state.rate_limit_remaining = remaining
  request.state.rate_limit_limit = limit


def auth_rate_limit_dependency(request: Request):
  """Strict rate limiting for authentication endpoints."""
  client_ip = request.client.host if request.client else "unknown"
  identifier = f"auth_ip:{client_ip}"

  # Get endpoint-specific limits
  path = request.url.path
  if "/login" in path:
    limit = env.AUTH_RATE_LIMIT_LOGIN
    window = get_int_env("RATE_LIMIT_LOGIN_WINDOW", "300")  # 5 minutes
  elif "/register" in path:
    limit = env.AUTH_RATE_LIMIT_REGISTER
    window = get_int_env("RATE_LIMIT_REGISTER_WINDOW", "3600")  # 1 hour
  else:
    # Default auth endpoint limits
    limit = get_int_env("RATE_LIMIT_AUTH", "10")  # 10 attempts
    window = get_int_env("RATE_LIMIT_AUTH_WINDOW", "300")  # 5 minutes

  allowed, remaining = rate_limit_cache.check_rate_limit(identifier, limit, window)

  if not allowed:
    # Log authentication rate limit violation - this is high risk
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    SecurityAuditLogger.log_rate_limit_exceeded(
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=f"auth_{path.split('/')[-1] if '/' in path else 'unknown'}",
    )

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

  # Add rate limit info to request state
  request.state.auth_rate_limit_remaining = remaining
  request.state.auth_rate_limit_limit = limit


def get_user_from_request(request: Request) -> Optional[str]:
  """Extract user ID from request for user-specific rate limiting."""
  # Check for JWT authentication in Authorization header
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

  # Check for API key authentication - we'll need to look up the user
  api_key = request.headers.get("X-API-Key")
  if api_key:
    # For now, we'll use API key hashing for anonymity
    # In production, you'd want to look up the user associated with the API key
    import hashlib

    api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    return f"apikey_{api_key_hash}"

  return None


# Custom rate limiting dependencies for different endpoint categories
def create_custom_rate_limit_dependency(
  limit_per_hour: int, window_seconds: int = 3600, limit_name: str = "custom"
):
  """
  Create a custom rate limiting dependency with specific limits.

  Args:
    limit_per_hour: Number of requests allowed per hour
    window_seconds: Time window in seconds (default: 3600 = 1 hour)
    limit_name: Name for the limit type (for logging/headers)
  """

  def dependency(request: Request):
    identifier = get_user_identifier(request)

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

    allowed, remaining = rate_limit_cache.check_rate_limit(cache_key, limit, window)

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

    # Add rate limit info to request state
    setattr(request.state, f"{limit_name}_rate_limit_remaining", remaining)
    setattr(request.state, f"{limit_name}_rate_limit_limit", limit)

  return dependency


# Specific rate limiting dependencies for different endpoint categories
# BURST-FOCUSED: All use per-minute windows for burst protection


def user_management_rate_limit_dependency(request: Request):
  """Rate limiting for user profile and settings endpoints."""
  limit = get_int_env("RATE_LIMIT_USER_MANAGEMENT", "600")  # 600/minute (10/second)
  return create_custom_rate_limit_dependency(limit, 60, "user_management")(request)


def sync_operations_rate_limit_dependency(request: Request):
  """Rate limiting for external sync operations (QB, Plaid, SEC)."""
  limit = get_int_env("RATE_LIMIT_SYNC_OPS", "50")  # 50/minute
  return create_custom_rate_limit_dependency(limit, 60, "sync_operations")(request)


def connection_management_rate_limit_dependency(request: Request):
  """Rate limiting for external connection setup/management."""
  limit = get_int_env("RATE_LIMIT_CONNECTION_MGMT", "30")  # 30/minute
  return create_custom_rate_limit_dependency(limit, 60, "connection_mgmt")(request)


def analytics_rate_limit_dependency(request: Request):
  """Rate limiting for graph analytics and metrics endpoints."""
  limit = get_int_env("RATE_LIMIT_ANALYTICS", "100")  # 100/minute
  return create_custom_rate_limit_dependency(limit, 60, "analytics")(request)


def backup_operations_rate_limit_dependency(request: Request):
  """Rate limiting for backup creation and export operations."""
  limit = get_int_env("RATE_LIMIT_BACKUP_OPS", "10")  # 10/minute (expensive operations)
  return create_custom_rate_limit_dependency(limit, 60, "backup_operations")(request)


def sensitive_auth_rate_limit_dependency(request: Request):
  """Rate limiting for sensitive auth operations (refresh, SSO)."""
  limit = get_int_env("RATE_LIMIT_SENSITIVE_AUTH", "60")  # 60/minute
  return create_custom_rate_limit_dependency(limit, 60, "sensitive_auth")(request)


def logout_rate_limit_dependency(request: Request):
  """Rate limiting for logout endpoint - more generous to handle expired tokens."""
  # 300/minute for authenticated, 30/minute for anonymous (after division by 10)
  limit = get_int_env("RATE_LIMIT_LOGOUT", "300")
  return create_custom_rate_limit_dependency(limit, 60, "logout")(request)


def tasks_management_rate_limit_dependency(request: Request):
  """Rate limiting for task monitoring and management."""
  limit = get_int_env("RATE_LIMIT_TASKS", "200")  # 200/minute
  return create_custom_rate_limit_dependency(limit, 60, "tasks")(request)


def auth_status_rate_limit_dependency(request: Request):
  """Rate limiting for auth status check endpoints (like /auth/me)."""
  limit = get_int_env("RATE_LIMIT_AUTH_STATUS", "600")  # 600/minute (10/second)
  return create_custom_rate_limit_dependency(limit, 60, "auth_status")(request)


def sso_rate_limit_dependency(request: Request):
  """Rate limiting for SSO operations (token generation/exchange)."""
  limit = get_int_env("RATE_LIMIT_SSO", "100")  # 100/minute
  return create_custom_rate_limit_dependency(limit, 60, "sso")(request)


def general_api_rate_limit_dependency(request: Request):
  """General rate limiting for standard API endpoints."""
  limit = get_int_env("RATE_LIMIT_GENERAL_API", "200")  # 200/minute
  return create_custom_rate_limit_dependency(limit, 60, "general_api")(request)


def public_api_rate_limit_dependency(request: Request):
  """Rate limiting for public API endpoints (no auth required)."""
  # More generous for anonymous users since these endpoints are meant to be public
  # 600/minute for authenticated, 60/minute for anonymous (after division by 10)
  limit = get_int_env("RATE_LIMIT_PUBLIC_API", "600")
  return create_custom_rate_limit_dependency(limit, 60, "public_api")(request)


def jwt_refresh_rate_limit_dependency(request: Request):
  """Very strict rate limiting for JWT refresh operations."""
  # More restrictive than general sensitive auth limits
  limit = env.JWT_REFRESH_RATE_LIMIT
  return create_custom_rate_limit_dependency(limit, 60, "jwt_refresh")(request)


def subscription_aware_rate_limit_dependency(request: Request):
  """
  Rate limiting that adapts based on user's subscription tier.

  This dependency:
  1. Identifies the user and their subscription tier
  2. Determines the endpoint category
  3. Applies appropriate rate limits based on subscription
  """
  # Check if this endpoint should use subscription limits
  if not should_use_subscription_limits(request.url.path):
    # Fall back to general rate limiting for non-subscription endpoints
    return rate_limit_dependency(request)

  # Get user identification
  user_id = get_user_from_request(request)
  if not user_id:
    # Anonymous users get free tier limits
    subscription_tier = "free"
    identifier = f"anon_sub:{request.client.host if request.client else 'unknown'}"
  else:
    # All authenticated users get ladybug-standard tier rate limits
    # Graph-specific subscriptions are handled at the graph level
    subscription_tier = "ladybug-standard"
    identifier = f"user_sub:{user_id}"

  # Determine endpoint category
  category = get_endpoint_category(request.url.path, request.method)
  if not category:
    # Fall back to general rate limiting if category not found
    return rate_limit_dependency(request)

  # Get subscription-based limits
  limit_config = get_subscription_rate_limit(subscription_tier, category)
  if not limit_config:
    # Fall back to general rate limiting if no specific limits
    return rate_limit_dependency(request)

  limit, window = limit_config

  # Create a unique identifier for this category
  category_identifier = f"{identifier}:{category.value}"

  # Check rate limit
  allowed, remaining = rate_limit_cache.check_rate_limit(
    category_identifier, limit, window
  )

  if not allowed:
    # Log rate limit violation
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    SecurityAuditLogger.log_rate_limit_exceeded(
      user_id=user_id,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type=f"subscription_{subscription_tier}_{category.value}",
    )

    # Calculate reset time
    current_time = getattr(request.state, "current_time", None) or int(time.time())
    reset_time = int(current_time + window)

    # Provide helpful error message
    upgrade_msg = ""
    if subscription_tier in ["free", "starter"]:
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

  # Add rate limit info to request state
  request.state.rate_limit_remaining = remaining
  request.state.rate_limit_limit = limit
  request.state.rate_limit_tier = subscription_tier
  request.state.rate_limit_category = category.value


def graph_scoped_rate_limit_dependency(request: Request):
  """
  Rate limiting specifically for graph-scoped endpoints.
  Always uses subscription-based limits.
  """
  return subscription_aware_rate_limit_dependency(request)


def sse_connection_rate_limit_dependency(request: Request):
  """
  Rate limiting specifically for SSE connection endpoints.
  Limits the rate at which authenticated users can establish new SSE connections.
  Uses subscription-tier-based rate limits from centralized configuration.
  Note: SSE endpoints require authentication, so anonymous users cannot access them.
  """
  from robosystems.config.rate_limits import RateLimitConfig, EndpointCategory

  # Get user ID from request (SSE requires authentication)
  user_id = get_user_from_request(request)

  # Determine subscription tier
  # For now, all authenticated users get ladybug-standard tier
  # In the future, this could check actual subscription status
  subscription_tier = "ladybug-standard" if user_id else "free"

  # Get rate limit for SSE based on subscription tier
  rate_limit = RateLimitConfig.get_rate_limit(subscription_tier, EndpointCategory.SSE)

  if rate_limit:
    limit, window = rate_limit
  else:
    # Fallback to environment variables if not configured
    limit = get_int_env("RATE_LIMIT_SSE_CONNECTIONS", "10")
    window = get_int_env("RATE_LIMIT_SSE_CONNECTIONS_WINDOW", "60")

  identifier = get_user_identifier(request)

  # Check rate limit
  allowed, remaining = rate_limit_cache.check_rate_limit(
    f"{identifier}:sse_connections", limit, window
  )

  if not allowed:
    # Log SSE rate limit violation
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("user-agent")
    endpoint = str(request.url.path)

    SecurityAuditLogger.log_rate_limit_exceeded(
      user_id=get_user_from_request(request),
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      limit_type="sse_connections",
    )

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

  # Add rate limit info to request state
  request.state.sse_rate_limit_remaining = remaining
  request.state.sse_rate_limit_limit = limit
